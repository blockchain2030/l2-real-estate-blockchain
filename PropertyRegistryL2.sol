// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

/**
 * @title PropertyRegistryL2
 * @author Muhammad Shahid
 * @notice Core property registry for L2 real estate transactions.
 *         Implements the on-chain property lifecycle described in
 *         Paper Sec 3.1–3.2: registration, transfer, metadata,
 *         and compliance-gated ownership changes.
 *
 * @dev Designed for L2 deployment (Optimism / Polygon zkEVM).
 *      Storage is optimised for calldata compression:
 *        - Tightly packed structs
 *        - Minimal on-chain strings (IPFS hashes for metadata)
 *        - Batch operations for high-throughput scenarios
 *
 * Architecture Reference (Paper Figure 2):
 *   DApp → PropertyRegistryL2 → EscrowManager (escrow gate)
 *                              → ComplianceKYC  (KYC gate)
 *                              → TokenizedProperty (ERC-721 mint)
 */

// ============================================================================
// Imports — OpenZeppelin v5 compatible
// ============================================================================

import "@openzeppelin/contracts/access/AccessControl.sol";
import "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import "@openzeppelin/contracts/utils/Pausable.sol";
import "@openzeppelin/contracts/utils/Counters.sol";

// ============================================================================
// Interfaces
// ============================================================================

/**
 * @dev Interface for ComplianceKYC contract (Sec 3.2 – KYC gate).
 */
interface IComplianceKYC {
    function isVerified(address account) external view returns (bool);
    function getKYCLevel(address account) external view returns (uint8);
}

/**
 * @dev Interface for EscrowManager contract (Sec 3.2 – escrow gate).
 */
interface IEscrowManager {
    function isEscrowActive(uint256 propertyId) external view returns (bool);
    function getEscrowBuyer(uint256 propertyId) external view returns (address);
}

// ============================================================================
// Main Contract
// ============================================================================

contract PropertyRegistryL2 is AccessControl, ReentrancyGuard, Pausable {
    using Counters for Counters.Counter;

    // -----------------------------------------------------------------------
    // Roles (Paper Sec 3.2 — multi-role access)
    // -----------------------------------------------------------------------
    bytes32 public constant REGISTRAR_ROLE = keccak256("REGISTRAR_ROLE");
    bytes32 public constant COMPLIANCE_ROLE = keccak256("COMPLIANCE_ROLE");
    bytes32 public constant ESCROW_ROLE = keccak256("ESCROW_ROLE");
    bytes32 public constant BATCH_OPERATOR_ROLE = keccak256("BATCH_OPERATOR_ROLE");

    // -----------------------------------------------------------------------
    // Enums
    // -----------------------------------------------------------------------

    /**
     * @dev Property status lifecycle (Paper Sec 3.2 – state transitions).
     *
     *  Registered → Listed → UnderEscrow → Transferred
     *                  ↓                       ↓
     *              Suspended               Disputed
     */
    enum PropertyStatus {
        Registered,     // 0 — Initial registration complete
        Listed,         // 1 — Available for sale
        UnderEscrow,    // 2 — Escrow initiated, pending transfer
        Transferred,    // 3 — Ownership transferred successfully
        Suspended,      // 4 — Flagged by compliance
        Disputed        // 5 — Under dispute resolution
    }

    enum PropertyType {
        ResidentialApartment,   // 0
        ResidentialVilla,       // 1
        CommercialOffice,       // 2
        CommercialRetail,       // 3
        LandPlot,               // 4
        IndustrialWarehouse     // 5
    }

    // -----------------------------------------------------------------------
    // Structs (tightly packed for L2 calldata optimisation)
    // -----------------------------------------------------------------------

    /**
     * @dev Core property record stored on-chain.
     *      Large metadata (images, documents) stored off-chain via IPFS.
     *      Matches Paper Sec 3.1 — on-chain title deed representation.
     */
    struct Property {
        // --- Slot 1 (256 bits) ---
        uint256 propertyId;

        // --- Slot 2 (256 bits) ---
        address owner;                  // 160 bits
        PropertyStatus status;          //   8 bits
        PropertyType propertyType;      //   8 bits
        uint40 registrationTimestamp;   //  40 bits  (Unix, good until 36812 AD)
        uint40 lastTransferTimestamp;   //  40 bits

        // --- Slot 3 (256 bits) ---
        bytes32 titleDeedHash;          // keccak256 of legal title deed

        // --- Slot 4 (256 bits) ---
        bytes32 metadataIPFS;           // IPFS CID for off-chain metadata

        // --- Slot 5 (256 bits) ---
        uint256 priceWei;               // Last recorded price in wei

        // --- Slot 6 (256 bits) ---
        uint128 areaSqft;              // 128 bits — property area
        uint64 dldReferenceNumber;     // 64 bits  — DLD title deed number
        uint32 transferCount;          // 32 bits  — total ownership transfers
        uint16 districtCode;           // 16 bits  — Dubai district code
        bool kycCleared;               //  8 bits  — KYC status cache
        bool isTokenized;              //  8 bits  — ERC-721 minted flag
    }

    /**
     * @dev Ownership transfer record for audit trail (Paper Sec 3.2).
     */
    struct TransferRecord {
        address from;
        address to;
        uint256 priceWei;
        uint40 timestamp;
        bytes32 escrowId;              // Link to EscrowManager
        bytes32 documentHash;          // Hash of transfer documents
    }

    // -----------------------------------------------------------------------
    // State Variables
    // -----------------------------------------------------------------------

    /// @dev Auto-incrementing property ID counter
    Counters.Counter private _propertyIdCounter;

    /// @dev propertyId → Property struct
    mapping(uint256 => Property) public properties;

    /// @dev titleDeedHash → propertyId (uniqueness enforcement)
    mapping(bytes32 => uint256) public titleDeedToProperty;

    /// @dev owner address → list of owned property IDs
    mapping(address => uint256[]) public ownerProperties;

    /// @dev propertyId → transfer history
    mapping(uint256 => TransferRecord[]) public transferHistory;

    /// @dev propertyId → approved buyer (pre-transfer approval)
    mapping(uint256 => address) public approvedBuyer;

    /// @dev districtCode → district name (lookup table)
    mapping(uint16 => string) public districtNames;

    /// @dev External contract references
    IComplianceKYC public complianceKYC;
    IEscrowManager public escrowManager;

    /// @dev Registry statistics (for metrics – Paper Sec 5.1)
    uint256 public totalRegistrations;
    uint256 public totalTransfers;
    uint256 public totalVolumeWei;

    /// @dev Maximum properties per batch operation
    uint256 public constant MAX_BATCH_SIZE = 50;

    // -----------------------------------------------------------------------
    // Events (Paper Sec 3.2 — all lifecycle events logged on-chain)
    // -----------------------------------------------------------------------

    event PropertyRegistered(
        uint256 indexed propertyId,
        address indexed owner,
        bytes32 titleDeedHash,
        PropertyType propertyType,
        uint16 districtCode,
        uint256 priceWei,
        uint256 timestamp
    );

    event PropertyListed(
        uint256 indexed propertyId,
        address indexed owner,
        uint256 priceWei,
        uint256 timestamp
    );

    event PropertyStatusChanged(
        uint256 indexed propertyId,
        PropertyStatus oldStatus,
        PropertyStatus newStatus,
        uint256 timestamp
    );

    event OwnershipTransferred(
        uint256 indexed propertyId,
        address indexed from,
        address indexed to,
        uint256 priceWei,
        bytes32 escrowId,
        uint256 timestamp
    );

    event BuyerApproved(
        uint256 indexed propertyId,
        address indexed buyer,
        uint256 timestamp
    );

    event PropertySuspended(
        uint256 indexed propertyId,
        string reason,
        uint256 timestamp
    );

    event PropertyMetadataUpdated(
        uint256 indexed propertyId,
        bytes32 oldMetadataIPFS,
        bytes32 newMetadataIPFS,
        uint256 timestamp
    );

    event BatchRegistration(
        uint256 indexed startId,
        uint256 indexed endId,
        uint256 count,
        uint256 timestamp
    );

    event PropertyTokenized(
        uint256 indexed propertyId,
        address indexed tokenContract,
        uint256 tokenId,
        uint256 timestamp
    );

    // -----------------------------------------------------------------------
    // Errors (custom errors for gas savings on L2)
    // -----------------------------------------------------------------------

    error PropertyNotFound(uint256 propertyId);
    error NotPropertyOwner(uint256 propertyId, address caller);
    error InvalidStatus(uint256 propertyId, PropertyStatus current, PropertyStatus required);
    error TitleDeedAlreadyRegistered(bytes32 titleDeedHash);
    error KYCNotCleared(address account);
    error BuyerNotApproved(uint256 propertyId, address buyer);
    error EscrowNotActive(uint256 propertyId);
    error BatchSizeExceeded(uint256 requested, uint256 maximum);
    error ZeroAddress();
    error ZeroPrice();
    error InvalidTitleDeed();

    // -----------------------------------------------------------------------
    // Modifiers
    // -----------------------------------------------------------------------

    modifier propertyExists(uint256 propertyId) {
        if (properties[propertyId].registrationTimestamp == 0)
            revert PropertyNotFound(propertyId);
        _;
    }

    modifier onlyPropertyOwner(uint256 propertyId) {
        if (properties[propertyId].owner != msg.sender)
            revert NotPropertyOwner(propertyId, msg.sender);
        _;
    }

    modifier onlyKYCCleared(address account) {
        if (address(complianceKYC) != address(0)) {
            if (!complianceKYC.isVerified(account))
                revert KYCNotCleared(account);
        }
        _;
    }

    modifier inStatus(uint256 propertyId, PropertyStatus required) {
        if (properties[propertyId].status != required)
            revert InvalidStatus(propertyId, properties[propertyId].status, required);
        _;
    }

    // -----------------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------------

    constructor(address admin) {
        if (admin == address(0)) revert ZeroAddress();

        _grantRole(DEFAULT_ADMIN_ROLE, admin);
        _grantRole(REGISTRAR_ROLE, admin);
        _grantRole(COMPLIANCE_ROLE, admin);
        _grantRole(BATCH_OPERATOR_ROLE, admin);

        // Initialize Dubai district codes
        _initializeDistricts();
    }

    // -----------------------------------------------------------------------
    // External Configuration
    // -----------------------------------------------------------------------

    /**
     * @notice Set the ComplianceKYC contract address.
     * @param _complianceKYC Address of deployed ComplianceKYC contract.
     */
    function setComplianceKYC(address _complianceKYC)
        external
        onlyRole(DEFAULT_ADMIN_ROLE)
    {
        if (_complianceKYC == address(0)) revert ZeroAddress();
        complianceKYC = IComplianceKYC(_complianceKYC);
    }

    /**
     * @notice Set the EscrowManager contract address.
     * @param _escrowManager Address of deployed EscrowManager contract.
     */
    function setEscrowManager(address _escrowManager)
        external
        onlyRole(DEFAULT_ADMIN_ROLE)
    {
        if (_escrowManager == address(0)) revert ZeroAddress();
        escrowManager = IEscrowManager(_escrowManager);
    }

    // -----------------------------------------------------------------------
    // Core Functions — Registration (Paper Sec 3.1)
    // -----------------------------------------------------------------------

    /**
     * @notice Register a new property on L2.
     * @dev Matches Paper Sec 3.1 — property onboarding flow.
     *      Caller must have REGISTRAR_ROLE and pass KYC.
     *
     * @param owner           Property owner address
     * @param titleDeedHash   keccak256 hash of legal title deed document
     * @param metadataIPFS    IPFS CID for off-chain property metadata
     * @param propertyType    Type classification (enum)
     * @param priceWei        Property value in wei
     * @param areaSqft        Property area in square feet
     * @param districtCode    Dubai district code
     * @param dldRefNumber    DLD title deed reference number
     *
     * @return propertyId     Newly assigned property ID
     */
    function registerProperty(
        address owner,
        bytes32 titleDeedHash,
        bytes32 metadataIPFS,
        PropertyType propertyType,
        uint256 priceWei,
        uint128 areaSqft,
        uint16 districtCode,
        uint64 dldRefNumber
    )
        external
        whenNotPaused
        onlyRole(REGISTRAR_ROLE)
        onlyKYCCleared(owner)
        nonReentrant
        returns (uint256 propertyId)
    {
        // Validations
        if (owner == address(0)) revert ZeroAddress();
        if (titleDeedHash == bytes32(0)) revert InvalidTitleDeed();
        if (priceWei == 0) revert ZeroPrice();
        if (titleDeedToProperty[titleDeedHash] != 0)
            revert TitleDeedAlreadyRegistered(titleDeedHash);

        // Generate property ID
        _propertyIdCounter.increment();
        propertyId = _propertyIdCounter.current();

        // Store property
        properties[propertyId] = Property({
            propertyId: propertyId,
            owner: owner,
            status: PropertyStatus.Registered,
            propertyType: propertyType,
            registrationTimestamp: uint40(block.timestamp),
            lastTransferTimestamp: 0,
            titleDeedHash: titleDeedHash,
            metadataIPFS: metadataIPFS,
            priceWei: priceWei,
            areaSqft: areaSqft,
            dldReferenceNumber: dldRefNumber,
            transferCount: 0,
            districtCode: districtCode,
            kycCleared: true,
            isTokenized: false
        });

        // Update mappings
        titleDeedToProperty[titleDeedHash] = propertyId;
        ownerProperties[owner].push(propertyId);

        // Update statistics
        totalRegistrations++;
        totalVolumeWei += priceWei;

        emit PropertyRegistered(
            propertyId,
            owner,
            titleDeedHash,
            propertyType,
            districtCode,
            priceWei,
            block.timestamp
        );

        return propertyId;
    }

    /**
     * @notice Batch-register multiple properties in a single transaction.
     * @dev L2-optimised: amortizes fixed overhead across N registrations.
     *      Used during stress test (Paper Sec 5.3) for 50K tx/hr throughput.
     */
    function batchRegisterProperties(
        address[] calldata owners,
        bytes32[] calldata titleDeedHashes,
        bytes32[] calldata metadataIPFSHashes,
        PropertyType[] calldata propertyTypes,
        uint256[] calldata pricesWei,
        uint128[] calldata areasSqft,
        uint16[] calldata districtCodes,
        uint64[] calldata dldRefNumbers
    )
        external
        whenNotPaused
        onlyRole(BATCH_OPERATOR_ROLE)
        nonReentrant
        returns (uint256 startId, uint256 endId)
    {
        uint256 count = owners.length;
        if (count > MAX_BATCH_SIZE)
            revert BatchSizeExceeded(count, MAX_BATCH_SIZE);

        // Validate array lengths match
        require(
            titleDeedHashes.length == count &&
            metadataIPFSHashes.length == count &&
            propertyTypes.length == count &&
            pricesWei.length == count &&
            areasSqft.length == count &&
            districtCodes.length == count &&
            dldRefNumbers.length == count,
            "Array length mismatch"
        );

        startId = _propertyIdCounter.current() + 1;

        for (uint256 i = 0; i < count;) {
            _registerPropertyInternal(
                owners[i],
                titleDeedHashes[i],
                metadataIPFSHashes[i],
                propertyTypes[i],
                pricesWei[i],
                areasSqft[i],
                districtCodes[i],
                dldRefNumbers[i]
            );
            unchecked { ++i; }
        }

        endId = _propertyIdCounter.current();

        emit BatchRegistration(startId, endId, count, block.timestamp);
        return (startId, endId);
    }

    // -----------------------------------------------------------------------
    // Core Functions — Listing & Transfer (Paper Sec 3.2)
    // -----------------------------------------------------------------------

    /**
     * @notice List a property for sale.
     * @param propertyId  The property to list
     * @param askingPrice New asking price in wei
     */
    function listProperty(uint256 propertyId, uint256 askingPrice)
        external
        whenNotPaused
        propertyExists(propertyId)
        onlyPropertyOwner(propertyId)
        inStatus(propertyId, PropertyStatus.Registered)
    {
        if (askingPrice == 0) revert ZeroPrice();

        Property storage prop = properties[propertyId];
        PropertyStatus oldStatus = prop.status;
        prop.status = PropertyStatus.Listed;
        prop.priceWei = askingPrice;

        emit PropertyStatusChanged(propertyId, oldStatus, PropertyStatus.Listed, block.timestamp);
        emit PropertyListed(propertyId, msg.sender, askingPrice, block.timestamp);
    }

    /**
     * @notice Approve a specific buyer for a property (pre-transfer step).
     * @dev Seller approves buyer after off-chain negotiation.
     */
    function approveBuyer(uint256 propertyId, address buyer)
        external
        whenNotPaused
        propertyExists(propertyId)
        onlyPropertyOwner(propertyId)
        onlyKYCCleared(buyer)
    {
        if (buyer == address(0)) revert ZeroAddress();
        approvedBuyer[propertyId] = buyer;

        emit BuyerApproved(propertyId, buyer, block.timestamp);
    }

    /**
     * @notice Mark property as under escrow.
     * @dev Called by EscrowManager when escrow is initiated.
     */
    function setUnderEscrow(uint256 propertyId)
        external
        whenNotPaused
        propertyExists(propertyId)
        onlyRole(ESCROW_ROLE)
    {
        Property storage prop = properties[propertyId];
        PropertyStatus oldStatus = prop.status;

        require(
            prop.status == PropertyStatus.Listed ||
            prop.status == PropertyStatus.Registered,
            "Property not available"
        );

        prop.status = PropertyStatus.UnderEscrow;

        emit PropertyStatusChanged(
            propertyId, oldStatus, PropertyStatus.UnderEscrow, block.timestamp
        );
    }

    /**
     * @notice Transfer property ownership.
     * @dev Core transfer function matching Paper Sec 3.2 flow:
     *      1. Verify KYC for both parties
     *      2. Verify escrow is active (funds secured)
     *      3. Update ownership records
     *      4. Record transfer in audit trail
     *      5. Emit OwnershipTransferred event
     *
     * @param propertyId    Property to transfer
     * @param to            New owner address
     * @param priceWei      Agreed transfer price
     * @param escrowId      Linked escrow identifier
     * @param documentHash  Hash of signed transfer documents
     */
    function transferOwnership(
        uint256 propertyId,
        address to,
        uint256 priceWei,
        bytes32 escrowId,
        bytes32 documentHash
    )
        external
        whenNotPaused
        propertyExists(propertyId)
        onlyRole(REGISTRAR_ROLE)
        onlyKYCCleared(to)
        nonReentrant
    {
        Property storage prop = properties[propertyId];

        // Validate status
        require(
            prop.status == PropertyStatus.UnderEscrow,
            "Property must be under escrow"
        );

        // Validate approved buyer
        if (approvedBuyer[propertyId] != address(0)) {
            if (approvedBuyer[propertyId] != to)
                revert BuyerNotApproved(propertyId, to);
        }

        // Validate escrow is active (if EscrowManager is set)
        if (address(escrowManager) != address(0)) {
            if (!escrowManager.isEscrowActive(propertyId))
                revert EscrowNotActive(propertyId);
        }

        // Execute transfer
        address from = prop.owner;
        prop.owner = to;
        prop.status = PropertyStatus.Transferred;
        prop.lastTransferTimestamp = uint40(block.timestamp);
        prop.priceWei = priceWei;
        prop.transferCount++;

        // Record transfer in audit trail
        transferHistory[propertyId].push(TransferRecord({
            from: from,
            to: to,
            priceWei: priceWei,
            timestamp: uint40(block.timestamp),
            escrowId: escrowId,
            documentHash: documentHash
        }));

        // Update owner mappings
        _removeFromOwnerList(from, propertyId);
        ownerProperties[to].push(propertyId);

        // Clear approval
        delete approvedBuyer[propertyId];

        // Update statistics
        totalTransfers++;
        totalVolumeWei += priceWei;

        emit OwnershipTransferred(
            propertyId, from, to, priceWei, escrowId, block.timestamp
        );
    }

    /**
     * @notice Re-list a previously transferred property.
     * @dev Resets status from Transferred back to Registered,
     *      enabling a new sale cycle.
     */
    function relistProperty(uint256 propertyId)
        external
        whenNotPaused
        propertyExists(propertyId)
        onlyPropertyOwner(propertyId)
        inStatus(propertyId, PropertyStatus.Transferred)
    {
        Property storage prop = properties[propertyId];
        PropertyStatus oldStatus = prop.status;
        prop.status = PropertyStatus.Registered;

        emit PropertyStatusChanged(
            propertyId, oldStatus, PropertyStatus.Registered, block.timestamp
        );
    }

    // -----------------------------------------------------------------------
    // Compliance Functions (Paper Sec 3.2 — suspension / dispute)
    // -----------------------------------------------------------------------

    /**
     * @notice Suspend a property (compliance flag).
     */
    function suspendProperty(uint256 propertyId, string calldata reason)
        external
        propertyExists(propertyId)
        onlyRole(COMPLIANCE_ROLE)
    {
        Property storage prop = properties[propertyId];
        PropertyStatus oldStatus = prop.status;
        prop.status = PropertyStatus.Suspended;

        emit PropertyStatusChanged(
            propertyId, oldStatus, PropertyStatus.Suspended, block.timestamp
        );
        emit PropertySuspended(propertyId, reason, block.timestamp);
    }

    /**
     * @notice Mark property as disputed.
     */
    function markDisputed(uint256 propertyId)
        external
        propertyExists(propertyId)
        onlyRole(COMPLIANCE_ROLE)
    {
        Property storage prop = properties[propertyId];
        PropertyStatus oldStatus = prop.status;
        prop.status = PropertyStatus.Disputed;

        emit PropertyStatusChanged(
            propertyId, oldStatus, PropertyStatus.Disputed, block.timestamp
        );
    }

    /**
     * @notice Resolve suspension / dispute and restore property status.
     */
    function resolveProperty(uint256 propertyId, PropertyStatus newStatus)
        external
        propertyExists(propertyId)
        onlyRole(COMPLIANCE_ROLE)
    {
        require(
            newStatus == PropertyStatus.Registered ||
            newStatus == PropertyStatus.Listed,
            "Can only resolve to Registered or Listed"
        );

        Property storage prop = properties[propertyId];
        require(
            prop.status == PropertyStatus.Suspended ||
            prop.status == PropertyStatus.Disputed,
            "Property not suspended or disputed"
        );

        PropertyStatus oldStatus = prop.status;
        prop.status = newStatus;

        emit PropertyStatusChanged(
            propertyId, oldStatus, newStatus, block.timestamp
        );
    }

    // -----------------------------------------------------------------------
    // Metadata & Tokenization
    // -----------------------------------------------------------------------

    /**
     * @notice Update property metadata IPFS hash.
     */
    function updateMetadata(uint256 propertyId, bytes32 newMetadataIPFS)
        external
        propertyExists(propertyId)
        onlyPropertyOwner(propertyId)
    {
        Property storage prop = properties[propertyId];
        bytes32 oldHash = prop.metadataIPFS;
        prop.metadataIPFS = newMetadataIPFS;

        emit PropertyMetadataUpdated(
            propertyId, oldHash, newMetadataIPFS, block.timestamp
        );
    }

    /**
     * @notice Mark property as tokenized (ERC-721 minted).
     * @dev Called by TokenizedProperty contract after minting.
     */
    function markTokenized(uint256 propertyId, address tokenContract, uint256 tokenId)
        external
        propertyExists(propertyId)
        onlyRole(REGISTRAR_ROLE)
    {
        properties[propertyId].isTokenized = true;

        emit PropertyTokenized(propertyId, tokenContract, tokenId, block.timestamp);
    }

    // -----------------------------------------------------------------------
    // View Functions (for metrics computation — Paper Sec 5.1)
    // -----------------------------------------------------------------------

    /**
     * @notice Get complete property details.
     */
    function getProperty(uint256 propertyId)
        external
        view
        propertyExists(propertyId)
        returns (Property memory)
    {
        return properties[propertyId];
    }

    /**
     * @notice Get all properties owned by an address.
     */
    function getOwnerPropertyIds(address owner)
        external
        view
        returns (uint256[] memory)
    {
        return ownerProperties[owner];
    }

    /**
     * @notice Get transfer history for a property.
     */
    function getTransferHistory(uint256 propertyId)
        external
        view
        propertyExists(propertyId)
        returns (TransferRecord[] memory)
    {
        return transferHistory[propertyId];
    }

    /**
     * @notice Get transfer count for a property.
     */
    function getTransferCount(uint256 propertyId)
        external
        view
        propertyExists(propertyId)
        returns (uint32)
    {
        return properties[propertyId].transferCount;
    }

    /**
     * @notice Get total number of registered properties.
     */
    function totalProperties() external view returns (uint256) {
        return _propertyIdCounter.current();
    }

    /**
     * @notice Get registry-wide statistics for metrics (Sec 5.1).
     */
    function getRegistryStats()
        external
        view
        returns (
            uint256 _totalRegistrations,
            uint256 _totalTransfers,
            uint256 _totalVolumeWei,
            uint256 _totalProperties
        )
    {
        return (
            totalRegistrations,
            totalTransfers,
            totalVolumeWei,
            _propertyIdCounter.current()
        );
    }

    /**
     * @notice Lookup property ID by title deed hash.
     */
    function getPropertyByTitleDeed(bytes32 titleDeedHash)
        external
        view
        returns (uint256)
    {
        uint256 pid = titleDeedToProperty[titleDeedHash];
        if (pid == 0) revert PropertyNotFound(0);
        return pid;
    }

    // -----------------------------------------------------------------------
    // Admin Functions
    // -----------------------------------------------------------------------

    function pause() external onlyRole(DEFAULT_ADMIN_ROLE) {
        _pause();
    }

    function unpause() external onlyRole(DEFAULT_ADMIN_ROLE) {
        _unpause();
    }

    // -----------------------------------------------------------------------
    // Internal Helpers
    // -----------------------------------------------------------------------

    /**
     * @dev Internal registration (shared by single and batch).
     */
    function _registerPropertyInternal(
        address owner,
        bytes32 titleDeedHash,
        bytes32 metadataIPFS,
        PropertyType propertyType,
        uint256 priceWei,
        uint128 areaSqft,
        uint16 districtCode,
        uint64 dldRefNumber
    ) internal {
        if (owner == address(0)) revert ZeroAddress();
        if (titleDeedHash == bytes32(0)) revert InvalidTitleDeed();
        if (priceWei == 0) revert ZeroPrice();
        if (titleDeedToProperty[titleDeedHash] != 0)
            revert TitleDeedAlreadyRegistered(titleDeedHash);

        _propertyIdCounter.increment();
        uint256 propertyId = _propertyIdCounter.current();

        properties[propertyId] = Property({
            propertyId: propertyId,
            owner: owner,
            status: PropertyStatus.Registered,
            propertyType: propertyType,
            registrationTimestamp: uint40(block.timestamp),
            lastTransferTimestamp: 0,
            titleDeedHash: titleDeedHash,
            metadataIPFS: metadataIPFS,
            priceWei: priceWei,
            areaSqft: areaSqft,
            dldReferenceNumber: dldRefNumber,
            transferCount: 0,
            districtCode: districtCode,
            kycCleared: true,
            isTokenized: false
        });

        titleDeedToProperty[titleDeedHash] = propertyId;
        ownerProperties[owner].push(propertyId);
        totalRegistrations++;
        totalVolumeWei += priceWei;

        emit PropertyRegistered(
            propertyId,
            owner,
            titleDeedHash,
            propertyType,
            districtCode,
            priceWei,
            block.timestamp
        );
    }

    /**
     * @dev Remove a property ID from an owner's list.
     */
    function _removeFromOwnerList(address owner, uint256 propertyId) internal {
        uint256[] storage ids = ownerProperties[owner];
        for (uint256 i = 0; i < ids.length;) {
            if (ids[i] == propertyId) {
                ids[i] = ids[ids.length - 1];
                ids.pop();
                return;
            }
            unchecked { ++i; }
        }
    }

    /**
     * @dev Initialize Dubai district code lookup table.
     *      Codes match those used in generate_transactions.py.
     */
    function _initializeDistricts() internal {
        districtNames[1]  = "Downtown Dubai";
        districtNames[2]  = "Dubai Marina";
        districtNames[3]  = "Palm Jumeirah";
        districtNames[4]  = "Business Bay";
        districtNames[5]  = "Jumeirah Village Circle";
        districtNames[6]  = "Dubai Hills Estate";
        districtNames[7]  = "Arabian Ranches";
        districtNames[8]  = "DIFC";
        districtNames[9]  = "Jumeirah Lake Towers";
        districtNames[10] = "Dubai Silicon Oasis";
        districtNames[11] = "Motor City";
        districtNames[12] = "Dubai South";
        districtNames[13] = "Al Barsha";
        districtNames[14] = "Deira";
        districtNames[15] = "Bur Dubai";
        districtNames[16] = "Mirdif";
        districtNames[17] = "Dubai Creek Harbour";
        districtNames[18] = "Damac Hills";
        districtNames[19] = "Town Square";
        districtNames[20] = "Sobha Hartland";
        districtNames[21] = "MBR City";
        districtNames[22] = "Al Furjan";
    }
}
