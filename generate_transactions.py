#!/usr/bin/env python3
"""
generate_transactions.py — Synthetic Real Estate Transaction Generator
=====================================================================
Paper: "A Scalable Layer Two Blockchain Framework for Enhancing
        Real Estate Transaction Efficiency"
Section: 4.1 (Data Generation & DLD Calibration)

Generates 50K+ synthetic real estate transactions calibrated to
Dubai Land Department (DLD) statistics, including:
  - Property type distributions (residential, commercial, land, industrial)
  - Price ranges with log-normal distributions
  - Seasonal transaction patterns (quarterly & monthly)
  - Participant demographics (repeat buyers, institutional, international)
  - Transaction complexity tiers (simple, standard, complex)

Usage:
    python src/data_generation/generate_transactions.py --all
    python src/data_generation/generate_transactions.py --count 10000
    python src/data_generation/generate_transactions.py --quick
"""

import os
import sys
import json
import hashlib
import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any

import numpy as np
import pandas as pd
import yaml
from faker import Faker
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
CONFIG_PATH = PROJECT_ROOT / "data" / "config" / "sim_config.yml"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed"
RAW_DIR = PROJECT_ROOT / "data" / "raw"

# Dubai/UAE locale + English fallback
fake = Faker(["en_US", "ar_AE"])

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration Loader
# ---------------------------------------------------------------------------
def load_config(config_path: Path = CONFIG_PATH) -> Dict[str, Any]:
    """Load simulation configuration from YAML."""
    if not config_path.exists():
        logger.error(f"Config not found: {config_path}")
        sys.exit(1)
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    logger.info(f"Loaded config from {config_path}")
    return config


# ---------------------------------------------------------------------------
# Participant Generator
# ---------------------------------------------------------------------------
class ParticipantPool:
    """
    Generates and manages buyer/seller pools with demographics
    matching Sec 4.1 DLD statistics.
    """

    def __init__(self, config: Dict[str, Any], rng: np.random.Generator):
        self.rng = rng
        pconf = config["data_generation"]["participants"]
        self.total_buyers = pconf["total_buyers"]
        self.total_sellers = pconf["total_sellers"]
        self.repeat_buyer_pct = pconf["repeat_buyer_pct"]
        self.institutional_pct = pconf["institutional_buyer_pct"]
        self.international_pct = pconf["international_buyer_pct"]

        self.buyers = self._generate_participants(self.total_buyers, "buyer")
        self.sellers = self._generate_participants(self.total_sellers, "seller")

        # Pre-compute repeat buyer pool
        n_repeat = int(self.total_buyers * self.repeat_buyer_pct)
        self.repeat_buyer_ids = [b["participant_id"] for b in self.buyers[:n_repeat]]

        logger.info(
            f"Generated {self.total_buyers} buyers, {self.total_sellers} sellers"
        )

    def _generate_participants(
        self, count: int, role: str
    ) -> List[Dict[str, Any]]:
        participants = []
        for i in range(count):
            is_institutional = self.rng.random() < self.institutional_pct
            is_international = self.rng.random() < self.international_pct

            if is_institutional:
                name = fake.company()
                p_type = "institutional"
            else:
                name = fake.name()
                p_type = "individual"

            nationality = (
                fake.country_code()
                if is_international
                else "AE"
            )

            pid = hashlib.sha256(
                f"{role}_{i}_{name}".encode()
            ).hexdigest()[:16]

            participants.append(
                {
                    "participant_id": pid,
                    "role": role,
                    "name": name,
                    "participant_type": p_type,
                    "nationality": nationality,
                    "is_international": is_international,
                    "kyc_level": self.rng.choice(
                        ["basic", "enhanced", "institutional"],
                        p=[0.60, 0.30, 0.10],
                    ),
                    "registration_date": fake.date_between(
                        start_date="-5y", end_date="-30d"
                    ).isoformat(),
                }
            )
        return participants

    def sample_buyer(self) -> Dict[str, Any]:
        """Sample a buyer, with bias toward repeat buyers."""
        if self.rng.random() < self.repeat_buyer_pct and self.repeat_buyer_ids:
            pid = self.rng.choice(self.repeat_buyer_ids)
            return next(b for b in self.buyers if b["participant_id"] == pid)
        return self.buyers[self.rng.integers(0, len(self.buyers))]

    def sample_seller(self) -> Dict[str, Any]:
        return self.sellers[self.rng.integers(0, len(self.sellers))]


# ---------------------------------------------------------------------------
# Property Generator
# ---------------------------------------------------------------------------
class PropertyGenerator:
    """
    Generates synthetic property records with DLD-calibrated
    price distributions and type weights (Sec 4.1).
    """

    # Dubai districts / communities
    DUBAI_DISTRICTS = [
        "Downtown Dubai", "Dubai Marina", "Palm Jumeirah",
        "Business Bay", "Jumeirah Village Circle", "Dubai Hills Estate",
        "Arabian Ranches", "DIFC", "Jumeirah Lake Towers",
        "Dubai Silicon Oasis", "Motor City", "Dubai South",
        "Al Barsha", "Deira", "Bur Dubai", "Mirdif",
        "Dubai Creek Harbour", "Damac Hills", "Town Square",
        "Sobha Hartland", "MBR City", "Al Furjan",
    ]

    def __init__(self, config: Dict[str, Any], rng: np.random.Generator):
        self.rng = rng
        self.property_types = config["data_generation"]["property_types"]
        self._type_names = list(self.property_types.keys())
        self._type_weights = np.array(
            [self.property_types[t]["weight"] for t in self._type_names]
        )
        # Normalize weights
        self._type_weights /= self._type_weights.sum()
        self._property_counter = 0

    def generate(self) -> Dict[str, Any]:
        """Generate a single property record."""
        self._property_counter += 1

        # Sample property type by weight
        ptype = self.rng.choice(self._type_names, p=self._type_weights)
        pconfig = self.property_types[ptype]

        # Log-normal price distribution (realistic real estate)
        price_mean = pconfig["price_mean_usd"]
        price_std = pconfig["price_std_usd"]
        mu = np.log(price_mean**2 / np.sqrt(price_std**2 + price_mean**2))
        sigma = np.sqrt(np.log(1 + (price_std**2 / price_mean**2)))
        price = float(self.rng.lognormal(mu, sigma))

        # Clamp to configured range
        price = np.clip(
            price, pconfig["price_range_usd"][0], pconfig["price_range_usd"][1]
        )
        price = round(price, 2)

        # Square footage with some variance
        sqft = int(
            pconfig["avg_sqft"] * self.rng.normal(1.0, 0.25)
        )
        sqft = max(sqft, 200)  # Minimum 200 sqft

        district = self.rng.choice(self.DUBAI_DISTRICTS)

        # Generate unique property ID (simulates DLD title deed number)
        prop_id = hashlib.sha256(
            f"PROP_{self._property_counter}_{district}".encode()
        ).hexdigest()[:12]

        # Title deed hash (for on-chain registration)
        title_hash = hashlib.keccak_256(
            f"{prop_id}_{price}_{district}".encode()
        ).hexdigest() if hasattr(hashlib, 'keccak_256') else hashlib.sha256(
            f"title_{prop_id}_{price}_{district}".encode()
        ).hexdigest()

        return {
            "property_id": prop_id,
            "property_type": ptype,
            "price_usd": price,
            "price_aed": round(price * 3.6725, 2),  # USD to AED
            "sqft": sqft,
            "price_per_sqft": round(price / sqft, 2),
            "district": district,
            "title_deed_hash": f"0x{title_hash}",
            "bedrooms": self._estimate_bedrooms(ptype, sqft),
            "year_built": int(self.rng.integers(1990, 2025)),
            "is_off_plan": bool(self.rng.random() < 0.20),  # 20% off-plan
        }

    def _estimate_bedrooms(self, ptype: str, sqft: int) -> Optional[int]:
        """Estimate bedrooms based on type and size."""
        if ptype in ("commercial_office", "commercial_retail",
                      "land_plot", "industrial_warehouse"):
            return None
        if sqft < 500:
            return int(self.rng.choice([0, 1], p=[0.3, 0.7]))  # Studio/1BR
        elif sqft < 1000:
            return int(self.rng.choice([1, 2], p=[0.4, 0.6]))
        elif sqft < 2000:
            return int(self.rng.choice([2, 3], p=[0.5, 0.5]))
        elif sqft < 4000:
            return int(self.rng.choice([3, 4, 5], p=[0.3, 0.5, 0.2]))
        else:
            return int(self.rng.choice([5, 6, 7], p=[0.5, 0.3, 0.2]))


# ---------------------------------------------------------------------------
# Transaction Timeline Generator
# ---------------------------------------------------------------------------
class TimelineGenerator:
    """
    Distributes transactions across a 365-day window using
    DLD seasonal patterns (Sec 4.1).
    """

    def __init__(self, config: Dict[str, Any], rng: np.random.Generator):
        self.rng = rng
        dg = config["data_generation"]
        self.start_date = datetime.fromisoformat(dg["start_date"])
        self.time_horizon_days = dg["time_horizon_days"]
        self.monthly_weights = np.array(dg["monthly_weights"])
        # Normalize
        self.monthly_weights /= self.monthly_weights.sum()

    def generate_timestamps(self, n: int) -> List[datetime]:
        """Generate n transaction timestamps following seasonal patterns."""
        # Assign each transaction to a month
        months = self.rng.choice(12, size=n, p=self.monthly_weights)

        timestamps = []
        for month_idx in months:
            # Random day within the month
            year = self.start_date.year
            month = month_idx + 1  # 0-indexed to 1-indexed

            # Days in this month
            if month == 12:
                days_in_month = (
                    datetime(year + 1, 1, 1) - datetime(year, month, 1)
                ).days
            else:
                days_in_month = (
                    datetime(year, month + 1, 1) - datetime(year, month, 1)
                ).days

            day = int(self.rng.integers(1, days_in_month + 1))
            hour = int(self.rng.integers(8, 18))  # Business hours 8AM-6PM
            minute = int(self.rng.integers(0, 60))
            second = int(self.rng.integers(0, 60))

            ts = datetime(year, month, day, hour, minute, second)
            timestamps.append(ts)

        # Sort chronologically
        timestamps.sort()
        return timestamps


# ---------------------------------------------------------------------------
# Complexity & Transaction Event Generator
# ---------------------------------------------------------------------------
class ComplexityAssigner:
    """
    Assigns transaction complexity tiers and generates
    the corresponding on-chain event sequences (Sec 3.2, 4.1).
    """

    # Event sequences per complexity tier
    EVENT_SEQUENCES = {
        "simple": [
            "KYC_VERIFY",
            "ESCROW_INITIATE",
            "OWNERSHIP_TRANSFER",
            "DEED_RECORD",
        ],
        "standard": [
            "KYC_VERIFY",
            "MORTGAGE_CHECK",
            "VALUATION_SUBMIT",
            "NOC_REQUEST",
            "ESCROW_INITIATE",
            "OWNERSHIP_TRANSFER",
            "DEED_RECORD",
        ],
        "complex": [
            "KYC_VERIFY",
            "KYC_ENHANCED",
            "AML_SCREENING",
            "LEGAL_REVIEW",
            "MULTI_SIG_APPROVAL",
            "MORTGAGE_CHECK",
            "VALUATION_SUBMIT",
            "NOC_REQUEST",
            "ESCROW_INITIATE",
            "COMPLIANCE_FINAL",
            "OWNERSHIP_TRANSFER",
            "DEED_RECORD",
        ],
    }

    def __init__(self, config: Dict[str, Any], rng: np.random.Generator):
        self.rng = rng
        tiers = config["data_generation"]["complexity_tiers"]
        self._tier_names = list(tiers.keys())
        self._tier_weights = np.array([tiers[t]["weight"] for t in self._tier_names])
        self._tier_weights /= self._tier_weights.sum()
        self._tiers = tiers

    def assign(self) -> Dict[str, Any]:
        """Assign a complexity tier and its event sequence."""
        tier = self.rng.choice(self._tier_names, p=self._tier_weights)
        tier_config = self._tiers[tier]

        return {
            "complexity_tier": tier,
            "num_steps": tier_config["steps"],
            "num_documents": tier_config["avg_documents"]
            + int(self.rng.integers(-1, 3)),
            "event_sequence": self.EVENT_SEQUENCES[tier].copy(),
        }


# ---------------------------------------------------------------------------
# Gas Cost Estimator
# ---------------------------------------------------------------------------
class GasCostEstimator:
    """
    Estimates gas costs for each transaction across L1/L2 environments
    using parameters from sim_config.yml (Sec 4.2, 5.2).
    """

    # Map event types to gas cost keys
    EVENT_GAS_MAP = {
        "KYC_VERIFY": "kyc_verification",
        "KYC_ENHANCED": "kyc_verification",
        "AML_SCREENING": "kyc_verification",
        "ESCROW_INITIATE": "escrow_initiate",
        "ESCROW_RELEASE": "escrow_release",
        "OWNERSHIP_TRANSFER": "ownership_transfer",
        "DEED_RECORD": "property_registration",
        "DEED_TOKENIZE": "deed_tokenize",
        "MORTGAGE_CHECK": "kyc_verification",
        "VALUATION_SUBMIT": "property_registration",
        "NOC_REQUEST": "kyc_verification",
        "LEGAL_REVIEW": "kyc_verification",
        "MULTI_SIG_APPROVAL": "escrow_release",
        "COMPLIANCE_FINAL": "kyc_verification",
        "DAO_VOTE": "dao_vote",
        "DISPUTE_FILE": "dispute_file",
    }

    def __init__(self, config: Dict[str, Any], rng: np.random.Generator):
        self.rng = rng
        self.l1 = config["l1_ethereum"]
        self.l2_opt = config["l2_optimism"]
        self.l2_zk = config["l2_polygon_zkevm"]

    def estimate_l1_cost(self, events: List[str]) -> Dict[str, float]:
        """Estimate total L1 gas cost for a transaction's event sequence."""
        total_gas = 0
        for event in events:
            gas_key = self.EVENT_GAS_MAP.get(event, "kyc_verification")
            base_gas = self.l1["gas_costs"][gas_key]
            # Add ±10% variance
            gas = int(base_gas * self.rng.normal(1.0, 0.10))
            total_gas += max(gas, 21000)  # Minimum 21K gas

        gas_price_gwei = (
            self.l1["base_fee_gwei"] + self.l1["priority_fee_gwei"]
        )
        cost_eth = (total_gas * gas_price_gwei) / 1e9
        cost_usd = cost_eth * self.l1["eth_price_usd"]

        return {
            "l1_total_gas": total_gas,
            "l1_gas_price_gwei": gas_price_gwei,
            "l1_cost_eth": round(cost_eth, 8),
            "l1_cost_usd": round(cost_usd, 2),
        }

    def estimate_l2_optimism_cost(self, events: List[str]) -> Dict[str, float]:
        """Estimate L2 Optimism cost."""
        total_gas = 0
        for event in events:
            gas_key = self.EVENT_GAS_MAP.get(event, "kyc_verification")
            base_gas = self.l2_opt["gas_costs"][gas_key]
            gas = int(base_gas * self.rng.normal(1.0, 0.08))
            total_gas += max(gas, 21000)

        l2_exec_cost = (total_gas * self.l2_opt["l2_gas_price_gwei"]) / 1e9
        l1_data_cost = l2_exec_cost * self.l2_opt["l1_data_fee_multiplier"]
        total_cost_eth = l2_exec_cost + l1_data_cost
        cost_usd = total_cost_eth * self.l1["eth_price_usd"]

        # Apply blob fee reduction (EIP-4844)
        cost_usd *= (1 - self.l2_opt["blob_fee_reduction"])

        return {
            "l2_opt_total_gas": total_gas,
            "l2_opt_cost_eth": round(total_cost_eth, 10),
            "l2_opt_cost_usd": round(max(cost_usd, 0.01), 4),
        }

    def estimate_l2_zkevm_cost(self, events: List[str]) -> Dict[str, float]:
        """Estimate L2 Polygon zkEVM cost."""
        total_gas = 0
        for event in events:
            gas_key = self.EVENT_GAS_MAP.get(event, "kyc_verification")
            base_gas = self.l2_zk["gas_costs"][gas_key]
            gas = int(base_gas * self.rng.normal(1.0, 0.08))
            total_gas += max(gas, 21000)

        l2_exec_cost = (total_gas * self.l2_zk["l2_gas_price_gwei"]) / 1e9
        # ZK proof amortization
        proof_cost_per_tx = (
            self.l2_zk["l1_verification_gas"]
            * self.l1["base_fee_gwei"]
            / 1e9
            / self.l2_zk["proof_amortization_factor"]
        )
        total_cost_eth = l2_exec_cost + proof_cost_per_tx
        cost_usd = total_cost_eth * self.l1["eth_price_usd"]

        # Apply blob fee reduction
        cost_usd *= (1 - self.l2_zk["blob_fee_reduction"])

        return {
            "l2_zk_total_gas": total_gas,
            "l2_zk_cost_eth": round(total_cost_eth, 10),
            "l2_zk_cost_usd": round(max(cost_usd, 0.01), 4),
        }


# ---------------------------------------------------------------------------
# Main Transaction Generator
# ---------------------------------------------------------------------------
class TransactionGenerator:
    """
    Orchestrates synthetic transaction generation combining all
    sub-generators to produce a complete dataset (Sec 4.1).
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        seed = config["global"]["random_seed"]
        self.rng = np.random.default_rng(seed)

        Faker.seed(seed)

        self.participants = ParticipantPool(config, self.rng)
        self.property_gen = PropertyGenerator(config, self.rng)
        self.timeline_gen = TimelineGenerator(config, self.rng)
        self.complexity = ComplexityAssigner(config, self.rng)
        self.gas_estimator = GasCostEstimator(config, self.rng)

        logger.info("TransactionGenerator initialized")

    def generate(self, n_transactions: int) -> pd.DataFrame:
        """Generate n synthetic real estate transactions."""
        logger.info(f"Generating {n_transactions} transactions...")

        # Generate timestamps
        timestamps = self.timeline_gen.generate_timestamps(n_transactions)

        transactions = []
        for i in tqdm(range(n_transactions), desc="Generating transactions"):
            # Sample participants
            buyer = self.participants.sample_buyer()
            seller = self.participants.sample_seller()

            # Ensure buyer != seller
            while seller["participant_id"] == buyer["participant_id"]:
                seller = self.participants.sample_seller()

            # Generate property
            prop = self.property_gen.generate()

            # Assign complexity
            complexity = self.complexity.assign()

            # Estimate gas costs across environments
            events = complexity["event_sequence"]
            l1_costs = self.gas_estimator.estimate_l1_cost(events)
            l2_opt_costs = self.gas_estimator.estimate_l2_optimism_cost(events)
            l2_zk_costs = self.gas_estimator.estimate_l2_zkevm_cost(events)

            # Transaction ID (simulates on-chain tx hash)
            tx_hash = hashlib.sha256(
                f"TX_{i}_{timestamps[i].isoformat()}_{prop['property_id']}".encode()
            ).hexdigest()

            tx = {
                # -- Transaction Metadata --
                "tx_id": f"0x{tx_hash[:64]}",
                "tx_index": i,
                "timestamp": timestamps[i].isoformat(),
                "date": timestamps[i].date().isoformat(),
                "month": timestamps[i].month,
                "quarter": f"Q{(timestamps[i].month - 1) // 3 + 1}",
                "day_of_week": timestamps[i].strftime("%A"),
                # -- Buyer --
                "buyer_id": buyer["participant_id"],
                "buyer_type": buyer["participant_type"],
                "buyer_nationality": buyer["nationality"],
                "buyer_is_international": buyer["is_international"],
                "buyer_kyc_level": buyer["kyc_level"],
                # -- Seller --
                "seller_id": seller["participant_id"],
                "seller_type": seller["participant_type"],
                "seller_nationality": seller["nationality"],
                # -- Property --
                "property_id": prop["property_id"],
                "property_type": prop["property_type"],
                "price_usd": prop["price_usd"],
                "price_aed": prop["price_aed"],
                "sqft": prop["sqft"],
                "price_per_sqft": prop["price_per_sqft"],
                "district": prop["district"],
                "title_deed_hash": prop["title_deed_hash"],
                "bedrooms": prop["bedrooms"],
                "year_built": prop["year_built"],
                "is_off_plan": prop["is_off_plan"],
                # -- Complexity --
                "complexity_tier": complexity["complexity_tier"],
                "num_steps": complexity["num_steps"],
                "num_documents": complexity["num_documents"],
                "event_sequence": json.dumps(complexity["event_sequence"]),
                # -- L1 Ethereum Costs --
                "l1_total_gas": l1_costs["l1_total_gas"],
                "l1_gas_price_gwei": l1_costs["l1_gas_price_gwei"],
                "l1_cost_eth": l1_costs["l1_cost_eth"],
                "l1_cost_usd": l1_costs["l1_cost_usd"],
                # -- L2 Optimism Costs --
                "l2_opt_total_gas": l2_opt_costs["l2_opt_total_gas"],
                "l2_opt_cost_eth": l2_opt_costs["l2_opt_cost_eth"],
                "l2_opt_cost_usd": l2_opt_costs["l2_opt_cost_usd"],
                # -- L2 Polygon zkEVM Costs --
                "l2_zk_total_gas": l2_zk_costs["l2_zk_total_gas"],
                "l2_zk_cost_eth": l2_zk_costs["l2_zk_cost_eth"],
                "l2_zk_cost_usd": l2_zk_costs["l2_zk_cost_usd"],
                # -- Derived Metrics --
                "l2_opt_savings_pct": round(
                    (1 - l2_opt_costs["l2_opt_cost_usd"] / max(l1_costs["l1_cost_usd"], 0.01))
                    * 100, 2
                ),
                "l2_zk_savings_pct": round(
                    (1 - l2_zk_costs["l2_zk_cost_usd"] / max(l1_costs["l1_cost_usd"], 0.01))
                    * 100, 2
                ),
            }
            transactions.append(tx)

        df = pd.DataFrame(transactions)
        logger.info(f"Generated {len(df)} transactions")
        return df

    def generate_participants_csv(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Export buyer and seller pools as DataFrames."""
        buyers_df = pd.DataFrame(self.participants.buyers)
        sellers_df = pd.DataFrame(self.participants.sellers)
        return buyers_df, sellers_df


# ---------------------------------------------------------------------------
# Data Validation & Summary
# ---------------------------------------------------------------------------
def validate_dataset(df: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
    """Validate generated data against expected distributions (Sec 4.1)."""
    report = {"status": "PASS", "checks": []}

    # Check 1: Total transaction count
    expected = config["data_generation"]["total_transactions"]
    actual = len(df)
    check = {
        "name": "Transaction Count",
        "expected": expected,
        "actual": actual,
        "pass": actual == expected,
    }
    report["checks"].append(check)

    # Check 2: Property type distribution (within ±3% of target)
    for ptype, pconf in config["data_generation"]["property_types"].items():
        expected_pct = pconf["weight"] * 100
        actual_pct = (df["property_type"] == ptype).mean() * 100
        check = {
            "name": f"Property Type: {ptype}",
            "expected_pct": round(expected_pct, 1),
            "actual_pct": round(actual_pct, 1),
            "pass": abs(actual_pct - expected_pct) < 3.0,
        }
        report["checks"].append(check)

    # Check 3: Quarterly distribution
    q_counts = df["quarter"].value_counts(normalize=True) * 100
    for q in ["Q1", "Q2", "Q3", "Q4"]:
        check = {
            "name": f"Quarter {q} Distribution",
            "actual_pct": round(q_counts.get(q, 0), 1),
            "pass": 10 < q_counts.get(q, 0) < 40,
        }
        report["checks"].append(check)

    # Check 4: Complexity tier distribution
    for tier, tconf in config["data_generation"]["complexity_tiers"].items():
        expected_pct = tconf["weight"] * 100
        actual_pct = (df["complexity_tier"] == tier).mean() * 100
        check = {
            "name": f"Complexity: {tier}",
            "expected_pct": round(expected_pct, 1),
            "actual_pct": round(actual_pct, 1),
            "pass": abs(actual_pct - expected_pct) < 3.0,
        }
        report["checks"].append(check)

    # Check 5: L2 cost savings > 95%
    avg_l2_opt_savings = df["l2_opt_savings_pct"].mean()
    avg_l2_zk_savings = df["l2_zk_savings_pct"].mean()
    report["checks"].append({
        "name": "L2 Optimism Avg Savings",
        "actual_pct": round(avg_l2_opt_savings, 1),
        "pass": avg_l2_opt_savings > 95.0,
    })
    report["checks"].append({
        "name": "L2 zkEVM Avg Savings",
        "actual_pct": round(avg_l2_zk_savings, 1),
        "pass": avg_l2_zk_savings > 95.0,
    })

    # Overall status
    if not all(c["pass"] for c in report["checks"]):
        report["status"] = "WARN"
        failed = [c["name"] for c in report["checks"] if not c["pass"]]
        logger.warning(f"Validation warnings: {failed}")
    else:
        logger.info("All validation checks PASSED")

    return report


def print_summary(df: pd.DataFrame) -> None:
    """Print dataset summary statistics."""
    print("\n" + "=" * 70)
    print("  GENERATED DATASET SUMMARY")
    print("=" * 70)
    print(f"  Total Transactions : {len(df):,}")
    print(f"  Date Range         : {df['date'].min()} → {df['date'].max()}")
    print(f"  Unique Buyers      : {df['buyer_id'].nunique():,}")
    print(f"  Unique Sellers     : {df['seller_id'].nunique():,}")
    print(f"  Unique Properties  : {df['property_id'].nunique():,}")
    print()

    # Property type breakdown
    print("  Property Type Distribution:")
    for ptype in df["property_type"].value_counts().index:
        count = (df["property_type"] == ptype).sum()
        pct = count / len(df) * 100
        print(f"    {ptype:30s} {count:6,} ({pct:5.1f}%)")
    print()

    # Price statistics
    print("  Price Statistics (USD):")
    print(f"    Mean   : ${df['price_usd'].mean():>14,.2f}")
    print(f"    Median : ${df['price_usd'].median():>14,.2f}")
    print(f"    Min    : ${df['price_usd'].min():>14,.2f}")
    print(f"    Max    : ${df['price_usd'].max():>14,.2f}")
    print()

    # Complexity breakdown
    print("  Complexity Distribution:")
    for tier in ["simple", "standard", "complex"]:
        count = (df["complexity_tier"] == tier).sum()
        pct = count / len(df) * 100
        print(f"    {tier:12s} {count:6,} ({pct:5.1f}%)")
    print()

    # Cost comparison
    print("  Average Cost per Transaction:")
    print(f"    L1 Ethereum      : ${df['l1_cost_usd'].mean():>10,.2f}")
    print(f"    L2 Optimism      : ${df['l2_opt_cost_usd'].mean():>10,.4f}")
    print(f"    L2 Polygon zkEVM : ${df['l2_zk_cost_usd'].mean():>10,.4f}")
    print(f"    L2 Opt Savings   : {df['l2_opt_savings_pct'].mean():.1f}%")
    print(f"    L2 ZK Savings    : {df['l2_zk_savings_pct'].mean():.1f}%")
    print()

    # Quarterly distribution
    print("  Quarterly Distribution:")
    for q in ["Q1", "Q2", "Q3", "Q4"]:
        count = (df["quarter"] == q).sum()
        pct = count / len(df) * 100
        print(f"    {q} : {count:6,} ({pct:5.1f}%)")

    print("=" * 70 + "\n")


# ---------------------------------------------------------------------------
# CLI & Main
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate synthetic real estate transactions (Sec 4.1)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python generate_transactions.py --all          # Full 50K dataset
  python generate_transactions.py --count 5000   # Custom count
  python generate_transactions.py --quick         # Quick 1K test
  python generate_transactions.py --all --validate # With validation
        """,
    )
    parser.add_argument(
        "--all", action="store_true",
        help="Generate full dataset (50K transactions from config)",
    )
    parser.add_argument(
        "--count", type=int, default=None,
        help="Number of transactions to generate",
    )
    parser.add_argument(
        "--quick", action="store_true",
        help="Quick mode: 1K transactions for testing",
    )
    parser.add_argument(
        "--config", type=str, default=str(CONFIG_PATH),
        help="Path to sim_config.yml",
    )
    parser.add_argument(
        "--output-dir", type=str, default=str(OUTPUT_DIR),
        help="Output directory for generated files",
    )
    parser.add_argument(
        "--validate", action="store_true", default=True,
        help="Run validation checks after generation",
    )
    parser.add_argument(
        "--seed", type=int, default=None,
        help="Override random seed (default: from config)",
    )
    parser.add_argument(
        "--format", choices=["csv", "parquet", "both"], default="csv",
        help="Output format (default: csv)",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true",
        help="Enable verbose logging",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Load config
    config = load_config(Path(args.config))

    # Override seed if provided
    if args.seed is not None:
        config["global"]["random_seed"] = args.seed
        logger.info(f"Seed overridden to {args.seed}")

    # Determine transaction count
    if args.quick:
        n_transactions = config["global"]["quick_mode_transactions"]
        logger.info(f"Quick mode: {n_transactions} transactions")
    elif args.count is not None:
        n_transactions = args.count
    elif args.all:
        n_transactions = config["data_generation"]["total_transactions"]
    else:
        n_transactions = config["data_generation"]["total_transactions"]

    # Update config for validation
    config["data_generation"]["total_transactions"] = n_transactions

    # Create output directories
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    # Generate transactions
    generator = TransactionGenerator(config)
    df = generator.generate(n_transactions)

    # Generate participant files
    buyers_df, sellers_df = generator.generate_participants_csv()

    # Save transactions
    tx_csv_path = output_dir / "transactions_all.csv"
    df.to_csv(tx_csv_path, index=False)
    logger.info(f"Saved transactions → {tx_csv_path}")

    if args.format in ("parquet", "both"):
        tx_parquet_path = output_dir / "transactions_all.parquet"
        df.to_parquet(tx_parquet_path, index=False)
        logger.info(f"Saved transactions → {tx_parquet_path}")

    # Save per-environment subsets
    for env_prefix, cost_col in [
        ("L1", "l1_cost_usd"),
        ("optimism", "l2_opt_cost_usd"),
        ("polygon_zkevm", "l2_zk_cost_usd"),
    ]:
        env_path = output_dir / f"transactions_{env_prefix}.csv"
        env_cols = [
            "tx_id", "tx_index", "timestamp", "property_type", "price_usd",
            "complexity_tier", "num_steps", "district", cost_col,
        ]
        df[env_cols].to_csv(env_path, index=False)
        logger.info(f"Saved {env_prefix} subset → {env_path}")

    # Save participants
    buyers_path = output_dir / "participants_buyers.csv"
    sellers_path = output_dir / "participants_sellers.csv"
    buyers_df.to_csv(buyers_path, index=False)
    sellers_df.to_csv(sellers_path, index=False)
    logger.info(f"Saved participants → {buyers_path}, {sellers_path}")

    # Print summary
    print_summary(df)

    # Validate
    if args.validate:
        report = validate_dataset(df, config)
        report_path = output_dir / "validation_report.json"
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2)
        logger.info(f"Validation report → {report_path}")

        print(f"\nValidation Status: {report['status']}")
        for check in report["checks"]:
            status = "✓" if check["pass"] else "✗"
            print(f"  {status} {check['name']}: {check.get('actual_pct', check.get('actual', 'N/A'))}")

    # Summary stats JSON (for downstream simulation)
    stats = {
        "total_transactions": len(df),
        "date_range": [df["date"].min(), df["date"].max()],
        "unique_buyers": int(df["buyer_id"].nunique()),
        "unique_sellers": int(df["seller_id"].nunique()),
        "avg_price_usd": round(float(df["price_usd"].mean()), 2),
        "median_price_usd": round(float(df["price_usd"].median()), 2),
        "avg_l1_cost_usd": round(float(df["l1_cost_usd"].mean()), 2),
        "avg_l2_opt_cost_usd": round(float(df["l2_opt_cost_usd"].mean()), 4),
        "avg_l2_zk_cost_usd": round(float(df["l2_zk_cost_usd"].mean()), 4),
        "complexity_distribution": df["complexity_tier"].value_counts().to_dict(),
        "property_type_distribution": df["property_type"].value_counts().to_dict(),
        "quarterly_distribution": df["quarter"].value_counts().to_dict(),
        "seed": config["global"]["random_seed"],
        "generated_at": datetime.now().isoformat(),
    }
    stats_path = output_dir / "generation_stats.json"
    with open(stats_path, "w") as f:
        json.dump(stats, f, indent=2, default=str)
    logger.info(f"Stats → {stats_path}")

    print(f"\n✅ All files saved to: {output_dir}/")
    print(f"   - transactions_all.csv ({len(df):,} rows)")
    print(f"   - transactions_L1.csv")
    print(f"   - transactions_optimism.csv")
    print(f"   - transactions_polygon_zkevm.csv")
    print(f"   - participants_buyers.csv ({len(buyers_df):,} rows)")
    print(f"   - participants_sellers.csv ({len(sellers_df):,} rows)")
    print(f"   - generation_stats.json")
    print(f"   - validation_report.json")


if __name__ == "__main__":
    main()
