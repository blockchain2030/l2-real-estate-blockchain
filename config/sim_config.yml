
L2 Real Estate Blockchain Framework — Complete Research Reproduction

Python: 3.9+
Solidity: 0.8+

Overview

This repository reproduces the experiments, data, and results from the paper:
"A Scalable Layer Two Blockchain Framework for Enhancing Real Estate Transaction Efficiency"


Key Reproduced Results

Metric | L1 (Ethereum) | L2 Optimism | L2 Polygon zkEVM
TPS | 12–15 | 487 | 1,034+
Latency | 15.7s | 2.9s | 2.1s
Cost per Transaction | $45–65 | $0.12–0.29 | $0.12–0.29
Stress Test | Fails at 50K/hr | 90% efficiency | 90% efficiency

Paper Section Mapping

Section | Repository Path | Description
Sec 3.1–3.3 | src/contracts/, contracts/ | Smart contract architecture and L2 design
Sec 4.1–4.2 | src/data_generation/, src/simulation/ | Synthetic data generation and simulation engine
Sec 5.1–5.3 | src/metrics/, notebooks/ | Performance metrics and analysis

Getting Started

Prerequisites
Python 3.9+
Node.js 18+
Git

Setup Steps

1. Clone and setup Python environment

git clone <repository>
cd l2-real-estate-blockchain
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

2. Install blockchain dependencies

npm install
npx hardhat compile

3. Generate synthetic data

python src/data_generation/generate_transactions.py --all

Creates processed transaction datasets calibrated to Dubai Land Department statistics.

4. Deploy contracts and run experiments

bash scripts/run_all_experiments.sh

or individual runs

bash scripts/run_simulation.sh --env L1
bash scripts/run_simulation.sh --env optimism
bash scripts/run_simulation.sh --env polygon_zkevm

5. Verify results

jupyter execute notebooks/01_metrics_analysis.ipynb
jupyter execute notebooks/02_visualizations.ipynb

Expected Output Files

data/processed/results_L1.json
data/processed/results_optimism.json
data/processed/results_polygon_zkevm.json
docs/figures/figure5_tps_comparison.png
docs/figures/table1_performance.png
docs/figures/table2_cost_analysis.png

Architecture Overview

DApp Layer (React / Web3.js)
Smart Contracts Layer (Solidity)
PropertyRegistryL2
EscrowManager
ComplianceKYC
TokenizedProperty
DAOGovernance
DisputeResolution
FeeDistributor
L2BridgeAdapter

Middleware Layer (Chainlink oracles and external integrations)

Layer 2 Rollup Layer (Optimistic or ZK rollups)

Data Availability Layer (IPFS or Celestia)

Layer 1 Ethereum settlement layer

Transaction Flow

1. Buyer initiates purchase through decentralized application.
2. EscrowManager smart contract locks transaction funds.
3. ComplianceKYC verifies identity through oracle services.
4. PropertyRegistryL2 records ownership transfer.
5. Batch transactions settle on Layer 1 Ethereum.

Repository Structure

Root
README.md
LICENSE
requirements.txt
package.json
hardhat.config.js
environment.yml

data/
raw/
processed/
config/

contracts/
PropertyRegistryL2.sol
EscrowManager.sol
ComplianceKYC.sol
TokenizedProperty.sol
DAOGovernance.sol
DisputeResolution.sol
FeeDistributor.sol
L2BridgeAdapter.sol

src/
data_generation/
simulation/
metrics/
visualization/
contracts/

notebooks/
metrics analysis notebooks

scripts/
experiment scripts

tests/
smart contract tests
simulation tests
metrics tests

docs/
methodology documentation
figures
results summary
troubleshooting

Expected Experiment Results

L1 Baseline Ethereum
TPS approximately 13
Latency approximately 15.7 seconds
Cost approximately 52 USD

L2 Optimism
TPS approximately 487
Latency approximately 2.9 seconds
Cost approximately 0.23 USD

L2 Polygon zkEVM
TPS approximately 1,034
Latency approximately 2.1 seconds
Cost approximately 0.19 USD

Stress Test
Approximately 52,000 transactions per hour
Efficiency above 90 percent

Metrics Definitions

TPS = confirmed transactions divided by time window
LCCI = lifecycle cost complexity index
TRS = successful transactions divided by total transactions multiplied by 100
LSL = latency scalability level

Full formulas documented in methodology documentation.

Reviewer Verification Checklist

1. TPS matches reported values.
2. Transaction cost reduction matches paper.
3. Latency improvement verified.
4. Stress test processes at least 50K transactions per hour.
5. Lifecycle cost complexity reduction approximately 89 percent.
6. Contract events log escrow, KYC verification, and ownership transfer.
7. Tables and figures reproducible from scripts.
8. Synthetic dataset generates more than 50,000 transactions.
9. Simulation tests pass.
10. Hardhat contract tests pass.

Quick Experiment Run

bash scripts/run_all_experiments.sh --quick

Methodology Documentation

data_generation.md explains Dubai Land Department data calibration.
simulation_design.md explains simulation architecture.
metrics_formulas.md explains performance metric derivations.

Support

Refer to troubleshooting documentation for setup and experiment issues.

Author
Muhammad Shahid

Verification Status
