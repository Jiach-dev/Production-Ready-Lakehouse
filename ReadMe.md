
# Lakehouse Productionization Project

## Project Overview
This repository contains a production-grade data pipeline that processes and correlates real-time events from GDELT and Wikimedia streams. The solution features:
- Delta Live Tables (DLT) pipeline
- CI/CD deployment with GitHub Actions
- Unity Catalog governance
- Observability integration
- AI-powered analytics with Databricks Genie

## Repository Structure
week4/
├── dlt_pipeline/ # Delta Live Tables implementation
│ ├── gdelt_loader.py # GDELT data ingestion
│ ├── wikimedia_listener.py # Wikimedia stream processing
│ └── correlation_engine.py # Semantic correlation logic
├── ci_cd/ # Deployment workflows
│ ├── deploy_staging.yml # Staging environment workflow
│ └── deploy_production.yml # Production promotion workflow
├── sql/ # Database setup
│ ├── schema_setup.sql # Unity Catalog configuration
│ └── secure_views.sql # Dynamic view definitions
├── docs/ # Documentation
│ ├── architecture.pdf # System architecture diagram
│ └── observability_setup.md # Monitoring configuration
└── technical_report.pdf # Final technical report

text

## Environment Configuration

### Prerequisites
- Databricks workspace with Unity Catalog enabled
- Service principal with Contributor access
- GitHub repository secrets configured

### Setup Instructions

1. **Service Principal Configuration**
```bash
# Create service principal
az ad sp create-for-rbac --name "lakehouse-deploy-sp" \
  --role contributor \
  --scopes /subscriptions/{subscription-id}
GitHub Secrets Setup
Add these secrets to your repository:

DATABRICKS_HOST: Your Databricks workspace URL

DATABRICKS_TOKEN: Service principal access token

DATABRICKS_CLUSTER_ID: Target cluster ID

CATALOG_NAME: Unity Catalog name (e.g., prod_lakehouse)

Local Development Setup

bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
CI/CD Pipeline
Workflow Overview
On Push to Main Branch:

Run unit tests (PyTest)

Validate notebook syntax

Deploy to staging environment

Run integration tests

Manual Approval:

Requires maintainer approval

Post-deployment verification

Production Promotion:

Version tagging

Final deployment

Observability setup

Key Jobs
Job	Description	Trigger
Validate	Syntax checks & unit tests	Push
Staging Deploy	Deploys to staging env	Push to main
Integration Test	Validates pipeline behavior	After staging deploy
Production Deploy	Promotes to production	Manual approval
Pipeline Configuration
Secrets Management
All sensitive values are stored as GitHub secrets and injected during workflow execution:

yaml
env:
  DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
  DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
Deployment Parameters
Configure in .github/workflows/deploy_production.yml:

yaml
- name: Deploy Pipeline
  run: |
    databricks pipelines deploy \
      --config dlt_pipeline/pipeline.json \
      --target ${{ env.TARGET_ENV }}
Monitoring Setup
Built-in Observability
Pipeline health metrics

Data quality checks

Latency monitoring

Custom Metrics
python
# Sample custom metric tracking
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
w.metrics.emit(
    "pipeline.latency",
    value=processing_time,
    tags={"pipeline": "gdelt_wiki"}
)
Access Control
Permission Matrix
Role	Catalog Access	Table Access	Views
Data Scientist	Read	Silver+	All
Analyst	Read	Gold only	Filtered
Compliance	Read/Write	All	Unmasked
Troubleshooting
Common Issues
Authentication Failures:

Verify service principal permissions

Check token expiration

Pipeline Stuck:

bash
databricks pipelines repair --pipeline-id 1234
Data Quality Alerts:

Check Delta Lake expectations



This README provides:
1. Clear environment setup instructions
2. CI/CD workflow documentation
3. Access control specifications
4. Troubleshooting guidance
5. Support contacts

The structure follows Databricks best practices for production deployments while maintaining security through proper secret management and role-based access controls.
