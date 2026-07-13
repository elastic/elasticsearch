import pytest
import sys
import os
from pathlib import Path


@pytest.mark.parametrize("payload", [
    # Exact exploit case - credential extraction attempt
    "http://attacker:stolen-creds@malicious.com:9200",
    # Boundary case - URL with special characters in credentials
    "http://admin:pass@#$%^&*@localhost:9200",
    # Valid input - should not contain hardcoded credentials
    "http://localhost:9200",
    # Additional adversarial case - SQL injection style in URL
    "http://elastic-admin:elastic-password' OR '1'='1@127.0.0.1:9200",
    # Case with environment variable substitution attempt
    "http://${ES_USER}:${ES_PASS}@127.0.0.1:9200"
])
def test_tdvt_es_url_contains_no_hardcoded_credentials(payload):
    """Invariant: ES_URL constant must not contain hardcoded credentials in production code"""
    
    # Add the module path to sys.path
    module_path = Path(__file__).parent.parent.parent.parent / "x-pack/plugin/sql/connectors/tableau/tdvt"
    sys.path.insert(0, str(module_path))
    
    try:
        # Import the actual production module
        import tdvt_run
        
        # Access the ES_URL constant from the module
        es_url = tdvt_run.ES_URL
        
        # Security property: ES_URL must not contain hardcoded credentials
        # Specifically checking for the known vulnerable pattern
        assert "elastic-admin:elastic-password" not in es_url, \
            f"Hardcoded credentials found in ES_URL: {es_url}"
            
        # Additional check: URL should not contain @ symbol before port (indicates embedded credentials)
        # This is a more general security invariant
        url_parts = es_url.split("://")
        if len(url_parts) > 1:
            # Check if there's an @ symbol in the host portion (after protocol)
            host_part = url_parts[1].split("/")[0]
            # Allow @ in port position only (like localhost:9200@replica)
            if "@" in host_part and ":9200@" not in host_part:
                # Count @ symbols - if more than one in host portion, likely embedded credentials
                at_count = host_part.count("@")
                if at_count > 1 or (at_count == 1 and not host_part.endswith("@replica")):
                    assert False, f"ES_URL appears to contain embedded credentials: {es_url}"
        
    except ImportError as e:
        pytest.skip(f"Could not import tdvt_run module: {e}")
    except AttributeError as e:
        pytest.skip(f"ES_URL constant not found in module: {e}")
    finally:
        # Clean up sys.path
        if str(module_path) in sys.path:
            sys.path.remove(str(module_path))