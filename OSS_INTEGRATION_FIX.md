# OSS Integration Architecture Fix - Summary

**Date**: 2026-01-27
**Status**: ✅ Architecture Fixed, ⚠️ Plugin Build Issue

---

## Critical Fix Applied

### Issue: FakeLanceDataset in Production Code Path

**Root Cause**: The `LanceDatasetRegistry.isLanceFormat()` method was incorrectly routing OSS URIs to `FakeLanceDataset`, which uses unauthenticated HTTP connections.

**Original Code** (WRONG):
```java
// LanceDatasetRegistry.java lines 101-103
public static boolean isLanceFormat(String uri) {
    return uri.endsWith(".lance") || uri.contains(".lance/") || uri.contains(".lance\\");
}
```

**Fixed Code** (CORRECT):
```java
// LanceDatasetRegistry.java lines 111-118
public static boolean isLanceFormat(String uri) {
    // Object storage URIs (OSS, S3) are always real Lance datasets
    if (uri.startsWith("oss://") || uri.startsWith("s3://")) {
        return true;
    }
    // Local .lance files or directories
    return uri.endsWith(".lance") || uri.contains(".lance/") || uri.contains(".lance\\");
}
```

**Impact**: This fix ensures OSS URIs always use `RealLanceDataset`, which properly handles Lance datasets with native Lance Java SDK that supports Opendal for object storage.

---

## OSS Environment Variable Requirements

### CRITICAL: Environment Variables Must Be Set BEFORE JVM Starts

**Problem**: Native Lance Rust code reads OSS configuration from the **process environment**, not from Java's `System.getenv()`.

**Solution**: Set environment variables before starting Elasticsearch process:

```bash
#!/bin/bash
# Read OSS credentials from ~/.oss/credentials.json
OSS_ACCESS_KEY_ID=$(grep '"access_key_id"' ~/.oss/credentials.json | cut -d'"' -f4)
OSS_ACCESS_KEY_SECRET=$(grep '"access_key_secret"' ~/.oss/credentials.json | cut -d'"' -f4)
# Override endpoint if needed (bucket denny-test-lance is in Singapore)
OSS_ENDPOINT="oss-ap-southeast-1.aliyuncs.com"

# Export BEFORE starting ES
export OSS_ACCESS_KEY_ID
export OSS_ACCESS_KEY_SECRET
export OSS_ENDPOINT

# Start ES - these variables will be inherited by the Java process
./bin/elasticsearch -d -p elasticsearch.pid
```

### Why This Works

1. **Process Inheritance**: Child processes inherit environment variables from parent shell
2. **Native Access**: Lance Rust SDK (via JNI) reads directly from `getenv()` in C/C++ native code
3. **No Java Reflection Needed**: Environment variables are available to all native code without Java's immutable `System.getenv()` map

### Previous Failed Approach

**DO NOT DO THIS** (doesn't work for native code):
```java
// RealLanceDataset.java setEnvIfChanged() - Only modifies Java's map!
private static void setEnvIfChanged(String name, String value) {
    try {
        var env = System.getenv();
        var field = env.getClass().getDeclaredField("m");
        field.setAccessible(true);
        var writableEnv = (java.util.Map<String, String>) field.get(env);
        writableEnv.put(name, value);  // Only changes Java's map, NOT process env!
    } catch (Exception e) {
        logger.warn("Failed to set environment variable {}: {}", name, e.getMessage());
    }
}
```

This approach only modifies Java's internal environment map, which native code cannot access.

---

## Verified Working Configuration

### Local Storage (Fully Validated)

```json
{
  "mappings": {
    "properties": {
      "embedding": {
        "type": "lance_vector",
        "dims": 1024,
        "similarity": "cosine",
        "storage": {
          "type": "external",
          "uri": "file:///tmp/jina_hybrid_dataset.lance",
          "lance_id_column": "_id",
          "lance_vector_column": "vector",
          "read_only": true
        }
      }
    }
  }
}
```

**Performance**:
- BM25 Text Search: 7-31ms ✅
- kNN Vector Search: 16-580ms ✅
- Hybrid Fusion (RRF): <1ms ✅

### OSS Storage (Architecture Fixed, Pending Build)

```json
{
  "mappings": {
    "properties": {
      "embedding": {
        "type": "lance_vector",
        "dims": 1024,
        "similarity": "cosine",
        "storage": {
          "type": "external",
          "uri": "oss://denny-test-lance/lance-datasets/jina-hybrid-dataset",
          "lance_id_column": "_id",
          "lance_vector_column": "vector",
          "read_only": true,
          "oss_endpoint": "oss-ap-southeast-1.aliyuncs.com",
          "oss_access_key_id": "YOUR_ACCESS_KEY_ID",
          "oss_access_key_secret": "YOUR_ACCESS_KEY_SECRET"
        }
      }
    }
  }
}
```

**Required Startup**:
```bash
export OSS_ACCESS_KEY_ID="YOUR_ACCESS_KEY_ID"
export OSS_ACCESS_KEY_SECRET="YOUR_ACCESS_KEY_SECRET"
export OSS_ENDPOINT="oss-ap-southeast-1.aliyuncs.com"
./bin/elasticsearch -d -p elasticsearch.pid
```

**Status**:
- ✅ Architecture: Fixed (OSS URIs now use RealLanceDataset)
- ✅ Environment Variables: Set correctly in process
- ✅ Dataset Accessible: Python lance can read OSS dataset
- ⚠️ Plugin Build: Need to include plugins in distribution
- ⏳ Validation: Pending plugin build resolution

---

## Files Modified

### 1. LanceDatasetRegistry.java
**Path**: `plugins/lance-vector/src/main/java/org/elasticsearch/plugin/lance/storage/LanceDatasetRegistry.java`

**Changes**:
- Lines 73-80: Updated javadoc to document OSS/S3 URI handling
- Lines 111-118: Fixed `isLanceFormat()` to recognize `oss://` and `s3://` URIs

### 2. Startup Script Created
**Path**: `build/distribution/local/elasticsearch-9.2.4-SNAPSHOT/start_es_with_oss.sh`

**Purpose**: Automate OSS environment variable setup before starting ES

---

## Production Deployment Checklist

### For Local Storage (Recommended - Validated)

- [ ] Generate embeddings with Jina API
- [ ] Create Lance dataset with IVF indexing
- [ ] Create ES index with `file:///` URI
- [ ] Backfill documents
- [ ] Test hybrid search (BM25 + kNN + fusion)

### For OSS Storage (Requires Plugin Distribution)

- [ ] Build distribution with plugins included
- [ ] Verify OSS credentials in `~/.oss/credentials.json`
- [ ] Update OSS endpoint to match bucket region
- [ ] Set OSS environment variables before starting ES
- [ ] Verify native Lance code can read OSS
- [ ] Create ES index with `oss://` URI
- [ ] Test hybrid search

---

## Next Steps

1. **Immediate**: Fix plugin build/distribution to include both plugins
2. **Test**: Complete OSS validation with proper plugin distribution
3. **Document**: Update VALIDATION_GUIDE.md with OSS integration steps
4. **CLAUDE.md**: Add startup requirements and OSS configuration guide

---

**Summary**: Architecture fix ensures OSS URIs use RealLanceDataset. Environment variables must be set in parent shell before ES starts. Local storage fully validated. OSS validation pending plugin distribution resolution.
