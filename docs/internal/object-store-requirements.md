# **Elasticsearch Requirements for an S3-Compatible Object Store**

🟢 = tested in `/_snapshot/{repository}/_analyze`
🟠 = partially tested in `/_snapshot/{repository}/_analyze`
🔴 = not tested in `/_snapshot/{repository}/_analyze`
🔵 = not testable in `/_snapshot/{repository}/_analyze`

This document describes the requirements for an S3-compatible object store, both for use as a snapshot repository and for use as a stateless backend. These requirements are subject to change from one Elasticsearch release to the next. The `/_snapshot/{repository}/_analyze` API can be used to test whether these requirements are met.

## **Basics**

### **🟠 HTTP Status Code Semantics**

**Description:** The backend must return the HTTP status codes that the S3 API spec says it should. For example, 404 for missing blobs/uploads, 409/412 for conditional write failures (when conditional writes are supported), 200 with NoSuchUpload error code for uploads aborted during completion.

**Why:** The S3 blob container implementation makes control flow decisions based on specific HTTP status codes returned by the backend. Incorrect status codes would cause the wrong exception type to be thrown or the wrong branch to be taken, potentially leading to data loss or infinite retries. The 409/412 status codes are only relevant when conditional writes are enabled (Registers section, Path 1).

---

### **🟢 Short Term Data Integrity**

**Description:** The storage backend must not silently corrupt data in transit or at rest. Once a blob has been written, its contents must remain unchanged until it is deliberately modified or deleted.

**Why:** Snapshot data and metadata blobs must be stored and retrieved without modification. Silent corruption of a data blob would cause restore failures; corruption of a metadata blob could render the repository unusable. In stateless Elasticsearch, corrupted Lucene commit data would cause shard-level data loss.

---

### **🔵 Long Term Data Integrity**

**Description:** The repository must perform durable writes. Once a blob has been written, it must remain in place until it is deleted, even after a power loss or similar disaster.

**Why:** Elasticsearch relies on blobs persisting once a write completes successfully. Snapshot metadata and data blobs, cluster state, translog files, and Lucene commit data must all survive infrastructure failures. A non-durable write that is silently lost would cause snapshot restore failures, missing cluster state, or shard-level data loss in stateless Elasticsearch.

---

### **🔵 Correct Behavior Under Service Disruption**

**Description:** The repository must behave correctly even if connectivity from the cluster is disrupted. Reads and writes may fail in this case, but they must not return incorrect results.

**Why:** In a distributed system, network partitions and connectivity disruptions are inevitable. The critical requirement is that the storage backend never returns stale data, partial data, or a success response for a write that was not actually persisted. It is acceptable for operations to fail with errors during a disruption — Elasticsearch has retry logic to handle transient failures — but silent data corruption or phantom success responses would be catastrophic. For example, a GetObject that returns an older version of a blob during a partition could cause a node to operate on stale cluster state or corrupt a snapshot restore.

---

### **🟢 Authenticating Requests (AWS Signature Version 4)**

**Description:** The backend must authenticate requests using [AWS Signature Version 4](https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html). All S3 API requests from Elasticsearch are signed with SigV4 using the configured credentials and region.

**Why:** Elasticsearch uses the AWS SDK v2 S3 client, which signs every request with SigV4. The backend must validate these signatures to authenticate requests. There is no option to disable signing or use a different authentication mechanism — the signer is hardcoded as AwsS3V4Signer. Credentials can be provided via static access key/secret key pairs, temporary session credentials (with session token), Web Identity Token (OIDC/IRSA), or the default AWS credential provider chain (environment variables, instance profile, etc.).

---

## **Reads**

### **🟢 GetObject Support**

**Description:** The backend must support the S3 GetObject API, returning the full blob content as a byte stream with correct Content-Length. Must return HTTP 404 when the blob does not exist. Must support the Range header for partial reads (see also Range Read Support). Must return an ETag header in the response (required for conditional write CAS path; used for resume-on-retry consistency).

**Why:** GetObject is the fundamental read operation. Every blob read in Elasticsearch — snapshot metadata, data blobs, Lucene index files, translog files, cluster state, register values — goes through GetObject. The S3RetryingInputStream wraps GetObject with retry logic: if a download fails mid-stream, it resumes with a new GetObject request using If-Match on the original ETag to ensure the blob hasn't changed between attempts. Without working GetObject, we cannot rely on any kind of read operation.

---

### **🟢 Blob Existence Check (HEAD)**

**Description:** Must efficiently check blob existence without retrieving content (HeadObject).

**Why:** Elasticsearch checks whether specific blobs exist (e.g., to determine if a snapshot already exists) without downloading the full blob content. This is more efficient than a full GET for large blobs.

---

### **🟢 Range Read Support**

**Description:** Must support reading arbitrary byte ranges (Range: bytes=start-end).

**Why:** Elasticsearch reads specific byte ranges from data blobs during restore to extract individual Lucene segments without downloading the entire blob. In stateless Elasticsearch, the shared blob cache service (ObjectStoreCacheBlobReader) uses range reads extensively to fetch specific byte ranges of Lucene files from the object store into the local cache, making range read performance critical for search latency.

---

## **Writes**

### **🟢 PutObject Support**

**Description:** The backend must support the S3 PutObject API, atomically creating or replacing a blob with the provided content. Must accept Content-Length, x-amz-storage-class, and x-amz-acl headers. When conditional writes are enabled, must also support If-Match and If-None-Match headers (see Registers section, Path 1). Must accept blobs up to 5 GiB (the S3 single-upload limit).

**Why:** PutObject is the fundamental write operation for blobs that fit within the single-upload size threshold (controlled by the repository's `buffer_size` setting, default 100 MB). All small blob writes — snapshot metadata, small data blobs, translog files, register values — use PutObject. The writeBlob implementation selects between PutObject (via executeSingleUpload) for blobs at or below the threshold and multipart upload for larger blobs. PutObject is also used directly for CAS register writes in the conditional write path (Path 1), where If-Match/If-None-Match headers are applied for compare-and-exchange semantics.

---

### **🟢 Multipart Upload Lifecycle (Basic)**

**Description:** Must support the basic multipart upload lifecycle: createMultipartUpload, uploadPart, completeMultipartUpload, abortMultipartUpload. Part minimum size 5 MB, max 10,000 parts, max object 5 TB. Must support concurrent part uploads.

**Why:** Blobs larger than the single-upload threshold (configurable, typically around the `buffer_size` setting which defaults to 100 MB) are uploaded using multipart uploads. Snapshot data blobs can be very large. The basic lifecycle (create, upload parts, complete, abort on failure) must work correctly, including cleanup of failed uploads to avoid storage leaks. In stateless Elasticsearch, ObjectStoreService uses concurrent multipart uploads for batched compound commits (controlled by `OBJECT_STORE_CONCURRENT_MULTIPART_UPLOADS`, default true). The supportsConcurrentMultipartUploads() check determines whether parts can be uploaded in parallel rather than sequentially — this is important for write throughput on large commits.

This requirement covers the multipart operations needed for regular large blob uploads. The additional multipart operations needed by the CAS consensus protocol (listMultipartUploads with prefix filtering, upload ID ordering) are covered in the Registers section (Path 2) and are only needed when conditional writes are unavailable.

---

### **🟢 Read-After-Write Consistency**

**Description:** A blob must be readable from any node immediately after the PutObject or CompleteMultipartUpload completes. A register value must be readable immediately after a successful CAS operation completes. This includes cross-node consistency — the reader may be a different client/connection on a different machine than the writer.

**Why:** In a multi-node Elasticsearch cluster, the node that writes a blob is typically different from the nodes that read it. For snapshot repositories, the system reads back metadata blobs immediately after writing them to verify success and serve subsequent operations. For stateless Elasticsearch, read-after-write consistency is critical on the hot path: after writeBlobAtomic uploads a commit, a different node's shard recovery process immediately lists and reads blobs to discover new commits — there is no polling or retry logic for data blobs. The StatelessClusterConsistencyService also depends on getRegister returning the latest lease value written by compareAndSetRegister. The code assumes immediate consistency throughout — any staleness would cause recovery failures or incorrect consistency verdicts.

---

### **🟢 Overwrite Atomicity With Respect to Concurrent Readers**

**Description:** When a blob is overwritten, concurrent readers must see either the complete old version or the complete new version, never a mixture of the two.

**Why:** During snapshot operations, metadata blobs may be overwritten (e.g., updating the repository index file). Concurrent readers performing restore or listing snapshots must see a complete version of the blob, never a mixture of old and new content.

---

### **🟢 Aborted Writes Must Not Leave Visible Data**

**Description:** A write that fails or is aborted mid-stream must not leave a partial or complete blob visible to readers.

**Why:** If a node crashes or a write operation is cancelled mid-stream, the partial upload must not be visible. A partially-written metadata blob could corrupt the repository, and a partially-written data blob would cause checksum failures on restore. In stateless Elasticsearch, ObjectStoreService upload tasks explicitly track success to prevent accepting failures as success — a failed commit upload that leaves a visible blob would corrupt shard state.

---

### **🟢 Atomic Writes (No Partial Reads)**

**Description:** A blob must be either fully visible or not visible at all. Readers must never see a partially-written blob.

**Why:** Snapshot metadata and data blobs must be fully written before they are visible to readers. A partially-written index file or snapshot metadata blob would corrupt the repository state and could cause data loss during restore. In stateless Elasticsearch, ObjectStoreService uses writeBlobAtomic for batched compound commits and writeBlob for translog files — partial visibility of a commit would cause shard recovery to read corrupted Lucene data.

---

## **Copies**

### **🟢 Server-Side Copy**

**Description:** Must copy objects server-side without client-side download/upload.

**Why:** Server-side copy allows efficient duplication of blobs without downloading and re-uploading through the Elasticsearch node. Used in snapshot operations where blobs need to be duplicated between paths. In stateless Elasticsearch, ObjectStoreService uses copyBlob during shard resharding (splitting/merging) to efficiently copy commit files between shard containers without transferring data through the node.

---

### **🟢 Server-Side Multipart Copy (uploadPartCopy)**

**Description:** Large-object copies require copying in parts via uploadPartCopy.

**Why:** Blobs larger than the copy size threshold (controlled by `s3.client.<name>.max_copy_size_before_multipart`, default 5 GiB) cannot be copied in a single copyObject request and must be copied using multipart copy (uploadPartCopy). This is needed for very large data blobs during snapshot operations and during stateless shard resharding when commit files exceed the single-copy size limit.

---

## **List**

### **🟢 Blob Listing Consistency**

**Description:** All written blobs must appear in listings promptly after writes complete.

**Why:** For snapshot repositories, the system lists blobs to discover available snapshots and indices after writes. For stateless Elasticsearch, listing consistency is on the critical path of normal operations: ObjectStoreService lists compound commits during shard recovery, enumerates term directories via children(), and lists translog files via listBlobs(). The GC services (StaleIndicesGCService, StaleTranslogsGCService) also depend on complete listings to safely identify and delete stale data. A missing blob in a listing during recovery means a commit is invisible and the shard operates on stale data; a missing blob in a GC listing means stale data accumulates indefinitely.

---

### **🟢 Prefix-Based Listing with Pagination**

**Description:** listObjectsV2-style listing must support prefix filtering, delimiter-based hierarchical listing, continuation tokens, and truncation handling.

**Why:** The repository uses prefix-based listing extensively: enumerating snapshots (snap-*), finding index files (index-*), listing shard data blobs. Pagination is required because a repository may contain thousands of blobs exceeding the per-request listing limit (1000 for S3). In stateless Elasticsearch, listBlobsByPrefix and children are used pervasively: StatelessPersistedState lists segment files by prefix, ObjectStoreService enumerates term directories and shard contents via children, and the GC services enumerate indices and translog containers the same way. The continuous nature of these listings (during normal operation, not just admin tasks) means pagination correctness is essential.

---

## **Delete**

### **🟠 Container and Bulk Deletion**

**Description:** deleteObjects() must accept bulk requests, and must not fail on missing blobs.

**Why:** After snapshots are deleted, the associated data and metadata blobs must be cleaned up to free storage space. Bulk deletion is used for efficiency (S3 allows deleting up to 1000 objects per request). Silently ignoring missing blobs is required because concurrent operations may delete the same blobs. In stateless Elasticsearch, the GC services perform continuous bulk deletions: StaleTranslogsGCService uses deleteBlobsIgnoringIfNotExists to clean up stale translog files, and StaleIndicesGCService uses delete() to remove entire stale index containers. Failed deletes are retried, and deletions must tolerate concurrent access (e.g., benign races during relocation).

---

## **Registers**

### **🟢 Linearizable Compare-And-Exchange (Register Operations)**

**Description:** The object store must provide linearizable semantics — operations appear to take effect at a single instant. This can be done by supporting one of two options: (1) conditional writes (preferred) or (2) multipart upload consensus protocol (fallback). See below for more information.

**Why:** Register operations are the foundation of distributed coordination across Elasticsearch. For snapshot repositories, multiple nodes may concurrently try to update shared state (e.g., the current generation of a repository). For stateless Elasticsearch, the CAS register is the mechanism for master election and cluster lease management — StatelessElectionStrategy uses compareAndSetRegister to claim terms and update leases, and StatelessClusterConsistencyService continuously verifies local cluster state against the lease stored in the object store via getRegister. Without linearizable CAS, split-brain scenarios or duplicate masters are possible.

**Implementation:** The S3 blob container provides two implementation paths for this requirement. A backend must support at least one:

* **Path 1 — Conditional writes (preferred):** Uses `If-Match: <etag>` and `If-None-Match: *` headers on PutObject and CompleteMultipartUpload to implement CAS directly. This is the simpler, more efficient path. Conditional write support is gated by the `unsafely_incompatible_with_s3_conditional_writes` S3 repository setting — when false (the default), conditional writes are enabled and this path is used. This path requires:
    1. 🟠 **ETag correctness:** GetObject and CompleteMultipartUpload responses must return stable, correct ETags. The CAS mechanism reads a blob's ETag, then writes back with `If-Match: <etag>` to ensure no concurrent modification. If ETags are missing, unstable, or incorrect, CAS breaks. *(Implicitly tested — CAS would fail without correct ETags — but not independently verified.)*
    2. 🟠 **Conditional header semantics:** `If-None-Match: *` must reject writes to existing blobs. `If-Match: <etag>` must reject writes when the current ETag doesn't match. Condition failures must return HTTP 409 or 412. *(Implicitly tested — CAS would fail without correct accept/reject behavior — but not independently verified at the HTTP level.)*
    3. 🟢 **failIfAlreadyExists protection:** When conditional writes are available, regular blob writes (writeBlob, writeBlobAtomic with failIfAlreadyExists=true) use `If-None-Match: *` to prevent concurrent writers from silently overwriting each other's data. This protection is lost when conditional writes are unavailable.
* **Path 2 — Multipart upload consensus protocol (fallback):** When conditional writes are unavailable (i.e., `unsafely_incompatible_with_s3_conditional_writes` is true), the implementation uses a multi-step consensus protocol to achieve CAS semantics. Note: this path does NOT provide failIfAlreadyExists protection on regular blob writes — the setting is named "unsafely" for this reason. This path requires, beyond the basic multipart lifecycle (Writes section, Multipart Upload Lifecycle):
    1. 🟠 **listMultipartUploads with prefix filtering:** Must return a consistent, promptly-updated view of all in-flight uploads for a given key prefix. *(Implicitly tested — consensus would fail without consistent listings — but not independently verified.)*
    2. 🟢 **abortMultipartUpload:** The winner attempts to abort competing uploads; losers back off based on their position. The abort does not need to be reliable — after calling abort, the code polls listMultipartUploads via ensureOtherUploadsComplete to wait until competing uploads have actually disappeared from listings (or times out). The real requirement is that listMultipartUploads accurately reflects which uploads are still in-flight. *(Tested — the S3 test fixture simulates abort failures, and the CAS tests still pass, confirming the polling mechanism handles unreliable aborts.)*
    3. 🟢 **Dangling upload cleanup:** After CAS operations, listMultipartUploads is used to discover and abort any dangling uploads left by failed or interrupted consensus rounds. *(Tested — ensureOtherUploadsComplete polls listMultipartUploads until competing uploads disappear; if dangling uploads persisted, the consensus protocol would time out and fail. The S3 test fixture simulates abort failures, exercising this cleanup path.)*

    The protocol works as follows:

    1. Initiate a multipart upload on the register key and upload the proposed value as a part.
    2. Call listMultipartUploads to discover all in-flight uploads on the same key.
    3. Order competing uploads by upload ID — the lexicographically first ID "wins."
    4. The winner attempts to abort competing uploads; losers back off based on their position.
    5. Read the current register value to verify the expected value still matches.
    6. Complete the multipart upload to atomically write the new value.

---

## **Appendix**

### **Notes on testing**

Note that even though a requirement is marked as "tested", we cannot test every possible path, and it is possible that rare anomalies might slip through.
Also note that "tested" assumes that the _analyze API was run with the recommended non-default parameters as described in the documentation at [https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-snapshot-repository-analyze](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-snapshot-repository-analyze).

### **Performance**

Specific performance requirements are not documented, and performance is not tested by the toolkit. However, it is expected that an object store is reasonably performant.

### **CAS Implementation Paths**

A backend must support at least one of the following to satisfy the Linearizable Compare-And-Exchange requirement:

|  | Path 1: Conditional Writes | Path 2: Multipart Consensus |
| ----- | ----- | ----- |
| **Mechanism** | If-Match/If-None-Match on PutObject and CompleteMultipartUpload | listMultipartUploads + upload ID ordering + abortMultipartUpload |
| **Complexity** | Simple — single conditional PUT | Complex — multi-step consensus protocol |
| **Additional benefit** | failIfAlreadyExists protection on regular blob writes | None — regular blob writes are unprotected |
| **ETag requirement** | Must return stable, correct ETags | ETags not required for CAS (dummy value used) |
| **Controlled by** | `unsafely_incompatible_with_s3_conditional_writes` = false (default) | `unsafely_incompatible_with_s3_conditional_writes` = true |
| **Status** | Preferred | Fallback |

