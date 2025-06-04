/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package fixture.azure;

import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestStatus;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class MockAzureBlobStore {

    private static final Logger logger = LogManager.getLogger(MockAzureBlobStore.class);
    private static final String BLOCK_BLOB_TYPE = "BlockBlob";
    private static final String PAGE_BLOB_TYPE = "PageBlob";
    private static final String APPEND_BLOB_TYPE = "AppendBlob";

    private final LeaseExpiryPredicate leaseExpiryPredicate;
    private final Map<String, AzureBlockBlob> blobs;

    /**
     * Provide the means of triggering lease expiration
     *
     * @param leaseExpiryPredicate A Predicate that takes an active lease ID and returns true when it should be expired, or null to never fail leases
     */
    public MockAzureBlobStore(LeaseExpiryPredicate leaseExpiryPredicate) {
        this.blobs = new ConcurrentHashMap<>();
        this.leaseExpiryPredicate = Objects.requireNonNull(leaseExpiryPredicate);
    }

    public void putBlock(String path, String blockId, BytesReference content, @Nullable String leaseId) {
        blobs.compute(path, (p, existing) -> {
            if (existing != null) {
                existing.putBlock(blockId, content, leaseId);
                return existing;
            } else {
                final AzureBlockBlob azureBlockBlob = new AzureBlockBlob();
                azureBlockBlob.putBlock(blockId, content, leaseId);
                return azureBlockBlob;
            }
        });
    }

    public void putBlockList(String path, List<String> blockIds, @Nullable String leaseId) {
        final AzureBlockBlob blob = getExistingBlob(path);
        blob.putBlockList(blockIds, leaseId);
    }

    public void putBlob(String path, BytesReference contents, String blobType, @Nullable String ifNoneMatch, @Nullable String leaseId) {
        blobs.compute(path, (p, existingValue) -> {
            if (existingValue != null) {
                existingValue.setContents(contents, leaseId, ifNoneMatch);
                return existingValue;
            } else {
                validateBlobType(blobType);
                final AzureBlockBlob newBlob = new AzureBlockBlob();
                newBlob.setContents(contents, leaseId);
                return newBlob;
            }
        });
    }

    private void validateBlobType(String blobType) {
        if (BLOCK_BLOB_TYPE.equals(blobType)) {
            return;
        }
        final String errorMessage;
        if (PAGE_BLOB_TYPE.equals(blobType) || APPEND_BLOB_TYPE.equals(blobType)) {
            errorMessage = "Only BlockBlob is supported. This is a limitation of the MockAzureBlobStore";
        } else {
            errorMessage = "Invalid blobType: " + blobType;
        }
        // Fail the test and respond with an error
        failTestWithAssertionError(errorMessage);
        throw new MockAzureBlobStore.BadRequestException("InvalidHeaderValue", errorMessage);
    }

    public AzureBlockBlob getBlob(String path, @Nullable String leaseId) {
        final AzureBlockBlob blob = getExistingBlob(path);
        // This is the public implementation of "get blob" which will 404 for uncommitted block blobs
        if (blob.isCommitted() == false) {
            throw new BlobNotFoundException();
        }
        blob.checkLeaseForRead(leaseId);
        return blob;
    }

    public void deleteBlob(String path, @Nullable String leaseId) {
        final AzureBlockBlob blob = getExistingBlob(path);
        blob.checkLeaseForWrite(leaseId);
        blobs.remove(path);
    }

    public Map<String, AzureBlockBlob> listBlobs(String prefix, @Nullable String leaseId) {
        return blobs.entrySet().stream().filter(e -> {
            if (prefix == null || e.getKey().startsWith(prefix)) {
                return true;
            }
            return false;
        })
            .filter(e -> e.getValue().isCommitted())
            .peek(e -> e.getValue().checkLeaseForRead(leaseId))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public String acquireLease(String path, int leaseTimeSeconds, @Nullable String proposedLeaseId) {
        final AzureBlockBlob blob = getExistingBlob(path);
        return blob.acquireLease(proposedLeaseId, leaseTimeSeconds);
    }

    public void releaseLease(String path, @Nullable String leaseId) {
        final AzureBlockBlob blob = getExistingBlob(path);
        blob.releaseLease(leaseId);
    }

    public void breakLease(String path, @Nullable Integer leaseBreakPeriod) {
        final AzureBlockBlob blob = getExistingBlob(path);
        blob.breakLease(leaseBreakPeriod);
    }

    public Map<String, BytesReference> blobs() {
        return Maps.transformValues(blobs, AzureBlockBlob::getContents);
    }

    private AzureBlockBlob getExistingBlob(String path) {
        final AzureBlockBlob blob = blobs.get(path);
        if (blob == null) {
            throw new BlobNotFoundException();
        }
        return blob;
    }

    static void failTestWithAssertionError(String message) {
        ExceptionsHelper.maybeDieOnAnotherThread(new AssertionError(message));
    }

    static void failTestWithAssertionError(String message, Throwable throwable) {
        ExceptionsHelper.maybeDieOnAnotherThread(new AssertionError(message, throwable));
    }

    public class AzureBlockBlob {
        private final Object writeLock = new Object();
        private final Lease lease = new Lease();
        private final Map<String, BytesReference> blocks;
        private volatile BytesReference contents;

        private AzureBlockBlob() {
            this.blocks = new ConcurrentHashMap<>();
        }

        public void putBlock(String blockId, BytesReference content, @Nullable String leaseId) {
            synchronized (writeLock) {
                lease.checkLeaseForWrite(leaseId);
                this.blocks.put(blockId, content);
            }
        }

        public void putBlockList(List<String> blockIds, @Nullable String leaseId) throws BadRequestException {
            synchronized (writeLock) {
                lease.checkLeaseForWrite(leaseId);
                final List<String> unresolvedBlocks = blockIds.stream().filter(bId -> blocks.containsKey(bId) == false).toList();
                if (unresolvedBlocks.isEmpty() == false) {
                    logger.warn("Block list contained non-existent block IDs: {}", unresolvedBlocks);
                    throw new BadRequestException("InvalidBlockList", "The specified blocklist is invalid.");
                }
                final BytesReference[] resolvedContents = blockIds.stream().map(blocks::get).toList().toArray(new BytesReference[0]);
                contents = CompositeBytesReference.of(resolvedContents);
            }
        }

        private boolean matches(String ifNoneMatchHeaderValue) {
            if (ifNoneMatchHeaderValue == null) {
                return false;
            }
            // We only support *
            if ("*".equals(ifNoneMatchHeaderValue)) {
                return true;
            }
            // Fail the test, trigger an internal server error
            failTestWithAssertionError("We've only implemented 'If-None-Match: *' in the MockAzureBlobStore");
            throw new AzureBlobStoreError(
                RestStatus.INTERNAL_SERVER_ERROR,
                "UnsupportedHeader",
                "The test fixture only supports * for If-None-Match"
            );
        }

        public synchronized void setContents(BytesReference contents, @Nullable String leaseId) {
            synchronized (writeLock) {
                lease.checkLeaseForWrite(leaseId);
                this.contents = contents;
                this.blocks.clear();
            }
        }

        public void setContents(BytesReference contents, @Nullable String leaseId, @Nullable String ifNoneMatchHeaderValue) {
            synchronized (writeLock) {
                if (matches(ifNoneMatchHeaderValue)) {
                    throw new PreconditionFailedException(
                        "TargetConditionNotMet",
                        "The target condition specified using HTTP conditional header(s) is not met."
                    );
                }
                setContents(contents, leaseId);
            }
        }

        /**
         * Get the committed contents of the blob
         *
         * @return The last committed contents of the blob, or null if the blob is uncommitted
         */
        @Nullable
        public BytesReference getContents() {
            return contents;
        }

        public String type() {
            return BLOCK_BLOB_TYPE;
        }

        public boolean isCommitted() {
            return contents != null;
        }

        @Override
        public String toString() {
            return "MockAzureBlockBlob{" + "blocks=" + blocks + ", contents=" + contents + '}';
        }

        public String acquireLease(@Nullable String proposedLeaseId, int leaseTimeSeconds) {
            synchronized (writeLock) {
                return lease.acquire(proposedLeaseId, leaseTimeSeconds);
            }
        }

        public void releaseLease(String leaseId) {
            synchronized (writeLock) {
                lease.release(leaseId);
            }
        }

        public void breakLease(@Nullable Integer leaseBreakPeriod) {
            synchronized (writeLock) {
                lease.breakLease(leaseBreakPeriod);
            }
        }

        public void checkLeaseForRead(@Nullable String leaseId) {
            lease.checkLeaseForRead(leaseId);
        }

        public void checkLeaseForWrite(@Nullable String leaseId) {
            lease.checkLeaseForWrite(leaseId);
        }
    }

    /**
     * @see <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/lease-blob#outcomes-of-lease-operations-on-blobs-by-lease-state">acquire/release rules</a>
     * @see <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/lease-blob#outcomes-of-use-attempts-on-blobs-by-lease-state">read/write rules</a>
     */
    public class Lease {

        /**
         * Minimal set of states, we don't support breaking/broken
         */
        enum State {
            Available,
            Leased,
            Expired,
            Broken
        }

        private String leaseId;
        private State state = State.Available;
        private int leaseDurationSeconds;

        public synchronized String acquire(@Nullable String proposedLeaseId, int leaseDurationSeconds) {
            maybeExpire(proposedLeaseId);
            switch (state) {
                case Available, Expired, Broken -> {
                    final State prevState = state;
                    state = State.Leased;
                    leaseId = proposedLeaseId != null ? proposedLeaseId : UUID.randomUUID().toString();
                    validateLeaseDuration(leaseDurationSeconds);
                    this.leaseDurationSeconds = leaseDurationSeconds;
                    logger.debug("Granting lease, prior state={}, leaseId={}, expires={}", prevState, leaseId);
                }
                case Leased -> {
                    if (leaseId.equals(proposedLeaseId) == false) {
                        logger.debug("Mismatch on acquire - proposed leaseId: {}, active leaseId: {}", proposedLeaseId, leaseId);
                        throw new ConflictException(
                            "LeaseIdMismatchWithLeaseOperation",
                            "The lease ID specified did not match the lease ID for the blob/container."
                        );
                    }
                    validateLeaseDuration(leaseDurationSeconds);
                }
            }
            return leaseId;
        }

        public synchronized void release(String requestLeaseId) {
            switch (state) {
                case Available -> throw new ConflictException(
                    "LeaseNotPresentWithLeaseOperation",
                    "There is currently no lease on the blob/container."
                );
                case Leased, Expired, Broken -> {
                    if (leaseId.equals(requestLeaseId) == false) {
                        logger.debug("Mismatch on release - submitted leaseId: {}, active leaseId: {}", requestLeaseId, this.leaseId);
                        throw new ConflictException(
                            "LeaseIdMismatchWithLeaseOperation",
                            "The lease ID specified did not match the lease ID for the blob/container."
                        );
                    }
                    state = State.Available;
                    this.leaseId = null;
                }
            }
        }

        public synchronized void breakLease(Integer leaseBreakPeriod) {
            // We haven't implemented the "Breaking" state so we don't support 'breaks' for non-infinite leases unless break-period is 0
            if (leaseDurationSeconds != -1 && (leaseBreakPeriod == null || leaseBreakPeriod != 0)) {
                failTestWithAssertionError(
                    "MockAzureBlobStore only supports breaking non-infinite leases with 'x-ms-lease-break-period: 0'"
                );
            }
            switch (state) {
                case Available -> throw new ConflictException(
                    "LeaseNotPresentWithLeaseOperation",
                    "There is currently no lease on the blob/container."
                );
                case Leased, Expired, Broken -> state = State.Broken;
            }
        }

        public synchronized void checkLeaseForWrite(@Nullable String requestLeaseId) {
            maybeExpire(requestLeaseId);
            switch (state) {
                case Available, Expired, Broken -> {
                    if (requestLeaseId != null) {
                        throw new PreconditionFailedException(
                            "LeaseLost",
                            "A lease ID was specified, but the lease for the blob/container has expired."
                        );
                    }
                }
                case Leased -> {
                    if (requestLeaseId == null) {
                        throw new PreconditionFailedException(
                            "LeaseIdMissing",
                            "There is currently a lease on the blob/container and no lease ID was specified in the request."
                        );
                    }
                    if (leaseId.equals(requestLeaseId) == false) {
                        throw new ConflictException(
                            "LeaseIdMismatchWithBlobOperation",
                            "The lease ID specified did not match the lease ID for the blob."
                        );
                    }
                }
            }
        }

        public synchronized void checkLeaseForRead(@Nullable String requestLeaseId) {
            maybeExpire(requestLeaseId);
            switch (state) {
                case Available, Expired, Broken -> {
                    if (requestLeaseId != null) {
                        throw new PreconditionFailedException(
                            "LeaseLost",
                            "A lease ID was specified, but the lease for the blob/container has expired."
                        );
                    }
                }
                case Leased -> {
                    if (requestLeaseId != null && requestLeaseId.equals(leaseId) == false) {
                        throw new ConflictException(
                            "LeaseIdMismatchWithBlobOperation",
                            "The lease ID specified did not match the lease ID for the blob."
                        );
                    }
                }
            }
        }

        /**
         * If there's an active lease, ask the predicate if we should expire the existing it
         *
         * @param requestLeaseId The lease of the request
         */
        private void maybeExpire(String requestLeaseId) {
            if (state == State.Leased && leaseExpiryPredicate.shouldExpireLease(leaseId, requestLeaseId)) {
                logger.debug("Expiring lease, id={}", leaseId);
                state = State.Expired;
            }
        }

        private void validateLeaseDuration(long leaseTimeSeconds) {
            if (leaseTimeSeconds != -1 && (leaseTimeSeconds < 15 || leaseTimeSeconds > 60)) {
                throw new BadRequestException(
                    "InvalidHeaderValue",
                    AzureHttpHandler.X_MS_LEASE_DURATION + " must be between 16 and 60 seconds (was " + leaseTimeSeconds + ")"
                );
            }
        }
    }

    public static class AzureBlobStoreError extends RuntimeException {
        private final RestStatus restStatus;
        private final String errorCode;

        public AzureBlobStoreError(RestStatus restStatus, String errorCode, String message) {
            super(message);
            this.restStatus = restStatus;
            this.errorCode = errorCode;
        }

        public RestStatus getRestStatus() {
            return restStatus;
        }

        public String getErrorCode() {
            return errorCode;
        }
    }

    public static class BlobNotFoundException extends AzureBlobStoreError {
        public BlobNotFoundException() {
            super(RestStatus.NOT_FOUND, "BlobNotFound", "The specified blob does not exist.");
        }
    }

    public static class BadRequestException extends AzureBlobStoreError {
        public BadRequestException(String errorCode, String message) {
            super(RestStatus.BAD_REQUEST, errorCode, message);
        }
    }

    public static class ConflictException extends AzureBlobStoreError {
        public ConflictException(String errorCode, String message) {
            super(RestStatus.CONFLICT, errorCode, message);
        }
    }

    public static class PreconditionFailedException extends AzureBlobStoreError {
        public PreconditionFailedException(String errorCode, String message) {
            super(RestStatus.PRECONDITION_FAILED, errorCode, message);
        }
    }

    public interface LeaseExpiryPredicate {

        LeaseExpiryPredicate NEVER_EXPIRE = (activeLeaseId, requestLeaseId) -> false;

        /**
         * Should the lease be expired?
         *
         * @param activeLeaseId The current active lease ID
         * @param requestLeaseId The request lease ID (if any)
         * @return true to expire the lease, false otherwise
         */
        boolean shouldExpireLease(String activeLeaseId, @Nullable String requestLeaseId);
    }
}
