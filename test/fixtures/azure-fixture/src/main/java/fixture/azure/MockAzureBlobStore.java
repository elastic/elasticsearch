/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package fixture.azure;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestStatus;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class MockAzureBlobStore {

    private static final Logger logger = LogManager.getLogger(MockAzureBlobStore.class);
    private static final BytesReference UNCOMMITTED = new BytesArray(new byte[] {});
    private static final String BLOCK_BLOB_TYPE = "BlockBlob";
    private static final String PAGE_BLOB_TYPE = "PageBlob";
    private static final String APPEND_BLOB_TYPE = "AppendBlob";

    private final Map<String, AzureBlob> blobs;

    public MockAzureBlobStore() {
        this.blobs = new ConcurrentHashMap<>();
    }

    public void putBlock(String path, String blockId, BytesReference content, @Nullable String leaseId) {
        blobs.compute(path, (p, existing) -> {
            if (existing != null) {
                if (existing instanceof MockAzureBlockBlob mabb) {
                    mabb.putBlock(blockId, content, leaseId);
                    return existing;
                } else {
                    throw new ConflictException("InvalidBlobType", "The blob type is invalid for this operation.");
                }
            } else {
                final MockAzureBlockBlob mockAzureBlockBlob = new MockAzureBlockBlob();
                mockAzureBlockBlob.putBlock(blockId, content, leaseId);
                return mockAzureBlockBlob;
            }
        });
    }

    public void putBlockList(String path, List<String> blockIds, @Nullable String leaseId) {
        final AzureBlob azureBlob = getExistingBlob(path);
        if (azureBlob instanceof MockAzureBlockBlob mabb) {
            mabb.putBlockList(blockIds, leaseId);
        } else {
            throw new ConflictException("InvalidBlobType", "The blob type is invalid for this operation.");
        }
    }

    public void putBlob(String path, BytesReference contents, String blobType, @Nullable String ifNoneMatch, @Nullable String leaseId) {
        blobs.compute(path, (p, existingValue) -> {
            if (existingValue != null) {
                existingValue.setContents(contents, leaseId, ifNoneMatch);
                return existingValue;
            } else {
                validateBlobType(blobType);
                final AzureBlob newBlob = new MockAzureBlockBlob();
                newBlob.setContents(contents, leaseId);
                return newBlob;
            }
        });
    }

    private void validateBlobType(String blobType) {
        if (BLOCK_BLOB_TYPE.equals(blobType)) {
            return;
        }
        if (PAGE_BLOB_TYPE.equals(blobType) || APPEND_BLOB_TYPE.equals(blobType)) {
            ExceptionsHelper.maybeDieOnAnotherThread(
                new AssertionError("Only BlockBlob is supported. This is a limitation of the MockAzureBlobStore")
            );
        }
        // Anything else is a malformed header
        throw new MockAzureBlobStore.ConflictException("InvalidHeaderValue", "Unable to parse blobType: " + blobType);
    }

    public AzureBlob getBlob(String path, @Nullable String leaseId) {
        final AzureBlob azureBlob = getExistingBlob(path);
        azureBlob.checkLeaseForRead(leaseId);
        return azureBlob;
    }

    public void deleteBlob(String path, @Nullable String leaseId) {
        final AzureBlob azureBlob = getExistingBlob(path);
        azureBlob.checkLeaseForWrite(leaseId);
        blobs.remove(path);
    }

    public Map<String, AzureBlob> listBlobs(String prefix, @Nullable String leaseId) {
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
        final AzureBlob azureBlob = getExistingBlob(path);
        return azureBlob.acquireLease(proposedLeaseId, leaseTimeSeconds);
    }

    public void releaseLease(String path, @Nullable String leaseId) {
        final AzureBlob azureBlob = getExistingBlob(path);
        azureBlob.releaseLease(leaseId);
    }

    public Map<String, BytesReference> blobs() {
        return Maps.transformValues(blobs, AzureBlob::getContents);
    }

    private AzureBlob getExistingBlob(String path) {
        final AzureBlob azureBlob = blobs.get(path);
        if (azureBlob == null) {
            throw new NotFoundException("BlobNotFound", "The specified blob does not exist.");
        }
        return azureBlob;
    }

    public interface AzureBlob {

        void setContents(BytesReference contents, @Nullable String leaseId);

        void setContents(BytesReference contents, @Nullable String leaseId, @Nullable String ifNoneMatchHeaderValue);

        BytesReference getContents();

        String type();

        String acquireLease(String proposedLeaseId, int leaseTimeSeconds);

        void releaseLease(String leaseId);

        void checkLeaseForRead(@Nullable String leaseId);

        void checkLeaseForWrite(@Nullable String leaseId);

        boolean isCommitted();
    }

    private abstract static class AbstractAzureBlob implements AzureBlob {

        protected final Object writeLock = new Object();
        protected final Lease lease = new Lease();

        @Override
        public String acquireLease(@Nullable String proposedLeaseId, int leaseTimeSeconds) {
            synchronized (writeLock) {
                return lease.acquire(proposedLeaseId, leaseTimeSeconds);
            }
        }

        @Override
        public void releaseLease(String leaseId) {
            synchronized (writeLock) {
                lease.release(leaseId);
            }
        }

        @Override
        public void checkLeaseForRead(@Nullable String leaseId) {
            lease.assertCanRead(leaseId);
        }

        @Override
        public void checkLeaseForWrite(@Nullable String leaseId) {
            lease.assertCanWrite(leaseId);
        }
    }

    private static class MockAzureBlockBlob extends AbstractAzureBlob implements AzureBlob {
        private final Map<String, BytesReference> blocks;
        private volatile BytesReference contents = UNCOMMITTED;

        private MockAzureBlockBlob() {
            this.blocks = new ConcurrentHashMap<>();
        }

        public void putBlock(String blockId, BytesReference content, @Nullable String leaseId) {
            synchronized (writeLock) {
                lease.assertCanWrite(leaseId);
                this.blocks.put(blockId, content);
            }
        }

        public void putBlockList(List<String> blockIds, @Nullable String leaseId) throws BadRequestException {
            synchronized (writeLock) {
                lease.assertCanWrite(leaseId);
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
            throw new BadRequestException("UnsupportedHeader", "The test fixture only supports * for if-none-match");
        }

        @Override
        public synchronized void setContents(BytesReference contents, @Nullable String leaseId) {
            synchronized (writeLock) {
                lease.assertCanWrite(leaseId);
                this.contents = contents;
                this.blocks.clear();
            }
        }

        @Override
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

        @Override
        public BytesReference getContents() {
            return contents;
        }

        @Override
        public String type() {
            return BLOCK_BLOB_TYPE;
        }

        @Override
        public boolean isCommitted() {
            return contents != UNCOMMITTED;
        }

        @Override
        public String toString() {
            return "MockAzureBlockBlob{" + "blocks=" + blocks + ", contents=" + contents + '}';
        }
    }

    /**
     * @see <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/lease-blob#outcomes-of-lease-operations-on-blobs-by-lease-state">acquire/release rules</a>
     * @see <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/lease-blob#outcomes-of-use-attempts-on-blobs-by-lease-state">read/write rules</a>
     */
    public static class Lease {

        /**
         * Minimal set of states, we don't support breaking/broken
         */
        enum State {
            Available,
            Leased,
            Expired
        }

        private String leaseId;
        private long expireTimeMillisSinceEpoch = Long.MAX_VALUE;
        private State state = State.Available;

        public synchronized String acquire(@Nullable String proposedLeaseId, int leaseTimeSeconds) {
            expireIfDue();
            switch (state) {
                case Available, Expired -> {
                    final State prevState = state;
                    state = State.Leased;
                    leaseId = proposedLeaseId != null ? proposedLeaseId : UUID.randomUUID().toString();
                    setExpireTimeMillisSinceEpoch(leaseTimeSeconds);
                    logger.debug(
                        "Granting lease, prior state={}, leaseId={}, expires={}",
                        prevState,
                        leaseId,
                        Instant.ofEpochMilli(expireTimeMillisSinceEpoch)
                    );
                }
                case Leased -> {
                    if (leaseId.equals(proposedLeaseId) == false) {
                        logger.debug("Mismatch on acquire - proposed leaseId: {}, active leaseId: {}", proposedLeaseId, leaseId);
                        throw new ConflictException(
                            "LeaseIdMismatchWithLeaseOperation",
                            "The lease ID specified did not match the lease ID for the blob/container."
                        );
                    }
                    setExpireTimeMillisSinceEpoch(leaseTimeSeconds);
                }
            }
            return leaseId;
        }

        public synchronized void release(String requestLeaseId) {
            expireIfDue();
            switch (state) {
                case Available -> throw new ConflictException(
                    "LeaseNotPresentWithLeaseOperation",
                    "There is currently no lease on the blob/container."
                );
                case Leased, Expired -> {
                    if (leaseId.equals(requestLeaseId) == false) {
                        logger.debug("Mismatch on release - submitted leaseId: {}, active leaseId: {}", requestLeaseId, this.leaseId);
                        throw new ConflictException(
                            "LeaseIdMismatchWithLeaseOperation",
                            "The lease ID specified did not match the lease ID for the blob/container."
                        );
                    }
                    state = State.Available;
                    this.leaseId = null;
                    this.expireTimeMillisSinceEpoch = Long.MAX_VALUE;
                }
            }
        }

        public synchronized void assertCanWrite(@Nullable String requestLeaseId) {
            expireIfDue();
            switch (state) {
                case Available, Expired -> {
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

        public synchronized void assertCanRead(@Nullable String requestLeaseId) {
            expireIfDue();
            switch (state) {
                case Available, Expired -> {
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

        private void expireIfDue() {
            if (state == State.Leased && System.currentTimeMillis() >= expireTimeMillisSinceEpoch) {
                logger.debug("Expiring lease, id={}", leaseId);
                state = State.Expired;
            }
        }

        private void setExpireTimeMillisSinceEpoch(long leaseTimeSeconds) {
            if (leaseTimeSeconds == -1) {
                expireTimeMillisSinceEpoch = Long.MAX_VALUE;
            } else {
                if (leaseTimeSeconds < 15 || leaseTimeSeconds > 60) {
                    throw new BadRequestException(
                        "InvalidHeaderValue",
                        AzureHttpHandler.X_MS_LEASE_DURATION + " must be between 16 and 60 seconds (was " + leaseTimeSeconds + ")"
                    );
                }
                expireTimeMillisSinceEpoch = System.currentTimeMillis() + leaseTimeSeconds * 1000L;
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

    public static class NotFoundException extends AzureBlobStoreError {
        public NotFoundException(String errorCode, String message) {
            super(RestStatus.NOT_FOUND, errorCode, message);
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
}
