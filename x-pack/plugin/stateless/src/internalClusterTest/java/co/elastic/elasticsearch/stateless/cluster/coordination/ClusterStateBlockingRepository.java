/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.cluster.coordination;

import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.mockstore.BlobStoreWrapper;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.test.ESTestCase.safeAwait;

/**
 * A mock repository implementation that can manipulate cluster state delete() operations run against the blob store.
 */
public class ClusterStateBlockingRepository extends FsRepository {
    private static final org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getLogger(
        ClusterStateBlockingRepository.class
    );

    public static class Plugin extends org.elasticsearch.plugins.Plugin implements RepositoryPlugin {
        @Override
        public Map<String, Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings
        ) {
            return Collections.singletonMap(
                "mock",
                (metadata) -> new ClusterStateBlockingRepository(
                    metadata,
                    env,
                    namedXContentRegistry,
                    clusterService,
                    bigArrays,
                    recoverySettings
                )
            );
        }

    }

    // This flag indicates whether an operation should block.
    private final AtomicBoolean clusterStateTermCleanupShouldBlock = new AtomicBoolean(false);
    // This latch is set to 1 when operations should be blocked, and decremented to 0 when they should resume.
    // Operations will wait on the latch until countDown reaches zero and then resume.
    private volatile CountDownLatch clusterStateTermCleanupShouldBlockedLatch = new CountDownLatch(0);

    // Injects throwing an error contacting the blob store.
    private final AtomicBoolean clusterStateTermCleanupThrowsIOException = new AtomicBoolean(false);

    ClusterStateBlockingRepository(
        RepositoryMetadata metadata,
        Environment environment,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        BigArrays bigArrays,
        RecoverySettings recoverySettings
    ) {
        super(metadata, environment, namedXContentRegistry, clusterService, bigArrays, recoverySettings);
    }

    @Override
    protected BlobStore createBlobStore() throws Exception {
        return new ClusterStateBlockingBlobStore(super.createBlobStore());
    }

    public void blockClusterStateTermCleanup() {
        if (clusterStateTermCleanupShouldBlock.compareAndExchange(false, true)) {
            clusterStateTermCleanupShouldBlockedLatch = new CountDownLatch(1);
        }
    }

    public void unblockClusterStateTermCleanup() {
        if (clusterStateTermCleanupShouldBlock.compareAndExchange(true, false)) {
            clusterStateTermCleanupShouldBlockedLatch.countDown();
        }
    }

    /**
     * Sets a flag to throw one IOException on the next operation.
     */
    public void throwOneIOExceptionOnClusterStateTermCleanup() {
        clusterStateTermCleanupThrowsIOException.set(true);
    }

    /**
     * Blocks if {@link #blockClusterStateTermCleanup} has been called.
     * Any blocked callers will not be released until {@link #unblockClusterStateTermCleanup} is called.
     */
    private void maybeBlockClusterStateTermCleanup() {
        if (clusterStateTermCleanupShouldBlock.get() == false) {
            return;
        }

        safeAwait(clusterStateTermCleanupShouldBlockedLatch);
    }

    /**
     * Throws one Exception if {@link #throwOneIOExceptionOnClusterStateTermCleanup} has been called.
     */
    private void maybeThrowClusterStateTermCleanup() throws IOException {
        if (clusterStateTermCleanupThrowsIOException.compareAndExchange(true, false)) {
            logger.info("Artificial IOException for cluster state cleanup.");
            throw new IOException("Artificial IOException");
        }
    }

    private class ClusterStateBlockingBlobStore extends BlobStoreWrapper {
        ClusterStateBlockingBlobStore(BlobStore delegate) {
            super(delegate);
        }

        public BlobContainer blobContainer(BlobPath path) {
            return new ClusterStateBlockingBlobContainer(super.blobContainer(path));
        }

        private class ClusterStateBlockingBlobContainer extends FilterBlobContainer {
            ClusterStateBlockingBlobContainer(BlobContainer delegate) {
                super(delegate);
            }

            @Override
            protected BlobContainer wrapChild(BlobContainer child) {
                return new ClusterStateBlockingBlobContainer(child);
            }

            /**
             * An override to optionally block cluster state deletion by term.
             */
            @Override
            public DeleteResult delete(OperationPurpose purpose) throws IOException {
                assert purpose.equals(OperationPurpose.CLUSTER_STATE);
                maybeBlockClusterStateTermCleanup();
                maybeThrowClusterStateTermCleanup();
                return super.delete(purpose);
            }

        }
    }
}
