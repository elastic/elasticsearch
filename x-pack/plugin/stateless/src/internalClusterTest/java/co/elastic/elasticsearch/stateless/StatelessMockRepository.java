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

package co.elastic.elasticsearch.stateless;

import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.mockstore.BlobStoreWrapper;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Map;

/**
 * A mock repository for stateless testing. It supports manipulating calls to the blob store: logic can be injected, via a
 * {@link StatelessMockRepositoryStrategy}, into methods to run before calling the blob store. A strategy implementation defines the
 * additional logic to run.
 *
 * <p>
 * To use this class in -IT (integration) tests, first make a custom {@link StatelessMockRepositoryStrategy} implementation. Then add the
 * {@link StatelessMockRepositoryPlugin} to the node plugins -- usually via overriding {@link AbstractStatelessIntegTestCase#nodePlugins()}.
 * A particular strategy implementation can then be set and fetched via helper methods
 * {@link AbstractStatelessIntegTestCase#setNodeRepositoryStrategy(String, StatelessMockRepositoryStrategy)} and
 * {@link AbstractStatelessIntegTestCase#getNodeRepositoryStrategy(String)} (cast to the strategy implementation), and helpers like
 * {@link AbstractStatelessIntegTestCase#startMasterOnlyNode(StatelessMockRepositoryStrategy)} can be added.
 *
 * <p>
 * The interfaces of this class, or one of its internal classes, and {@link StatelessMockRepositoryStrategy} should be extended with more
 * method overrides as needed for new testing.
 */
public class StatelessMockRepository extends FsRepository {
    private StatelessMockRepositoryStrategy strategy;

    public StatelessMockRepository(
        RepositoryMetadata metadata,
        Environment environment,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        BigArrays bigArrays,
        RecoverySettings recoverySettings,
        StatelessMockRepositoryStrategy strategy
    ) {
        super(metadata, environment, namedXContentRegistry, clusterService, bigArrays, recoverySettings);
        this.strategy = strategy;
    }

    public void setStrategy(StatelessMockRepositoryStrategy strategy) {
        this.strategy = strategy;
    }

    public StatelessMockRepositoryStrategy getStrategy() {
        return strategy;
    }

    @Override
    protected BlobStore createBlobStore() throws Exception {
        return new StatelessMockBlobStore(super.createBlobStore());
    }

    private class StatelessMockBlobStore extends BlobStoreWrapper {
        StatelessMockBlobStore(BlobStore delegate) {
            super(delegate);
        }

        public BlobContainer blobContainer(BlobPath path) {
            return new StatelessMockBlobContainer(super.blobContainer(path));
        }

        @Override
        public void deleteBlobsIgnoringIfNotExists(OperationPurpose purpose, Iterator<String> blobNames) throws IOException {
            getStrategy().blobStoreDeleteBlobsIgnoringIfNotExists(
                () -> super.deleteBlobsIgnoringIfNotExists(purpose, blobNames),
                purpose,
                blobNames
            );
        }

        /**
         * BlobContainer wrapper that calls a {@link StatelessMockRepositoryStrategy} implementation before falling through to the delegate
         * BlobContainer.
         */
        private class StatelessMockBlobContainer extends FilterBlobContainer {
            StatelessMockBlobContainer(BlobContainer delegate) {
                super(delegate);
            }

            @Override
            protected BlobContainer wrapChild(BlobContainer child) {
                return new StatelessMockBlobContainer(child);
            }

            @Override
            public Map<String, BlobContainer> children(OperationPurpose purpose) throws IOException {
                return getStrategy().blobContainerChildren(() -> super.children(purpose), purpose);
            }

            @Override
            public DeleteResult delete(OperationPurpose purpose) throws IOException {
                return getStrategy().blobContainerDelete(() -> super.delete(purpose), purpose);
            }

            @Override
            public InputStream readBlob(OperationPurpose purpose, String blobName) throws IOException {
                return getStrategy().blobContainerReadBlob(() -> super.readBlob(purpose, blobName), purpose, blobName);
            }

            @Override
            public InputStream readBlob(OperationPurpose purpose, String blobName, long position, long length) throws IOException {
                return getStrategy().blobContainerReadBlob(
                    () -> super.readBlob(purpose, blobName, position, length),
                    purpose,
                    blobName,
                    position,
                    length
                );
            }

            @Override
            public void writeBlob(
                OperationPurpose purpose,
                String blobName,
                InputStream inputStream,
                long blobSize,
                boolean failIfAlreadyExists
            ) throws IOException {
                getStrategy().blobContainerWriteBlob(
                    () -> super.writeBlob(purpose, blobName, inputStream, blobSize, failIfAlreadyExists),
                    purpose,
                    blobName,
                    inputStream,
                    blobSize,
                    failIfAlreadyExists
                );
            }

            @Override
            public void writeBlob(OperationPurpose purpose, String blobName, BytesReference bytes, boolean failIfAlreadyExists)
                throws IOException {
                getStrategy().blobContainerWriteBlob(
                    () -> super.writeBlob(purpose, blobName, bytes, failIfAlreadyExists),
                    purpose,
                    blobName,
                    bytes,
                    failIfAlreadyExists
                );
            }

            @Override
            public void writeMetadataBlob(
                OperationPurpose purpose,
                String blobName,
                boolean failIfAlreadyExists,
                boolean atomic,
                CheckedConsumer<OutputStream, IOException> writer
            ) throws IOException {
                getStrategy().blobContainerWriteMetadataBlob(
                    () -> super.writeMetadataBlob(purpose, blobName, failIfAlreadyExists, atomic, writer),
                    purpose,
                    blobName,
                    failIfAlreadyExists,
                    atomic,
                    writer
                );
            }

            @Override
            public Map<String, BlobMetadata> listBlobs(OperationPurpose purpose) throws IOException {
                return getStrategy().blobContainerListBlobs(() -> super.listBlobs(purpose), purpose);
            }

            @Override
            public Map<String, BlobMetadata> listBlobsByPrefix(OperationPurpose purpose, String blobNamePrefix) throws IOException {
                return getStrategy().blobContainerListBlobsByPrefix(
                    () -> super.listBlobsByPrefix(purpose, blobNamePrefix),
                    purpose,
                    blobNamePrefix
                );
            }
        }
    }
}
