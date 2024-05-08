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

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.NoLockFactory;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DelegatingActionListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.version.CompatibilityVersions;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.lucene.store.BytesReferenceIndexInput;
import org.elasticsearch.common.lucene.store.IndexOutputOutputStream;
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Streams;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.gateway.ClusterStateUpdaters;
import org.elasticsearch.gateway.GatewayMetaState;
import org.elasticsearch.gateway.PersistedClusterStateService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.LongFunction;

class StatelessPersistedState extends GatewayMetaState.LucenePersistedState {
    private final Logger logger = LogManager.getLogger(StatelessPersistedState.class);
    private final LongFunction<BlobContainer> blobContainerSupplier;
    private final PersistedClusterStateService persistedClusterStateService;
    private final Executor executor;
    private final Path clusterStateReadStagingPath;
    private final StatelessElectionStrategy statelessElectionStrategy;

    // used to ensure only one state is being read at once
    private final ThrottledTaskRunner readStateTaskRunner;

    // used to limit the number of concurrent file downloads
    private final ThrottledTaskRunner readFileTaskRunner;

    StatelessPersistedState(
        PersistedClusterStateService persistedClusterStateService,
        LongFunction<BlobContainer> blobContainerSupplier,
        Executor executor,
        Path clusterStateReadStagingPath,
        ClusterState lastAcceptedState,
        StatelessElectionStrategy statelessElectionStrategy
    ) throws IOException {
        super(persistedClusterStateService, lastAcceptedState.term(), lastAcceptedState);
        this.blobContainerSupplier = blobContainerSupplier;
        this.persistedClusterStateService = persistedClusterStateService;
        this.executor = executor;
        this.clusterStateReadStagingPath = clusterStateReadStagingPath;
        this.statelessElectionStrategy = statelessElectionStrategy;

        this.readStateTaskRunner = new ThrottledTaskRunner("cluster_state_downloader", 1, executor);
        this.readFileTaskRunner = new ThrottledTaskRunner("cluster_state_file_downloader", 5, executor);
    }

    @Override
    protected void maybeWriteInitialState(long currentTerm, ClusterState lastAcceptedState, PersistedClusterStateService.Writer writer) {
        // it's always empty
    }

    @Override
    protected void writeCurrentTermToDisk(long currentTerm) {
        // never write term to disk, the lease takes care
    }

    @Override
    protected void writeClusterStateToDisk(ClusterState clusterState) {
        final var newNodes = clusterState.nodes();
        if (newNodes.isLocalNodeElectedMaster()) {
            super.writeClusterStateToDisk(clusterState);

            if (newNodes.getNodeLeftGeneration() != getLastAcceptedState().nodes().getNodeLeftGeneration()) {
                statelessElectionStrategy.onNodeLeft(clusterState.term(), newNodes.getNodeLeftGeneration());
            }
        }
    }

    // visible for testing
    void readLatestClusterStateForTerm(long termTarget, ActionListener<Optional<PersistedClusterState>> listener) {
        readStateTaskRunner.enqueueTask(
            listener.delegateFailureAndWrap(
                (l, r) -> readLatestClusterStateForTermSingleThreaded(termTarget, ActionListener.releaseAfter(l, r))
            )
        );
    }

    // 'single-threaded' because it's executed using readStateTaskRunner which has a concurrency limit of 1
    private void readLatestClusterStateForTermSingleThreaded(long termTarget, ActionListener<Optional<PersistedClusterState>> listener) {
        final var termBlobContainer = blobContainerSupplier.apply(termTarget);
        try (Directory termDirectory = new TermBlobDirectory(termBlobContainer)) {
            final var files = SegmentInfos.readLatestCommit(termDirectory).files(true);
            final var downloadDirectory = new AutoCleanDirectory(clusterStateReadStagingPath);
            ActionListener.run(ActionListener.runBefore(listener, () -> {
                try {
                    downloadDirectory.close();
                } catch (IOException | RuntimeException e) {
                    logger.warn("Unable to clean temporary cluster state files from [" + clusterStateReadStagingPath + "]", e);
                }
            }), l -> downloadState(termBlobContainer, files, downloadDirectory, l));
        } catch (IndexNotFoundException e) {
            listener.onResponse(Optional.empty());
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }

    private void downloadState(
        BlobContainer termBlobContainer,
        Iterable<String> files,
        Directory downloadDirectory,
        ActionListener<Optional<PersistedClusterState>> listener
    ) {
        SubscribableListener

            .<Void>newForked(l -> {
                try (var refCountingListener = new RefCountingListener(new ThreadedActionListener<>(executor, l))) {
                    for (String file : files) {
                        readFileTaskRunner.enqueueTask(refCountingListener.acquire().map(r -> {
                            // TODO: retry
                            try (r; var inputStream = termBlobContainer.readBlob(OperationPurpose.CLUSTER_STATE, file)) {
                                Streams.copy(
                                    inputStream,
                                    new IndexOutputOutputStream(downloadDirectory.createOutput(file, IOContext.DEFAULT))
                                );
                            }
                            return null;
                        }));
                    }
                }
            })

            .<Optional<PersistedClusterState>>andThen((l, ignored) -> ActionListener.completeWith(l, () -> {
                try (DirectoryReader directoryReader = DirectoryReader.open(downloadDirectory)) {
                    PersistedClusterStateService.OnDiskState onDiskState = persistedClusterStateService.loadOnDiskState(
                        clusterStateReadStagingPath,
                        directoryReader
                    );
                    return Optional.of(
                        new PersistedClusterState(onDiskState.currentTerm, onDiskState.lastAcceptedVersion, onDiskState.metadata)
                    );
                }
            }))

            .addListener(listener);
    }

    private void getLatestStoredClusterStateMetadataForTerm(
        long targetTerm,
        ActionListener<Optional<PersistedClusterStateMetadata>> listener
    ) {
        if (targetTerm < 0) {
            listener.onFailure(new IllegalArgumentException("Unexpected term " + targetTerm));
            return;
        }

        readFileTaskRunner.enqueueTask(new DelegatingActionListener<>(listener) {
            @Override
            public void onResponse(Releasable releasable) {
                BlobContainer blobContainer = blobContainerSupplier.apply(targetTerm);
                try (releasable; Directory dir = new TermBlobDirectory(blobContainer)) {
                    SegmentInfos segmentCommitInfos = SegmentInfos.readLatestCommit(dir);
                    var onDiskStateMetadata = persistedClusterStateService.loadOnDiskStateMetadataFromUserData(
                        segmentCommitInfos.getUserData()
                    );

                    delegate.onResponse(
                        Optional.of(
                            new PersistedClusterStateMetadata(
                                onDiskStateMetadata.currentTerm(),
                                onDiskStateMetadata.lastAcceptedVersion(),
                                onDiskStateMetadata.clusterUUID()
                            )
                        )
                    );
                } catch (IndexNotFoundException e) {
                    // Keep looking in previous terms until we find a valid commit
                    if (targetTerm > 1) {
                        getLatestStoredClusterStateMetadataForTerm(targetTerm - 1, delegate);
                    } else {
                        delegate.onResponse(Optional.empty());
                    }
                } catch (IOException e) {
                    delegate.onFailure(e);
                }
            }
        });
    }

    @Override
    public void getLatestStoredState(long term, ActionListener<ClusterState> listener) {
        var getLatestTermAndVersionStep = new SubscribableListener<Optional<PersistedClusterStateMetadata>>();
        var readStateStep = new SubscribableListener<Optional<PersistedClusterState>>();

        getLatestTermAndVersionStep.addListener(listener.delegateFailureAndWrap((l, stateMetadata) -> {
            if (stateMetadata.isEmpty() || isLatestAcceptedStateStale(stateMetadata.get()) == false) {
                l.onResponse(null);
                return;
            }

            readLatestClusterStateForTerm(stateMetadata.get().term(), readStateStep);
        }));

        readStateStep.addListener(listener.delegateFailureAndWrap((delegate, persistedClusterStateOpt) -> {
            if (persistedClusterStateOpt.isEmpty()) {
                delegate.onFailure(new IllegalStateException("Unexpected empty state"));
                return;
            }
            var latestClusterState = persistedClusterStateOpt.get();
            assert latestClusterState.term() < getCurrentTerm();
            var latestAcceptedState = getLastAcceptedState();

            final var adaptedClusterState = ClusterStateUpdaters.recoverClusterBlocks(
                ClusterStateUpdaters.addStateNotRecoveredBlock(
                    ClusterState.builder(latestAcceptedState.getClusterName())
                        .metadata(
                            Metadata.builder(latestClusterState.metadata())
                                .coordinationMetadata(
                                    new CoordinationMetadata(
                                        latestClusterState.term(),
                                        // Keep the previous configuration so the assertions don't complain about
                                        // a different committed configuration, we'll change it right away
                                        latestAcceptedState.getLastCommittedConfiguration(),
                                        CoordinationMetadata.VotingConfiguration.of(latestAcceptedState.nodes().getLocalNode()),
                                        Set.of()
                                    )
                                )
                        )
                        .version(latestClusterState.version())
                        .nodes(DiscoveryNodes.builder(latestAcceptedState.nodes()).masterNodeId(null))
                        .nodeIdsToCompatibilityVersions(getCompatibilityVersions(latestAcceptedState))
                        .build()
                )
            );

            delegate.onResponse(adaptedClusterState);
        }));

        getLatestStoredClusterStateMetadataForTerm(term - 1, getLatestTermAndVersionStep);
    }

    @SuppressForbidden(reason = "copying ClusterState#compatibilityVersions requires reading them")
    private static Map<String, CompatibilityVersions> getCompatibilityVersions(ClusterState clusterState) {
        return clusterState.compatibilityVersions();
    }

    private boolean isLatestAcceptedStateStale(PersistedClusterStateMetadata latestStoredClusterState) {
        var latestAcceptedState = getLastAcceptedState();
        return latestStoredClusterState.clusterUUID().equals(latestAcceptedState.metadata().clusterUUID()) == false
            || latestStoredClusterState.term() > latestAcceptedState.term()
            || (latestStoredClusterState.term() == latestAcceptedState.term()
                && latestStoredClusterState.version() > latestAcceptedState.version());
    }

    private static class TermBlobDirectory extends BaseDirectory {
        public static final String SEGMENTS_INFO_EXTENSION = ".si";
        private final Map<String, BlobMetadata> termBlobs;
        private final BlobContainer blobContainer;

        private TermBlobDirectory(BlobContainer blobContainer) throws IOException {
            super(NoLockFactory.INSTANCE);
            this.termBlobs = Collections.unmodifiableMap(
                blobContainer.listBlobsByPrefix(OperationPurpose.CLUSTER_STATE, IndexFileNames.SEGMENTS)
            );
            this.blobContainer = blobContainer;
        }

        @Override
        public String[] listAll() {
            return termBlobs.keySet().toArray(new String[0]);
        }

        @Override
        public long fileLength(String name) {
            return termBlobs.get(name).length();
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            assert name.startsWith(IndexFileNames.SEGMENTS) || name.endsWith(SEGMENTS_INFO_EXTENSION);
            // TODO: download to disk?
            return new BytesReferenceIndexInput(
                name,
                org.elasticsearch.common.io.Streams.readFully(blobContainer.readBlob(OperationPurpose.CLUSTER_STATE, name))
            );
        }

        @Override
        public void close() throws IOException {
            // no-op
        }

        @Override
        public void deleteFile(String name) {
            assert false;
            throw new UnsupportedOperationException();
        }

        @Override
        public IndexOutput createOutput(String name, IOContext context) {
            assert false;
            throw new UnsupportedOperationException();
        }

        @Override
        public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) {
            assert false;
            throw new UnsupportedOperationException();
        }

        @Override
        public void sync(Collection<String> names) {
            assert false;
            throw new UnsupportedOperationException();
        }

        @Override
        public void syncMetaData() {
            assert false;
            throw new UnsupportedOperationException();
        }

        @Override
        public void rename(String source, String dest) {
            assert false;
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<String> getPendingDeletions() {
            assert false;
            throw new UnsupportedOperationException();
        }
    }

    private static class AutoCleanDirectory extends FilterDirectory {
        private final Path stagingDirectory;

        private AutoCleanDirectory(Path stagingDirectory) throws IOException {
            super(new NIOFSDirectory(stagingDirectory.resolve(UUIDs.randomBase64UUID())));
            this.stagingDirectory = stagingDirectory;
        }

        @Override
        public void close() throws IOException {
            super.close();
            IOUtils.rm(stagingDirectory);
        }
    }
}
