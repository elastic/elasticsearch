/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Streams;
import org.elasticsearch.gateway.ClusterStateUpdaters;
import org.elasticsearch.gateway.GatewayMetaState;
import org.elasticsearch.gateway.PersistedClusterStateService;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.LongFunction;

class StatelessPersistedState extends GatewayMetaState.LucenePersistedState {
    private static final int DOWNLOAD_BUFFER_SIZE = 8192;
    private final LongFunction<BlobContainer> blobContainerSupplier;
    private final PersistedClusterStateService persistedClusterStateService;
    private final ThrottledTaskRunner throttledTaskRunner;
    private final ExecutorService executorService;
    private final Path clusterStateReadStagingPath;

    StatelessPersistedState(
        PersistedClusterStateService persistedClusterStateService,
        LongFunction<BlobContainer> blobContainerSupplier,
        ExecutorService executorService,
        Path clusterStateReadStagingPath,
        ClusterState lastAcceptedState
    ) throws IOException {
        super(persistedClusterStateService, lastAcceptedState.term(), lastAcceptedState);
        this.blobContainerSupplier = blobContainerSupplier;
        this.persistedClusterStateService = persistedClusterStateService;

        this.throttledTaskRunner = new ThrottledTaskRunner("cluster_state_downloader", 5, executorService);
        this.executorService = executorService;
        this.clusterStateReadStagingPath = clusterStateReadStagingPath;
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
        if (clusterState.nodes().isLocalNodeElectedMaster()) {
            super.writeClusterStateToDisk(clusterState);
        }
    }

    // visible for testing
    void readLatestClusterStateForTerm(long termTarget, ActionListener<Optional<PersistedClusterState>> listener) {
        BlobContainer blobContainer = blobContainerSupplier.apply(termTarget);
        try (Directory termDirectory = new TermBlobDirectory(blobContainer)) {
            SegmentInfos segmentCommitInfos = SegmentInfos.readLatestCommit(termDirectory);

            downloadState(termTarget, segmentCommitInfos, listener);
        } catch (IndexNotFoundException e) {
            listener.onResponse(Optional.empty());
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }

    private void downloadState(long term, SegmentInfos segmentInfos, ActionListener<Optional<PersistedClusterState>> listener)
        throws IOException {
        final var luceneFilesToDownload = segmentInfos.files(true);

        final var downloadDirectory = createDirectoryForDownload(term);
        listener = ActionListener.runBefore(listener, () -> IOUtils.closeWhileHandlingException(downloadDirectory));

        ActionListener<Void> allFilesDownloadedListener = listener.map(unused -> {
            try (DirectoryReader directoryReader = DirectoryReader.open(downloadDirectory)) {
                PersistedClusterStateService.OnDiskState onDiskState = persistedClusterStateService.loadOnDiskState(
                    downloadDirectory.getPath(),
                    directoryReader
                );
                return Optional.of(
                    new PersistedClusterState(onDiskState.currentTerm, onDiskState.lastAcceptedVersion, onDiskState.metadata)
                );
            }
        });

        try (var refCountingListener = new RefCountingListener(new ThreadedActionListener<>(executorService, allFilesDownloadedListener))) {
            final var termBlobContainer = blobContainerSupplier.apply(term);
            for (String file : luceneFilesToDownload) {
                throttledTaskRunner.enqueueTask(refCountingListener.acquire().map(r -> {
                    try (r) {
                        downloadFile(file, downloadDirectory, termBlobContainer);
                    }
                    return null;
                }));
            }
        }
    }

    private AutoCleanDirectory createDirectoryForDownload(long term) throws IOException {
        final var directoryToDownload = clusterStateReadStagingPath.resolve(Long.toString(term));
        return new AutoCleanDirectory(directoryToDownload);
    }

    private void downloadFile(String fileName, Directory directory, BlobContainer blobContainer) throws IOException {
        // TODO: retry
        try (var inputStream = blobContainer.readBlob(fileName); var indexOutput = directory.createOutput(fileName, IOContext.DEFAULT)) {
            final var bufferSize = Math.toIntExact(
                Math.min(DOWNLOAD_BUFFER_SIZE, blobContainer.listBlobsByPrefix(fileName).get(fileName).length())
            );
            final var buffer = new byte[bufferSize];
            int length;
            while ((length = inputStream.read(buffer)) > 0) {
                indexOutput.writeBytes(buffer, length);
            }
        }
    }

    public void getLatestStoredClusterStateMetadataForTerm(
        long targetTerm,
        ActionListener<Optional<PersistedClusterStateMetadata>> listener
    ) {
        if (targetTerm < 0) {
            listener.onFailure(new IllegalArgumentException("Unexpected term " + targetTerm));
            return;
        }

        throttledTaskRunner.enqueueTask(new ActionListener<>() {
            @Override
            public void onResponse(Releasable releasable) {
                BlobContainer blobContainer = blobContainerSupplier.apply(targetTerm);
                try (releasable; Directory dir = new TermBlobDirectory(blobContainer)) {
                    SegmentInfos segmentCommitInfos = SegmentInfos.readLatestCommit(dir);
                    var onDiskStateMetadata = persistedClusterStateService.loadOnDiskStateMetadataFromUserData(
                        segmentCommitInfos.getUserData()
                    );

                    listener.onResponse(
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
                        getLatestStoredClusterStateMetadataForTerm(targetTerm - 1, listener);
                    } else {
                        listener.onResponse(Optional.empty());
                    }
                } catch (IOException e) {
                    listener.onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    @Override
    public void getLatestStoredState(long term, ActionListener<ClusterState> listener) {
        var getLatestTermAndVersionStep = new StepListener<Optional<PersistedClusterStateMetadata>>();
        var readStateStep = new StepListener<Optional<PersistedClusterState>>();

        getLatestTermAndVersionStep.whenComplete(stateMetadata -> {
            if (stateMetadata.isEmpty() || isLatestAcceptedStateStale(stateMetadata.get()) == false) {
                listener.onResponse(null);
                return;
            }

            readLatestClusterStateForTerm(stateMetadata.get().term(), readStateStep);
        }, listener::onFailure);

        readStateStep.whenComplete(persistedClusterStateOpt -> {
            if (persistedClusterStateOpt.isEmpty()) {
                listener.onFailure(new IllegalStateException("Unexpected empty state"));
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
                        .build()
                )
            );

            listener.onResponse(adaptedClusterState);
        }, listener::onFailure);

        getLatestStoredClusterStateMetadataForTerm(term - 1, getLatestTermAndVersionStep);
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
            this.termBlobs = Collections.unmodifiableMap(blobContainer.listBlobsByPrefix(IndexFileNames.SEGMENTS));
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
            try (var inputStream = blobContainer.readBlob(name)) {
                var outputStream = new ByteArrayOutputStream();
                Streams.copy(inputStream, outputStream);
                return new ByteArrayIndexInput(name, outputStream.toByteArray(), 0, outputStream.size());
            }
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
        private final Path directory;

        private AutoCleanDirectory(Path directory) throws IOException {
            super(new NIOFSDirectory(directory));
            this.directory = directory;
        }

        @Override
        public void close() throws IOException {
            super.close();
            IOUtils.rm(directory);
        }

        public Path getPath() {
            return directory;
        }
    }
}
