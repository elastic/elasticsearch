/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gateway;

import org.apache.logging.log4j.Level;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.tests.mockfile.ExtrasFS;
import org.apache.lucene.util.Bits;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.NodeMetadata;
import org.elasticsearch.gateway.PersistedClusterStateService.Writer;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.test.CorruptionUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.io.IOError;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.lucene.index.IndexWriter.WRITE_LOCK_NAME;
import static org.elasticsearch.gateway.PersistedClusterStateService.GLOBAL_TYPE_NAME;
import static org.elasticsearch.gateway.PersistedClusterStateService.INDEX_TYPE_NAME;
import static org.elasticsearch.gateway.PersistedClusterStateService.IS_LAST_PAGE;
import static org.elasticsearch.gateway.PersistedClusterStateService.IS_NOT_LAST_PAGE;
import static org.elasticsearch.gateway.PersistedClusterStateService.LAST_PAGE_FIELD_NAME;
import static org.elasticsearch.gateway.PersistedClusterStateService.MAPPING_TYPE_NAME;
import static org.elasticsearch.gateway.PersistedClusterStateService.METADATA_DIRECTORY_NAME;
import static org.elasticsearch.gateway.PersistedClusterStateService.PAGE_FIELD_NAME;
import static org.elasticsearch.gateway.PersistedClusterStateService.TYPE_FIELD_NAME;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class PersistedClusterStateServiceTests extends ESTestCase {

    private PersistedClusterStateService newPersistedClusterStateService(NodeEnvironment nodeEnvironment) {

        final Settings.Builder settings = Settings.builder();
        if (randomBoolean()) {
            settings.put(PersistedClusterStateService.DOCUMENT_PAGE_SIZE.getKey(), ByteSizeValue.ofBytes(randomLongBetween(1, 1024)));
        }

        return new PersistedClusterStateService(
            nodeEnvironment,
            xContentRegistry(),
            new ClusterSettings(settings.build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            () -> 0L
        );
    }

    public void testPersistsAndReloadsTerm() throws IOException {
        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(createDataPaths())) {
            final PersistedClusterStateService persistedClusterStateService = newPersistedClusterStateService(nodeEnvironment);
            final long newTerm = randomNonNegativeLong();

            assertThat(persistedClusterStateService.loadBestOnDiskState().currentTerm, equalTo(0L));
            try (Writer writer = persistedClusterStateService.createWriter()) {
                writer.writeFullStateAndCommit(newTerm, ClusterState.EMPTY_STATE);
                assertThat(persistedClusterStateService.loadBestOnDiskState(false).currentTerm, equalTo(newTerm));
            }

            assertThat(persistedClusterStateService.loadBestOnDiskState().currentTerm, equalTo(newTerm));
        }
    }

    public void testPersistsAndReloadsGlobalMetadata() throws IOException {
        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(createDataPaths())) {
            final PersistedClusterStateService persistedClusterStateService = newPersistedClusterStateService(nodeEnvironment);
            final String clusterUUID = UUIDs.randomBase64UUID(random());
            final long version = randomLongBetween(1L, Long.MAX_VALUE);

            ClusterState clusterState = loadPersistedClusterState(persistedClusterStateService);
            try (Writer writer = persistedClusterStateService.createWriter()) {
                writer.writeFullStateAndCommit(
                    0L,
                    ClusterState.builder(clusterState)
                        .metadata(
                            Metadata.builder(clusterState.metadata()).clusterUUID(clusterUUID).clusterUUIDCommitted(true).version(version)
                        )
                        .incrementVersion()
                        .build()
                );
                clusterState = loadPersistedClusterState(persistedClusterStateService);
                assertThat(clusterState.metadata().clusterUUID(), equalTo(clusterUUID));
                assertTrue(clusterState.metadata().clusterUUIDCommitted());
                assertThat(clusterState.metadata().version(), equalTo(version));
            }

            try (Writer writer = persistedClusterStateService.createWriter()) {
                writer.writeFullStateAndCommit(
                    0L,
                    ClusterState.builder(clusterState)
                        .metadata(
                            Metadata.builder(clusterState.metadata())
                                .clusterUUID(clusterUUID)
                                .clusterUUIDCommitted(true)
                                .version(version + 1)
                        )
                        .incrementVersion()
                        .build()
                );
            }

            clusterState = loadPersistedClusterState(persistedClusterStateService);
            assertThat(clusterState.metadata().clusterUUID(), equalTo(clusterUUID));
            assertTrue(clusterState.metadata().clusterUUIDCommitted());
            assertThat(clusterState.metadata().version(), equalTo(version + 1));
        }
    }

    private static void writeState(Writer writer, long currentTerm, ClusterState clusterState, ClusterState previousState)
        throws IOException {
        if (randomBoolean() || clusterState.term() != previousState.term() || writer.fullStateWritten == false) {
            writer.writeFullStateAndCommit(currentTerm, clusterState);
        } else {
            writer.writeIncrementalStateAndCommit(currentTerm, previousState, clusterState);
        }
    }

    public void testLoadsFreshestState() throws IOException {
        final Path[] dataPaths = createDataPaths();
        final long freshTerm = randomLongBetween(1L, Long.MAX_VALUE);
        final long staleTerm = randomBoolean() ? freshTerm : randomLongBetween(1L, freshTerm);
        final long freshVersion = randomLongBetween(2L, Long.MAX_VALUE);
        final long staleVersion = staleTerm == freshTerm ? randomLongBetween(1L, freshVersion - 1) : randomLongBetween(1L, Long.MAX_VALUE);

        final HashSet<Path> unimportantPaths = Arrays.stream(dataPaths).collect(Collectors.toCollection(HashSet::new));

        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(dataPaths)) {
            final ClusterState clusterState = loadPersistedClusterState(newPersistedClusterStateService(nodeEnvironment));
            try (Writer writer = newPersistedClusterStateService(nodeEnvironment).createWriter()) {
                writeState(
                    writer,
                    staleTerm,
                    ClusterState.builder(clusterState)
                        .version(staleVersion)
                        .metadata(
                            Metadata.builder(clusterState.metadata())
                                .coordinationMetadata(
                                    CoordinationMetadata.builder(clusterState.coordinationMetadata()).term(staleTerm).build()
                                )
                        )
                        .build(),
                    clusterState
                );
            }
        }

        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(new Path[] { randomFrom(dataPaths) })) {
            unimportantPaths.remove(nodeEnvironment.nodeDataPaths()[0]);
            try (Writer writer = newPersistedClusterStateService(nodeEnvironment).createWriter()) {
                final ClusterState clusterState = loadPersistedClusterState(newPersistedClusterStateService(nodeEnvironment));
                writeState(
                    writer,
                    freshTerm,
                    ClusterState.builder(clusterState)
                        .version(freshVersion)
                        .metadata(
                            Metadata.builder(clusterState.metadata())
                                .coordinationMetadata(
                                    CoordinationMetadata.builder(clusterState.coordinationMetadata()).term(freshTerm).build()
                                )
                        )
                        .build(),
                    clusterState
                );
            }
        }

        if (randomBoolean() && unimportantPaths.isEmpty() == false) {
            IOUtils.rm(randomFrom(unimportantPaths));
        }

        // verify that the freshest state is chosen
        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(dataPaths)) {
            final PersistedClusterStateService.OnDiskState onDiskState = newPersistedClusterStateService(nodeEnvironment)
                .loadBestOnDiskState();
            final ClusterState clusterState = clusterStateFromMetadata(onDiskState.lastAcceptedVersion, onDiskState.metadata);
            assertThat(clusterState.term(), equalTo(freshTerm));
            assertThat(clusterState.version(), equalTo(freshVersion));
        }
    }

    public void testFailsOnMismatchedNodeIds() throws IOException {
        final Path[] dataPaths1 = createDataPaths();
        final Path[] dataPaths2 = createDataPaths();

        final String[] nodeIds = new String[2];

        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(dataPaths1)) {
            nodeIds[0] = nodeEnvironment.nodeId();
            try (Writer writer = newPersistedClusterStateService(nodeEnvironment).createWriter()) {
                final ClusterState clusterState = loadPersistedClusterState(newPersistedClusterStateService(nodeEnvironment));
                writer.writeFullStateAndCommit(
                    0L,
                    ClusterState.builder(clusterState).version(randomLongBetween(1L, Long.MAX_VALUE)).build()
                );
            }
        }

        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(dataPaths2)) {
            nodeIds[1] = nodeEnvironment.nodeId();
            try (Writer writer = newPersistedClusterStateService(nodeEnvironment).createWriter()) {
                final ClusterState clusterState = loadPersistedClusterState(newPersistedClusterStateService(nodeEnvironment));
                writer.writeFullStateAndCommit(
                    0L,
                    ClusterState.builder(clusterState).version(randomLongBetween(1L, Long.MAX_VALUE)).build()
                );
            }
        }

        NodeMetadata.FORMAT.cleanupOldFiles(Long.MAX_VALUE, dataPaths2);

        final Path[] combinedPaths = Stream.concat(Arrays.stream(dataPaths1), Arrays.stream(dataPaths2)).toArray(Path[]::new);

        final String failure = expectThrows(CorruptStateException.class, () -> newNodeEnvironment(combinedPaths)).getMessage();
        assertThat(
            failure,
            allOf(containsString("unexpected node ID in metadata"), containsString(nodeIds[0]), containsString(nodeIds[1]))
        );
        assertTrue(
            "[" + failure + "] should match " + Arrays.toString(dataPaths2),
            Arrays.stream(dataPaths2).anyMatch(p -> failure.contains(p.toString()))
        );

        // verify that loadBestOnDiskState has same check
        final String message = expectThrows(
            CorruptStateException.class,
            () -> new PersistedClusterStateService(
                combinedPaths,
                nodeIds[0],
                xContentRegistry(),
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                () -> 0L
            ).loadBestOnDiskState()
        ).getMessage();
        assertThat(message, allOf(containsString("belongs to a node with ID"), containsString(nodeIds[0]), containsString(nodeIds[1])));
        assertTrue(
            "[" + message + "] should match " + Arrays.toString(dataPaths2),
            Arrays.stream(dataPaths2).anyMatch(p -> message.contains(p.toString()))
        );
    }

    public void testFailsOnMismatchedCommittedClusterUUIDs() throws IOException {
        final Path[] dataPaths1 = createDataPaths();
        final Path[] dataPaths2 = createDataPaths();
        final Path[] combinedPaths = Stream.concat(Arrays.stream(dataPaths1), Arrays.stream(dataPaths2)).toArray(Path[]::new);

        final String clusterUUID1 = UUIDs.randomBase64UUID(random());
        final String clusterUUID2 = UUIDs.randomBase64UUID(random());

        // first establish consistent node IDs and write initial metadata
        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(combinedPaths)) {
            try (Writer writer = newPersistedClusterStateService(nodeEnvironment).createWriter()) {
                final ClusterState clusterState = loadPersistedClusterState(newPersistedClusterStateService(nodeEnvironment));
                assertFalse(clusterState.metadata().clusterUUIDCommitted());
                writer.writeFullStateAndCommit(0L, clusterState);
            }
        }

        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(dataPaths1)) {
            try (Writer writer = newPersistedClusterStateService(nodeEnvironment).createWriter()) {
                final ClusterState clusterState = loadPersistedClusterState(newPersistedClusterStateService(nodeEnvironment));
                assertFalse(clusterState.metadata().clusterUUIDCommitted());
                writer.writeFullStateAndCommit(
                    0L,
                    ClusterState.builder(clusterState)
                        .metadata(Metadata.builder(clusterState.metadata()).clusterUUID(clusterUUID1).clusterUUIDCommitted(true).version(1))
                        .incrementVersion()
                        .build()
                );
            }
        }

        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(dataPaths2)) {
            try (Writer writer = newPersistedClusterStateService(nodeEnvironment).createWriter()) {
                final ClusterState clusterState = loadPersistedClusterState(newPersistedClusterStateService(nodeEnvironment));
                assertFalse(clusterState.metadata().clusterUUIDCommitted());
                writer.writeFullStateAndCommit(
                    0L,
                    ClusterState.builder(clusterState)
                        .metadata(Metadata.builder(clusterState.metadata()).clusterUUID(clusterUUID2).clusterUUIDCommitted(true).version(1))
                        .incrementVersion()
                        .build()
                );
            }
        }

        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(combinedPaths)) {
            final String message = expectThrows(
                CorruptStateException.class,
                () -> newPersistedClusterStateService(nodeEnvironment).loadBestOnDiskState()
            ).getMessage();
            assertThat(
                message,
                allOf(containsString("mismatched cluster UUIDs in metadata"), containsString(clusterUUID1), containsString(clusterUUID2))
            );
            assertTrue(
                "[" + message + "] should match " + Arrays.toString(dataPaths1),
                Arrays.stream(dataPaths1).anyMatch(p -> message.contains(p.toString()))
            );
            assertTrue(
                "[" + message + "] should match " + Arrays.toString(dataPaths2),
                Arrays.stream(dataPaths2).anyMatch(p -> message.contains(p.toString()))
            );
        }
    }

    public void testFailsIfFreshestStateIsInStaleTerm() throws IOException {
        final Path[] dataPaths1 = createDataPaths();
        final Path[] dataPaths2 = createDataPaths();
        final Path[] combinedPaths = Stream.concat(Arrays.stream(dataPaths1), Arrays.stream(dataPaths2)).toArray(Path[]::new);

        final long staleCurrentTerm = randomLongBetween(1L, Long.MAX_VALUE - 1);
        final long freshCurrentTerm = randomLongBetween(staleCurrentTerm + 1, Long.MAX_VALUE);

        final long freshTerm = randomLongBetween(1L, Long.MAX_VALUE);
        final long staleTerm = randomBoolean() ? freshTerm : randomLongBetween(1L, freshTerm);
        final long freshVersion = randomLongBetween(2L, Long.MAX_VALUE);
        final long staleVersion = staleTerm == freshTerm ? randomLongBetween(1L, freshVersion - 1) : randomLongBetween(1L, Long.MAX_VALUE);

        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(combinedPaths)) {
            try (Writer writer = newPersistedClusterStateService(nodeEnvironment).createWriter()) {
                final ClusterState clusterState = loadPersistedClusterState(newPersistedClusterStateService(nodeEnvironment));
                assertFalse(clusterState.metadata().clusterUUIDCommitted());
                writeState(
                    writer,
                    staleCurrentTerm,
                    ClusterState.builder(clusterState)
                        .metadata(
                            Metadata.builder(clusterState.metadata())
                                .version(1)
                                .coordinationMetadata(
                                    CoordinationMetadata.builder(clusterState.coordinationMetadata()).term(staleTerm).build()
                                )
                        )
                        .version(staleVersion)
                        .build(),
                    clusterState
                );
            }
        }

        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(dataPaths1)) {
            try (Writer writer = newPersistedClusterStateService(nodeEnvironment).createWriter()) {
                final ClusterState clusterState = loadPersistedClusterState(newPersistedClusterStateService(nodeEnvironment));
                writeState(writer, freshCurrentTerm, clusterState, clusterState);
            }
        }

        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(dataPaths2)) {
            try (Writer writer = newPersistedClusterStateService(nodeEnvironment).createWriter()) {
                final PersistedClusterStateService.OnDiskState onDiskState = newPersistedClusterStateService(nodeEnvironment)
                    .loadBestOnDiskState(false);
                final ClusterState clusterState = clusterStateFromMetadata(onDiskState.lastAcceptedVersion, onDiskState.metadata);
                writeState(
                    writer,
                    onDiskState.currentTerm,
                    ClusterState.builder(clusterState)
                        .metadata(
                            Metadata.builder(clusterState.metadata())
                                .version(2)
                                .coordinationMetadata(
                                    CoordinationMetadata.builder(clusterState.coordinationMetadata()).term(freshTerm).build()
                                )
                        )
                        .version(freshVersion)
                        .build(),
                    clusterState
                );
            }
        }

        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(combinedPaths)) {
            final String message = expectThrows(
                CorruptStateException.class,
                () -> newPersistedClusterStateService(nodeEnvironment).loadBestOnDiskState()
            ).getMessage();
            assertThat(
                message,
                allOf(
                    containsString("inconsistent terms found"),
                    containsString(Long.toString(staleCurrentTerm)),
                    containsString(Long.toString(freshCurrentTerm))
                )
            );
            assertTrue(
                "[" + message + "] should match " + Arrays.toString(dataPaths1),
                Arrays.stream(dataPaths1).anyMatch(p -> message.contains(p.toString()))
            );
            assertTrue(
                "[" + message + "] should match " + Arrays.toString(dataPaths2),
                Arrays.stream(dataPaths2).anyMatch(p -> message.contains(p.toString()))
            );
        }
    }

    public void testFailsGracefullyOnExceptionDuringFlush() throws IOException {
        final AtomicBoolean throwException = new AtomicBoolean();

        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(createDataPaths())) {
            final PersistedClusterStateService persistedClusterStateService = new PersistedClusterStateService(
                nodeEnvironment,
                xContentRegistry(),
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                () -> 0L
            ) {
                @Override
                protected Directory createDirectory(Path path) throws IOException {
                    return new FilterDirectory(super.createDirectory(path)) {
                        @Override
                        public IndexOutput createOutput(String name, IOContext context) throws IOException {
                            if (throwException.get()) {
                                throw new IOException("simulated");
                            }
                            return super.createOutput(name, context);
                        }
                    };
                }
            };

            try (Writer writer = persistedClusterStateService.createWriter()) {
                final ClusterState clusterState = loadPersistedClusterState(persistedClusterStateService);
                final long newTerm = randomNonNegativeLong();
                final ClusterState newState = ClusterState.builder(clusterState)
                    .metadata(
                        Metadata.builder(clusterState.metadata())
                            .clusterUUID(UUIDs.randomBase64UUID(random()))
                            .clusterUUIDCommitted(true)
                            .version(randomLongBetween(1L, Long.MAX_VALUE))
                    )
                    .incrementVersion()
                    .build();
                throwException.set(true);
                assertThat(
                    expectThrows(IllegalStateException.class, IOException.class, () -> writeState(writer, newTerm, newState, clusterState))
                        .getMessage(),
                    containsString("simulated")
                );
            }
        }
    }

    public void testClosesWriterOnFatalError() throws IOException {
        final AtomicBoolean throwException = new AtomicBoolean();

        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(createDataPaths())) {
            final PersistedClusterStateService persistedClusterStateService = new PersistedClusterStateService(
                nodeEnvironment,
                xContentRegistry(),
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                () -> 0L
            ) {
                @Override
                protected Directory createDirectory(Path path) throws IOException {
                    return new FilterDirectory(super.createDirectory(path)) {
                        @Override
                        public void sync(Collection<String> names) {
                            throw new OutOfMemoryError("simulated");
                        }
                    };
                }
            };

            try (Writer writer = persistedClusterStateService.createWriter()) {
                final ClusterState clusterState = loadPersistedClusterState(persistedClusterStateService);
                final long newTerm = randomNonNegativeLong();
                final ClusterState newState = ClusterState.builder(clusterState)
                    .metadata(
                        Metadata.builder(clusterState.metadata())
                            .clusterUUID(UUIDs.randomBase64UUID(random()))
                            .clusterUUIDCommitted(true)
                            .version(randomLongBetween(1L, Long.MAX_VALUE))
                    )
                    .incrementVersion()
                    .build();
                throwException.set(true);
                assertThat(expectThrows(OutOfMemoryError.class, () -> {
                    if (randomBoolean()) {
                        writeState(writer, newTerm, newState, clusterState);
                    } else {
                        writer.commit(
                            newTerm,
                            newState.version(),
                            newState.metadata().oldestIndexVersion(),
                            newState.metadata().clusterUUID(),
                            newState.metadata().clusterUUIDCommitted()
                        );
                    }
                }).getMessage(), containsString("simulated"));
                assertFalse(writer.isOpen());
            }

            // noinspection EmptyTryBlock - we are just checking that opening the writer again doesn't throw any exceptions
            try (Writer ignored = persistedClusterStateService.createWriter()) {}
        }
    }

    public void testCrashesWithIOErrorOnCommitFailure() throws IOException {
        final AtomicBoolean throwException = new AtomicBoolean();

        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(createDataPaths())) {
            final PersistedClusterStateService persistedClusterStateService = new PersistedClusterStateService(
                nodeEnvironment,
                xContentRegistry(),
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                () -> 0L
            ) {
                @Override
                protected Directory createDirectory(Path path) throws IOException {
                    return new FilterDirectory(super.createDirectory(path)) {
                        @Override
                        public void rename(String source, String dest) throws IOException {
                            if (throwException.get() && dest.startsWith("segments")) {
                                throw new IOException("simulated");
                            }
                        }
                    };
                }
            };

            try (Writer writer = persistedClusterStateService.createWriter()) {
                final ClusterState clusterState = loadPersistedClusterState(persistedClusterStateService);
                final long newTerm = randomNonNegativeLong();
                final ClusterState newState = ClusterState.builder(clusterState)
                    .metadata(
                        Metadata.builder(clusterState.metadata())
                            .clusterUUID(UUIDs.randomBase64UUID(random()))
                            .clusterUUIDCommitted(true)
                            .version(randomLongBetween(1L, Long.MAX_VALUE))
                    )
                    .incrementVersion()
                    .build();
                throwException.set(true);
                assertThat(expectThrows(IOError.class, () -> {
                    if (randomBoolean()) {
                        writeState(writer, newTerm, newState, clusterState);
                    } else {
                        writer.commit(
                            newTerm,
                            newState.version(),
                            newState.metadata().oldestIndexVersion(),
                            newState.metadata().clusterUUID(),
                            newState.metadata().clusterUUIDCommitted()
                        );
                    }
                }).getMessage(), containsString("simulated"));
                assertFalse(writer.isOpen());
            }

            // noinspection EmptyTryBlock - we are just checking that opening the writer again doesn't throw any exceptions
            try (Writer ignored = persistedClusterStateService.createWriter()) {}
        }
    }

    public void testFailsIfGlobalMetadataIsMissing() throws IOException {
        // if someone attempted surgery on the metadata index by hand, e.g. deleting broken segments, then maybe the global metadata
        // isn't there any more

        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(createDataPaths())) {
            try (Writer writer = newPersistedClusterStateService(nodeEnvironment).createWriter()) {
                final ClusterState clusterState = loadPersistedClusterState(newPersistedClusterStateService(nodeEnvironment));
                writeState(
                    writer,
                    0L,
                    ClusterState.builder(clusterState).version(randomLongBetween(1L, Long.MAX_VALUE)).build(),
                    clusterState
                );
            }

            final Path brokenPath = randomFrom(nodeEnvironment.nodeDataPaths());
            try (Directory directory = newFSDirectory(brokenPath.resolve(METADATA_DIRECTORY_NAME))) {
                final IndexWriterConfig indexWriterConfig = new IndexWriterConfig();
                indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
                try (IndexWriter indexWriter = new IndexWriter(directory, indexWriterConfig)) {
                    indexWriter.commit();
                }
            }

            final String message = expectThrows(
                CorruptStateException.class,
                () -> newPersistedClusterStateService(nodeEnvironment).loadBestOnDiskState()
            ).getMessage();
            assertThat(message, allOf(containsString("no global metadata found"), containsString(brokenPath.toString())));
        }
    }

    public void testFailsIfGlobalMetadataIsDuplicated() throws IOException {
        // if someone attempted surgery on the metadata index by hand, e.g. deleting broken segments, then maybe the global metadata
        // is duplicated

        final Path[] dataPaths1 = createDataPaths();
        final Path[] dataPaths2 = createDataPaths();
        final Path[] combinedPaths = Stream.concat(Arrays.stream(dataPaths1), Arrays.stream(dataPaths2)).toArray(Path[]::new);

        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(combinedPaths)) {
            try (Writer writer = newPersistedClusterStateService(nodeEnvironment).createWriter()) {
                final ClusterState clusterState = loadPersistedClusterState(newPersistedClusterStateService(nodeEnvironment));
                writeState(
                    writer,
                    0L,
                    ClusterState.builder(clusterState).version(randomLongBetween(1L, Long.MAX_VALUE)).build(),
                    clusterState
                );
            }

            final Path brokenPath = randomFrom(nodeEnvironment.nodeDataPaths());
            final Path dupPath = randomValueOtherThan(brokenPath, () -> randomFrom(nodeEnvironment.nodeDataPaths()));
            try (
                Directory directory = newFSDirectory(brokenPath.resolve(METADATA_DIRECTORY_NAME));
                Directory dupDirectory = newFSDirectory(dupPath.resolve(METADATA_DIRECTORY_NAME))
            ) {
                try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                    indexWriter.addIndexes(dupDirectory);
                    indexWriter.commit();
                }
            }

            final String message = expectThrows(
                CorruptStateException.class,
                () -> newPersistedClusterStateService(nodeEnvironment).loadBestOnDiskState()
            ).getMessage();
            assertThat(message, allOf(containsString("duplicate global metadata found"), containsString(brokenPath.toString())));
        }
    }

    public void testFailsIfIndexMetadataIsDuplicated() throws IOException {
        // if someone attempted surgery on the metadata index by hand, e.g. deleting broken segments, then maybe some index metadata
        // is duplicated

        final Path[] dataPaths1 = createDataPaths();
        final Path[] dataPaths2 = createDataPaths();
        final Path[] combinedPaths = Stream.concat(Arrays.stream(dataPaths1), Arrays.stream(dataPaths2)).toArray(Path[]::new);

        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(combinedPaths)) {
            final String indexUUID = UUIDs.randomBase64UUID(random());
            final String indexName = randomAlphaOfLength(10);

            try (Writer writer = newPersistedClusterStateService(nodeEnvironment).createWriter()) {
                final ClusterState clusterState = loadPersistedClusterState(newPersistedClusterStateService(nodeEnvironment));
                writeState(
                    writer,
                    0L,
                    ClusterState.builder(clusterState)
                        .metadata(
                            Metadata.builder(clusterState.metadata())
                                .version(1L)
                                .coordinationMetadata(CoordinationMetadata.builder(clusterState.coordinationMetadata()).term(1L).build())
                                .put(
                                    IndexMetadata.builder(indexName)
                                        .version(1L)
                                        .putMapping(randomMappingMetadataOrNull())
                                        .settings(
                                            Settings.builder()
                                                .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                                                .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
                                                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                                .put(IndexMetadata.SETTING_INDEX_UUID, indexUUID)
                                        )
                                )
                        )
                        .incrementVersion()
                        .build(),
                    clusterState
                );
            }

            final Path brokenPath = randomFrom(nodeEnvironment.nodeDataPaths());
            final Path dupPath = randomValueOtherThan(brokenPath, () -> randomFrom(nodeEnvironment.nodeDataPaths()));
            try (
                Directory directory = newFSDirectory(brokenPath.resolve(METADATA_DIRECTORY_NAME));
                Directory dupDirectory = newFSDirectory(dupPath.resolve(METADATA_DIRECTORY_NAME))
            ) {
                try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                    indexWriter.deleteDocuments(new Term(TYPE_FIELD_NAME, GLOBAL_TYPE_NAME)); // do not duplicate global metadata
                    indexWriter.deleteDocuments(new Term(TYPE_FIELD_NAME, MAPPING_TYPE_NAME)); // do not duplicate mappings
                    indexWriter.addIndexes(dupDirectory);
                    indexWriter.commit();
                }
            }

            final String message = expectThrows(
                CorruptStateException.class,
                () -> newPersistedClusterStateService(nodeEnvironment).loadBestOnDiskState()
            ).getMessage();
            assertThat(
                message,
                allOf(
                    containsString("duplicate metadata found"),
                    containsString(brokenPath.toString()),
                    containsString(indexName),
                    containsString(indexUUID)
                )
            );
        }
    }

    public void testPersistsAndReloadsIndexMetadataIffVersionOrTermChanges() throws IOException {
        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(createDataPaths())) {
            final PersistedClusterStateService persistedClusterStateService = newPersistedClusterStateService(nodeEnvironment);
            final long globalVersion = randomLongBetween(1L, Long.MAX_VALUE);
            final String indexUUID = UUIDs.randomBase64UUID(random());
            final long indexMetadataVersion = randomLongBetween(1L, Long.MAX_VALUE);

            final long oldTerm = randomLongBetween(1L, Long.MAX_VALUE - 1);
            final long newTerm = randomLongBetween(oldTerm + 1, Long.MAX_VALUE);

            try (Writer writer = persistedClusterStateService.createWriter()) {
                ClusterState clusterState = loadPersistedClusterState(persistedClusterStateService);
                writeState(
                    writer,
                    0L,
                    ClusterState.builder(clusterState)
                        .metadata(
                            Metadata.builder(clusterState.metadata())
                                .version(globalVersion)
                                .coordinationMetadata(
                                    CoordinationMetadata.builder(clusterState.coordinationMetadata()).term(oldTerm).build()
                                )
                                .put(
                                    IndexMetadata.builder("test")
                                        .version(indexMetadataVersion - 1) // -1 because it's incremented in .put()
                                        .putMapping(randomMappingMetadataOrNull())
                                        .settings(
                                            Settings.builder()
                                                .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                                                .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
                                                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                                .put(IndexMetadata.SETTING_INDEX_UUID, indexUUID)
                                        )
                                )
                        )
                        .incrementVersion()
                        .build(),
                    clusterState
                );

                clusterState = loadPersistedClusterState(persistedClusterStateService);
                IndexMetadata indexMetadata = clusterState.metadata().index("test");
                assertThat(indexMetadata.getIndexUUID(), equalTo(indexUUID));
                assertThat(indexMetadata.getVersion(), equalTo(indexMetadataVersion));
                assertThat(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(indexMetadata.getSettings()), equalTo(0));
                // ensure we do not wastefully persist the same index metadata version by making a bad update with the same version
                writer.writeIncrementalStateAndCommit(
                    0L,
                    clusterState,
                    ClusterState.builder(clusterState)
                        .metadata(
                            Metadata.builder(clusterState.metadata())
                                .put(
                                    IndexMetadata.builder(indexMetadata)
                                        .putMapping(indexMetadata.mapping())
                                        .settings(
                                            Settings.builder()
                                                .put(indexMetadata.getSettings())
                                                .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
                                        )
                                        .build(),
                                    false
                                )
                        )
                        .incrementVersion()
                        .build()
                );

                clusterState = loadPersistedClusterState(persistedClusterStateService);
                indexMetadata = clusterState.metadata().index("test");
                assertThat(indexMetadata.getIndexUUID(), equalTo(indexUUID));
                assertThat(indexMetadata.getVersion(), equalTo(indexMetadataVersion));
                assertThat(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(indexMetadata.getSettings()), equalTo(0));
                // ensure that we do persist the same index metadata version by making an update with a higher version
                writeState(
                    writer,
                    0L,
                    ClusterState.builder(clusterState)
                        .metadata(
                            Metadata.builder(clusterState.metadata())
                                .put(
                                    IndexMetadata.builder(indexMetadata)
                                        .putMapping(randomMappingMetadataOrNull())
                                        .settings(
                                            Settings.builder()
                                                .put(indexMetadata.getSettings())
                                                .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 2)
                                        )
                                        .build(),
                                    true
                                )
                        )
                        .incrementVersion()
                        .build(),
                    clusterState
                );

                clusterState = loadPersistedClusterState(persistedClusterStateService);
                indexMetadata = clusterState.metadata().index("test");
                assertThat(indexMetadata.getVersion(), equalTo(indexMetadataVersion + 1));
                assertThat(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(indexMetadata.getSettings()), equalTo(2));
                // ensure that we also persist the index metadata when the term changes
                writeState(
                    writer,
                    0L,
                    ClusterState.builder(clusterState)
                        .metadata(
                            Metadata.builder(clusterState.metadata())
                                .coordinationMetadata(
                                    CoordinationMetadata.builder(clusterState.coordinationMetadata()).term(newTerm).build()
                                )
                                .put(
                                    IndexMetadata.builder(indexMetadata)
                                        .putMapping(randomMappingMetadataOrNull())
                                        .settings(
                                            Settings.builder()
                                                .put(indexMetadata.getSettings())
                                                .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 3)
                                        )
                                        .build(),
                                    false
                                )
                        )
                        .incrementVersion()
                        .build(),
                    clusterState
                );
            }

            final ClusterState clusterState = loadPersistedClusterState(persistedClusterStateService);
            final IndexMetadata indexMetadata = clusterState.metadata().index("test");
            assertThat(indexMetadata.getIndexUUID(), equalTo(indexUUID));
            assertThat(indexMetadata.getVersion(), equalTo(indexMetadataVersion + 1));
            assertThat(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(indexMetadata.getSettings()), equalTo(3));
        }
    }

    public void testPersistsAndReloadsIndexMetadataForMultipleIndices() throws IOException {
        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(createDataPaths())) {
            final PersistedClusterStateService persistedClusterStateService = newPersistedClusterStateService(nodeEnvironment);

            final long term = randomLongBetween(1L, Long.MAX_VALUE);
            final String addedIndexUuid = UUIDs.randomBase64UUID(random());
            final String updatedIndexUuid = UUIDs.randomBase64UUID(random());
            final String deletedIndexUuid = UUIDs.randomBase64UUID(random());

            try (Writer writer = persistedClusterStateService.createWriter()) {
                final ClusterState clusterState = loadPersistedClusterState(persistedClusterStateService);
                writeState(
                    writer,
                    0L,
                    ClusterState.builder(clusterState)
                        .metadata(
                            Metadata.builder(clusterState.metadata())
                                .version(clusterState.metadata().version() + 1)
                                .coordinationMetadata(CoordinationMetadata.builder(clusterState.coordinationMetadata()).term(term).build())
                                .put(
                                    IndexMetadata.builder("updated")
                                        .putMapping(randomMappingMetadataOrNull())
                                        .version(randomLongBetween(0L, Long.MAX_VALUE - 1) - 1) // -1 because it's incremented in .put()
                                        .settings(
                                            Settings.builder()
                                                .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
                                                .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                                                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                                .put(IndexMetadata.SETTING_INDEX_UUID, updatedIndexUuid)
                                        )
                                )
                                .put(
                                    IndexMetadata.builder("deleted")
                                        .putMapping(randomMappingMetadataOrNull())
                                        .version(randomLongBetween(0L, Long.MAX_VALUE - 1) - 1) // -1 because it's incremented in .put()
                                        .settings(
                                            Settings.builder()
                                                .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
                                                .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                                                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                                .put(IndexMetadata.SETTING_INDEX_UUID, deletedIndexUuid)
                                        )
                                )
                        )
                        .incrementVersion()
                        .build(),
                    clusterState
                );
            }

            try (Writer writer = persistedClusterStateService.createWriter()) {
                final ClusterState clusterState = loadPersistedClusterState(persistedClusterStateService);

                assertThat(clusterState.metadata().indices().size(), equalTo(2));
                assertThat(clusterState.metadata().index("updated").getIndexUUID(), equalTo(updatedIndexUuid));
                assertThat(
                    IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(clusterState.metadata().index("updated").getSettings()),
                    equalTo(1)
                );
                assertThat(clusterState.metadata().index("deleted").getIndexUUID(), equalTo(deletedIndexUuid));

                writeState(
                    writer,
                    0L,
                    ClusterState.builder(clusterState)
                        .metadata(
                            Metadata.builder(clusterState.metadata())
                                .version(clusterState.metadata().version() + 1)
                                .remove("deleted")
                                .put(
                                    IndexMetadata.builder("updated")
                                        .putMapping(randomMappingMetadataOrNull())
                                        .settings(
                                            Settings.builder()
                                                .put(clusterState.metadata().index("updated").getSettings())
                                                .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 2)
                                        )
                                )
                                .put(
                                    IndexMetadata.builder("added")
                                        .version(randomLongBetween(0L, Long.MAX_VALUE - 1) - 1) // -1 because it's incremented in .put()
                                        .putMapping(randomMappingMetadataOrNull())
                                        .settings(
                                            Settings.builder()
                                                .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
                                                .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                                                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                                .put(IndexMetadata.SETTING_INDEX_UUID, addedIndexUuid)
                                        )
                                )
                        )
                        .incrementVersion()
                        .build(),
                    clusterState
                );
            }

            final ClusterState clusterState = loadPersistedClusterState(persistedClusterStateService);

            assertThat(clusterState.metadata().indices().size(), equalTo(2));
            assertThat(clusterState.metadata().index("updated").getIndexUUID(), equalTo(updatedIndexUuid));
            assertThat(
                IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(clusterState.metadata().index("updated").getSettings()),
                equalTo(2)
            );
            assertThat(clusterState.metadata().index("added").getIndexUUID(), equalTo(addedIndexUuid));
            assertThat(clusterState.metadata().index("deleted"), nullValue());
        }
    }

    public void testReloadsMetadataAcrossMultipleSegments() throws IOException {
        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(createDataPaths())) {
            final PersistedClusterStateService persistedClusterStateService = newPersistedClusterStateService(nodeEnvironment);

            final int writes = between(5, 20);
            final List<Index> indices = new ArrayList<>(writes);

            try (Writer writer = persistedClusterStateService.createWriter()) {
                for (int i = 0; i < writes; i++) {
                    final Index index = new Index("test-" + i, UUIDs.randomBase64UUID(random()));
                    indices.add(index);
                    final ClusterState clusterState = loadPersistedClusterState(persistedClusterStateService);
                    writeState(
                        writer,
                        0L,
                        ClusterState.builder(clusterState)
                            .metadata(
                                Metadata.builder(clusterState.metadata())
                                    .version(i + 2)
                                    .put(
                                        IndexMetadata.builder(index.getName())
                                            .putMapping(randomMappingMetadataOrNull())
                                            .settings(
                                                Settings.builder()
                                                    .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                                                    .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
                                                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                                    .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
                                            )
                                    )
                            )
                            .incrementVersion()
                            .build(),
                        clusterState
                    );
                }
            }

            final ClusterState clusterState = loadPersistedClusterState(persistedClusterStateService);
            for (Index index : indices) {
                final IndexMetadata indexMetadata = clusterState.metadata().index(index.getName());
                assertThat(indexMetadata.getIndexUUID(), equalTo(index.getUUID()));
            }
        }
    }

    public void testHandlesShuffledDocuments() throws IOException {
        final Path dataPath = createTempDir();
        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(new Path[] { dataPath })) {
            final PersistedClusterStateService persistedClusterStateService = newPersistedClusterStateService(nodeEnvironment);

            final Metadata.Builder metadata = Metadata.builder();
            for (int i = between(5, 20); i >= 0; i--) {
                metadata.put(
                    IndexMetadata.builder("test-" + i)
                        .putMapping(randomMappingMetadataOrNull())
                        .settings(
                            Settings.builder()
                                .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                                .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
                                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID(random()))
                        )
                );
            }

            final Settings.Builder persistentSettings = Settings.builder();
            persistentSettings.put(
                PersistedClusterStateService.SLOW_WRITE_LOGGING_THRESHOLD.getKey(),
                TimeValue.timeValueMillis(randomLongBetween(0, 10000))
            );
            metadata.persistentSettings(persistentSettings.build());

            final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();
            try (Writer writer = persistedClusterStateService.createWriter()) {
                writer.writeFullStateAndCommit(0L, clusterState);
            }

            final List<Document> documents = new ArrayList<>();
            final Map<String, String> commitUserData;

            try (
                Directory directory = new NIOFSDirectory(dataPath.resolve(METADATA_DIRECTORY_NAME));
                DirectoryReader reader = DirectoryReader.open(directory)
            ) {
                commitUserData = reader.getIndexCommit().getUserData();
                forEachDocument(reader, Set.of(GLOBAL_TYPE_NAME, MAPPING_TYPE_NAME, INDEX_TYPE_NAME), documents::add);
            }

            Randomness.shuffle(documents);

            writeDocumentsAndCommit(dataPath.resolve(METADATA_DIRECTORY_NAME), commitUserData, documents);

            final ClusterState loadedState = loadPersistedClusterState(persistedClusterStateService);
            assertEquals(clusterState.metadata().indices(), loadedState.metadata().indices());
            assertEquals(clusterState.metadata().persistentSettings(), loadedState.metadata().persistentSettings());

            // Now corrupt one of the docs, breaking pagination invariants, and ensure it yields a CorruptStateException

            final int corruptIndex = between(0, documents.size() - 1);
            final Document corruptDocument = documents.get(corruptIndex);
            final int corruptDocPage = corruptDocument.getField(PAGE_FIELD_NAME).numericValue().intValue();
            final boolean corruptDocIsLastPage = corruptDocument.getField(LAST_PAGE_FIELD_NAME).numericValue().intValue() == IS_LAST_PAGE;
            final boolean isOnlyPageForIndex = corruptDocument.getField(TYPE_FIELD_NAME).stringValue().equals(INDEX_TYPE_NAME)
                && corruptDocPage == 0
                && corruptDocIsLastPage;
            final boolean isOnlyPageForMapping = corruptDocument.getField(TYPE_FIELD_NAME).stringValue().equals(MAPPING_TYPE_NAME)
                && corruptDocPage == 0
                && corruptDocIsLastPage;
            if (isOnlyPageForIndex == false // don't remove the only doc for an index, this just loses the index and doesn't corrupt
                && isOnlyPageForMapping == false // similarly, don't remove the only doc for a mapping, this causes an AssertionError
                && rarely()) {
                documents.remove(corruptIndex);
            } else {
                if (randomBoolean()) {
                    corruptDocument.removeFields(PAGE_FIELD_NAME);
                    corruptDocument.add(
                        new StoredField(PAGE_FIELD_NAME, randomValueOtherThan(corruptDocPage, () -> between(0, corruptDocPage + 10)))
                    );
                } else {
                    corruptDocument.removeFields(LAST_PAGE_FIELD_NAME);
                    corruptDocument.add(new StoredField(LAST_PAGE_FIELD_NAME, corruptDocIsLastPage ? IS_NOT_LAST_PAGE : IS_LAST_PAGE));
                }
            }

            writeDocumentsAndCommit(dataPath.resolve(METADATA_DIRECTORY_NAME), commitUserData, documents);

            expectThrows(CorruptStateException.class, () -> loadPersistedClusterState(persistedClusterStateService));
        }
    }

    @TestLogging(value = "org.elasticsearch.gateway:WARN", reason = "to ensure that we log gateway events on WARN level")
    public void testSlowLogging() throws IOException, IllegalAccessException {
        final long slowWriteLoggingThresholdMillis;
        final Settings settings;
        if (randomBoolean()) {
            slowWriteLoggingThresholdMillis = PersistedClusterStateService.SLOW_WRITE_LOGGING_THRESHOLD.get(Settings.EMPTY).millis();
            settings = Settings.EMPTY;
        } else {
            slowWriteLoggingThresholdMillis = randomLongBetween(2, 100000);
            settings = Settings.builder()
                .put(PersistedClusterStateService.SLOW_WRITE_LOGGING_THRESHOLD.getKey(), slowWriteLoggingThresholdMillis + "ms")
                .build();
        }

        final DiscoveryNode localNode = DiscoveryNodeUtils.create("node");
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()))
            .build();

        final long startTimeMillis = randomLongBetween(0L, Long.MAX_VALUE - slowWriteLoggingThresholdMillis * 10);
        final AtomicLong currentTime = new AtomicLong(startTimeMillis);
        final AtomicLong writeDurationMillis = new AtomicLong(slowWriteLoggingThresholdMillis);

        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(createDataPaths())) {
            PersistedClusterStateService persistedClusterStateService = new PersistedClusterStateService(
                nodeEnvironment,
                xContentRegistry(),
                clusterSettings,
                () -> currentTime.getAndAdd(writeDurationMillis.get())
            );

            try (Writer writer = persistedClusterStateService.createWriter()) {
                assertExpectedLogs(
                    1L,
                    null,
                    clusterState,
                    writer,
                    new MockLogAppender.SeenEventExpectation(
                        "should see warning at threshold",
                        PersistedClusterStateService.class.getCanonicalName(),
                        Level.WARN,
                        """
                            writing full cluster state took [*] which is above the warn threshold of [*]; \
                            wrote global metadata, [0] mappings, and metadata for [0] indices"""
                    )
                );

                writeDurationMillis.set(randomLongBetween(slowWriteLoggingThresholdMillis, slowWriteLoggingThresholdMillis * 2));
                assertExpectedLogs(
                    1L,
                    null,
                    clusterState,
                    writer,
                    new MockLogAppender.SeenEventExpectation(
                        "should see warning above threshold",
                        PersistedClusterStateService.class.getCanonicalName(),
                        Level.WARN,
                        """
                            writing full cluster state took [*] which is above the warn threshold of [*]; \
                            wrote global metadata, [0] mappings, and metadata for [0] indices"""
                    )
                );

                writeDurationMillis.set(randomLongBetween(1, slowWriteLoggingThresholdMillis - 1));
                assertExpectedLogs(
                    1L,
                    null,
                    clusterState,
                    writer,
                    new MockLogAppender.UnseenEventExpectation(
                        "should not see warning below threshold",
                        PersistedClusterStateService.class.getCanonicalName(),
                        Level.WARN,
                        "*"
                    )
                );

                clusterSettings.applySettings(
                    Settings.builder()
                        .put(PersistedClusterStateService.SLOW_WRITE_LOGGING_THRESHOLD.getKey(), writeDurationMillis.get() + "ms")
                        .build()
                );
                assertExpectedLogs(
                    1L,
                    null,
                    clusterState,
                    writer,
                    new MockLogAppender.SeenEventExpectation(
                        "should see warning at reduced threshold",
                        PersistedClusterStateService.class.getCanonicalName(),
                        Level.WARN,
                        """
                            writing full cluster state took [*] which is above the warn threshold of [*]; \
                            wrote global metadata, [0] mappings, and metadata for [0] indices"""
                    )
                );

                final ClusterState newClusterState = ClusterState.builder(clusterState)
                    .metadata(
                        Metadata.builder(clusterState.metadata())
                            .version(clusterState.version())
                            .put(
                                IndexMetadata.builder("test")
                                    .putMapping(randomMappingMetadata())
                                    .settings(
                                        Settings.builder()
                                            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                                            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
                                            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                            .put(IndexMetadata.SETTING_INDEX_UUID, "test-uuid")
                                    )
                            )
                    )
                    .incrementVersion()
                    .build();

                assertExpectedLogs(
                    1L,
                    clusterState,
                    newClusterState,
                    writer,
                    new MockLogAppender.SeenEventExpectation(
                        "should see warning at threshold",
                        PersistedClusterStateService.class.getCanonicalName(),
                        Level.WARN,
                        """
                            writing cluster state took [*] which is above the warn threshold of [*]; [skipped writing] global metadata, \
                            wrote [1] new mappings, removed [0] mappings and skipped [0] unchanged mappings, \
                            wrote metadata for [1] new indices and [0] existing indices, removed metadata for [0] indices and \
                            skipped [0] unchanged indices"""
                    )
                );

                // force a full write, so that the next write is an actual incremental write from clusterState->newClusterState
                writeDurationMillis.set(randomLongBetween(0, writeDurationMillis.get() - 1));
                assertExpectedLogs(
                    1L,
                    null,
                    clusterState,
                    writer,
                    new MockLogAppender.UnseenEventExpectation(
                        "should not see warning below threshold",
                        PersistedClusterStateService.class.getCanonicalName(),
                        Level.WARN,
                        "*"
                    )
                );

                assertExpectedLogs(
                    1L,
                    clusterState,
                    newClusterState,
                    writer,
                    new MockLogAppender.UnseenEventExpectation(
                        "should not see warning below threshold",
                        PersistedClusterStateService.class.getCanonicalName(),
                        Level.WARN,
                        "*"
                    )
                );

                assertThat(currentTime.get(), lessThan(startTimeMillis + 16 * slowWriteLoggingThresholdMillis)); // ensure no overflow
            }
        }
    }

    public void testFailsIfCorrupt() throws IOException {
        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(createDataPaths())) {
            final PersistedClusterStateService persistedClusterStateService = newPersistedClusterStateService(nodeEnvironment);

            try (Writer writer = persistedClusterStateService.createWriter()) {
                writer.writeFullStateAndCommit(1, ClusterState.EMPTY_STATE);
            }

            Path pathToCorrupt = randomFrom(nodeEnvironment.nodeDataPaths());
            try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(pathToCorrupt.resolve("_state"))) {
                CorruptionUtils.corruptFile(random(), randomFrom(StreamSupport.stream(directoryStream.spliterator(), false).filter(p -> {
                    final String filename = p.getFileName().toString();
                    return ExtrasFS.isExtra(filename) == false && filename.equals(WRITE_LOCK_NAME) == false;
                }).toList()));
            }

            assertThat(
                expectThrows(CorruptStateException.class, persistedClusterStateService::loadBestOnDiskState).getMessage(),
                allOf(
                    startsWith("the index containing the cluster metadata under the data path ["),
                    endsWith("] has been changed by an external force after it was last written by Elasticsearch and is now unreadable")
                )
            );
        }
    }

    public void testLimitsFileCount() throws IOException {
        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(createDataPaths())) {
            final PersistedClusterStateService persistedClusterStateService = newPersistedClusterStateService(nodeEnvironment);

            try (Writer writer = persistedClusterStateService.createWriter()) {

                ClusterState clusterState = ClusterState.EMPTY_STATE;
                writer.writeFullStateAndCommit(1, ClusterState.EMPTY_STATE);

                final int indexCount = between(2, usually() ? 20 : 1000);

                final int maxSegmentCount = (indexCount / 100) + 100; // only expect to have two tiers, each with max 100 segments
                final int filesPerSegment = 3; // .cfe, .cfs, .si
                final int extraFiles = 2; // segments_*, write.lock
                final int maxFileCount = (maxSegmentCount * filesPerSegment) + extraFiles;

                logger.info("--> adding [{}] indices one-by-one, verifying file count does not exceed [{}]", indexCount, maxFileCount);
                for (int i = 0; i < indexCount; i++) {
                    final ClusterState previousClusterState = clusterState;

                    clusterState = ClusterState.builder(clusterState)
                        .metadata(
                            Metadata.builder(clusterState.metadata())
                                .version(i + 2)
                                .put(
                                    IndexMetadata.builder("index-" + i)
                                        .putMapping(randomMappingMetadataOrNull())
                                        .settings(
                                            Settings.builder()
                                                .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                                                .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
                                                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                                .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID(random()))
                                        )
                                )
                        )
                        .incrementVersion()
                        .build();

                    writer.writeIncrementalStateAndCommit(1, previousClusterState, clusterState);

                    for (Path dataPath : nodeEnvironment.nodeDataPaths()) {
                        try (DirectoryStream<Path> files = Files.newDirectoryStream(dataPath.resolve(METADATA_DIRECTORY_NAME))) {

                            int fileCount = 0;
                            final List<String> fileNames = new ArrayList<>();
                            for (Path filePath : files) {
                                final String fileName = filePath.getFileName().toString();
                                if (ExtrasFS.isExtra(fileName) == false) {
                                    fileNames.add(fileName);
                                    fileCount += 1;
                                }
                            }

                            if (maxFileCount < fileCount) {
                                // don't bother preparing the description unless we are failing
                                fileNames.sort(Comparator.naturalOrder());
                                fail(
                                    "after "
                                        + indexCount
                                        + " indices have "
                                        + fileCount
                                        + " files vs max of "
                                        + maxFileCount
                                        + ": "
                                        + fileNames
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    public void testOverrideLuceneVersion() throws IOException {
        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(createDataPaths())) {
            final PersistedClusterStateService persistedClusterStateService = newPersistedClusterStateService(nodeEnvironment);
            final String clusterUUID = UUIDs.randomBase64UUID(random());
            final long version = randomLongBetween(1L, Long.MAX_VALUE);

            ClusterState clusterState = loadPersistedClusterState(persistedClusterStateService);
            try (Writer writer = persistedClusterStateService.createWriter()) {
                writer.writeFullStateAndCommit(
                    0L,
                    ClusterState.builder(clusterState)
                        .metadata(
                            Metadata.builder(clusterState.metadata()).clusterUUID(clusterUUID).clusterUUIDCommitted(true).version(version)
                        )
                        .incrementVersion()
                        .build()
                );
                clusterState = loadPersistedClusterState(persistedClusterStateService);
                assertThat(clusterState.metadata().clusterUUID(), equalTo(clusterUUID));
                assertTrue(clusterState.metadata().clusterUUIDCommitted());
                assertThat(clusterState.metadata().version(), equalTo(version));

            }
            NodeMetadata prevMetadata = PersistedClusterStateService.nodeMetadata(persistedClusterStateService.getDataPaths());
            assertEquals(Version.CURRENT, prevMetadata.nodeVersion());
            PersistedClusterStateService.overrideVersion(Version.V_8_0_0, persistedClusterStateService.getDataPaths());
            NodeMetadata metadata = PersistedClusterStateService.nodeMetadata(persistedClusterStateService.getDataPaths());
            assertEquals(Version.V_8_0_0, metadata.nodeVersion());
            for (Path p : persistedClusterStateService.getDataPaths()) {
                NodeMetadata individualMetadata = PersistedClusterStateService.nodeMetadata(p);
                assertEquals(Version.V_8_0_0, individualMetadata.nodeVersion());
            }
        }
    }

    public void testDeleteAllPaths() throws IOException {
        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(createDataPaths())) {
            final PersistedClusterStateService persistedClusterStateService = newPersistedClusterStateService(nodeEnvironment);
            final String clusterUUID = UUIDs.randomBase64UUID(random());
            final long version = randomLongBetween(1L, Long.MAX_VALUE);

            ClusterState clusterState = loadPersistedClusterState(persistedClusterStateService);
            try (Writer writer = persistedClusterStateService.createWriter()) {
                writer.writeFullStateAndCommit(
                    0L,
                    ClusterState.builder(clusterState)
                        .metadata(
                            Metadata.builder(clusterState.metadata()).clusterUUID(clusterUUID).clusterUUIDCommitted(true).version(version)
                        )
                        .incrementVersion()
                        .build()
                );
                clusterState = loadPersistedClusterState(persistedClusterStateService);
                assertThat(clusterState.metadata().clusterUUID(), equalTo(clusterUUID));
                assertTrue(clusterState.metadata().clusterUUIDCommitted());
                assertThat(clusterState.metadata().version(), equalTo(version));
            }

            for (Path dataPath : persistedClusterStateService.getDataPaths()) {
                assertTrue(findSegmentInDirectory(dataPath));
            }

            PersistedClusterStateService.deleteAll(persistedClusterStateService.getDataPaths());

            for (Path dataPath : persistedClusterStateService.getDataPaths()) {
                assertFalse(findSegmentInDirectory(dataPath));
            }
        }
    }

    public void testOldestIndexVersionIsCorrectlySerialized() throws IOException {
        final Path[] dataPaths1 = createDataPaths();
        final Path[] dataPaths2 = createDataPaths();
        final Path[] combinedPaths = Stream.concat(Arrays.stream(dataPaths1), Arrays.stream(dataPaths2)).toArray(Path[]::new);

        Version oldVersion = Version.fromId(Version.CURRENT.minimumIndexCompatibilityVersion().id - 1);

        final Version[] indexVersions = new Version[] { oldVersion, Version.CURRENT, Version.fromId(Version.CURRENT.id + 1) };
        int lastIndexNum = randomIntBetween(9, 50);
        Metadata.Builder b = Metadata.builder();
        for (Version indexVersion : indexVersions) {
            String indexUUID = UUIDs.randomBase64UUID(random());
            IndexMetadata im = IndexMetadata.builder(DataStream.getDefaultBackingIndexName("index", lastIndexNum))
                .putMapping(randomMappingMetadataOrNull())
                .settings(settings(indexVersion).put(IndexMetadata.SETTING_INDEX_UUID, indexUUID))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .build();
            b.put(im, false);
            lastIndexNum = randomIntBetween(lastIndexNum + 1, lastIndexNum + 50);
        }

        Metadata metadata = b.build();

        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(combinedPaths)) {
            try (Writer writer = newPersistedClusterStateService(nodeEnvironment).createWriter()) {
                final ClusterState clusterState = loadPersistedClusterState(newPersistedClusterStateService(nodeEnvironment));
                writeState(
                    writer,
                    0L,
                    ClusterState.builder(clusterState).metadata(metadata).version(randomLongBetween(1L, Long.MAX_VALUE)).build(),
                    clusterState
                );
            }

            PersistedClusterStateService.OnDiskState fromDisk = newPersistedClusterStateService(nodeEnvironment).loadBestOnDiskState();
            NodeMetadata nodeMetadata = PersistedClusterStateService.nodeMetadata(nodeEnvironment.nodeDataPaths());

            assertEquals(oldVersion, nodeMetadata.oldestIndexVersion());
            assertEquals(oldVersion, fromDisk.metadata.oldestIndexVersion());
        }
    }

    @TestLogging(value = "org.elasticsearch.gateway.PersistedClusterStateService:DEBUG", reason = "testing contents of DEBUG log")
    public void testDebugLogging() throws IOException, IllegalAccessException {
        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(createDataPaths())) {
            final PersistedClusterStateService persistedClusterStateService = newPersistedClusterStateService(nodeEnvironment);
            try (Writer writer = persistedClusterStateService.createWriter()) {
                writer.writeFullStateAndCommit(randomNonNegativeLong(), ClusterState.EMPTY_STATE);
            }

            MockLogAppender mockAppender = new MockLogAppender();
            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "should see checkindex message",
                    PersistedClusterStateService.class.getCanonicalName(),
                    Level.DEBUG,
                    "checking cluster state integrity"
                )
            );
            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "should see commit message including timestamps",
                    PersistedClusterStateService.class.getCanonicalName(),
                    Level.DEBUG,
                    "loading cluster state from commit [*] in [*creationTime*"
                )
            );
            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "should see user data",
                    PersistedClusterStateService.class.getCanonicalName(),
                    Level.DEBUG,
                    "cluster state commit user data: *" + PersistedClusterStateService.NODE_VERSION_KEY + "*"
                )
            );
            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "should see segment message including timestamp",
                    PersistedClusterStateService.class.getCanonicalName(),
                    Level.DEBUG,
                    "loading cluster state from segment: *timestamp=*"
                )
            );

            try (var ignored = mockAppender.capturing(PersistedClusterStateService.class)) {
                persistedClusterStateService.loadBestOnDiskState();
                mockAppender.assertAllExpectationsMatched();
            }
        }
    }

    public void testFailsIfMappingIsDuplicated() throws IOException {
        final Path dataPath = createTempDir();
        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(new Path[] { dataPath })) {
            final PersistedClusterStateService persistedClusterStateService = newPersistedClusterStateService(nodeEnvironment);

            ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
                .metadata(
                    Metadata.builder()
                        .put(
                            IndexMetadata.builder("test-1")
                                .putMapping(randomMappingMetadata())
                                .settings(
                                    Settings.builder()
                                        .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                                        .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
                                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                        .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID(random()))
                                )
                        )
                )
                .build();

            String hash = clusterState.metadata().getMappingsByHash().keySet().iterator().next();

            try (Writer writer = persistedClusterStateService.createWriter()) {
                writer.writeFullStateAndCommit(0L, clusterState);
            }

            final List<Document> documents = new ArrayList<>();
            final Map<String, String> commitUserData;

            try (
                Directory directory = new NIOFSDirectory(dataPath.resolve(METADATA_DIRECTORY_NAME));
                DirectoryReader reader = DirectoryReader.open(directory)
            ) {
                commitUserData = reader.getIndexCommit().getUserData();
                forEachDocument(reader, Set.of(GLOBAL_TYPE_NAME, MAPPING_TYPE_NAME, INDEX_TYPE_NAME), documents::add);
            }

            // duplicate all documents associated with the mapping in question
            for (Document document : new ArrayList<>(documents)) { // iterating a copy
                IndexableField mappingHash = document.getField("mapping_hash");
                if (mappingHash != null && mappingHash.stringValue().equals(hash)) {
                    documents.add(document);
                }
            }

            writeDocumentsAndCommit(dataPath.resolve(METADATA_DIRECTORY_NAME), commitUserData, documents);

            final String message = expectThrows(CorruptStateException.class, () -> persistedClusterStateService.loadBestOnDiskState())
                .getMessage();
            assertEquals("duplicate metadata found for mapping hash [" + hash + "]", message);
        }
    }

    public void testFailsIfMappingIsMissing() throws IOException {
        final Path dataPath = createTempDir();
        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(new Path[] { dataPath })) {
            final PersistedClusterStateService persistedClusterStateService = newPersistedClusterStateService(nodeEnvironment);

            ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
                .metadata(
                    Metadata.builder()
                        .put(
                            IndexMetadata.builder("test-1")
                                .putMapping(randomMappingMetadata())
                                .settings(
                                    Settings.builder()
                                        .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                                        .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
                                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                        .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID(random()))
                                )
                        )
                )
                .build();

            String hash = clusterState.metadata().getMappingsByHash().keySet().iterator().next();

            try (Writer writer = persistedClusterStateService.createWriter()) {
                writer.writeFullStateAndCommit(0L, clusterState);
            }

            final List<Document> documents = new ArrayList<>();
            final Map<String, String> commitUserData;

            try (
                Directory directory = new NIOFSDirectory(dataPath.resolve(METADATA_DIRECTORY_NAME));
                DirectoryReader reader = DirectoryReader.open(directory)
            ) {
                commitUserData = reader.getIndexCommit().getUserData();
                forEachDocument(reader, Set.of(GLOBAL_TYPE_NAME, MAPPING_TYPE_NAME, INDEX_TYPE_NAME), documents::add);
            }

            // remove all documents associated with the mapping in question
            for (Document document : new ArrayList<>(documents)) { // iterating a copy
                IndexableField mappingHash = document.getField("mapping_hash");
                if (mappingHash != null && mappingHash.stringValue().equals(hash)) {
                    documents.remove(document);
                }
            }

            writeDocumentsAndCommit(dataPath.resolve(METADATA_DIRECTORY_NAME), commitUserData, documents);

            final String message = expectThrows(CorruptStateException.class, () -> persistedClusterStateService.loadBestOnDiskState())
                .getCause()
                .getMessage();
            assertEquals("java.lang.IllegalArgumentException: mapping with hash [" + hash + "] not found", message);
        }
    }

    public void testDeduplicatedMappings() throws IOException {
        final Path dataPath = createTempDir();
        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(new Path[] { dataPath })) {
            final PersistedClusterStateService persistedClusterStateService = newPersistedClusterStateService(nodeEnvironment);
            try (Writer writer = persistedClusterStateService.createWriter()) {

                Set<String> hashes;
                Metadata.Builder metadata;
                ClusterState clusterState;
                ClusterState previousState;

                // generate two mappings
                MappingMetadata mapping1 = randomMappingMetadata();
                MappingMetadata mapping2 = randomValueOtherThan(mapping1, () -> randomMappingMetadata());

                // build and write a cluster state with metadata that has all indices using a single mapping
                metadata = Metadata.builder();
                for (int i = between(5, 20); i >= 0; i--) {
                    metadata.put(
                        IndexMetadata.builder("test-" + i)
                            .putMapping(mapping1)
                            .settings(
                                Settings.builder()
                                    .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                                    .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
                                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                    .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID(random()))
                            )
                    );
                }
                clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();
                assertThat(clusterState.metadata().getMappingsByHash().size(), equalTo(1));
                writer.writeFullStateAndCommit(0L, clusterState);

                // verify that the on-disk state reflects 1 mapping
                hashes = loadPersistedMappingHashes(dataPath.resolve(METADATA_DIRECTORY_NAME));
                assertThat(hashes.size(), equalTo(1));
                assertThat(clusterState.metadata().getMappingsByHash().keySet(), equalTo(hashes));

                previousState = clusterState;
                metadata = Metadata.builder(previousState.metadata());

                // add a second mapping -- either by adding a new index or changing an existing one
                if (randomBoolean()) {
                    // add another index with a different mapping
                    metadata.put(
                        IndexMetadata.builder("test-" + 99)
                            .putMapping(mapping2)
                            .settings(
                                Settings.builder()
                                    .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                                    .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
                                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                    .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID(random()))
                            )
                    );
                } else {
                    // change an existing index to a different mapping
                    String index = randomFrom(previousState.metadata().getIndices().keySet());
                    metadata.put(IndexMetadata.builder(metadata.get(index)).putMapping(mapping2));
                }
                clusterState = ClusterState.builder(previousState).metadata(metadata).build();
                assertThat(clusterState.metadata().getMappingsByHash().size(), equalTo(2));
                writer.writeIncrementalStateAndCommit(0L, previousState, clusterState);

                // verify that the on-disk state reflects 2 mappings
                hashes = loadPersistedMappingHashes(dataPath.resolve(METADATA_DIRECTORY_NAME));
                assertThat(hashes.size(), equalTo(2));
                assertThat(clusterState.metadata().getMappingsByHash().keySet(), equalTo(hashes));

                previousState = clusterState;
                metadata = Metadata.builder(previousState.metadata());

                // update all indices to use the second mapping
                for (String index : previousState.metadata().getIndices().keySet()) {
                    metadata.put(IndexMetadata.builder(metadata.get(index)).putMapping(mapping2));
                }
                clusterState = ClusterState.builder(previousState).metadata(metadata).build();
                assertThat(clusterState.metadata().getMappingsByHash().size(), equalTo(1));
                writer.writeIncrementalStateAndCommit(0L, previousState, clusterState);

                // verify that the on-disk reflects 1 mapping
                hashes = loadPersistedMappingHashes(dataPath.resolve(METADATA_DIRECTORY_NAME));
                assertThat(hashes.size(), equalTo(1));
                assertThat(clusterState.metadata().getMappingsByHash().keySet(), equalTo(hashes));
            }
        }
    }

    public void testClusterUUIDIsStoredInCommitUserData() throws Exception {
        final Path dataPath = createTempDir();
        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(new Path[] { dataPath })) {
            final PersistedClusterStateService persistedClusterStateService = newPersistedClusterStateService(nodeEnvironment);
            String clusterUUID = UUIDs.randomBase64UUID();
            boolean clusterUUIDCommitted = randomBoolean();
            try (Writer writer = persistedClusterStateService.createWriter()) {
                ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
                    .metadata(Metadata.builder().clusterUUID(clusterUUID).clusterUUIDCommitted(clusterUUIDCommitted))
                    .build();
                writer.writeFullStateAndCommit(0, clusterState);
            }

            var onDiskState = persistedClusterStateService.loadBestOnDiskState();
            assertThat(onDiskState.clusterUUID, is(equalTo(clusterUUID)));
            assertThat(onDiskState.clusterUUIDCommitted, is(clusterUUIDCommitted));
        }
    }

    /**
     * Utility method for applying a consumer to each document (of the given types) associated with a DirectoryReader.
     */
    private static void forEachDocument(DirectoryReader reader, Set<String> types, Consumer<Document> consumer) throws IOException {
        final IndexSearcher indexSearcher = new IndexSearcher(reader);
        indexSearcher.setQueryCache(null);
        for (String typeName : types) {
            final Query query = new TermQuery(new Term(TYPE_FIELD_NAME, typeName));
            final Weight weight = indexSearcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 0.0f);
            for (LeafReaderContext leafReaderContext : indexSearcher.getIndexReader().leaves()) {
                final Scorer scorer = weight.scorer(leafReaderContext);
                if (scorer != null) {
                    final Bits liveDocs = leafReaderContext.reader().getLiveDocs();
                    final IntPredicate isLiveDoc = liveDocs == null ? i -> true : liveDocs::get;
                    final DocIdSetIterator docIdSetIterator = scorer.iterator();
                    while (docIdSetIterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                        if (isLiveDoc.test(docIdSetIterator.docID())) {
                            final Document document = leafReaderContext.reader().document(docIdSetIterator.docID());
                            document.add(new StringField(TYPE_FIELD_NAME, typeName, Field.Store.NO));
                            consumer.accept(document);
                        }
                    }
                }
            }
        }
    }

    /**
     * Utility method writing documents back to a directory.
     */
    private static void writeDocumentsAndCommit(Path metadataDirectory, Map<String, String> commitUserData, List<Document> documents)
        throws IOException {
        try (Directory directory = new NIOFSDirectory(metadataDirectory)) {
            final IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new KeywordAnalyzer());
            indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            try (IndexWriter indexWriter = new IndexWriter(directory, indexWriterConfig)) {
                for (Document document : documents) {
                    indexWriter.addDocument(document);
                }
                indexWriter.setLiveCommitData(commitUserData.entrySet());
                indexWriter.commit();
            }
        }
    }

    /**
     * Search the underlying persisted state indices for non-deleted mapping_hash documents that represent the
     * first page of data, collecting and returning the distinct mapping_hashes themselves.
     */
    private static Set<String> loadPersistedMappingHashes(Path metadataDirectory) throws IOException {
        Set<String> hashes = new HashSet<>();
        try (Directory directory = new NIOFSDirectory(metadataDirectory); DirectoryReader reader = DirectoryReader.open(directory)) {
            forEachDocument(reader, Set.of(MAPPING_TYPE_NAME), document -> {
                int page = document.getField("page").numericValue().intValue();
                if (page == 0) {
                    String hash = document.getField("mapping_hash").stringValue();
                    assertTrue(hashes.add(hash));
                }
            });
        }
        return hashes;
    }

    private boolean findSegmentInDirectory(Path dataPath) throws IOException {
        Directory d = new NIOFSDirectory(dataPath.resolve(METADATA_DIRECTORY_NAME));

        for (final String file : d.listAll()) {
            if (file.startsWith(IndexFileNames.SEGMENTS)) {
                return true;
            }
        }

        return false;
    }

    private void assertExpectedLogs(
        long currentTerm,
        ClusterState previousState,
        ClusterState clusterState,
        PersistedClusterStateService.Writer writer,
        MockLogAppender.LoggingExpectation expectation
    ) throws IOException {
        MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.addExpectation(expectation);
        try (var ignored = mockAppender.capturing(PersistedClusterStateService.class)) {
            if (previousState == null) {
                writer.writeFullStateAndCommit(currentTerm, clusterState);
            } else {
                writer.writeIncrementalStateAndCommit(currentTerm, previousState, clusterState);
            }
            mockAppender.assertAllExpectationsMatched();
        }
    }

    @Override
    public Settings buildEnvSettings(Settings settings) {
        assertTrue(settings.hasValue(Environment.PATH_DATA_SETTING.getKey()));
        return Settings.builder().put(settings).put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath()).build();
    }

    public static Path[] createDataPaths() {
        final Path[] dataPaths = new Path[randomIntBetween(1, 4)];
        for (int i = 0; i < dataPaths.length; i++) {
            dataPaths[i] = createTempDir();
        }
        return dataPaths;
    }

    private NodeEnvironment newNodeEnvironment(Path[] dataPaths) throws IOException {
        return newNodeEnvironment(
            Settings.builder()
                .putList(Environment.PATH_DATA_SETTING.getKey(), Arrays.stream(dataPaths).map(Path::toString).toList())
                .build()
        );
    }

    private static MappingMetadata randomMappingMetadata() {
        int i = randomIntBetween(1, 4);
        return new MappingMetadata(MapperService.SINGLE_MAPPING_NAME, Map.of("_doc", Map.of("properties", Map.of("field" + i, "text"))));
    }

    private static MappingMetadata randomMappingMetadataOrNull() {
        int i = randomIntBetween(0, 4);
        if (i == 0) {
            return null;
        } else {
            return randomMappingMetadata();
        }
    }

    private static ClusterState loadPersistedClusterState(PersistedClusterStateService persistedClusterStateService) throws IOException {
        final PersistedClusterStateService.OnDiskState onDiskState = persistedClusterStateService.loadBestOnDiskState(false);
        return clusterStateFromMetadata(onDiskState.lastAcceptedVersion, onDiskState.metadata);
    }

    private static ClusterState clusterStateFromMetadata(long version, Metadata metadata) {
        return ClusterState.builder(ClusterName.DEFAULT).version(version).metadata(metadata).build();
    }

}
