/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gateway;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.mockfile.ExtrasFS;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.PersistedClusterStateService.Writer;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.lucene.index.IndexWriter.WRITE_LOCK_NAME;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class PersistedClusterStateServiceTests extends ESTestCase {

    private PersistedClusterStateService newPersistedClusterStateService(NodeEnvironment nodeEnvironment) {
        return new PersistedClusterStateService(nodeEnvironment, xContentRegistry(), getBigArrays(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            () -> 0L);
    }

    public void testPersistsAndReloadsTerm() throws IOException {
        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(createTempDir())) {
            final PersistedClusterStateService persistedClusterStateService = newPersistedClusterStateService(nodeEnvironment);
            final long newTerm = randomNonNegativeLong();

            assertThat(persistedClusterStateService.loadOnDiskState().currentTerm, equalTo(0L));
            try (Writer writer = persistedClusterStateService.createWriter()) {
                writer.writeFullStateAndCommit(newTerm, ClusterState.EMPTY_STATE);
                assertThat(persistedClusterStateService.loadOnDiskState(false).currentTerm, equalTo(newTerm));
            }

            assertThat(persistedClusterStateService.loadOnDiskState().currentTerm, equalTo(newTerm));
        }
    }

    public void testPersistsAndReloadsGlobalMetadata() throws IOException {
        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(createTempDir())) {
            final PersistedClusterStateService persistedClusterStateService = newPersistedClusterStateService(nodeEnvironment);
            final String clusterUUID = UUIDs.randomBase64UUID(random());
            final long version = randomLongBetween(1L, Long.MAX_VALUE);

            ClusterState clusterState = loadPersistedClusterState(persistedClusterStateService);
            try (Writer writer = persistedClusterStateService.createWriter()) {
                writer.writeFullStateAndCommit(0L, ClusterState.builder(clusterState)
                    .metadata(Metadata.builder(clusterState.metadata())
                        .clusterUUID(clusterUUID)
                        .clusterUUIDCommitted(true)
                        .version(version))
                    .incrementVersion().build());
                clusterState = loadPersistedClusterState(persistedClusterStateService);
                assertThat(clusterState.metadata().clusterUUID(), equalTo(clusterUUID));
                assertTrue(clusterState.metadata().clusterUUIDCommitted());
                assertThat(clusterState.metadata().version(), equalTo(version));
            }

            try (Writer writer = persistedClusterStateService.createWriter()) {
                writer.writeFullStateAndCommit(0L, ClusterState.builder(clusterState)
                    .metadata(Metadata.builder(clusterState.metadata())
                        .clusterUUID(clusterUUID)
                        .clusterUUIDCommitted(true)
                        .version(version + 1))
                    .incrementVersion().build());
            }

            clusterState = loadPersistedClusterState(persistedClusterStateService);
            assertThat(clusterState.metadata().clusterUUID(), equalTo(clusterUUID));
            assertTrue(clusterState.metadata().clusterUUIDCommitted());
            assertThat(clusterState.metadata().version(), equalTo(version + 1));
        }
    }

    private static void writeState(Writer writer, long currentTerm, ClusterState clusterState,
                                   ClusterState previousState) throws IOException {
        if (randomBoolean() || clusterState.term() != previousState.term() || writer.fullStateWritten == false) {
            writer.writeFullStateAndCommit(currentTerm, clusterState);
        } else {
            writer.writeIncrementalStateAndCommit(currentTerm, previousState, clusterState);
        }
    }

    public void testFailsGracefullyOnExceptionDuringFlush() throws IOException {
        final AtomicBoolean throwException = new AtomicBoolean();

        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(createTempDir())) {
            final PersistedClusterStateService persistedClusterStateService
                = new PersistedClusterStateService(nodeEnvironment, xContentRegistry(), getBigArrays(),
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), () -> 0L) {
                @Override
                Directory createDirectory(Path path) throws IOException {
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
                    .metadata(Metadata.builder(clusterState.metadata())
                        .clusterUUID(UUIDs.randomBase64UUID(random()))
                        .clusterUUIDCommitted(true)
                        .version(randomLongBetween(1L, Long.MAX_VALUE)))
                    .incrementVersion().build();
                throwException.set(true);
                assertThat(expectThrows(IOException.class, () ->
                        writeState(writer, newTerm, newState, clusterState)).getMessage(),
                    containsString("simulated"));
            }
        }
    }

    public void testClosesWriterOnFatalError() throws IOException {
        final AtomicBoolean throwException = new AtomicBoolean();

        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(createTempDir())) {
            final PersistedClusterStateService persistedClusterStateService
                = new PersistedClusterStateService(nodeEnvironment, xContentRegistry(), getBigArrays(),
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), () -> 0L) {
                @Override
                Directory createDirectory(Path path) throws IOException {
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
                    .metadata(Metadata.builder(clusterState.metadata())
                        .clusterUUID(UUIDs.randomBase64UUID(random()))
                        .clusterUUIDCommitted(true)
                        .version(randomLongBetween(1L, Long.MAX_VALUE)))
                    .incrementVersion().build();
                throwException.set(true);
                assertThat(expectThrows(OutOfMemoryError.class, () -> {
                        if (randomBoolean()) {
                            writeState(writer, newTerm, newState, clusterState);
                        } else {
                            writer.commit(newTerm, newState.version());
                        }
                    }).getMessage(),
                    containsString("simulated"));
                assertFalse(writer.isOpen());
            }

            // check if we can open writer again
            try (Writer ignored = persistedClusterStateService.createWriter()) {

            }
        }
    }

    public void testCrashesWithIOErrorOnCommitFailure() throws IOException {
        final AtomicBoolean throwException = new AtomicBoolean();

        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(createTempDir())) {
            final PersistedClusterStateService persistedClusterStateService
                = new PersistedClusterStateService(nodeEnvironment, xContentRegistry(), getBigArrays(),
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), () -> 0L) {
                @Override
                Directory createDirectory(Path path) throws IOException {
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
                    .metadata(Metadata.builder(clusterState.metadata())
                        .clusterUUID(UUIDs.randomBase64UUID(random()))
                        .clusterUUIDCommitted(true)
                        .version(randomLongBetween(1L, Long.MAX_VALUE)))
                    .incrementVersion().build();
                throwException.set(true);
                assertThat(expectThrows(IOError.class, () -> {
                        if (randomBoolean()) {
                            writeState(writer, newTerm, newState, clusterState);
                        } else {
                            writer.commit(newTerm, newState.version());
                        }
                    }).getMessage(),
                    containsString("simulated"));
                assertFalse(writer.isOpen());
            }

            // check if we can open writer again
            try (Writer ignored = persistedClusterStateService.createWriter()) {

            }
        }
    }

    public void testFailsIfGlobalMetadataIsMissing() throws IOException {
        // if someone attempted surgery on the metadata index by hand, e.g. deleting broken segments, then maybe the global metadata
        // isn't there any more

        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(createTempDir())) {
            try (Writer writer = newPersistedClusterStateService(nodeEnvironment).createWriter()) {
                final ClusterState clusterState = loadPersistedClusterState(newPersistedClusterStateService(nodeEnvironment));
                writeState(writer, 0L, ClusterState.builder(clusterState).version(randomLongBetween(1L, Long.MAX_VALUE)).build(),
                    clusterState);
            }

            final Path brokenPath = nodeEnvironment.nodeDataPath();
            try (Directory directory = newFSDirectory(brokenPath.resolve(PersistedClusterStateService.METADATA_DIRECTORY_NAME))) {
                final IndexWriterConfig indexWriterConfig = new IndexWriterConfig();
                indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
                try (IndexWriter indexWriter = new IndexWriter(directory, indexWriterConfig)) {
                    indexWriter.commit();
                }
            }

            final String message = expectThrows(IllegalStateException.class,
                () -> newPersistedClusterStateService(nodeEnvironment).loadOnDiskState()).getMessage();
            assertThat(message, allOf(containsString("no global metadata found"), containsString(brokenPath.toString())));
        }
    }

    public void testFailsIfGlobalMetadataIsDuplicated() throws IOException {
        // if someone attempted surgery on the metadata index by hand, e.g. deleting broken segments, then maybe the global metadata
        // is duplicated

        final Path brokenPath = createTempDir();
        final Path dupPath = createTempDir(); // this exists only to create a duplicate structure that can be copied

        try (NodeEnvironment nodeEnvironment1 = newNodeEnvironment(brokenPath);
             NodeEnvironment nodeEnvironment2 = newNodeEnvironment(dupPath)) {
            try (Writer writer1 = newPersistedClusterStateService(nodeEnvironment1).createWriter();
                 Writer writer2 = newPersistedClusterStateService(nodeEnvironment2).createWriter()) {
                final ClusterState oldClusterState = loadPersistedClusterState(newPersistedClusterStateService(nodeEnvironment1));
                final long newVersion = randomLongBetween(1L, Long.MAX_VALUE);
                final ClusterState newClusterState = ClusterState.builder(oldClusterState).version(newVersion).build();
                writeState(writer1, 0L, newClusterState, oldClusterState);
                writeState(writer2, 0L, newClusterState, oldClusterState);
            }

            try (Directory directory = newFSDirectory(brokenPath.resolve(PersistedClusterStateService.METADATA_DIRECTORY_NAME));
                 Directory dupDirectory = newFSDirectory(dupPath.resolve(PersistedClusterStateService.METADATA_DIRECTORY_NAME))) {
                try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                    indexWriter.addIndexes(dupDirectory);
                    indexWriter.commit();
                }
            }

            final String message = expectThrows(IllegalStateException.class,
                () -> newPersistedClusterStateService(nodeEnvironment1).loadOnDiskState()).getMessage();
            assertThat(message, allOf(containsString("duplicate global metadata found"), containsString(brokenPath.toString())));
        }
    }

    public void testFailsIfIndexMetadataIsDuplicated() throws IOException {
        // if someone attempted surgery on the metadata index by hand, e.g. deleting broken segments, then maybe some index metadata
        // is duplicated

        final Path brokenPath = createTempDir();
        final Path dupPath = createTempDir(); // this exists only to create a duplicate structure that can be copied

        try (NodeEnvironment nodeEnvironment1 = newNodeEnvironment(brokenPath);
             NodeEnvironment nodeEnvironment2 = newNodeEnvironment(dupPath)) {
            final String indexUUID = UUIDs.randomBase64UUID(random());
            final String indexName = randomAlphaOfLength(10);

            try (Writer writer1 = newPersistedClusterStateService(nodeEnvironment1).createWriter();
                 Writer writer2 = newPersistedClusterStateService(nodeEnvironment2).createWriter()) {
                final ClusterState oldClusterState = loadPersistedClusterState(newPersistedClusterStateService(nodeEnvironment1));
                final ClusterState newClusterState = ClusterState.builder(oldClusterState)
                    .metadata(Metadata.builder(oldClusterState.metadata())
                        .version(1L)
                        .coordinationMetadata(CoordinationMetadata.builder(oldClusterState.coordinationMetadata()).term(1L).build())
                        .put(IndexMetadata.builder(indexName)
                            .version(1L)
                            .settings(Settings.builder()
                                .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                                .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
                                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                .put(IndexMetadata.SETTING_INDEX_UUID, indexUUID))))
                    .incrementVersion().build();
                writeState(writer1, 0L, newClusterState, oldClusterState);
                writeState(writer2, 0L, newClusterState, oldClusterState);
            }

            try (Directory directory = newFSDirectory(brokenPath.resolve(PersistedClusterStateService.METADATA_DIRECTORY_NAME));
                 Directory dupDirectory = newFSDirectory(dupPath.resolve(PersistedClusterStateService.METADATA_DIRECTORY_NAME))) {
                try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                    indexWriter.deleteDocuments(new Term("type", "global")); // do not duplicate global metadata
                    indexWriter.addIndexes(dupDirectory);
                    indexWriter.commit();
                }
            }

            final String message = expectThrows(IllegalStateException.class,
                () -> newPersistedClusterStateService(nodeEnvironment1).loadOnDiskState()).getMessage();
            assertThat(message, allOf(
                containsString("duplicate metadata found"),
                containsString(brokenPath.toString()),
                containsString(indexName),
                containsString(indexUUID)));
        }
    }

    public void testPersistsAndReloadsIndexMetadataIffVersionOrTermChanges() throws IOException {
        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(createTempDir())) {
            final PersistedClusterStateService persistedClusterStateService = newPersistedClusterStateService(nodeEnvironment);
            final long globalVersion = randomLongBetween(1L, Long.MAX_VALUE);
            final String indexUUID = UUIDs.randomBase64UUID(random());
            final long indexMetadataVersion = randomLongBetween(1L, Long.MAX_VALUE);

            final long oldTerm = randomLongBetween(1L, Long.MAX_VALUE - 1);
            final long newTerm = randomLongBetween(oldTerm + 1, Long.MAX_VALUE);

            try (Writer writer = persistedClusterStateService.createWriter()) {
                ClusterState clusterState = loadPersistedClusterState(persistedClusterStateService);
                writeState(writer, 0L, ClusterState.builder(clusterState)
                        .metadata(Metadata.builder(clusterState.metadata())
                            .version(globalVersion)
                            .coordinationMetadata(CoordinationMetadata.builder(clusterState.coordinationMetadata()).term(oldTerm).build())
                            .put(IndexMetadata.builder("test")
                                .version(indexMetadataVersion - 1) // -1 because it's incremented in .put()
                                .settings(Settings.builder()
                                    .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                                    .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
                                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                    .put(IndexMetadata.SETTING_INDEX_UUID, indexUUID))))
                        .incrementVersion().build(),
                    clusterState);


                clusterState = loadPersistedClusterState(persistedClusterStateService);
                IndexMetadata indexMetadata = clusterState.metadata().index("test");
                assertThat(indexMetadata.getIndexUUID(), equalTo(indexUUID));
                assertThat(indexMetadata.getVersion(), equalTo(indexMetadataVersion));
                assertThat(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(indexMetadata.getSettings()), equalTo(0));
                // ensure we do not wastefully persist the same index metadata version by making a bad update with the same version
                writer.writeIncrementalStateAndCommit(0L, clusterState, ClusterState.builder(clusterState)
                        .metadata(Metadata.builder(clusterState.metadata())
                            .put(IndexMetadata.builder(indexMetadata).settings(Settings.builder()
                                .put(indexMetadata.getSettings())
                                .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)).build(), false))
                        .incrementVersion().build());

                clusterState = loadPersistedClusterState(persistedClusterStateService);
                indexMetadata = clusterState.metadata().index("test");
                assertThat(indexMetadata.getIndexUUID(), equalTo(indexUUID));
                assertThat(indexMetadata.getVersion(), equalTo(indexMetadataVersion));
                assertThat(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(indexMetadata.getSettings()), equalTo(0));
                // ensure that we do persist the same index metadata version by making an update with a higher version
                writeState(writer, 0L, ClusterState.builder(clusterState)
                        .metadata(Metadata.builder(clusterState.metadata())
                            .put(IndexMetadata.builder(indexMetadata).settings(Settings.builder()
                                .put(indexMetadata.getSettings())
                                .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 2)).build(), true))
                        .incrementVersion().build(),
                    clusterState);

                clusterState = loadPersistedClusterState(persistedClusterStateService);
                indexMetadata = clusterState.metadata().index("test");
                assertThat(indexMetadata.getVersion(), equalTo(indexMetadataVersion + 1));
                assertThat(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(indexMetadata.getSettings()), equalTo(2));
                // ensure that we also persist the index metadata when the term changes
                writeState(writer, 0L, ClusterState.builder(clusterState)
                        .metadata(Metadata.builder(clusterState.metadata())
                            .coordinationMetadata(CoordinationMetadata.builder(clusterState.coordinationMetadata()).term(newTerm).build())
                            .put(IndexMetadata.builder(indexMetadata).settings(Settings.builder()
                                .put(indexMetadata.getSettings())
                                .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 3)).build(), false))
                        .incrementVersion().build(),
                    clusterState);
            }

            final ClusterState clusterState = loadPersistedClusterState(persistedClusterStateService);
            final IndexMetadata indexMetadata = clusterState.metadata().index("test");
            assertThat(indexMetadata.getIndexUUID(), equalTo(indexUUID));
            assertThat(indexMetadata.getVersion(), equalTo(indexMetadataVersion + 1));
            assertThat(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(indexMetadata.getSettings()), equalTo(3));
        }
    }

    public void testPersistsAndReloadsIndexMetadataForMultipleIndices() throws IOException {
        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(createTempDir())) {
            final PersistedClusterStateService persistedClusterStateService = newPersistedClusterStateService(nodeEnvironment);

            final long term = randomLongBetween(1L, Long.MAX_VALUE);
            final String addedIndexUuid = UUIDs.randomBase64UUID(random());
            final String updatedIndexUuid = UUIDs.randomBase64UUID(random());
            final String deletedIndexUuid = UUIDs.randomBase64UUID(random());

            try (Writer writer = persistedClusterStateService.createWriter()) {
                final ClusterState clusterState = loadPersistedClusterState(persistedClusterStateService);
                writeState(writer, 0L, ClusterState.builder(clusterState)
                    .metadata(Metadata.builder(clusterState.metadata())
                        .version(clusterState.metadata().version() + 1)
                        .coordinationMetadata(CoordinationMetadata.builder(clusterState.coordinationMetadata()).term(term).build())
                        .put(IndexMetadata.builder("updated")
                            .version(randomLongBetween(0L, Long.MAX_VALUE - 1) - 1) // -1 because it's incremented in .put()
                            .settings(Settings.builder()
                                .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
                                .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                .put(IndexMetadata.SETTING_INDEX_UUID, updatedIndexUuid)))
                    .put(IndexMetadata.builder("deleted")
                        .version(randomLongBetween(0L, Long.MAX_VALUE - 1) - 1) // -1 because it's incremented in .put()
                        .settings(Settings.builder()
                            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
                            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                            .put(IndexMetadata.SETTING_INDEX_UUID, deletedIndexUuid))))
                    .incrementVersion().build(),
                    clusterState);
            }

            try (Writer writer = persistedClusterStateService.createWriter()) {
                final ClusterState clusterState = loadPersistedClusterState(persistedClusterStateService);

                assertThat(clusterState.metadata().indices().size(), equalTo(2));
                assertThat(clusterState.metadata().index("updated").getIndexUUID(), equalTo(updatedIndexUuid));
                assertThat(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(clusterState.metadata().index("updated").getSettings()),
                    equalTo(1));
                assertThat(clusterState.metadata().index("deleted").getIndexUUID(), equalTo(deletedIndexUuid));

                writeState(writer, 0L, ClusterState.builder(clusterState)
                    .metadata(Metadata.builder(clusterState.metadata())
                        .version(clusterState.metadata().version() + 1)
                        .remove("deleted")
                        .put(IndexMetadata.builder("updated")
                            .settings(Settings.builder()
                                .put(clusterState.metadata().index("updated").getSettings())
                                .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 2)))
                        .put(IndexMetadata.builder("added")
                            .version(randomLongBetween(0L, Long.MAX_VALUE - 1) - 1) // -1 because it's incremented in .put()
                            .settings(Settings.builder()
                                .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
                                .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                .put(IndexMetadata.SETTING_INDEX_UUID, addedIndexUuid))))
                    .incrementVersion().build(),
                    clusterState);
            }

            final ClusterState clusterState = loadPersistedClusterState(persistedClusterStateService);

            assertThat(clusterState.metadata().indices().size(), equalTo(2));
            assertThat(clusterState.metadata().index("updated").getIndexUUID(), equalTo(updatedIndexUuid));
            assertThat(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(clusterState.metadata().index("updated").getSettings()),
                equalTo(2));
            assertThat(clusterState.metadata().index("added").getIndexUUID(), equalTo(addedIndexUuid));
            assertThat(clusterState.metadata().index("deleted"), nullValue());
        }
    }

    public void testReloadsMetadataAcrossMultipleSegments() throws IOException {
        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(createTempDir())) {
            final PersistedClusterStateService persistedClusterStateService = newPersistedClusterStateService(nodeEnvironment);

            final int writes = between(5, 20);
            final List<Index> indices = new ArrayList<>(writes);

            try (Writer writer = persistedClusterStateService.createWriter()) {
                for (int i = 0; i < writes; i++) {
                    final Index index = new Index("test-" + i, UUIDs.randomBase64UUID(random()));
                    indices.add(index);
                    final ClusterState clusterState = loadPersistedClusterState(persistedClusterStateService);
                    writeState(writer, 0L, ClusterState.builder(clusterState)
                        .metadata(Metadata.builder(clusterState.metadata())
                            .version(i + 2)
                            .put(IndexMetadata.builder(index.getName())
                                .settings(Settings.builder()
                                    .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                                    .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
                                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                    .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID()))))
                        .incrementVersion().build(),
                        clusterState);
                }
            }

            final ClusterState clusterState = loadPersistedClusterState(persistedClusterStateService);
            for (Index index : indices) {
                final IndexMetadata indexMetadata = clusterState.metadata().index(index.getName());
                assertThat(indexMetadata.getIndexUUID(), equalTo(index.getUUID()));
            }
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

        final DiscoveryNode localNode = new DiscoveryNode("node", buildNewFakeTransportAddress(), Version.CURRENT);
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId())).build();

        final long startTimeMillis = randomLongBetween(0L, Long.MAX_VALUE - slowWriteLoggingThresholdMillis * 10);
        final AtomicLong currentTime = new AtomicLong(startTimeMillis);
        final AtomicLong writeDurationMillis = new AtomicLong(slowWriteLoggingThresholdMillis);

        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(createTempDir())) {
            PersistedClusterStateService persistedClusterStateService = new PersistedClusterStateService(nodeEnvironment,
                    xContentRegistry(), getBigArrays(), clusterSettings, () -> currentTime.getAndAdd(writeDurationMillis.get()));

            try (Writer writer = persistedClusterStateService.createWriter()) {
                assertExpectedLogs(1L, null, clusterState, writer, new MockLogAppender.SeenEventExpectation(
                    "should see warning at threshold",
                    PersistedClusterStateService.class.getCanonicalName(),
                    Level.WARN,
                    "writing cluster state took [*] which is above the warn threshold of [*]; " +
                        "wrote full state with [0] indices"));

                writeDurationMillis.set(randomLongBetween(slowWriteLoggingThresholdMillis, slowWriteLoggingThresholdMillis * 2));
                assertExpectedLogs(1L, null, clusterState, writer, new MockLogAppender.SeenEventExpectation(
                    "should see warning above threshold",
                    PersistedClusterStateService.class.getCanonicalName(),
                    Level.WARN,
                    "writing cluster state took [*] which is above the warn threshold of [*]; " +
                        "wrote full state with [0] indices"));

                writeDurationMillis.set(randomLongBetween(1, slowWriteLoggingThresholdMillis - 1));
                assertExpectedLogs(1L, null, clusterState, writer, new MockLogAppender.UnseenEventExpectation(
                    "should not see warning below threshold",
                    PersistedClusterStateService.class.getCanonicalName(),
                    Level.WARN,
                    "*"));

                clusterSettings.applySettings(Settings.builder()
                    .put(PersistedClusterStateService.SLOW_WRITE_LOGGING_THRESHOLD.getKey(), writeDurationMillis.get() + "ms")
                    .build());
                assertExpectedLogs(1L, null, clusterState, writer, new MockLogAppender.SeenEventExpectation(
                    "should see warning at reduced threshold",
                    PersistedClusterStateService.class.getCanonicalName(),
                    Level.WARN,
                    "writing cluster state took [*] which is above the warn threshold of [*]; " +
                        "wrote full state with [0] indices"));

                final ClusterState newClusterState = ClusterState.builder(clusterState)
                    .metadata(Metadata.builder(clusterState.metadata())
                        .version(clusterState.version())
                        .put(IndexMetadata.builder("test")
                            .settings(Settings.builder()
                                .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                                .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
                                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                .put(IndexMetadata.SETTING_INDEX_UUID, "test-uuid"))))
                    .incrementVersion().build();

                assertExpectedLogs(1L, clusterState, newClusterState, writer, new MockLogAppender.SeenEventExpectation(
                    "should see warning at threshold",
                    PersistedClusterStateService.class.getCanonicalName(),
                    Level.WARN,
                    "writing cluster state took [*] which is above the warn threshold of [*]; " +
                        "wrote global metadata [false] and metadata for [1] indices and skipped [0] unchanged indices"));

                writeDurationMillis.set(randomLongBetween(0, writeDurationMillis.get() - 1));
                assertExpectedLogs(1L, clusterState, newClusterState, writer, new MockLogAppender.UnseenEventExpectation(
                    "should not see warning below threshold",
                    PersistedClusterStateService.class.getCanonicalName(),
                    Level.WARN,
                    "*"));

                assertThat(currentTime.get(), lessThan(startTimeMillis + 14 * slowWriteLoggingThresholdMillis)); // ensure no overflow
            }
        }
    }

    public void testFailsIfCorrupt() throws IOException {
        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(createTempDir())) {
            final PersistedClusterStateService persistedClusterStateService = newPersistedClusterStateService(nodeEnvironment);

            try (Writer writer = persistedClusterStateService.createWriter()) {
                writer.writeFullStateAndCommit(1, ClusterState.EMPTY_STATE);
            }

            try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(nodeEnvironment.nodeDataPath().resolve("_state"))) {
                CorruptionUtils.corruptFile(random(), randomFrom(StreamSupport
                    .stream(directoryStream.spliterator(), false)
                    .filter(p -> {
                        final String filename = p.getFileName().toString();
                        return ExtrasFS.isExtra(filename) == false && filename.equals(WRITE_LOCK_NAME) == false;
                    })
                    .collect(Collectors.toList())));
            }

            assertThat(expectThrows(IllegalStateException.class, persistedClusterStateService::loadOnDiskState).getMessage(), allOf(
                    startsWith("the index containing the cluster metadata under the data path ["),
                    endsWith("] has been changed by an external force after it was last written by Elasticsearch and is now unreadable")));
        }
    }

    private void assertExpectedLogs(long currentTerm, ClusterState previousState, ClusterState clusterState,
                                    PersistedClusterStateService.Writer writer, MockLogAppender.LoggingExpectation expectation)
        throws IllegalAccessException, IOException {
        MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();
        mockAppender.addExpectation(expectation);
        Logger classLogger = LogManager.getLogger(PersistedClusterStateService.class);
        Loggers.addAppender(classLogger, mockAppender);

        try {
            if (previousState == null) {
                writer.writeFullStateAndCommit(currentTerm, clusterState);
            } else {
                writer.writeIncrementalStateAndCommit(currentTerm, previousState, clusterState);
            }
        } finally {
            Loggers.removeAppender(classLogger, mockAppender);
            mockAppender.stop();
        }
        mockAppender.assertAllExpectationsMatched();
    }

    @Override
    public Settings buildEnvSettings(Settings settings) {
        assertTrue(settings.hasValue(Environment.PATH_DATA_SETTING.getKey()));
        return Settings.builder()
            .put(settings)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath()).build();
    }

    private NodeEnvironment newNodeEnvironment(Path dataPath) throws IOException {
        return newNodeEnvironment(Settings.builder()
            .put(Environment.PATH_DATA_SETTING.getKey(), dataPath.toString())
            .build());
    }

    private static ClusterState loadPersistedClusterState(PersistedClusterStateService persistedClusterStateService) throws IOException {
        final PersistedClusterStateService.OnDiskState onDiskState = persistedClusterStateService.loadOnDiskState(false);
        return clusterStateFromMetadata(onDiskState.lastAcceptedVersion, onDiskState.metadata);
    }

    private static ClusterState clusterStateFromMetadata(long version, Metadata metadata) {
        return ClusterState.builder(ClusterName.DEFAULT).version(version).metadata(metadata).build();
    }

    private static BigArrays getBigArrays() {
        return usually()
                ? BigArrays.NON_RECYCLING_INSTANCE
                : new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
    }

}
