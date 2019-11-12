/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.gateway;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.SimpleFSDirectory;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetaData;
import org.elasticsearch.cluster.coordination.CoordinationState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.io.IOError;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class LucenePersistedStateFactoryTests extends ESTestCase {

    private LucenePersistedStateFactory newPersistedStateFactory(NodeEnvironment nodeEnvironment) {
        return new LucenePersistedStateFactory(nodeEnvironment, xContentRegistry(),
            usually()
                ? BigArrays.NON_RECYCLING_INSTANCE
                : new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService()));
    }

    public void testPersistsAndReloadsTerm() throws IOException {
        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(createDataPaths())) {
            final LucenePersistedStateFactory persistedStateFactory = newPersistedStateFactory(nodeEnvironment);
            final long newTerm = randomNonNegativeLong();

            try (CoordinationState.PersistedState persistedState = loadPersistedState(persistedStateFactory)) {
                assertThat(persistedState.getCurrentTerm(), equalTo(0L));
                persistedState.setCurrentTerm(newTerm);
                assertThat(persistedState.getCurrentTerm(), equalTo(newTerm));
            }

            try (CoordinationState.PersistedState persistedState = loadPersistedState(persistedStateFactory)) {
                assertThat(persistedState.getCurrentTerm(), equalTo(newTerm));
            }
        }
    }

    public void testPersistsAndReloadsGlobalMetadata() throws IOException {
        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(createDataPaths())) {
            final LucenePersistedStateFactory persistedStateFactory = newPersistedStateFactory(nodeEnvironment);
            final String clusterUUID = UUIDs.randomBase64UUID(random());
            final long version = randomLongBetween(1L, Long.MAX_VALUE);

            try (CoordinationState.PersistedState persistedState = loadPersistedState(persistedStateFactory)) {
                final ClusterState clusterState = persistedState.getLastAcceptedState();
                persistedState.setLastAcceptedState(ClusterState.builder(clusterState)
                    .metaData(MetaData.builder(clusterState.metaData())
                        .clusterUUID(clusterUUID)
                        .clusterUUIDCommitted(true)
                        .version(version))
                    .incrementVersion().build());
                assertThat(persistedState.getLastAcceptedState().metaData().clusterUUID(), equalTo(clusterUUID));
                assertTrue(persistedState.getLastAcceptedState().metaData().clusterUUIDCommitted());
            }

            try (CoordinationState.PersistedState persistedState = loadPersistedState(persistedStateFactory)) {
                final ClusterState clusterState = persistedState.getLastAcceptedState();
                assertThat(clusterState.metaData().clusterUUID(), equalTo(clusterUUID));
                assertTrue(clusterState.metaData().clusterUUIDCommitted());
                assertThat(clusterState.metaData().version(), equalTo(version));

                persistedState.setLastAcceptedState(ClusterState.builder(clusterState)
                    .metaData(MetaData.builder(clusterState.metaData())
                        .clusterUUID(clusterUUID)
                        .clusterUUIDCommitted(true)
                        .version(version + 1))
                    .incrementVersion().build());
            }

            try (CoordinationState.PersistedState persistedState = loadPersistedState(persistedStateFactory)) {
                final ClusterState clusterState = persistedState.getLastAcceptedState();
                assertThat(clusterState.metaData().clusterUUID(), equalTo(clusterUUID));
                assertTrue(clusterState.metaData().clusterUUIDCommitted());
                assertThat(clusterState.metaData().version(), equalTo(version + 1));
            }
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
            try (CoordinationState.PersistedState persistedState
                     = loadPersistedState(newPersistedStateFactory(nodeEnvironment))) {
                final ClusterState clusterState = persistedState.getLastAcceptedState();
                persistedState.setCurrentTerm(randomLongBetween(1L, Long.MAX_VALUE));
                persistedState.setLastAcceptedState(
                    ClusterState.builder(clusterState).version(staleVersion)
                        .metaData(MetaData.builder(clusterState.metaData()).coordinationMetaData(
                            CoordinationMetaData.builder(clusterState.coordinationMetaData()).term(staleTerm).build())).build());
            }
        }

        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(new Path[]{randomFrom(dataPaths)})) {
            unimportantPaths.remove(nodeEnvironment.nodeDataPaths()[0]);
            try (CoordinationState.PersistedState persistedState
                     = loadPersistedState(newPersistedStateFactory(nodeEnvironment))) {
                final ClusterState clusterState = persistedState.getLastAcceptedState();
                persistedState.setLastAcceptedState(
                    ClusterState.builder(clusterState).version(freshVersion)
                        .metaData(MetaData.builder(clusterState.metaData()).coordinationMetaData(
                            CoordinationMetaData.builder(clusterState.coordinationMetaData()).term(freshTerm).build())).build());
            }
        }

        if (randomBoolean() && unimportantPaths.isEmpty() == false) {
            IOUtils.rm(randomFrom(unimportantPaths));
        }

        // verify that the freshest state is chosen
        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(dataPaths)) {
            try (CoordinationState.PersistedState persistedState
                     = loadPersistedState(newPersistedStateFactory(nodeEnvironment))) {
                assertThat(persistedState.getLastAcceptedState().term(), equalTo(freshTerm));
                assertThat(persistedState.getLastAcceptedState().version(), equalTo(freshVersion));
            }
        }

        // verify that the freshest state was rewritten to each data path
        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(new Path[]{randomFrom(dataPaths)})) {
            try (CoordinationState.PersistedState persistedState
                     = loadPersistedState(newPersistedStateFactory(nodeEnvironment))) {
                assertThat(persistedState.getLastAcceptedState().term(), equalTo(freshTerm));
                assertThat(persistedState.getLastAcceptedState().version(), equalTo(freshVersion));
            }
        }
    }

    public void testFailsOnMismatchedNodeIds() throws IOException {
        final Path[] dataPaths1 = createDataPaths();
        final Path[] dataPaths2 = createDataPaths();

        final String[] nodeIds = new String[2];

        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(dataPaths1)) {
            nodeIds[0] = nodeEnvironment.nodeId();
            try (CoordinationState.PersistedState persistedState
                     = loadPersistedState(newPersistedStateFactory(nodeEnvironment))) {
                persistedState.setLastAcceptedState(
                    ClusterState.builder(persistedState.getLastAcceptedState()).version(randomLongBetween(1L, Long.MAX_VALUE)).build());
            }
        }

        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(dataPaths2)) {
            nodeIds[1] = nodeEnvironment.nodeId();
            try (CoordinationState.PersistedState persistedState
                     = loadPersistedState(newPersistedStateFactory(nodeEnvironment))) {
                persistedState.setLastAcceptedState(
                    ClusterState.builder(persistedState.getLastAcceptedState()).version(randomLongBetween(1L, Long.MAX_VALUE)).build());
            }
        }

        for (Path dataPath : dataPaths2) {
            IOUtils.rm(dataPath.resolve(MetaDataStateFormat.STATE_DIR_NAME));
        }

        final Path[] combinedPaths = Stream.concat(Arrays.stream(dataPaths1), Arrays.stream(dataPaths2)).toArray(Path[]::new);

        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(combinedPaths)) {
            final String message = expectThrows(IllegalStateException.class,
                () -> loadPersistedState(newPersistedStateFactory(nodeEnvironment))).getMessage();
            assertThat(message,
                allOf(containsString("unexpected node ID in metadata"), containsString(nodeIds[0]), containsString(nodeIds[1])));
            assertTrue("[" + message + "] should match " + Arrays.toString(dataPaths2),
                Arrays.stream(dataPaths2).anyMatch(p -> message.contains(p.toString())));
        }
    }

    public void testFailsOnMismatchedCommittedClusterUUIDs() throws IOException {
        final Path[] dataPaths1 = createDataPaths();
        final Path[] dataPaths2 = createDataPaths();
        final Path[] combinedPaths = Stream.concat(Arrays.stream(dataPaths1), Arrays.stream(dataPaths2)).toArray(Path[]::new);

        final String clusterUUID1 = UUIDs.randomBase64UUID(random());
        final String clusterUUID2 = UUIDs.randomBase64UUID(random());

        // first establish consistent node IDs and write initial metadata
        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(combinedPaths)) {
            try (CoordinationState.PersistedState persistedState
                     = loadPersistedState(newPersistedStateFactory(nodeEnvironment))) {
                assertFalse(persistedState.getLastAcceptedState().metaData().clusterUUIDCommitted());
            }
        }

        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(dataPaths1)) {
            try (CoordinationState.PersistedState persistedState
                     = loadPersistedState(newPersistedStateFactory(nodeEnvironment))) {
                final ClusterState clusterState = persistedState.getLastAcceptedState();
                persistedState.setLastAcceptedState(ClusterState.builder(clusterState)
                    .metaData(MetaData.builder(clusterState.metaData())
                        .clusterUUID(clusterUUID1)
                        .clusterUUIDCommitted(true)
                        .version(1))
                    .incrementVersion().build());
            }
        }

        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(dataPaths2)) {
            try (CoordinationState.PersistedState persistedState
                     = loadPersistedState(newPersistedStateFactory(nodeEnvironment))) {
                final ClusterState clusterState = persistedState.getLastAcceptedState();
                persistedState.setLastAcceptedState(ClusterState.builder(clusterState)
                    .metaData(MetaData.builder(clusterState.metaData())
                        .clusterUUID(clusterUUID2)
                        .clusterUUIDCommitted(true)
                        .version(1))
                    .incrementVersion().build());
            }
        }

        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(combinedPaths)) {
            final String message = expectThrows(IllegalStateException.class,
                () -> loadPersistedState(newPersistedStateFactory(nodeEnvironment))).getMessage();
            assertThat(message,
                allOf(containsString("mismatched cluster UUIDs in metadata"), containsString(clusterUUID1), containsString(clusterUUID2)));
            assertTrue("[" + message + "] should match " + Arrays.toString(dataPaths1),
                Arrays.stream(dataPaths1).anyMatch(p -> message.contains(p.toString())));
            assertTrue("[" + message + "] should match " + Arrays.toString(dataPaths2),
                Arrays.stream(dataPaths2).anyMatch(p -> message.contains(p.toString())));
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
            try (CoordinationState.PersistedState persistedState
                     = loadPersistedState(newPersistedStateFactory(nodeEnvironment))) {
                final ClusterState clusterState = persistedState.getLastAcceptedState();
                persistedState.setCurrentTerm(staleCurrentTerm);
                persistedState.setLastAcceptedState(ClusterState.builder(clusterState)
                    .metaData(MetaData.builder(clusterState.metaData()).version(1)
                        .coordinationMetaData(CoordinationMetaData.builder(clusterState.coordinationMetaData()).term(staleTerm).build()))
                    .version(staleVersion)
                    .build());
            }
        }

        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(dataPaths1)) {
            try (CoordinationState.PersistedState persistedState
                     = loadPersistedState(newPersistedStateFactory(nodeEnvironment))) {
                persistedState.setCurrentTerm(freshCurrentTerm);
            }
        }

        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(dataPaths2)) {
            try (CoordinationState.PersistedState persistedState
                     = loadPersistedState(newPersistedStateFactory(nodeEnvironment))) {
                final ClusterState clusterState = persistedState.getLastAcceptedState();
                persistedState.setLastAcceptedState(ClusterState.builder(clusterState)
                    .metaData(MetaData.builder(clusterState.metaData()).version(2)
                        .coordinationMetaData(CoordinationMetaData.builder(clusterState.coordinationMetaData()).term(freshTerm).build()))
                    .version(freshVersion)
                    .build());
            }
        }

        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(combinedPaths)) {
            final String message = expectThrows(IllegalStateException.class,
                () -> loadPersistedState(newPersistedStateFactory(nodeEnvironment))).getMessage();
            assertThat(message, allOf(
                    containsString("inconsistent terms found"),
                    containsString(Long.toString(staleCurrentTerm)),
                    containsString(Long.toString(freshCurrentTerm))));
            assertTrue("[" + message + "] should match " + Arrays.toString(dataPaths1),
                Arrays.stream(dataPaths1).anyMatch(p -> message.contains(p.toString())));
            assertTrue("[" + message + "] should match " + Arrays.toString(dataPaths2),
                Arrays.stream(dataPaths2).anyMatch(p -> message.contains(p.toString())));
        }
    }

    public void testFailsGracefullyOnExceptionDuringFlush() throws IOException {
        final AtomicBoolean throwException = new AtomicBoolean();

        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(createDataPaths())) {
            final LucenePersistedStateFactory persistedStateFactory
                = new LucenePersistedStateFactory(nodeEnvironment, xContentRegistry(), BigArrays.NON_RECYCLING_INSTANCE) {
                @Override
                Directory createDirectory(Path path) throws IOException {
                    return new FilterDirectory(new SimpleFSDirectory(path)) {
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
            final long newTerm = randomNonNegativeLong();

            try (CoordinationState.PersistedState persistedState = loadPersistedState(persistedStateFactory)) {
                persistedState.setCurrentTerm(newTerm);
                final ClusterState clusterState = persistedState.getLastAcceptedState();
                final ClusterState newState = ClusterState.builder(clusterState)
                    .metaData(MetaData.builder(clusterState.metaData())
                        .clusterUUID(UUIDs.randomBase64UUID(random()))
                        .clusterUUIDCommitted(true)
                        .version(randomLongBetween(1L, Long.MAX_VALUE)))
                    .incrementVersion().build();
                throwException.set(true);
                assertThat(expectThrows(UncheckedIOException.class, () -> persistedState.setLastAcceptedState(newState)).getMessage(),
                    containsString("simulated"));
            }
        }
    }

    public void testThrowsIOErrorOnExceptionDuringCommit() throws IOException {
        final AtomicBoolean throwException = new AtomicBoolean();

        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(createDataPaths())) {
            final LucenePersistedStateFactory persistedStateFactory
                = new LucenePersistedStateFactory(nodeEnvironment, xContentRegistry(), BigArrays.NON_RECYCLING_INSTANCE) {
                @Override
                Directory createDirectory(Path path) throws IOException {
                    return new FilterDirectory(new SimpleFSDirectory(path)) {
                        @Override
                        public void sync(Collection<String> names) throws IOException {
                            if (throwException.get() && names.stream().anyMatch(n -> n.startsWith("pending_segments_"))) {
                                throw new IOException("simulated");
                            }
                        }
                    };
                }
            };
            final long newTerm = randomNonNegativeLong();

            try (CoordinationState.PersistedState persistedState = loadPersistedState(persistedStateFactory)) {
                persistedState.setCurrentTerm(newTerm);
                final ClusterState clusterState = persistedState.getLastAcceptedState();
                final ClusterState newState = ClusterState.builder(clusterState)
                    .metaData(MetaData.builder(clusterState.metaData())
                        .clusterUUID(UUIDs.randomBase64UUID(random()))
                        .clusterUUIDCommitted(true)
                        .version(randomLongBetween(1L, Long.MAX_VALUE)))
                    .incrementVersion().build();
                throwException.set(true);
                assertThat(expectThrows(IOError.class, () -> persistedState.setLastAcceptedState(newState)).getMessage(),
                    containsString("simulated"));
            }
        }
    }

    public void testFailsIfGlobalMetadataIsMissing() throws IOException {
        // if someone attempted surgery on the metadata index by hand, e.g. deleting broken segments, then maybe the global metadata
        // isn't there any more

        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(createDataPaths())) {
            try (CoordinationState.PersistedState persistedState
                     = loadPersistedState(newPersistedStateFactory(nodeEnvironment))) {
                persistedState.setLastAcceptedState(
                    ClusterState.builder(persistedState.getLastAcceptedState()).version(randomLongBetween(1L, Long.MAX_VALUE)).build());
            }

            final Path brokenPath = randomFrom(nodeEnvironment.nodeDataPaths());
            try (Directory directory = new SimpleFSDirectory(brokenPath.resolve(LucenePersistedStateFactory.METADATA_DIRECTORY_NAME))) {
                final IndexWriterConfig indexWriterConfig = new IndexWriterConfig();
                indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
                try (IndexWriter indexWriter = new IndexWriter(directory, indexWriterConfig)) {
                    indexWriter.commit();
                }
            }

            final String message = expectThrows(IllegalStateException.class,
                () -> loadPersistedState(newPersistedStateFactory(nodeEnvironment))).getMessage();
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
            try (CoordinationState.PersistedState persistedState
                     = loadPersistedState(newPersistedStateFactory(nodeEnvironment))) {
                persistedState.setLastAcceptedState(
                    ClusterState.builder(persistedState.getLastAcceptedState()).version(randomLongBetween(1L, Long.MAX_VALUE)).build());
            }

            final Path brokenPath = randomFrom(nodeEnvironment.nodeDataPaths());
            final Path dupPath = randomValueOtherThan(brokenPath, () -> randomFrom(nodeEnvironment.nodeDataPaths()));
            try (Directory directory = new SimpleFSDirectory(brokenPath.resolve(LucenePersistedStateFactory.METADATA_DIRECTORY_NAME));
                 Directory dupDirectory = new SimpleFSDirectory(dupPath.resolve(LucenePersistedStateFactory.METADATA_DIRECTORY_NAME))) {
                try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                    indexWriter.addIndexes(dupDirectory);
                    indexWriter.commit();
                }
            }

            final String message = expectThrows(IllegalStateException.class,
                () -> loadPersistedState(newPersistedStateFactory(nodeEnvironment))).getMessage();
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

            try (CoordinationState.PersistedState persistedState
                     = loadPersistedState(newPersistedStateFactory(nodeEnvironment))) {
                final ClusterState clusterState = persistedState.getLastAcceptedState();
                persistedState.setLastAcceptedState(ClusterState.builder(clusterState)
                    .metaData(MetaData.builder(clusterState.metaData())
                        .version(1L)
                        .coordinationMetaData(CoordinationMetaData.builder(clusterState.coordinationMetaData()).term(1L).build())
                        .put(IndexMetaData.builder(indexName)
                            .version(1L)
                            .settings(Settings.builder()
                                .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                                .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
                                .put(IndexMetaData.SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
                                .put(IndexMetaData.SETTING_INDEX_UUID, indexUUID))))
                    .incrementVersion().build());
            }

            final Path brokenPath = randomFrom(nodeEnvironment.nodeDataPaths());
            final Path dupPath = randomValueOtherThan(brokenPath, () -> randomFrom(nodeEnvironment.nodeDataPaths()));
            try (Directory directory = new SimpleFSDirectory(brokenPath.resolve(LucenePersistedStateFactory.METADATA_DIRECTORY_NAME));
                 Directory dupDirectory = new SimpleFSDirectory(dupPath.resolve(LucenePersistedStateFactory.METADATA_DIRECTORY_NAME))) {
                try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())) {
                    indexWriter.deleteDocuments(new Term("type", "global")); // do not duplicate global metadata
                    indexWriter.addIndexes(dupDirectory);
                    indexWriter.commit();
                }
            }

            final String message = expectThrows(IllegalStateException.class,
                () -> loadPersistedState(newPersistedStateFactory(nodeEnvironment))).getMessage();
            assertThat(message, allOf(
                containsString("duplicate metadata found"),
                containsString(brokenPath.toString()),
                containsString(indexName),
                containsString(indexUUID)));
        }
    }

    public void testPersistsAndReloadsIndexMetadataIffVersionOrTermChanges() throws IOException {
        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(createDataPaths())) {
            final LucenePersistedStateFactory persistedStateFactory = newPersistedStateFactory(nodeEnvironment);
            final long globalVersion = randomLongBetween(1L, Long.MAX_VALUE);
            final String indexUUID = UUIDs.randomBase64UUID(random());
            final long indexMetaDataVersion = randomLongBetween(1L, Long.MAX_VALUE);

            final long oldTerm = randomLongBetween(1L, Long.MAX_VALUE - 1);
            final long newTerm = randomLongBetween(oldTerm + 1, Long.MAX_VALUE);

            try (CoordinationState.PersistedState persistedState = loadPersistedState(persistedStateFactory)) {
                final ClusterState clusterState = persistedState.getLastAcceptedState();
                persistedState.setLastAcceptedState(ClusterState.builder(clusterState)
                    .metaData(MetaData.builder(clusterState.metaData())
                        .version(globalVersion)
                        .coordinationMetaData(CoordinationMetaData.builder(clusterState.coordinationMetaData()).term(oldTerm).build())
                        .put(IndexMetaData.builder("test")
                            .version(indexMetaDataVersion - 1) // -1 because it's incremented in .put()
                            .settings(Settings.builder()
                                .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                                .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
                                .put(IndexMetaData.SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
                                .put(IndexMetaData.SETTING_INDEX_UUID, indexUUID))))
                    .incrementVersion().build());
            }

            try (CoordinationState.PersistedState persistedState = loadPersistedState(persistedStateFactory)) {
                final ClusterState clusterState = persistedState.getLastAcceptedState();
                final IndexMetaData indexMetaData = clusterState.metaData().index("test");
                assertThat(indexMetaData.getIndexUUID(), equalTo(indexUUID));
                assertThat(indexMetaData.getVersion(), equalTo(indexMetaDataVersion));
                assertThat(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.get(indexMetaData.getSettings()), equalTo(0));

                // ensure we do not wastefully persist the same index metadata version by making a bad update with the same version
                persistedState.setLastAcceptedState(ClusterState.builder(clusterState)
                    .metaData(MetaData.builder(clusterState.metaData())
                        .put(IndexMetaData.builder(indexMetaData).settings(Settings.builder()
                                .put(indexMetaData.getSettings())
                                .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)).build(), false))
                    .incrementVersion().build());
            }

            try (CoordinationState.PersistedState persistedState = loadPersistedState(persistedStateFactory)) {
                final ClusterState clusterState = persistedState.getLastAcceptedState();
                final IndexMetaData indexMetaData = clusterState.metaData().index("test");
                assertThat(indexMetaData.getVersion(), equalTo(indexMetaDataVersion));
                assertThat(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.get(indexMetaData.getSettings()), equalTo(0));

                // ensure that we do persist the same index metadata version by making an update with a higher version
                persistedState.setLastAcceptedState(ClusterState.builder(clusterState)
                    .metaData(MetaData.builder(clusterState.metaData())
                        .put(IndexMetaData.builder(indexMetaData).settings(Settings.builder()
                            .put(indexMetaData.getSettings())
                            .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 2)).build(), true))
                    .incrementVersion().build());
            }

            try (CoordinationState.PersistedState persistedState = loadPersistedState(persistedStateFactory)) {
                final ClusterState clusterState = persistedState.getLastAcceptedState();
                final IndexMetaData indexMetaData = clusterState.metaData().index("test");
                assertThat(indexMetaData.getVersion(), equalTo(indexMetaDataVersion + 1));
                assertThat(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.get(indexMetaData.getSettings()), equalTo(2));

                // ensure that we also persist the index metadata when the term changes
                persistedState.setLastAcceptedState(ClusterState.builder(clusterState)
                    .metaData(MetaData.builder(clusterState.metaData())
                        .coordinationMetaData(CoordinationMetaData.builder(clusterState.coordinationMetaData()).term(newTerm).build())
                        .put(IndexMetaData.builder(indexMetaData).settings(Settings.builder()
                            .put(indexMetaData.getSettings())
                            .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 3)).build(), false))
                    .incrementVersion().build());
            }

            try (CoordinationState.PersistedState persistedState = loadPersistedState(persistedStateFactory)) {
                final ClusterState clusterState = persistedState.getLastAcceptedState();
                final IndexMetaData indexMetaData = clusterState.metaData().index("test");
                assertThat(indexMetaData.getIndexUUID(), equalTo(indexUUID));
                assertThat(indexMetaData.getVersion(), equalTo(indexMetaDataVersion + 1));
                assertThat(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.get(indexMetaData.getSettings()), equalTo(3));
            }
        }
    }

    public void testPersistsAndReloadsIndexMetadataForMultipleIndices() throws IOException {
        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(createDataPaths())) {
            final LucenePersistedStateFactory persistedStateFactory = newPersistedStateFactory(nodeEnvironment);

            final long term = randomLongBetween(1L, Long.MAX_VALUE);
            final String addedIndexUuid = UUIDs.randomBase64UUID(random());
            final String updatedIndexUuid = UUIDs.randomBase64UUID(random());
            final String deletedIndexUuid = UUIDs.randomBase64UUID(random());

            try (CoordinationState.PersistedState persistedState = loadPersistedState(persistedStateFactory)) {
                final ClusterState clusterState = persistedState.getLastAcceptedState();
                persistedState.setLastAcceptedState(ClusterState.builder(clusterState)
                    .metaData(MetaData.builder(clusterState.metaData())
                        .version(clusterState.metaData().version() + 1)
                        .coordinationMetaData(CoordinationMetaData.builder(clusterState.coordinationMetaData()).term(term).build())
                        .put(IndexMetaData.builder("updated")
                            .version(randomLongBetween(0L, Long.MAX_VALUE - 1) - 1) // -1 because it's incremented in .put()
                            .settings(Settings.builder()
                                .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
                                .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                                .put(IndexMetaData.SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
                                .put(IndexMetaData.SETTING_INDEX_UUID, updatedIndexUuid)))
                    .put(IndexMetaData.builder("deleted")
                        .version(randomLongBetween(0L, Long.MAX_VALUE - 1) - 1) // -1 because it's incremented in .put()
                        .settings(Settings.builder()
                            .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
                            .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                            .put(IndexMetaData.SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
                            .put(IndexMetaData.SETTING_INDEX_UUID, deletedIndexUuid))))
                    .incrementVersion().build());
            }

            try (CoordinationState.PersistedState persistedState = loadPersistedState(persistedStateFactory)) {
                final ClusterState clusterState = persistedState.getLastAcceptedState();

                assertThat(clusterState.metaData().indices().size(), equalTo(2));
                assertThat(clusterState.metaData().index("updated").getIndexUUID(), equalTo(updatedIndexUuid));
                assertThat(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.get(clusterState.metaData().index("updated").getSettings()),
                    equalTo(1));
                assertThat(clusterState.metaData().index("deleted").getIndexUUID(), equalTo(deletedIndexUuid));

                persistedState.setLastAcceptedState(ClusterState.builder(clusterState)
                    .metaData(MetaData.builder(clusterState.metaData())
                        .version(clusterState.metaData().version() + 1)
                        .remove("deleted")
                        .put(IndexMetaData.builder("updated")
                            .settings(Settings.builder()
                                .put(clusterState.metaData().index("updated").getSettings())
                                .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 2)))
                        .put(IndexMetaData.builder("added")
                            .version(randomLongBetween(0L, Long.MAX_VALUE - 1) - 1) // -1 because it's incremented in .put()
                            .settings(Settings.builder()
                                .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
                                .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                                .put(IndexMetaData.SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
                                .put(IndexMetaData.SETTING_INDEX_UUID, addedIndexUuid))))
                    .incrementVersion().build());
            }

            try (CoordinationState.PersistedState persistedState = loadPersistedState(persistedStateFactory)) {
                final ClusterState clusterState = persistedState.getLastAcceptedState();

                assertThat(clusterState.metaData().indices().size(), equalTo(2));
                assertThat(clusterState.metaData().index("updated").getIndexUUID(), equalTo(updatedIndexUuid));
                assertThat(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.get(clusterState.metaData().index("updated").getSettings()),
                    equalTo(2));
                assertThat(clusterState.metaData().index("added").getIndexUUID(), equalTo(addedIndexUuid));
                assertThat(clusterState.metaData().index("deleted"), nullValue());
            }
        }
    }

    public void testReloadsMetadataAcrossMultipleSegments() throws IOException {
        try (NodeEnvironment nodeEnvironment = newNodeEnvironment(createDataPaths())) {
            final LucenePersistedStateFactory persistedStateFactory = newPersistedStateFactory(nodeEnvironment);

            final int writes = between(5, 20);
            final List<Index> indices = new ArrayList<>(writes);

            try (CoordinationState.PersistedState persistedState = loadPersistedState(persistedStateFactory)) {
                for (int i = 0; i < writes; i++) {
                    final Index index = new Index("test-" + i, UUIDs.randomBase64UUID(random()));
                    indices.add(index);
                    final ClusterState clusterState = persistedState.getLastAcceptedState();
                    persistedState.setLastAcceptedState(ClusterState.builder(clusterState)
                        .metaData(MetaData.builder(clusterState.metaData())
                            .version(i + 2)
                            .put(IndexMetaData.builder(index.getName())
                                .settings(Settings.builder()
                                    .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                                    .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
                                    .put(IndexMetaData.SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
                                    .put(IndexMetaData.SETTING_INDEX_UUID, index.getUUID()))))
                        .incrementVersion().build());
                }
            }

            try (CoordinationState.PersistedState persistedState = loadPersistedState(persistedStateFactory)) {
                final ClusterState clusterState = persistedState.getLastAcceptedState();
                for (Index index : indices) {
                    final IndexMetaData indexMetaData = clusterState.metaData().index(index.getName());
                    assertThat(indexMetaData.getIndexUUID(), equalTo(index.getUUID()));
                }
            }
        }
    }

    @Override
    public Settings buildEnvSettings(Settings settings) {
        assertTrue(settings.hasValue(Environment.PATH_DATA_SETTING.getKey()));
        return Settings.builder()
            .put(settings)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath()).build();
    }

    public static Path[] createDataPaths() {
        final Path[] dataPaths = new Path[randomIntBetween(1, 4)];
        for (int i = 0; i < dataPaths.length; i++) {
            dataPaths[i] = createTempDir();
        }
        return dataPaths;
    }

    private NodeEnvironment newNodeEnvironment(Path[] dataPaths) throws IOException {
        return newNodeEnvironment(Settings.builder()
            .putList(Environment.PATH_DATA_SETTING.getKey(), Arrays.stream(dataPaths).map(Path::toString).collect(Collectors.toList()))
            .build());
    }

    private static CoordinationState.PersistedState loadPersistedState(LucenePersistedStateFactory persistedStateFactory)
        throws IOException {

        return persistedStateFactory.loadPersistedState(LucenePersistedStateFactoryTests::clusterStateFromMetadata);
    }

    private static ClusterState clusterStateFromMetadata(long version, MetaData metaData) {
        return ClusterState.builder(ClusterName.DEFAULT).version(version).metaData(metaData).build();
    }


}
