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

import co.elastic.elasticsearch.stateless.engine.TranslogMetadata;
import co.elastic.elasticsearch.stateless.lucene.DefaultDirectoryListener;
import co.elastic.elasticsearch.stateless.lucene.StatelessDirectory;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class StatelessIT extends AbstractStatelessIntegTestCase {

    public void testClusterCanFormWithStatelessEnabled() {
        startMasterOnlyNode();

        final int numIndexNodes = randomIntBetween(1, 5);
        startIndexNodes(numIndexNodes);
        ensureStableCluster(numIndexNodes + 1);

        var plugins = StreamSupport.stream(internalCluster().getInstances(PluginsService.class).spliterator(), false)
            .flatMap(ps -> ps.filterPlugins(Stateless.class).stream())
            .toList();
        assertThat(plugins.size(), greaterThan(0));
    }

    @TestLogging(reason = "testing logging at TRACE level", value = "co.elastic.elasticsearch.stateless:TRACE")
    public void testDirectoryListener() throws Exception {
        startMasterAndIndexNode();
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        final Logger listenerLogger = LogManager.getLogger(DefaultDirectoryListener.class);
        final MockLogAppender mockLogAppender = new MockLogAppender();
        mockLogAppender.start();
        try {
            Loggers.addAppender(listenerLogger, mockLogAppender);
            mockLogAppender.addExpectation(
                new MockLogAppender.PatternSeenEventExpectation(
                    "Creating pending_segments_1 before first commit",
                    listenerLogger.getName(),
                    Level.TRACE,
                    "\\[" + indexName + "\\]\\[0\\] opening \\[pending_segments_1\\] for \\[write\\] with primary term \\[1\\].*"
                )
            );
            mockLogAppender.addExpectation(
                new MockLogAppender.PatternSeenEventExpectation(
                    "Synchronizing pending_segments_1",
                    listenerLogger.getName(),
                    Level.TRACE,
                    "\\[" + indexName + "\\]\\[0\\] file \\[pending_segments_1\\] synced with primary term \\[1\\]"
                )
            );
            mockLogAppender.addExpectation(
                new MockLogAppender.PatternSeenEventExpectation(
                    "Renaming to segments_1",
                    listenerLogger.getName(),
                    Level.TRACE,
                    "\\[" + indexName + "\\]\\[0\\] file \\[pending_segments_1\\] renamed to \\[segments_1\\]"
                )
            );
            mockLogAppender.addExpectation(
                new MockLogAppender.PatternSeenEventExpectation(
                    "Reading segments_1",
                    listenerLogger.getName(),
                    Level.TRACE,
                    "\\[" + indexName + "\\]\\[0\\] opening \\[segments_1\\] for \\[read\\] with IOContext \\[context=READ, .*\\].*"
                )
            );
            mockLogAppender.addExpectation(
                new MockLogAppender.PatternSeenEventExpectation(
                    "Creating pending_segments_2 before second commit",
                    listenerLogger.getName(),
                    Level.TRACE,
                    "\\[" + indexName + "\\]\\[0\\] opening \\[pending_segments_2\\] for \\[write\\] with primary term \\[1\\].*"
                )
            );
            mockLogAppender.addExpectation(
                new MockLogAppender.PatternSeenEventExpectation(
                    "Synchronizing pending_segments_2",
                    listenerLogger.getName(),
                    Level.TRACE,
                    "\\[" + indexName + "\\]\\[0\\] file \\[pending_segments_2\\] synced with primary term \\[1\\]"
                )
            );
            mockLogAppender.addExpectation(
                new MockLogAppender.PatternSeenEventExpectation(
                    "Renaming pending_segments_2 to segments_2",
                    listenerLogger.getName(),
                    Level.TRACE,
                    "\\[" + indexName + "\\]\\[0\\] file \\[pending_segments_2\\] renamed to \\[segments_2\\]"
                )
            );
            mockLogAppender.addExpectation(
                new MockLogAppender.PatternSeenEventExpectation(
                    "Deleting segments_1",
                    listenerLogger.getName(),
                    Level.TRACE,
                    "\\[" + indexName + "\\]\\[0\\] file \\[segments_1\\] deleted"
                )
            );

            createIndex(
                indexName,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
                    .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                    .build()
            );
            ensureGreen(indexName);

            assertBusy(mockLogAppender::assertAllExpectationsMatched);

            int checks = 0;
            for (IndicesService indicesServices : internalCluster().getDataNodeInstances(IndicesService.class)) {
                var indexService = indicesServices.indexService(resolveIndex(indexName));
                if (indexService != null) {
                    for (int shardId : indexService.shardIds()) {
                        var indexShard = indexService.getShard(shardId);
                        assertThat(indexShard, notNullValue());
                        var store = indexShard.store();
                        assertThat(store, notNullValue());
                        var directory = StatelessDirectory.unwrapDirectory(store.directory());
                        assertThat(directory, notNullValue());
                        assertThat(directory, instanceOf(StatelessDirectory.class));
                        checks += 1;
                    }
                }
            }
            assertThat(checks, equalTo(getNumShards(indexName).totalNumShards));
        } finally {
            Loggers.removeAppender(listenerLogger, mockLogAppender);
            mockLogAppender.stop();
        }
    }

    public void testUploadToObjectStore() {
        startMasterOnlyNode();
        final int numberOfShards = randomIntBetween(1, 5);
        startIndexNodes(numberOfShards);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                .build()
        );
        ensureGreen(indexName);

        assertObjectStoreConsistentWithIndexShards();

        indexDocuments(indexName);
    }

    public void testTranslogWrittenToObjectStore() throws Exception {
        startMasterOnlyNode();
        final int numberOfShards = randomIntBetween(1, 5);
        startIndexNodes(numberOfShards);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                .build()
        );
        ensureGreen(indexName);

        assertObjectStoreConsistentWithIndexShards();

        indexDocuments(indexName);

        final Map<Index, Integer> indices = resolveIndices();
        assertThat(indices.isEmpty(), is(false));

        for (Map.Entry<Index, Integer> entry : indices.entrySet()) {
            assertThat(entry.getValue(), greaterThan(0));
            for (int shardId = 0; shardId < entry.getValue(); shardId++) {
                DiscoveryNode indexNode = findIndexNode(entry.getKey(), shardId);
                IndexShard indexShard = findIndexShard(entry.getKey(), shardId);
                var blobContainer = internalCluster().getDataNodeInstance(ObjectStoreService.class).getTranslogBlobContainer(indexNode);
                final int finalShardId = shardId;
                assertBusy(() -> {
                    long maxSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
                    long totalOps = 0;
                    for (String file : blobContainer.listBlobs().keySet()) {
                        try (StreamInput remote = new InputStreamStreamInput(blobContainer.readBlob(file))) {
                            ShardId objShardId = new ShardId(entry.getKey(), finalShardId);
                            Map<ShardId, TranslogMetadata> map = remote.readMap(ShardId::new, TranslogMetadata::new);
                            if (map.containsKey(objShardId)) {
                                TranslogMetadata translogMetadata = map.get(objShardId);
                                maxSeqNo = SequenceNumbers.max(maxSeqNo, translogMetadata.getMaxSeqNo());
                                totalOps += translogMetadata.getTotalOps();
                            }
                        }
                    }
                    assertThat(maxSeqNo, equalTo(indexShard.seqNoStats().getMaxSeqNo()));
                    assertThat(totalOps, equalTo(indexShard.seqNoStats().getMaxSeqNo() + 1));
                });
            }
        }
    }

    public void testDownloadNewCommitsFromObjectStore() throws Exception {
        startMasterOnlyNode();
        final int numberOfShards = randomIntBetween(1, 2);
        startIndexNodes(numberOfShards);
        startSearchNodes(numberOfShards);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                .build()
        );
        ensureGreen(indexName);

        assertObjectStoreConsistentWithIndexShards();

        // Index more documents
        indexDocuments(indexName);
        // Force a flush if necessary since it is possible that indexDocuments(indexName) only refreshed
        flush(indexName);

        final Map<Index, Integer> indices = resolveIndices();
        assertThat(indices.isEmpty(), is(false));

        for (Map.Entry<Index, Integer> entry : indices.entrySet()) {
            assertThat(entry.getValue(), greaterThan(0));
            for (int shardId = 0; shardId < entry.getValue(); shardId++) {
                IndexShard indexShard = findIndexShard(entry.getKey(), shardId);
                IndexShard searchShard = findSearchShard(entry.getKey(), shardId);
                assertObjectStoreConsistentWithIndexShards();
                assertBusy(() -> assertThatSearchShardIsConsistentWithLastCommit(indexShard, searchShard));
            }
        }

        // Index more documents
        indexDocuments(indexName);
        // Force a flush if necessary since it is possible that indexDocuments(indexName) only refreshed
        flush(indexName);

        for (Map.Entry<Index, Integer> entry : indices.entrySet()) {
            assertThat(entry.getValue(), greaterThan(0));
            for (int shardId = 0; shardId < entry.getValue(); shardId++) {
                IndexShard indexShard = findIndexShard(entry.getKey(), shardId);
                IndexShard searchShard = findSearchShard(entry.getKey(), shardId);
                assertObjectStoreConsistentWithIndexShards();
                assertBusy(() -> assertThatSearchShardIsConsistentWithLastCommit(indexShard, searchShard));
            }
        }
    }

    public void testDownloadNewReplicasFromObjectStore() {
        startMasterOnlyNode();
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final int numberOfShards = randomIntBetween(1, 5);
        startIndexNodes(numberOfShards);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
                .build()
        );
        ensureGreen(indexName);
        for (int i = 0; i < 3; i++) {
            indexDocuments(indexName);
            flush(indexName);
            assertObjectStoreConsistentWithIndexShards();
        }

        startSearchNodes(numberOfShards);
        updateIndexSettings(indexName, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1));
        ensureGreen(indexName);

        assertObjectStoreConsistentWithSearchShards();
    }

    private static void indexDocuments(String indexName) {
        final int iters = randomIntBetween(1, 20);
        for (int i = 0; i < iters; i++) {
            indexDocs(indexName, randomIntBetween(1, 100));
            switch (randomInt(2)) {
                case 0 -> client().admin().indices().prepareFlush().setForce(randomBoolean()).get();
                case 1 -> client().admin().indices().prepareRefresh().get();
                case 2 -> client().admin().indices().prepareForceMerge().get();
            }
            assertObjectStoreConsistentWithIndexShards();
        }
    }

    private static void assertObjectStoreConsistentWithIndexShards() {
        final Map<Index, Integer> indices = resolveIndices();
        assertThat(indices.isEmpty(), is(false));

        for (Map.Entry<Index, Integer> entry : indices.entrySet()) {
            assertThat(entry.getValue(), greaterThan(0));
            for (int shardId = 0; shardId < entry.getValue(); shardId++) {
                assertThatObjectStoreIsConsistentWithLastCommit(findIndexShard(entry.getKey(), shardId));
            }
        }
    }

    private static void assertObjectStoreConsistentWithSearchShards() {
        final Map<Index, Integer> indices = resolveIndices();
        assertThat(indices.isEmpty(), is(false));

        for (Map.Entry<Index, Integer> entry : indices.entrySet()) {
            assertThat(entry.getValue(), greaterThan(0));
            for (int shardId = 0; shardId < entry.getValue(); shardId++) {
                assertThatObjectStoreIsConsistentWithLastCommit(findSearchShard(entry.getKey(), shardId));
            }
        }
    }

    private static void assertThatObjectStoreIsConsistentWithLastCommit(final IndexShard indexShard) {
        final Store store = indexShard.store();
        store.incRef();
        try {
            var blobContainer = internalCluster().getDataNodeInstance(ObjectStoreService.class)
                .getBlobContainer(indexShard.shardId(), indexShard.getOperationPrimaryTerm());

            final SegmentInfos segmentInfos = Lucene.readSegmentInfos(store.directory());

            // can take some time for files to be uploaded to the object store
            assertBusy(() -> {
                var localFiles = segmentInfos.files(true);
                var remoteFiles = blobContainer.listBlobs().keySet();
                assertThat(
                    "Expected that all local files " + localFiles + " exist in remote " + remoteFiles,
                    remoteFiles,
                    hasItems(localFiles.toArray(String[]::new))
                );
                for (String file : segmentInfos.files(true)) {
                    assertThat("" + file, blobContainer.blobExists(file), is(true));
                    try (
                        IndexInput input = store.directory().openInput(file, IOContext.READONCE);
                        InputStream local = new InputStreamIndexInput(input, input.length());
                        InputStream remote = blobContainer.readBlob(file)
                    ) {
                        assertEquals("File [" + file + "] in object store has a different content than local file ", local, remote);
                    }
                }
            });
        } catch (Exception e) {
            throw new AssertionError(e);
        } finally {
            store.decRef();
        }
    }

    private static void assertThatSearchShardIsConsistentWithLastCommit(final IndexShard indexShard, final IndexShard searchShard) {
        final Store indexStore = indexShard.store();
        final Store searchStore = searchShard.store();
        indexStore.incRef();
        searchStore.incRef();
        try {
            var blobContainer = internalCluster().getDataNodeInstance(ObjectStoreService.class)
                .getBlobContainer(indexShard.shardId(), indexShard.getOperationPrimaryTerm());

            final SegmentInfos segmentInfos = Lucene.readSegmentInfos(indexStore.directory());

            for (String file : segmentInfos.files(true)) {
                // can take some time for files to be uploaded to the object store
                assertBusy(() -> {
                    assertThat("" + file, blobContainer.blobExists(file), is(true));

                    try (
                        IndexInput input = indexStore.directory().openInput(file, IOContext.READONCE);
                        InputStream local = new InputStreamIndexInput(input, input.length());
                        IndexInput searchInput = searchStore.directory().openInput(file, IOContext.READONCE);
                        InputStream searchInputStream = new InputStreamIndexInput(searchInput, searchInput.length());
                    ) {
                        assertEquals(
                            "File [" + file + "] on search shard has a different content than local file ",
                            local,
                            searchInputStream
                        );
                    }
                });
            }
        } catch (Exception e) {
            throw new AssertionError(e);
        } finally {
            indexStore.decRef();
            searchStore.decRef();
        }
    }

    private static void assertEquals(String message, InputStream expected, InputStream actual) throws IOException {
        // adapted from Files.mismatch()
        final int BUFFER_SIZE = 8192;
        byte[] buffer1 = new byte[BUFFER_SIZE];
        byte[] buffer2 = new byte[BUFFER_SIZE];
        try (
            InputStream expectedStream = new BufferedInputStream(expected, BUFFER_SIZE);
            InputStream actualStream = new BufferedInputStream(actual, BUFFER_SIZE)
        ) {
            long totalRead = 0;
            while (true) {
                int nRead1 = expectedStream.readNBytes(buffer1, 0, BUFFER_SIZE);
                int nRead2 = actualStream.readNBytes(buffer2, 0, BUFFER_SIZE);

                int i = Arrays.mismatch(buffer1, 0, nRead1, buffer2, 0, nRead2);
                assertThat(message + "(position: " + (totalRead + i) + ')', i, equalTo(-1));
                if (nRead1 < BUFFER_SIZE) {
                    // we've reached the end of the files, but found no mismatch
                    break;
                }
                totalRead += nRead1;
            }
        }
    }

    private static DiscoveryNode findIndexNode(Index index, int shardId) {
        for (IndicesService indicesService : internalCluster().getDataNodeInstances(IndicesService.class)) {
            if (DiscoveryNode.hasRole(indicesService.clusterService().getSettings(), DiscoveryNodeRole.INDEX_ROLE)) {
                IndexService indexService = indicesService.indexService(index);
                if (indexService != null) {
                    IndexShard shardOrNull = indexService.getShardOrNull(shardId);
                    if (shardOrNull != null && shardOrNull.isActive()) {
                        assertTrue(shardOrNull.routingEntry().primary());
                        return indicesService.clusterService().localNode();
                    }
                }
            }
        }
        throw new AssertionError("Cannot finding indexing node for: " + shardId);
    }

    private static IndexShard findIndexShard(Index index, int shardId) {
        IndexShard indexShard = null;
        for (IndicesService indicesService : internalCluster().getDataNodeInstances(IndicesService.class)) {
            if (DiscoveryNode.hasRole(indicesService.clusterService().getSettings(), DiscoveryNodeRole.INDEX_ROLE)) {
                IndexService indexService = indicesService.indexService(index);
                if (indexService != null) {
                    IndexShard shardOrNull = indexService.getShardOrNull(shardId);
                    // TODO: Don't filter on primary once allocation fixed
                    if (shardOrNull != null && shardOrNull.isActive() && shardOrNull.routingEntry().primary()) {
                        indexShard = shardOrNull;
                    }
                }
            }
        }
        assertThat("IndexShard instance not found on nodes with [index] role for " + shardId, indexShard, notNullValue());
        assertThat("IndexShard should be primary but got " + indexShard.routingEntry(), indexShard.routingEntry().primary(), is(true));
        return indexShard;
    }

    private static IndexShard findSearchShard(Index index, int shardId) {
        IndexShard indexShard = null;
        for (IndicesService indicesService : internalCluster().getDataNodeInstances(IndicesService.class)) {
            if (DiscoveryNode.hasRole(indicesService.clusterService().getSettings(), DiscoveryNodeRole.SEARCH_ROLE)) {
                IndexService indexService = indicesService.indexService(index);
                if (indexService != null) {
                    IndexShard shardOrNull = indexService.getShardOrNull(shardId);
                    // TODO: Don't filter on primary once allocation fixed
                    if (shardOrNull != null && shardOrNull.isActive() && shardOrNull.routingEntry().primary() == false) {
                        indexShard = shardOrNull;
                    }
                }
            }
        }
        assertThat("IndexShard instance not found on nodes with [search] role for " + shardId, indexShard, notNullValue());
        assertThat("IndexShard should be replica but got " + indexShard.routingEntry(), indexShard.routingEntry().primary(), is(false));
        return indexShard;
    }

    private static Map<Index, Integer> resolveIndices() {
        return client().admin()
            .indices()
            .prepareGetIndex()
            .get()
            .getSettings()
            .values()
            .stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    settings -> new Index(
                        settings.get(IndexMetadata.SETTING_INDEX_PROVIDED_NAME),
                        settings.get(IndexMetadata.SETTING_INDEX_UUID)
                    ),
                    settings -> settings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 0)
                )
            );
    }
}
