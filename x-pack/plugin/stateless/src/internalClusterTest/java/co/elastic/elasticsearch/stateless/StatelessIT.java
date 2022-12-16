package co.elastic.elasticsearch.stateless;

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
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexShard;
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

    public void testUploadToObjectStore() throws Exception {
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

        assertObjectStoreConsistency();

        final int iters = randomIntBetween(1, 20);
        for (int i = 0; i < iters; i++) {
            indexDocs(indexName, randomIntBetween(1, 100));
            switch (randomInt(2)) {
                case 0 -> client().admin().indices().prepareFlush().setForce(randomBoolean()).get();
                case 1 -> client().admin().indices().prepareRefresh().get();
                case 2 -> client().admin().indices().prepareForceMerge().get();
            }
            assertObjectStoreConsistency();
        }
    }

    private static void assertObjectStoreConsistency() throws IOException {
        final Map<Index, Integer> indices = resolveIndices();
        assertThat(indices.isEmpty(), is(false));

        for (Map.Entry<Index, Integer> entry : indices.entrySet()) {
            assertThat(entry.getValue(), greaterThan(0));
            for (int shardId = 0; shardId < entry.getValue(); shardId++) {
                assertThatObjectStoreIsConsistentWithLastCommit(findIndexShard(entry.getKey(), shardId));
            }
        }
    }

    private static void assertThatObjectStoreIsConsistentWithLastCommit(final IndexShard indexShard) throws IOException {
        final Store store = indexShard.store();
        store.incRef();
        try {
            var blobContainer = internalCluster().getDataNodeInstance(ObjectStoreService.class)
                .getBlobContainer(indexShard.shardId(), indexShard.getOperationPrimaryTerm());

            final SegmentInfos segmentInfos = Lucene.readSegmentInfos(store.directory());

            for (String file : segmentInfos.files(true)) {
                // can take some time for files to be uploaded to the object store
                assertBusy(() -> {
                    assertThat("" + file, blobContainer.blobExists(file), is(true));

                    try (
                        IndexInput input = store.directory().openInput(file, IOContext.READONCE);
                        InputStream local = new InputStreamIndexInput(input, input.length());
                        InputStream remote = blobContainer.readBlob(file)
                    ) {
                        assertEquals("File [" + file + "] in object store has a different content than local file ", local, remote);
                    }
                });
            }
        } catch (Exception e) {
            throw new AssertionError(e);
        } finally {
            store.decRef();
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

    private static IndexShard findIndexShard(Index index, int shardId) {
        IndexShard indexShard = null;
        for (IndicesService indicesService : internalCluster().getDataNodeInstances(IndicesService.class)) {
            if (DiscoveryNode.hasRole(indicesService.clusterService().getSettings(), DiscoveryNodeRole.INDEX_ROLE)) {
                IndexService indexService = indicesService.indexService(index);
                if (indexService != null) {
                    IndexShard shardOrNull = indexService.getShardOrNull(shardId);
                    if (shardOrNull != null && shardOrNull.isActive()) {
                        indexShard = shardOrNull;
                    }
                }
            }
        }
        assertThat("IndexShard instance not found on nodes with [index] role for " + shardId, indexShard, notNullValue());
        assertThat("IndexShard should be primary but got " + indexShard.routingEntry(), indexShard.routingEntry().primary(), is(true));
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
