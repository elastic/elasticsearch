package co.elastic.elasticsearch.stateless;

import co.elastic.elasticsearch.stateless.lucene.DefaultDirectoryListener;
import co.elastic.elasticsearch.stateless.lucene.StatelessDirectory;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.util.Locale;
import java.util.stream.StreamSupport;

import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
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
                    .put(INDEX_SOFT_DELETES_SETTING.getKey(), true)
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
}
