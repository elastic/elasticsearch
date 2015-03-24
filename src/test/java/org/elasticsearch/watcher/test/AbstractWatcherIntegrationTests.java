/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.test;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.watcher.watch.Watch;
import org.elasticsearch.watcher.WatcherPlugin;
import org.elasticsearch.watcher.watch.WatchService;
import org.elasticsearch.watcher.actions.email.service.Authentication;
import org.elasticsearch.watcher.actions.email.service.Email;
import org.elasticsearch.watcher.actions.email.service.EmailService;
import org.elasticsearch.watcher.actions.email.service.Profile;
import org.elasticsearch.watcher.actions.webhook.HttpClient;
import org.elasticsearch.watcher.client.WatcherClient;
import org.elasticsearch.watcher.history.WatchRecord;
import org.elasticsearch.watcher.history.HistoryStore;
import org.elasticsearch.watcher.scheduler.Scheduler;
import org.elasticsearch.watcher.scheduler.SchedulerMock;
import org.elasticsearch.watcher.scheduler.schedule.Schedule;
import org.elasticsearch.watcher.scheduler.schedule.Schedules;
import org.elasticsearch.watcher.support.WatcherUtils;
import org.elasticsearch.watcher.support.clock.ClockMock;
import org.elasticsearch.watcher.support.init.proxy.ScriptServiceProxy;
import org.elasticsearch.watcher.support.template.Template;
import org.elasticsearch.watcher.transport.actions.stats.WatcherStatsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.common.netty.util.internal.SystemPropertyUtil;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.TestCluster;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope.SUITE;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;

/**
 */
@ClusterScope(scope = SUITE, numClientNodes = 0, transportClientRatio = 0, randomDynamicTemplates = false)
public abstract class AbstractWatcherIntegrationTests extends ElasticsearchIntegrationTest {

    private static final boolean timeWarpEnabled = SystemPropertyUtil.getBoolean("tests.timewarp", true);

    private TimeWarp timeWarp;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("scroll.size", randomIntBetween(1, 100))
                .put("plugin.types", timeWarped() ? TimeWarpedWatcherPlugin.class.getName() : WatcherPlugin.class.getName())
                .build();
    }

    /**
     * @return  whether the test suite should run in time warp mode. By default this will be determined globally
     *          to all test suites based on {@code -Dtests.timewarp} system property (when missing, defaults to
     *          {@code true}). If a test suite requires to force the mode or force not running under this mode
     *          this method can be overridden.
     */
    protected boolean timeWarped() {
        return timeWarpEnabled;
    }

    @Before
    public void setupTimeWarp() throws Exception {
        if (timeWarped()) {
            timeWarp = new TimeWarp(
                    internalTestCluster().getInstance(SchedulerMock.class, internalTestCluster().getMasterName()),
                    internalTestCluster().getInstance(ClockMock.class, internalTestCluster().getMasterName()));
        }
    }

    protected TimeWarp timeWarp() {
        assert timeWarped() : "cannot access TimeWarp when test context is not time warped";
        return timeWarp;
    }

    public boolean randomizeNumberOfShardsAndReplicas() {
        return false;
    }

    @Override
    protected TestCluster buildTestCluster(Scope scope, long seed) throws IOException {
        // This overwrites the wipe logic of the test cluster to not remove the watches and watch_history templates. By default all templates are removed
        // TODO: We should have the notion of a hidden template (like hidden index / type) that only gets removed when specifically mentioned.
        final TestCluster testCluster = super.buildTestCluster(scope, seed);
        return new WatcherWrappingCluster(seed, testCluster);
    }

    @Before
    public void startWatcherIfNodesExist() throws Exception {
        if (internalTestCluster().size() > 0) {
            WatcherStatsResponse response = watcherClient().prepareWatcherStats().get();
            if (response.getWatchServiceState() == WatchService.State.STOPPED) {
                logger.info("[{}#{}]: starting watcher", getTestClass().getSimpleName(), getTestName());
                startWatcher();
            } else {
                logger.info("[{}#{}]: not starting watcher, because watcher is in state [{}]", getTestClass().getSimpleName(), getTestName(), response.getWatchServiceState());
            }
        } else {
            logger.info("[{}#{}]: not starting watcher, because test cluster has no nodes", getTestClass().getSimpleName(), getTestName());
        }
    }

    @After
    public void clearWatches() throws Exception {
        // Clear all internal watcher state for the next test method:
        logger.info("[{}#{}]: clearing watches", getTestClass().getSimpleName(), getTestName());
        stopWatcher();
    }

    protected long docCount(String index, String type, QueryBuilder query) {
        return docCount(index, type, SearchSourceBuilder.searchSource().query(query));
    }

    protected long docCount(String index, String type, SearchSourceBuilder source) {
        SearchRequestBuilder builder = client().prepareSearch(index).setSearchType(SearchType.COUNT);
        if (type != null) {
            builder.setTypes(type);
        }
        builder.setSource(source.buildAsBytes());
        return builder.get().getHits().getTotalHits();
    }

    protected BytesReference createWatchSource(String cron, SearchRequest conditionRequest, String conditionScript) throws IOException {
        return createWatchSource(cron, conditionRequest, conditionScript, null);
    }

    protected BytesReference createWatchSource(Schedule schedule, SearchRequest conditionRequest, String conditionScript) throws IOException {
        return createWatchSource(schedule, conditionRequest, conditionScript, null);
    }

    protected BytesReference createWatchSource(String cron, SearchRequest conditionRequest, String conditionScript, Map<String, Object> metadata) throws IOException {
        return createWatchSource(Schedules.cron(cron), conditionRequest, conditionScript, metadata);
    }

    protected BytesReference createWatchSource(Schedule schedule, SearchRequest conditionRequest, String conditionScript, Map<String, Object> metadata) throws IOException {
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        {
            builder.startObject("schedule")
                    .field(schedule.type(), schedule)
                    .endObject();

            if (metadata != null) {
                builder.field("meta", metadata);
            }

            builder.startObject("input");
            {
                builder.field("search");
                WatcherUtils.writeSearchRequest(conditionRequest, builder, ToXContent.EMPTY_PARAMS);
            }
            builder.endObject();

            builder.startObject("condition");
            {
                builder.startObject("script");
                builder.field("script", conditionScript);
                builder.endObject();
            }
            builder.endObject();


            builder.startArray("actions");
            {
                builder.startObject();
                {
                    builder.startObject("index");
                    builder.field("index", "my-index");
                    builder.field("type", "trail");
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endArray();
        }
        builder.endObject();

        return builder.bytes();
    }

    protected Watch.Parser watchParser() {
        return internalTestCluster().getInstance(Watch.Parser.class, internalTestCluster().getMasterName());
    }

    protected Scheduler scheduler() {
        return internalTestCluster().getInstance(Scheduler.class, internalTestCluster().getMasterName());
    }

    protected WatcherClient watcherClient() {
        return internalTestCluster().getInstance(WatcherClient.class);
    }

    protected ScriptServiceProxy scriptService() {
        return internalTestCluster().getInstance(ScriptServiceProxy.class);
    }

    protected Template.Parser templateParser() {
        return internalTestCluster().getInstance(Template.Parser.class);
    }

    protected HttpClient httpClient() {
        return internalTestCluster().getInstance(HttpClient.class);
    }

    protected EmailService noopEmailService() {
        return new NoopEmailService();
    }

    protected WatchRecord.Parser watchRecordParser() {
        return internalTestCluster().getInstance(WatchRecord.Parser.class);
    }

    protected void assertWatchWithExactPerformedActionsCount(final String watchName, final long expectedWatchActionsWithActionPerformed) throws Exception {
        assertBusy(new Runnable() {
            @Override
            public void run() {
                ClusterState state = client().admin().cluster().prepareState().get().getState();
                String[] watchHistoryIndices = state.metaData().concreteIndices(IndicesOptions.lenientExpandOpen(), HistoryStore.INDEX_PREFIX + "*");
                assertThat(watchHistoryIndices, not(emptyArray()));
                for (String index : watchHistoryIndices) {
                    IndexRoutingTable routingTable = state.getRoutingTable().index(index);
                    assertThat(routingTable, notNullValue());
                    assertThat(routingTable.allPrimaryShardsActive(), is(true));
                }

                assertThat(findNumberOfPerformedActions(watchName), equalTo(expectedWatchActionsWithActionPerformed));
            }
        });
    }

    protected void assertWatchWithMinimumPerformedActionsCount(final String watchName, final long minimumExpectedWatchActionsWithActionPerformed) throws Exception {
        assertWatchWithMinimumPerformedActionsCount(watchName, minimumExpectedWatchActionsWithActionPerformed, true);
    }

    protected void assertWatchWithMinimumPerformedActionsCount(final String watchName, final long minimumExpectedWatchActionsWithActionPerformed, final boolean assertConditionMet) throws Exception {
        assertBusy(new Runnable() {
            @Override
            public void run() {
                ClusterState state = client().admin().cluster().prepareState().get().getState();
                String[] watchHistoryIndices = state.metaData().concreteIndices(IndicesOptions.lenientExpandOpen(), HistoryStore.INDEX_PREFIX + "*");
                assertThat(watchHistoryIndices, not(emptyArray()));
                for (String index : watchHistoryIndices) {
                    IndexRoutingTable routingTable = state.getRoutingTable().index(index);
                    assertThat(routingTable, notNullValue());
                    assertThat(routingTable.allPrimaryShardsActive(), is(true));
                }

                SearchResponse searchResponse = client().prepareSearch(HistoryStore.INDEX_PREFIX + "*")
                        .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                        .setQuery(boolQuery().must(matchQuery("watch_name", watchName)).must(matchQuery("state", WatchRecord.State.EXECUTED.id())))
                        .get();
                assertThat("could not find executed watch record", searchResponse.getHits().getTotalHits(), greaterThanOrEqualTo(minimumExpectedWatchActionsWithActionPerformed));
                if (assertConditionMet) {
                    assertThat((Integer) XContentMapValues.extractValue("watch_execution.input_result.search.payload.hits.total", searchResponse.getHits().getAt(0).sourceAsMap()), greaterThanOrEqualTo(1));
                }
            }
        });
    }

    protected long findNumberOfPerformedActions(String watchName) {
        SearchResponse searchResponse = client().prepareSearch(HistoryStore.INDEX_PREFIX + "*")
                .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .setQuery(boolQuery().must(matchQuery("watch_name", watchName)).must(matchQuery("state", WatchRecord.State.EXECUTED.id())))
                .get();
        return searchResponse.getHits().getTotalHits();
    }

    protected void assertWatchWithNoActionNeeded(final String watchName, final long expectedWatchActionsWithNoActionNeeded) throws Exception {
        assertBusy(new Runnable() {
            @Override
            public void run() {
                // The watch_history index gets created in the background when the first watch is triggered, so we to check first is this index is created and shards are started
                ClusterState state = client().admin().cluster().prepareState().get().getState();
                String[] watchHistoryIndices = state.metaData().concreteIndices(IndicesOptions.lenientExpandOpen(), HistoryStore.INDEX_PREFIX + "*");
                assertThat(watchHistoryIndices, not(emptyArray()));
                for (String index : watchHistoryIndices) {
                    IndexRoutingTable routingTable = state.getRoutingTable().index(index);
                    assertThat(routingTable, notNullValue());
                    assertThat(routingTable.allPrimaryShardsActive(), is(true));
                }

                SearchResponse searchResponse = client().prepareSearch(HistoryStore.INDEX_PREFIX + "*")
                        .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                        .setQuery(boolQuery().must(matchQuery("watch_name", watchName)).must(matchQuery("state", WatchRecord.State.EXECUTION_NOT_NEEDED.id())))
                        .get();
                assertThat(searchResponse.getHits().getTotalHits(), greaterThanOrEqualTo(expectedWatchActionsWithNoActionNeeded));
            }
        });
    }

    protected void ensureWatcherStarted() throws Exception {
        assertBusy(new Runnable() {
            @Override
            public void run() {
                assertThat(watcherClient().prepareWatcherStats().get().getWatchServiceState(), is(WatchService.State.STARTED));
            }
        });
    }

    protected void ensureWatcherStopped() throws Exception {
        assertBusy(new Runnable() {
            @Override
            public void run() {
                assertThat(watcherClient().prepareWatcherStats().get().getWatchServiceState(), is(WatchService.State.STOPPED));
            }
        });
    }

    protected void startWatcher() throws Exception {
        watcherClient().prepareWatchService().start().get();
        ensureWatcherStarted();
    }

    protected void stopWatcher() throws Exception {
        watcherClient().prepareWatchService().stop().get();
        ensureWatcherStopped();
    }

    protected static InternalTestCluster internalTestCluster() {
        return (InternalTestCluster) ((WatcherWrappingCluster) cluster()).testCluster;
    }

    private final class WatcherWrappingCluster extends TestCluster {

        private final TestCluster testCluster;

        private WatcherWrappingCluster(long seed, TestCluster testCluster) {
            super(seed);
            this.testCluster = testCluster;
        }

        @Override
        public void beforeTest(Random random, double transportClientRatio) throws IOException {
            testCluster.beforeTest(random, transportClientRatio);
        }

        @Override
        public void wipe() {
            wipeIndices("_all");
            wipeRepositories();

            if (size() > 0) {
                List<String> templatesToWipe = new ArrayList<>();
                ClusterState state = client().admin().cluster().prepareState().get().getState();
                for (ObjectObjectCursor<String, IndexTemplateMetaData> cursor : state.getMetaData().templates()) {
                    if (cursor.key.equals("watches") || cursor.key.equals("watch_history")) {
                        continue;
                    }
                    templatesToWipe.add(cursor.key);
                }
                if (!templatesToWipe.isEmpty()) {
                    wipeTemplates(templatesToWipe.toArray(new String[templatesToWipe.size()]));
                }
            }
        }

        @Override
        public void afterTest() throws IOException {
            testCluster.afterTest();
        }

        @Override
        public Client client() {
            return testCluster.client();
        }

        @Override
        public int size() {
            return testCluster.size();
        }

        @Override
        public int numDataNodes() {
            return testCluster.numDataNodes();
        }

        @Override
        public int numDataAndMasterNodes() {
            return testCluster.numDataAndMasterNodes();
        }

        @Override
        public int numBenchNodes() {
            return testCluster.numBenchNodes();
        }

        @Override
        public InetSocketAddress[] httpAddresses() {
            return testCluster.httpAddresses();
        }

        @Override
        public void close() throws IOException {
            InternalTestCluster _testCluster = (InternalTestCluster) testCluster;
            Set<String> nodes = new HashSet<>(Arrays.asList(_testCluster.getNodeNames()));
            String masterNode = _testCluster.getMasterName();
            nodes.remove(masterNode);

            // First manually stop watcher on non elected master node, this will prevent that watcher becomes active
            // on these nodes
            for (String node : nodes) {
                WatchService watchService = _testCluster.getInstance(WatchService.class, node);
                assertThat(watchService.state(), equalTo(WatchService.State.STOPPED));
                watchService.stop(); // Prevents these nodes from starting watcher when new elected master node is picked.
            }

            // Then stop watcher on elected master node and wait until watcher has stopped on it.
            final WatchService watchService = _testCluster.getInstance(WatchService.class, masterNode);
            try {
                assertBusy(new Runnable() {
                    @Override
                    public void run() {
                        assertThat(watchService.state(), not(equalTo(WatchService.State.STARTING)));
                    }
                });
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            watchService.stop();
            try {
                assertBusy(new Runnable() {
                    @Override
                    public void run() {
                        assertThat(watchService.state(), equalTo(WatchService.State.STOPPED));
                    }
                });
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            // Now when can close nodes, without watcher trying to become active while nodes briefly become master
            // during cluster shutdown.
            testCluster.close();
        }

        @Override
        public void ensureEstimatedStats() {
            testCluster.ensureEstimatedStats();
        }

        @Override
        public boolean hasFilterCache() {
            return testCluster.hasFilterCache();
        }

        @Override
        public String getClusterName() {
            return testCluster.getClusterName();
        }

        @Override
        public Iterator<Client> iterator() {
            return testCluster.iterator();
        }

    }

    private static class NoopEmailService implements EmailService {

        @Override
        public EmailSent send(Email email, Authentication auth, Profile profile) {
            return new EmailSent(auth.user(), email);
        }

        @Override
        public EmailSent send(Email email, Authentication auth, Profile profile, String accountName) {
            return new EmailSent(accountName, email);
        }
    }

    protected static class TimeWarp {

        protected final SchedulerMock scheduler;
        protected final ClockMock clock;

        public TimeWarp(SchedulerMock scheduler, ClockMock clock) {
            this.scheduler = scheduler;
            this.clock = clock;
        }

        public SchedulerMock scheduler() {
            return scheduler;
        }

        public ClockMock clock() {
            return clock;
        }
    }

}
