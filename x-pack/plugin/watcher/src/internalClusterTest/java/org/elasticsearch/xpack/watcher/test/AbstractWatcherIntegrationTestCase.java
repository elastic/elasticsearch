/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.template.get.GetComposableIndexTemplateAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockMustacheScriptEngine;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.disruption.ServiceDisruptionScheme;
import org.elasticsearch.test.store.MockFSIndexStore;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.watcher.WatcherField;
import org.elasticsearch.xpack.core.watcher.WatcherState;
import org.elasticsearch.xpack.core.watcher.execution.ExecutionState;
import org.elasticsearch.xpack.core.watcher.execution.TriggeredWatchStoreField;
import org.elasticsearch.xpack.core.watcher.history.HistoryStoreField;
import org.elasticsearch.xpack.core.watcher.support.WatcherIndexTemplateRegistryField;
import org.elasticsearch.xpack.core.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.core.watcher.transport.actions.service.WatcherServiceRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.stats.WatcherStatsRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.stats.WatcherStatsResponse;
import org.elasticsearch.xpack.core.watcher.watch.ClockMock;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.ilm.IndexLifecycle;
import org.elasticsearch.xpack.watcher.ClockHolder;
import org.elasticsearch.xpack.watcher.notification.email.Authentication;
import org.elasticsearch.xpack.watcher.notification.email.Email;
import org.elasticsearch.xpack.watcher.notification.email.EmailService;
import org.elasticsearch.xpack.watcher.notification.email.Profile;
import org.elasticsearch.xpack.watcher.trigger.ScheduleTriggerEngineMock;
import org.elasticsearch.xpack.watcher.watch.WatchParser;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.core.watcher.support.WatcherIndexTemplateRegistryField.HISTORY_TEMPLATE_NAME;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.mockito.Mockito.mock;

/**
 * Base class for Watcher integration tests
 *
 * Note that SLM has been observed to cause timing issues during testsuite teardown:
 * https://github.com/elastic/elasticsearch/issues/50302
 */
@ClusterScope(scope = SUITE, numClientNodes = 0, maxNumDataNodes = 3)
public abstract class AbstractWatcherIntegrationTestCase extends ESIntegTestCase {

    private TimeWarp timeWarp;

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(XPackSettings.SECURITY_ENABLED.getKey(), false)
            .put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial")
            // we do this by default in core, but for watcher this isn't needed and only adds noise.
            .put("index.store.mock.check_index_on_close", false)
            // watcher settings that should work despite randomization
            .put("xpack.watcher.execution.scroll.size", randomIntBetween(1, 100))
            .put("xpack.watcher.watch.scroll.size", randomIntBetween(1, 100))
            .put("indices.lifecycle.history_index_enabled", false)
            .build();
    }

    @Override
    protected Set<String> excludeTemplates() {
        Set<String> excludes = new HashSet<>();
        excludes.addAll(Arrays.asList(WatcherIndexTemplateRegistryField.TEMPLATE_NAMES));
        return Collections.unmodifiableSet(excludes);
    }

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        Set<Class<? extends Plugin>> plugins = new HashSet<>(super.getMockPlugins());
        // security has its own transport service
        plugins.remove(MockTransportService.TestPlugin.class);
        // security has its own transport
        // we have to explicitly add it otherwise we will fail to set the check_index_on_close setting
        plugins.add(MockFSIndexStore.TestPlugin.class);
        plugins.add(MockMustacheScriptEngine.TestPlugin.class);
        return plugins;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginTypes();
    }

    protected List<Class<? extends Plugin>> pluginTypes() {
        List<Class<? extends Plugin>> types = new ArrayList<>();

        if (timeWarped()) {
            types.add(TimeWarpedWatcher.class);
        } else {
            types.add(LocalStateWatcher.class);
        }

        types.add(CommonAnalysisPlugin.class);
        // ILM is required for watcher template index settings
        types.add(IndexLifecycle.class);
        types.add(DataStreamsPlugin.class);
        return types;
    }

    /**
     * @return  whether the test suite should run in time warp mode. By default this will be determined globally
     *          to all test suites based on {@code -Dtests.timewarp} system property (when missing, defaults to
     *          {@code true}). If a test suite requires to force the mode or force not running under this mode
     *          this method can be overridden.
     */
    protected boolean timeWarped() {
        return true;
    }

    @Before
    public void _setup() throws Exception {
        if (timeWarped()) {
            timeWarp = new TimeWarp(
                internalCluster().getInstances(ScheduleTriggerEngineMock.class),
                (ClockMock) getInstanceFromMaster(ClockHolder.class).clock
            );
        }

        if (internalCluster().size() > 0) {
            ensureLicenseEnabled();

            if (timeWarped()) {
                // now that the license is enabled and valid we can freeze all nodes clocks
                logger.info("[{}#{}]: freezing time on nodes", getTestClass().getSimpleName(), getTestName());
                TimeFreezeDisruption ice = new TimeFreezeDisruption();
                internalCluster().setDisruptionScheme(ice);
                ice.startDisrupting();
            }
            stopWatcher();
            createWatcherIndicesOrAliases();
            startWatcher();
        }
    }

    @After
    public void _cleanup() throws Exception {
        // Clear all internal watcher state for the next test method:
        logger.info("[#{}]: clearing watcher state", getTestName());
        stopWatcher();
        // Wait for all pending tasks to complete, this to avoid any potential incoming writes
        // to watcher history data stream to recreate the data stream after it has been created.
        // Otherwise ESIntegTestCase test cluster's wipe cluster logic that deletes all indices may fail,
        // because it attempts to remove the write index of an existing data stream.
        waitNoPendingTasksOnAll();
    }

    /**
     * In order to test, that .watches and .triggered-watches indices can also point to an alias, we will rarely create those
     * after starting watcher
     *
     * The idea behind this is the possible use of the migration helper for upgrades, see
     * https://github.com/elastic/elasticsearch-migration/
     *
     */
    private void createWatcherIndicesOrAliases() throws Exception {
        if (internalCluster().size() > 0) {
            ensureWatcherTemplatesAdded();
            // alias for .watches, setting the index template to the same as well
            String watchIndexName;
            String triggeredWatchIndexName;
            if (randomBoolean()) {
                // Create an index to get the template
                CreateIndexResponse response = indicesAdmin().prepareCreate(Watch.INDEX)
                    .setCause("Index to test aliases with .watches index")
                    .get();
                assertAcked(response);

                // Now replace it with a randomly named index
                watchIndexName = randomAlphaOfLengthBetween(5, 10).toLowerCase(Locale.ROOT);
                replaceWatcherIndexWithRandomlyNamedIndex(Watch.INDEX, watchIndexName);

                logger.info("set alias for .watches index to [{}]", watchIndexName);
            } else {
                watchIndexName = Watch.INDEX;
                Settings.Builder builder = Settings.builder();
                if (randomBoolean()) {
                    builder.put("index.number_of_shards", scaledRandomIntBetween(1, 5));
                }
                assertAcked(indicesAdmin().prepareCreate(watchIndexName).setSettings(builder));
            }

            // alias for .triggered-watches, ensuring the index template is set appropriately
            if (randomBoolean()) {
                CreateIndexResponse response = indicesAdmin().prepareCreate(TriggeredWatchStoreField.INDEX_NAME)
                    .setCause("Index to test aliases with .triggered-watches index")
                    .get();
                assertAcked(response);

                // Now replace it with a randomly-named index
                triggeredWatchIndexName = randomValueOtherThan(
                    watchIndexName,
                    () -> randomAlphaOfLengthBetween(5, 10).toLowerCase(Locale.ROOT)
                );
                replaceWatcherIndexWithRandomlyNamedIndex(TriggeredWatchStoreField.INDEX_NAME, triggeredWatchIndexName);
                logger.info("set alias for .triggered-watches index to [{}]", triggeredWatchIndexName);
            } else {
                triggeredWatchIndexName = TriggeredWatchStoreField.INDEX_NAME;
                assertAcked(indicesAdmin().prepareCreate(triggeredWatchIndexName));
            }
        }
    }

    public void replaceWatcherIndexWithRandomlyNamedIndex(String originalIndexOrAlias, String to) {
        GetIndexResponse index = indicesAdmin().prepareGetIndex().setIndices(originalIndexOrAlias).get();
        MappingMetadata mapping = index.getMappings().get(index.getIndices()[0]);

        Settings settings = index.getSettings().get(index.getIndices()[0]);
        Settings.Builder newSettings = Settings.builder().put(settings);
        newSettings.remove("index.provided_name");
        newSettings.remove("index.uuid");
        newSettings.remove("index.creation_date");
        newSettings.remove("index.version.created");

        CreateIndexResponse createIndexResponse = indicesAdmin().prepareCreate(to)
            .setMapping(mapping.sourceAsMap())
            .setSettings(newSettings)
            .get();
        assertTrue(createIndexResponse.isAcknowledged());
        ensureGreen(to);

        AtomicReference<String> originalIndex = new AtomicReference<>(originalIndexOrAlias);
        boolean watchesIsAlias = indicesAdmin().prepareGetAliases(originalIndexOrAlias).get().getAliases().isEmpty() == false;
        if (watchesIsAlias) {
            GetAliasesResponse aliasesResponse = indicesAdmin().prepareGetAliases(originalIndexOrAlias).get();
            assertEquals(1, aliasesResponse.getAliases().size());
            aliasesResponse.getAliases().entrySet().forEach((aliasRecord) -> {
                assertEquals(1, aliasRecord.getValue().size());
                originalIndex.set(aliasRecord.getKey());
            });
        }
        indicesAdmin().prepareDelete(originalIndex.get()).get();
        indicesAdmin().prepareAliases().addAlias(to, originalIndexOrAlias).get();
    }

    protected TimeWarp timeWarp() {
        assert timeWarped() : "cannot access TimeWarp when test context is not time warped";
        return timeWarp;
    }

    public boolean randomizeNumberOfShardsAndReplicas() {
        return false;
    }

    protected long docCount(String index, QueryBuilder query) {
        refresh();
        return docCount(index, SearchSourceBuilder.searchSource().query(query));
    }

    protected long watchRecordCount(QueryBuilder query) {
        refresh();
        return docCount(HistoryStoreField.DATA_STREAM + "*", SearchSourceBuilder.searchSource().query(query));
    }

    protected long docCount(String index, SearchSourceBuilder source) {
        SearchRequestBuilder builder = client().prepareSearch(index).setSource(source).setSize(0);
        return builder.get().getHits().getTotalHits().value;
    }

    protected SearchResponse searchHistory(SearchSourceBuilder builder) {
        return client().prepareSearch(HistoryStoreField.DATA_STREAM + "*").setSource(builder).get();
    }

    protected <T> T getInstanceFromMaster(Class<T> type) {
        return internalCluster().getInstance(type, internalCluster().getMasterName());
    }

    protected WatchParser watchParser() {
        return getInstanceFromMaster(WatchParser.class);
    }

    private IndexNameExpressionResolver indexNameExpressionResolver() {
        return internalCluster().getInstance(IndexNameExpressionResolver.class);
    }

    @SuppressWarnings("unchecked")
    protected void assertValue(XContentSource source, String path, Matcher<?> matcher) {
        assertThat(source.getValue(path), (Matcher<Object>) matcher);
    }

    protected void assertWatchWithMinimumPerformedActionsCount(
        final String watchName,
        final long minimumExpectedWatchActionsWithActionPerformed
    ) throws Exception {
        assertWatchWithMinimumPerformedActionsCount(watchName, minimumExpectedWatchActionsWithActionPerformed, true);
    }

    // TODO remove this shitty method... the `assertConditionMet` is bogus
    protected void assertWatchWithMinimumPerformedActionsCount(
        final String watchName,
        final long minimumExpectedWatchActionsWithActionPerformed,
        final boolean assertConditionMet
    ) throws Exception {
        final AtomicReference<SearchResponse> lastResponse = new AtomicReference<>();
        try {
            assertBusy(() -> {
                ClusterState state = client().admin().cluster().prepareState().get().getState();
                String[] watchHistoryIndices = indexNameExpressionResolver().concreteIndexNames(
                    state,
                    IndicesOptions.lenientExpandOpen(),
                    true,
                    HistoryStoreField.DATA_STREAM + "*"
                );
                assertThat(watchHistoryIndices, not(emptyArray()));
                for (String index : watchHistoryIndices) {
                    IndexRoutingTable routingTable = state.getRoutingTable().index(index);
                    assertThat(routingTable, notNullValue());
                    assertThat(routingTable.allPrimaryShardsActive(), is(true));
                }

                refresh();
                SearchResponse searchResponse = client().prepareSearch(HistoryStoreField.DATA_STREAM + "*")
                    .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                    .setQuery(boolQuery().must(matchQuery("watch_id", watchName)).must(matchQuery("state", ExecutionState.EXECUTED.id())))
                    .get();
                lastResponse.set(searchResponse);
                assertThat(
                    "could not find executed watch record for watch " + watchName,
                    searchResponse.getHits().getTotalHits().value,
                    greaterThanOrEqualTo(minimumExpectedWatchActionsWithActionPerformed)
                );
                if (assertConditionMet) {
                    assertThat(
                        (Integer) XContentMapValues.extractValue(
                            "result.input.payload.hits.total",
                            searchResponse.getHits().getAt(0).getSourceAsMap()
                        ),
                        greaterThanOrEqualTo(1)
                    );
                }
            });
        } catch (AssertionError error) {
            SearchResponse searchResponse = lastResponse.get();
            logger.info("Found [{}] records for watch [{}]", searchResponse.getHits().getTotalHits().value, watchName);
            int counter = 1;
            for (SearchHit hit : searchResponse.getHits().getHits()) {
                logger.info("hit [{}]=\n {}", counter++, XContentHelper.convertToJson(hit.getSourceRef(), true, true));
            }
            throw error;
        }
    }

    protected SearchResponse searchWatchRecords(Consumer<SearchRequestBuilder> requestBuilderCallback) {
        SearchRequestBuilder builder = client().prepareSearch(HistoryStoreField.DATA_STREAM + "*");
        requestBuilderCallback.accept(builder);
        return builder.get();
    }

    protected long findNumberOfPerformedActions(String watchName) {
        refresh();
        SearchResponse searchResponse = client().prepareSearch(HistoryStoreField.DATA_STREAM + "*")
            .setIndicesOptions(IndicesOptions.lenientExpandOpen())
            .setQuery(boolQuery().must(matchQuery("watch_id", watchName)).must(matchQuery("state", ExecutionState.EXECUTED.id())))
            .get();
        return searchResponse.getHits().getTotalHits().value;
    }

    protected void assertWatchWithNoActionNeeded(final String watchName, final long expectedWatchActionsWithNoActionNeeded)
        throws Exception {
        final AtomicReference<SearchResponse> lastResponse = new AtomicReference<>();
        try {
            assertBusy(() -> {
                // The watch_history index gets created in the background when the first watch is triggered
                // so we to check first is this index is created and shards are started
                ClusterState state = client().admin().cluster().prepareState().get().getState();
                String[] watchHistoryIndices = indexNameExpressionResolver().concreteIndexNames(
                    state,
                    IndicesOptions.lenientExpandOpen(),
                    true,
                    HistoryStoreField.DATA_STREAM + "*"
                );
                assertThat(watchHistoryIndices, not(emptyArray()));
                for (String index : watchHistoryIndices) {
                    IndexRoutingTable routingTable = state.getRoutingTable().index(index);
                    assertThat(routingTable, notNullValue());
                    assertThat(routingTable.allPrimaryShardsActive(), is(true));
                }
                refresh();
                SearchResponse searchResponse = client().prepareSearch(HistoryStoreField.DATA_STREAM + "*")
                    .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                    .setQuery(
                        boolQuery().must(matchQuery("watch_id", watchName))
                            .must(matchQuery("state", ExecutionState.EXECUTION_NOT_NEEDED.id()))
                    )
                    .get();
                lastResponse.set(searchResponse);
                assertThat(searchResponse.getHits().getTotalHits().value, greaterThanOrEqualTo(expectedWatchActionsWithNoActionNeeded));
            });
        } catch (AssertionError error) {
            SearchResponse searchResponse = lastResponse.get();
            logger.info("Found [{}] records for watch [{}]", searchResponse.getHits().getTotalHits().value, watchName);
            int counter = 1;
            for (SearchHit hit : searchResponse.getHits().getHits()) {
                logger.info("hit [{}]=\n {}", counter++, XContentHelper.convertToJson(hit.getSourceRef(), true, true));
            }
            throw error;
        }
    }

    protected void assertWatchWithMinimumActionsCount(final String watchName, final ExecutionState recordState, final long recordCount)
        throws Exception {
        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            String[] watchHistoryIndices = indexNameExpressionResolver().concreteIndexNames(
                state,
                IndicesOptions.lenientExpandOpen(),
                true,
                HistoryStoreField.DATA_STREAM + "*"
            );
            assertThat(watchHistoryIndices, not(emptyArray()));
            for (String index : watchHistoryIndices) {
                IndexRoutingTable routingTable = state.getRoutingTable().index(index);
                assertThat(routingTable, notNullValue());
                assertThat(routingTable.allPrimaryShardsActive(), is(true));
            }

            refresh();
            SearchResponse searchResponse = client().prepareSearch(HistoryStoreField.DATA_STREAM + "*")
                .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .setQuery(boolQuery().must(matchQuery("watch_id", watchName)).must(matchQuery("state", recordState.id())))
                .get();
            assertThat(
                "could not find executed watch record",
                searchResponse.getHits().getTotalHits().value,
                greaterThanOrEqualTo(recordCount)
            );
        });
    }

    private void ensureWatcherTemplatesAdded() throws Exception {
        // Verify that the index templates exist:
        assertBusy(() -> {
            GetComposableIndexTemplateAction.Response response = client().execute(
                GetComposableIndexTemplateAction.INSTANCE,
                new GetComposableIndexTemplateAction.Request(HISTORY_TEMPLATE_NAME)
            ).get();
            assertThat("[" + HISTORY_TEMPLATE_NAME + "] is missing", response.indexTemplates().size(), equalTo(1));
        });
    }

    protected void startWatcher() throws Exception {
        assertBusy(() -> {
            WatcherStatsResponse watcherStatsResponse = new WatcherStatsRequestBuilder(client()).get();
            assertThat(watcherStatsResponse.hasFailures(), is(false));
            List<Tuple<String, WatcherState>> currentStatesFromStatsRequest = watcherStatsResponse.getNodes()
                .stream()
                .map(response -> Tuple.tuple(response.getNode().getName(), response.getWatcherState()))
                .collect(Collectors.toList());
            List<WatcherState> states = currentStatesFromStatsRequest.stream().map(Tuple::v2).collect(Collectors.toList());

            logger.info("waiting to start watcher, current states {}", currentStatesFromStatsRequest);

            boolean isAllStateStarted = states.stream().allMatch(w -> w == WatcherState.STARTED);
            if (isAllStateStarted) {
                return;
            }

            boolean isAnyStopping = states.stream().anyMatch(w -> w == WatcherState.STOPPING);
            if (isAnyStopping) {
                throw new AssertionError("at least one node is in state stopping, waiting to be stopped");
            }

            boolean isAllStateStopped = states.stream().allMatch(w -> w == WatcherState.STOPPED);
            if (isAllStateStopped) {
                assertAcked(new WatcherServiceRequestBuilder(client()).start().get());
                throw new AssertionError("all nodes are stopped, restarting");
            }

            boolean isAnyStarting = states.stream().anyMatch(w -> w == WatcherState.STARTING);
            if (isAnyStarting) {
                throw new AssertionError("at least one node is in state starting, waiting to be stopped");
            }

            throw new AssertionError("unexpected state, retrying with next run");
        }, 30, TimeUnit.SECONDS);
    }

    protected void ensureLicenseEnabled() throws Exception {
        assertBusy(() -> {
            for (XPackLicenseState licenseState : internalCluster().getInstances(XPackLicenseState.class)) {
                assertThat(WatcherField.WATCHER_FEATURE.check(licenseState), is(true));
            }
        });
    }

    protected void stopWatcher() throws Exception {
        assertBusy(() -> {

            WatcherStatsResponse watcherStatsResponse = new WatcherStatsRequestBuilder(client()).setIncludeCurrentWatches(true).get();
            assertThat(watcherStatsResponse.hasFailures(), is(false));
            List<Tuple<String, WatcherState>> currentStatesFromStatsRequest = watcherStatsResponse.getNodes()
                .stream()
                .map(
                    response -> Tuple.tuple(
                        response.getNode().getName() + " (" + response.getThreadPoolQueueSize() + ")",
                        response.getWatcherState()
                    )
                )
                .collect(Collectors.toList());
            List<WatcherState> states = currentStatesFromStatsRequest.stream().map(Tuple::v2).collect(Collectors.toList());

            long currentWatches = watcherStatsResponse.getNodes().stream().mapToLong(n -> n.getSnapshots().size()).sum();
            logger.info("waiting to stop watcher, current states {}, current watches [{}]", currentStatesFromStatsRequest, currentWatches);

            boolean isAllStateStarted = states.stream().allMatch(w -> w == WatcherState.STARTED);
            if (isAllStateStarted) {
                assertAcked(new WatcherServiceRequestBuilder(client()).stop().get());
                throw new AssertionError("all nodes are started, stopping");
            }

            boolean isAnyStopping = states.stream().anyMatch(w -> w == WatcherState.STOPPING);
            if (isAnyStopping) {
                throw new AssertionError("at least one node is in state stopping, waiting to be stopped");
            }

            boolean isAllStateStopped = states.stream().allMatch(w -> w == WatcherState.STOPPED);
            if (isAllStateStopped && currentWatches == 0) {
                return;
            }

            boolean isAnyStarting = states.stream().anyMatch(w -> w == WatcherState.STARTING);
            if (isAnyStarting) {
                throw new AssertionError("at least one node is in state starting, waiting to be started before stopping");
            }

            throw new AssertionError("unexpected state, retrying with next run");
        }, 60, TimeUnit.SECONDS);
    }

    public static class NoopEmailService extends EmailService {

        public NoopEmailService() {
            super(
                Settings.EMPTY,
                null,
                mock(SSLService.class),
                new ClusterSettings(Settings.EMPTY, new HashSet<>(EmailService.getSettings()))
            );
        }

        @Override
        public EmailSent send(Email email, Authentication auth, Profile profile, String accountName) {
            return new EmailSent(accountName, email);
        }
    }

    protected static class TimeWarp {
        private static final Logger logger = LogManager.getLogger(TimeWarp.class);

        private final List<ScheduleTriggerEngineMock> schedulers;
        private final ClockMock clock;

        TimeWarp(Iterable<ScheduleTriggerEngineMock> schedulers, ClockMock clock) {
            this.schedulers = StreamSupport.stream(schedulers.spliterator(), false).collect(Collectors.toList());
            this.clock = clock;
        }

        public void trigger(String jobName) throws Exception {
            trigger(jobName, 1, null);
        }

        public ClockMock clock() {
            return clock;
        }

        public void trigger(String watchId, int times, TimeValue timeValue) throws Exception {
            assertBusy(() -> {
                long triggeredCount = schedulers.stream().filter(scheduler -> scheduler.trigger(watchId, times, timeValue)).count();
                String msg = Strings.format("watch was triggered on [%d] schedulers, expected [1]", triggeredCount);
                if (triggeredCount > 1) {
                    logger.warn(msg);
                }
                assertThat(msg, triggeredCount, greaterThanOrEqualTo(1L));
            });
        }
    }

    /**
     * A disruption that prevents time from advancing on nodes. This is needed to allow time sensitive tests
     * to have full control of time. This disruption requires {@link ClockMock} being available on the nodes.
     */
    private static class TimeFreezeDisruption implements ServiceDisruptionScheme {

        private InternalTestCluster cluster;
        private boolean frozen;

        @Override
        public void applyToCluster(InternalTestCluster cluster) {
            this.cluster = cluster;
        }

        @Override
        public void removeFromCluster(InternalTestCluster cluster) {
            stopDisrupting();
        }

        @Override
        public void removeAndEnsureHealthy(InternalTestCluster cluster) {
            stopDisrupting();
        }

        @Override
        public synchronized void applyToNode(String node, InternalTestCluster cluster) {
            if (frozen) {
                ((ClockMock) cluster.getInstance(ClockHolder.class, node).clock).freeze();
            }
        }

        @Override
        public void removeFromNode(String node, InternalTestCluster cluster) {
            ((ClockMock) cluster.getInstance(ClockHolder.class, node).clock).unfreeze();
        }

        @Override
        public synchronized void startDisrupting() {
            frozen = true;
            for (String node : cluster.getNodeNames()) {
                applyToNode(node, cluster);
            }
        }

        @Override
        public void stopDisrupting() {
            frozen = false;
            for (String node : cluster.getNodeNames()) {
                removeFromNode(node, cluster);
            }
        }

        @Override
        public void testClusterClosed() {}

        @Override
        public TimeValue expectedTimeToHeal() {
            return TimeValue.ZERO;
        }

        @Override
        public String toString() {
            return "time frozen";
        }
    }
}
