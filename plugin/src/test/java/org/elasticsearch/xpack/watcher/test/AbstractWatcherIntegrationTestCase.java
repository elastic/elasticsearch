/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.test;

import io.netty.util.internal.SystemPropertyUtil;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.Callback;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockMustacheScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.TestCluster;
import org.elasticsearch.test.disruption.ServiceDisruptionScheme;
import org.elasticsearch.test.store.MockFSIndexStore;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.TimeWarpedXPackPlugin;
import org.elasticsearch.xpack.XPackClient;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.common.http.HttpClient;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.notification.email.Authentication;
import org.elasticsearch.xpack.notification.email.Email;
import org.elasticsearch.xpack.notification.email.EmailService;
import org.elasticsearch.xpack.notification.email.Profile;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.authc.file.FileRealm;
import org.elasticsearch.xpack.security.authc.support.Hasher;
import org.elasticsearch.xpack.security.authc.support.SecuredString;
import org.elasticsearch.xpack.security.crypto.CryptoService;
import org.elasticsearch.xpack.support.clock.ClockMock;
import org.elasticsearch.xpack.template.TemplateUtils;
import org.elasticsearch.xpack.watcher.WatcherLifeCycleService;
import org.elasticsearch.xpack.watcher.WatcherService;
import org.elasticsearch.xpack.watcher.WatcherState;
import org.elasticsearch.xpack.watcher.client.WatcherClient;
import org.elasticsearch.xpack.watcher.execution.ExecutionState;
import org.elasticsearch.xpack.watcher.execution.TriggeredWatchStore;
import org.elasticsearch.xpack.watcher.history.HistoryStore;
import org.elasticsearch.xpack.watcher.support.WatcherIndexTemplateRegistry;
import org.elasticsearch.xpack.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.watcher.trigger.ScheduleTriggerEngineMock;
import org.elasticsearch.xpack.watcher.watch.Watch;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.xpack.watcher.support.WatcherIndexTemplateRegistry.HISTORY_TEMPLATE_NAME;
import static org.elasticsearch.xpack.watcher.support.WatcherIndexTemplateRegistry.TRIGGERED_TEMPLATE_NAME;
import static org.elasticsearch.xpack.watcher.support.WatcherIndexTemplateRegistry.WATCHES_TEMPLATE_NAME;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;

@ClusterScope(scope = SUITE, numClientNodes = 0, transportClientRatio = 0, randomDynamicTemplates = false, maxNumDataNodes = 3)
public abstract class AbstractWatcherIntegrationTestCase extends ESIntegTestCase {

    public static final String WATCHER_LANG = Script.DEFAULT_SCRIPT_LANG;

    private static final boolean timeWarpEnabled = SystemPropertyUtil.getBoolean("tests.timewarp", true);

    private TimeWarp timeWarp;

    private static Boolean securityEnabled;

    @Override
    protected TestCluster buildTestCluster(Scope scope, long seed) throws IOException {
        if (securityEnabled == null) {
            securityEnabled = enableSecurity();
        }
        return super.buildTestCluster(scope, seed);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                //TODO: for now lets isolate watcher tests from monitoring (randomize this later)
                .put(XPackSettings.MONITORING_ENABLED.getKey(), false)
                // we do this by default in core, but for watcher this isn't needed and only adds noise.
                .put("index.store.mock.check_index_on_close", false)
                .put("xpack.watcher.execution.scroll.size", randomIntBetween(1, 100))
                .put("xpack.watcher.watch.scroll.size", randomIntBetween(1, 100))
                .put(SecuritySettings.settings(securityEnabled))
                .put("script.inline", "true")
                // Disable native ML autodetect_process as the c++ controller won't be available
                .put(MachineLearning.AUTODETECT_PROCESS.getKey(), false)
                .build();
    }

    @Override
    protected Set<String> excludeTemplates() {
        Set<String> excludes = new HashSet<>();
        for (WatcherIndexTemplateRegistry.TemplateConfig templateConfig : WatcherIndexTemplateRegistry.TEMPLATE_CONFIGS) {
            excludes.add(templateConfig.getTemplateName());
        }
        return excludes;
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

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    @Override
    protected Function<Client,Client> getClientWrapper() {
        if (securityEnabled == false) {
            return Function.identity();
        }
        Map<String, String> headers = Collections.singletonMap("Authorization",
                basicAuthHeaderValue(SecuritySettings.TEST_USERNAME, new SecuredString(SecuritySettings.TEST_PASSWORD.toCharArray())));
        // we need to wrap node clients because we do not specify a user for nodes and all requests will use the system
        // user. This is ok for internal n2n stuff but the test framework does other things like wiping indices, repositories, etc
        // that the system user cannot do. so we wrap the node client with a user that can do these things since the client() calls
        // are randomized to return both node clients and transport clients
        // transport clients do not need to be wrapped since we specify the xpack.security.user setting that sets the default user to be
        // used for the transport client. If we did not set a default user then the transport client would not even be allowed
        // to connect
        return client -> (client instanceof NodeClient) ? client.filterWithHeader(headers) : client;
    }

    protected List<Class<? extends Plugin>> pluginTypes() {
        List<Class<? extends Plugin>> types = new ArrayList<>();
        if (timeWarped()) {
            types.add(TimeWarpedXPackPlugin.class);
        } else {
            types.add(XPackPlugin.class);
        }
        return types;
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

    /**
     * @return whether security has been enabled
     */
    protected final boolean securityEnabled() {
        return securityEnabled;
    }

    /**
     * Override and returns {@code false} to force running without security
     */
    protected boolean enableSecurity() {
        return randomBoolean();
    }

    protected boolean checkWatcherRunningOnlyOnce() {
        return true;
    }

    @Before
    public void _setup() throws Exception {
        setupTimeWarp();
        startWatcherIfNodesExist();
        configureAliasesForWatcherIndices();
    }

    @After
    public void _cleanup() throws Exception {
        // Clear all internal watcher state for the next test method:
        logger.info("[{}#{}]: clearing watcher state", getTestClass().getSimpleName(), getTestName());
        if (checkWatcherRunningOnlyOnce()) {
            ensureWatcherOnlyRunningOnce();
        }
        stopWatcher(false);
    }

    @AfterClass
    public static void _cleanupClass() {
        securityEnabled = null;
    }

    @Override
    protected Settings transportClientSettings() {
        if (securityEnabled == false) {
            return super.transportClientSettings();
        }

        return Settings.builder()
                .put("client.transport.sniff", false)
                .put(Security.USER_SETTING.getKey(), "admin:changeme")
                .put(NetworkModule.TRANSPORT_TYPE_KEY, Security.NAME4)
                .put(NetworkModule.HTTP_TYPE_KEY, Security.NAME4)
                .build();
    }

    private void setupTimeWarp() throws Exception {
        if (timeWarped()) {
            timeWarp = new TimeWarp(getInstanceFromMaster(ScheduleTriggerEngineMock.class), (ClockMock)getInstanceFromMaster(Clock.class));
        }
    }

    private void startWatcherIfNodesExist() throws Exception {
        if (internalCluster().size() > 0) {
            ensureLicenseEnabled();
            if (timeWarped()) {
                // now that the license is enabled and valid we can freeze all nodes clocks
                logger.info("[{}#{}]: freezing time on nodes", getTestClass().getSimpleName(), getTestName());
                TimeFreezeDisruption ice = new TimeFreezeDisruption();
                internalCluster().setDisruptionScheme(ice);
                ice.startDisrupting();
            }
            WatcherState state = getInstanceFromMaster(WatcherService.class).state();
            if (state == WatcherState.STOPPED) {
                logger.info("[{}#{}]: starting watcher", getTestClass().getSimpleName(), getTestName());
                startWatcher(false);
            } else if (state == WatcherState.STARTING) {
                logger.info("[{}#{}]: watcher is starting, waiting for it to get in a started state",
                        getTestClass().getSimpleName(), getTestName());
                ensureWatcherStarted(false);
            } else {
                logger.info("[{}#{}]: not starting watcher, because watcher is in state [{}]",
                        getTestClass().getSimpleName(), getTestName(), state);
            }
        } else {
            logger.info("[{}#{}]: not starting watcher, because test cluster has no nodes",
                    getTestClass().getSimpleName(), getTestName());
        }
    }

    /**
     * In order to test, that .watches and .triggered-watches indices can also point to an alias, we will rarely create those
     * after starting watcher
     *
     * The idea behind this is the possible use of the migration helper for upgrades, see
     * https://github.com/elastic/elasticsearch-migration/
     *
     */
    private void configureAliasesForWatcherIndices() throws Exception {
        if (internalCluster().size() > 0) {
            // alias for .watches, setting the index template to the same as well
            if (rarely()) {
                String newIndex = ".watches-alias-index";
                BytesReference bytesReference = TemplateUtils.load("/watches.json");
                try (XContentParser parser = createParser(JsonXContent.jsonXContent, bytesReference.toBytesRef().bytes)) {
                    Map<String, Object> parserMap = parser.map();
                    Map<String, Object> allMappings = (Map<String, Object>) parserMap.get("mappings");

                    CreateIndexResponse response = client().admin().indices().prepareCreate(newIndex)
                            .setCause("Index to test aliases with .watches index")
                            .addAlias(new Alias(Watch.INDEX))
                            .setSettings((Map<String, Object>) parserMap.get("settings"))
                            .addMapping("watch", (Map<String, Object>) allMappings.get("watch"))
                            .get();
                    assertAcked(response);
                }
            }

            // alias for .triggered-watches, ensuring the index template is set appropriately
            if (rarely()) {
                String newIndex = ".triggered-watches-alias-index";
                BytesReference bytesReference = TemplateUtils.load("/triggered_watches.json");
                try (XContentParser parser = createParser(JsonXContent.jsonXContent, bytesReference.toBytesRef().bytes)) {
                    Map<String, Object> parserMap = parser.map();
                    Map<String, Object> allMappings = (Map<String, Object>) parserMap.get("mappings");

                    CreateIndexResponse response = client().admin().indices().prepareCreate(newIndex)
                            .setCause("Index to test aliases with .triggered-watches index")
                            .addAlias(new Alias(TriggeredWatchStore.INDEX_NAME))
                            .setSettings((Map<String, Object>) parserMap.get("settings"))
                            .addMapping("triggered_watch", (Map<String, Object>) allMappings.get("triggered_watch"))
                            .get();
                    assertAcked(response);
                }
            }
        }
    }

    protected TimeWarp timeWarp() {
        assert timeWarped() : "cannot access TimeWarp when test context is not time warped";
        return timeWarp;
    }

    public boolean randomizeNumberOfShardsAndReplicas() {
        return false;
    }

    protected long docCount(String index, String type, QueryBuilder query) {
        refresh();
        return docCount(index, type, SearchSourceBuilder.searchSource().query(query));
    }

    protected long watchRecordCount(QueryBuilder query) {
        refresh();
        return docCount(HistoryStore.INDEX_PREFIX_WITH_TEMPLATE + "*",
                HistoryStore.DOC_TYPE, SearchSourceBuilder.searchSource().query(query));
    }

    protected long docCount(String index, String type, SearchSourceBuilder source) {
        SearchRequestBuilder builder = client().prepareSearch(index).setSource(source).setSize(0);
        if (type != null) {
            builder.setTypes(type);
        }
        return builder.get().getHits().getTotalHits();
    }

    protected SearchResponse searchHistory(SearchSourceBuilder builder) {
        return client().prepareSearch(HistoryStore.INDEX_PREFIX_WITH_TEMPLATE + "*").setSource(builder).get();
    }

    protected <T> T getInstanceFromMaster(Class<T> type) {
        return internalCluster().getInstance(type, internalCluster().getMasterName());
    }

    protected Watch.Parser watchParser() {
        return getInstanceFromMaster(Watch.Parser.class);
    }

    public AbstractWatcherIntegrationTestCase() {
        super();
    }

    protected WatcherClient watcherClient() {
        Client client = securityEnabled ? internalCluster().transportClient() : client();
        return randomBoolean() ? new XPackClient(client).watcher() : new WatcherClient(client);
    }

    protected HttpClient watcherHttpClient() {
        return internalCluster().getInstance(HttpClient.class);
    }

    protected EmailService noopEmailService() {
        return new NoopEmailService();
    }

    protected IndexNameExpressionResolver indexNameExpressionResolver() {
        return internalCluster().getInstance(IndexNameExpressionResolver.class);
    }

    protected void assertValue(XContentSource source, String path, Matcher<?> matcher) {
        WatcherTestUtils.assertValue(source, path, matcher);
    }

    protected void assertValue(Map<String, Object> map, String path, Matcher<?> matcher) {
        WatcherTestUtils.assertValue(map, path, matcher);
    }

    protected void assertWatchWithMinimumPerformedActionsCount(final String watchName,
                                                               final long minimumExpectedWatchActionsWithActionPerformed) throws Exception {
        assertWatchWithMinimumPerformedActionsCount(watchName, minimumExpectedWatchActionsWithActionPerformed, true);
    }

    // TODO remove this shitty method... the `assertConditionMet` is bogus
    protected void assertWatchWithMinimumPerformedActionsCount(final String watchName,
                                                               final long minimumExpectedWatchActionsWithActionPerformed,
                                                               final boolean assertConditionMet) throws Exception {
        final AtomicReference<SearchResponse> lastResponse = new AtomicReference<>();
        try {
            assertBusy(() -> {
                ClusterState state = client().admin().cluster().prepareState().get().getState();
                String[] watchHistoryIndices = indexNameExpressionResolver().concreteIndexNames(state,
                        IndicesOptions.lenientExpandOpen(), HistoryStore.INDEX_PREFIX_WITH_TEMPLATE + "*");
                assertThat(watchHistoryIndices, not(emptyArray()));
                for (String index : watchHistoryIndices) {
                    IndexRoutingTable routingTable = state.getRoutingTable().index(index);
                    assertThat(routingTable, notNullValue());
                    assertThat(routingTable.allPrimaryShardsActive(), is(true));
                }

                refresh();
                SearchResponse searchResponse = client().prepareSearch(HistoryStore.INDEX_PREFIX_WITH_TEMPLATE + "*")
                        .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                        .setQuery(boolQuery().must(matchQuery("watch_id", watchName)).must(matchQuery("state",
                                ExecutionState.EXECUTED.id())))
                        .get();
                lastResponse.set(searchResponse);
                assertThat("could not find executed watch record", searchResponse.getHits().getTotalHits(),
                        greaterThanOrEqualTo(minimumExpectedWatchActionsWithActionPerformed));
                if (assertConditionMet) {
                    assertThat((Integer) XContentMapValues.extractValue("result.input.payload.hits.total",
                            searchResponse.getHits().getAt(0).getSourceAsMap()), greaterThanOrEqualTo(1));
                }
            });
        } catch (AssertionError error) {
            SearchResponse searchResponse = lastResponse.get();
            logger.info("Found [{}] records for watch [{}]", searchResponse.getHits().getTotalHits(), watchName);
            int counter = 1;
            for (SearchHit hit : searchResponse.getHits().getHits()) {
                logger.info("hit [{}]=\n {}", counter++, XContentHelper.convertToJson(hit.getSourceRef(), true, true));
            }
            throw error;
        }
    }

    protected SearchResponse searchWatchRecords(Callback<SearchRequestBuilder> requestBuilderCallback) {
        SearchRequestBuilder builder =
                client().prepareSearch(HistoryStore.INDEX_PREFIX_WITH_TEMPLATE + "*").setTypes(HistoryStore.DOC_TYPE);
        requestBuilderCallback.handle(builder);
        return builder.get();
    }

    protected long historyRecordsCount(String watchName) {
        refresh();
        SearchResponse searchResponse = client().prepareSearch(HistoryStore.INDEX_PREFIX_WITH_TEMPLATE + "*")
                .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .setSize(0)
                .setQuery(matchQuery("watch_id", watchName))
                .get();
        return searchResponse.getHits().getTotalHits();
    }

    protected long findNumberOfPerformedActions(String watchName) {
        refresh();
        SearchResponse searchResponse = client().prepareSearch(HistoryStore.INDEX_PREFIX_WITH_TEMPLATE + "*")
                .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .setQuery(boolQuery().must(matchQuery("watch_id", watchName)).must(matchQuery("state", ExecutionState.EXECUTED.id())))
                .get();
        return searchResponse.getHits().getTotalHits();
    }

    protected void assertWatchWithNoActionNeeded(final String watchName,
                                                 final long expectedWatchActionsWithNoActionNeeded) throws Exception {
        final AtomicReference<SearchResponse> lastResponse = new AtomicReference<>();
        try {
            assertBusy(() -> {
                // The watch_history index gets created in the background when the first watch is triggered
                // so we to check first is this index is created and shards are started
                ClusterState state = client().admin().cluster().prepareState().get().getState();
                String[] watchHistoryIndices = indexNameExpressionResolver().concreteIndexNames(state,
                        IndicesOptions.lenientExpandOpen(), HistoryStore.INDEX_PREFIX_WITH_TEMPLATE + "*");
                assertThat(watchHistoryIndices, not(emptyArray()));
                for (String index : watchHistoryIndices) {
                    IndexRoutingTable routingTable = state.getRoutingTable().index(index);
                    assertThat(routingTable, notNullValue());
                    assertThat(routingTable.allPrimaryShardsActive(), is(true));
                }
                refresh();
                SearchResponse searchResponse = client().prepareSearch(HistoryStore.INDEX_PREFIX_WITH_TEMPLATE + "*")
                        .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                        .setQuery(boolQuery().must(matchQuery("watch_id", watchName)).must(matchQuery("state",
                                ExecutionState.EXECUTION_NOT_NEEDED.id())))
                        .get();
                lastResponse.set(searchResponse);
                assertThat(searchResponse.getHits().getTotalHits(), greaterThanOrEqualTo(expectedWatchActionsWithNoActionNeeded));
            });
        } catch (AssertionError error) {
            SearchResponse searchResponse = lastResponse.get();
            logger.info("Found [{}] records for watch [{}]", searchResponse.getHits().getTotalHits(), watchName);
            int counter = 1;
            for (SearchHit hit : searchResponse.getHits().getHits()) {
                logger.info("hit [{}]=\n {}", counter++, XContentHelper.convertToJson(hit.getSourceRef(), true, true));
            }
            throw error;
        }
    }

    protected void assertWatchWithMinimumActionsCount(final String watchName, final ExecutionState recordState,
                                                      final long recordCount) throws Exception {
        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            String[] watchHistoryIndices = indexNameExpressionResolver().concreteIndexNames(state, IndicesOptions.lenientExpandOpen(),
                    HistoryStore.INDEX_PREFIX_WITH_TEMPLATE + "*");
            assertThat(watchHistoryIndices, not(emptyArray()));
            for (String index : watchHistoryIndices) {
                IndexRoutingTable routingTable = state.getRoutingTable().index(index);
                assertThat(routingTable, notNullValue());
                assertThat(routingTable.allPrimaryShardsActive(), is(true));
            }

            refresh();
            SearchResponse searchResponse = client().prepareSearch(HistoryStore.INDEX_PREFIX_WITH_TEMPLATE + "*")
                    .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                    .setQuery(boolQuery().must(matchQuery("watch_id", watchName)).must(matchQuery("state", recordState.id())))
                    .get();
            assertThat("could not find executed watch record", searchResponse.getHits().getTotalHits(),
                    greaterThanOrEqualTo(recordCount));
        });
    }

    protected void ensureWatcherStarted() throws Exception {
        ensureWatcherStarted(true);
    }

    protected void ensureWatcherStarted(final boolean useClient) throws Exception {
        assertBusy(() -> {
            if (useClient) {
                assertThat(watcherClient().prepareWatcherStats().get().getWatcherState(), is(WatcherState.STARTED));
            } else {
                assertThat(getInstanceFromMaster(WatcherService.class).state(), is(WatcherState.STARTED));
            }
        });

        // Verify that the index templates exist:
        assertBusy(() -> {
            GetIndexTemplatesResponse response = client().admin().indices().prepareGetTemplates(HISTORY_TEMPLATE_NAME).get();
            assertThat("[" + HISTORY_TEMPLATE_NAME + "] is missing", response.getIndexTemplates().size(), equalTo(1));
            response = client().admin().indices().prepareGetTemplates(TRIGGERED_TEMPLATE_NAME).get();
            assertThat("[" + TRIGGERED_TEMPLATE_NAME + "] is missing", response.getIndexTemplates().size(), equalTo(1));
            response = client().admin().indices().prepareGetTemplates(WATCHES_TEMPLATE_NAME).get();
            assertThat("[" + WATCHES_TEMPLATE_NAME + "] is missing", response.getIndexTemplates().size(), equalTo(1));
        });
    }

    protected void ensureLicenseEnabled() throws Exception {
        assertBusy(() -> {
            for (XPackLicenseState licenseState : internalCluster().getInstances(XPackLicenseState.class)) {
                assertThat(licenseState.isWatcherAllowed(), is(true));
            }
        });
    }

    private void progressClocksAboveMaster(InternalTestCluster cluster) {
        long minClock = Long.MAX_VALUE;
        long maxClock = Long.MIN_VALUE;
        for (Clock clock: cluster.getInstances(Clock.class)) {
            final long millis = clock.millis();
            minClock = Math.min(millis, minClock);
            maxClock = Math.max(millis, maxClock);
        }
        // now move all the clocks ahead to make sure they are beyond the highest clock
        final TimeValue delta = TimeValue.timeValueMillis(maxClock - minClock);
        for (Clock clock: cluster.getInstances(Clock.class)) {
            ((ClockMock)clock).fastForward(delta);
        }
    }

    protected void ensureWatcherStopped() throws Exception {
        ensureWatcherStopped(true);
    }

    protected void ensureWatcherStopped(final boolean useClient) throws Exception {
        assertBusy(() -> {
            if (useClient) {
                assertThat(watcherClient().prepareWatcherStats().get().getWatcherState(), is(WatcherState.STOPPED));
            } else {
                assertThat(getInstanceFromMaster(WatcherService.class).state(), is(WatcherState.STOPPED));
            }
        });
    }

    protected void startWatcher() throws Exception {
        startWatcher(true);
    }

    protected void stopWatcher() throws Exception {
        stopWatcher(true);
    }

    protected void startWatcher(boolean useClient) throws Exception {
        if (useClient) {
            watcherClient().prepareWatchService().start().get();
        } else {
            getInstanceFromMaster(WatcherLifeCycleService.class).start();
        }
        ensureWatcherStarted(useClient);
    }

    protected void stopWatcher(boolean useClient) throws Exception {
        if (useClient) {
            watcherClient().prepareWatchService().stop().get();
        } else {
            getInstanceFromMaster(WatcherLifeCycleService.class).stop();
        }
        ensureWatcherStopped(useClient);
    }

    protected void ensureWatcherOnlyRunningOnce() {
        int running = 0;
        for (WatcherService watcherService : internalCluster().getInstances(WatcherService.class)) {
            if (watcherService.state() == WatcherState.STARTED) {
                running++;
            }
        }
        assertThat("watcher should only run on the elected master node, but it is running on [" + running + "] nodes", running, equalTo(1));
    }

    public static class NoopEmailService extends EmailService {

        public NoopEmailService() {
            super(Settings.EMPTY, null,
                new ClusterSettings(Settings.EMPTY, Collections.singleton(EmailService.EMAIL_ACCOUNT_SETTING)));
        }

        @Override
        public EmailSent send(Email email, Authentication auth, Profile profile, String accountName) {
            return new EmailSent(accountName, email);
        }
    }

    protected static class TimeWarp {

        protected final ScheduleTriggerEngineMock scheduler;
        protected final ClockMock clock;

        public TimeWarp(ScheduleTriggerEngineMock scheduler, ClockMock clock) {
            this.scheduler = scheduler;
            this.clock = clock;
        }

        public ScheduleTriggerEngineMock scheduler() {
            return scheduler;
        }

        public ClockMock clock() {
            return clock;
        }
    }


    /** Security related settings */

    public static class SecuritySettings {

        public static final String TEST_USERNAME = "test";
        public static final String TEST_PASSWORD = "changeme";
        private static final String TEST_PASSWORD_HASHED =  new String(Hasher.BCRYPT.hash(new SecuredString(TEST_PASSWORD.toCharArray())));

        static boolean auditLogsEnabled = SystemPropertyUtil.getBoolean("tests.audit_logs", true);
        static byte[] systemKey = generateKey(); // must be the same for all nodes

        public static final String IP_FILTER = "allow: all\n";

        public static final String USERS =
                "transport_client:" + TEST_PASSWORD_HASHED + "\n" +
                TEST_USERNAME + ":" + TEST_PASSWORD_HASHED + "\n" +
                "admin:" + TEST_PASSWORD_HASHED + "\n" +
                "monitor:" + TEST_PASSWORD_HASHED;

        public static final String USER_ROLES =
                "transport_client:transport_client\n" +
                "test:test\n" +
                "admin:admin\n" +
                "monitor:monitor";

        public static final String ROLES =
                "test:\n" + // a user for the test infra.
                "  cluster: [ 'cluster:monitor/nodes/info', 'cluster:monitor/state', 'cluster:monitor/health', 'cluster:monitor/stats'," +
                        " 'cluster:admin/settings/update', 'cluster:admin/repository/delete', 'cluster:monitor/nodes/liveness'," +
                        " 'indices:admin/template/get', 'indices:admin/template/put', 'indices:admin/template/delete'," +
                        " 'cluster:admin/script/put' ]\n" +
                "  indices:\n" +
                "    - names: '*'\n" +
                "      privileges: [ all ]\n" +
                "\n" +
                "admin:\n" +
                "  cluster: [ 'manage' ]\n" +
                "monitor:\n" +
                "  cluster: [ 'monitor' ]\n"
                ;


        public static Settings settings(boolean enabled)  {
            Settings.Builder builder = Settings.builder();
            if (!enabled) {
                return builder.put("xpack.security.enabled", false).build();
            }
            try {
                Path conf = createTempDir().resolve("watcher_security");
                Path xpackConf = conf.resolve(XPackPlugin.NAME);
                Files.createDirectories(xpackConf);
                writeFile(xpackConf, "users", USERS);
                writeFile(xpackConf, "users_roles", USER_ROLES);
                writeFile(xpackConf, "roles.yml", ROLES);
                writeFile(xpackConf, "system_key", systemKey);

                builder.put("xpack.security.enabled", true)
                        .put("xpack.security.authc.realms.esusers.type", FileRealm.TYPE)
                        .put("xpack.security.authc.realms.esusers.order", 0)
                        .put("xpack.security.audit.enabled", auditLogsEnabled)
                        .put(Environment.PATH_CONF_SETTING.getKey(), conf);
                        // security should always use one of its transports so if it is enabled explicitly declare one otherwise a local
                        // transport could be used
                        builder.put(NetworkModule.TRANSPORT_TYPE_KEY, Security.NAME4);
                        builder.put(NetworkModule.HTTP_TYPE_KEY, Security.NAME4);
                return builder.build();
            } catch (IOException ex) {
                throw new RuntimeException("failed to build settings for security", ex);
            }
        }

        static byte[] generateKey() {
            try {
                return CryptoService.generateKey();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public static String writeFile(Path folder, String name, String content) throws IOException {
            Path file = folder.resolve(name);
            try (BufferedWriter stream = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
                Streams.copy(content, stream);
            } catch (IOException e) {
                throw new ElasticsearchException("error writing file in test", e);
            }
            return file.toAbsolutePath().toString();
        }

        public static String writeFile(Path folder, String name, byte[] content) throws IOException {
            Path file = folder.resolve(name);
            try (OutputStream stream = Files.newOutputStream(file)) {
                Streams.copy(content, stream);
            } catch (IOException e) {
                throw new ElasticsearchException("error writing file in test", e);
            }
            return file.toAbsolutePath().toString();
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
                ((ClockMock)cluster.getInstance(Clock.class, node)).freeze();
            }
        }

        @Override
        public void removeFromNode(String node, InternalTestCluster cluster) {
            ((ClockMock)cluster.getInstance(Clock.class, node)).unfreeze();
        }

        @Override
        public synchronized void startDisrupting() {
            frozen = true;
            for (String node: cluster.getNodeNames()) {
                applyToNode(node, cluster);
            }
        }

        @Override
        public void stopDisrupting() {
            frozen = false;
            for (String node: cluster.getNodeNames()) {
                removeFromNode(node, cluster);
            }
        }

        @Override
        public void testClusterClosed() {
        }

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
