/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.test;

import io.netty.util.internal.SystemPropertyUtil;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
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
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.TestCluster;
import org.elasticsearch.test.disruption.ServiceDisruptionScheme;
import org.elasticsearch.test.store.MockFSIndexStore;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.TimeWarpedXPackPlugin;
import org.elasticsearch.xpack.XPackClient;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.notification.email.Authentication;
import org.elasticsearch.xpack.notification.email.Email;
import org.elasticsearch.xpack.notification.email.EmailService;
import org.elasticsearch.xpack.notification.email.Profile;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.authc.file.FileRealm;
import org.elasticsearch.xpack.security.authc.support.Hasher;
import org.elasticsearch.xpack.support.clock.ClockMock;
import org.elasticsearch.xpack.template.TemplateUtils;
import org.elasticsearch.xpack.watcher.WatcherState;
import org.elasticsearch.xpack.watcher.client.WatcherClient;
import org.elasticsearch.xpack.watcher.execution.ExecutionState;
import org.elasticsearch.xpack.watcher.execution.TriggeredWatchStore;
import org.elasticsearch.xpack.watcher.history.HistoryStore;
import org.elasticsearch.xpack.watcher.support.WatcherIndexTemplateRegistry;
import org.elasticsearch.xpack.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.watcher.transport.actions.stats.WatcherStatsResponse;
import org.elasticsearch.xpack.watcher.trigger.ScheduleTriggerEngineMock;
import org.elasticsearch.xpack.watcher.watch.Watch;
import org.hamcrest.Matcher;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

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
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;

@ClusterScope(scope = SUITE, numClientNodes = 0, transportClientRatio = 0, maxNumDataNodes = 3)
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
                // Disable native ML autodetect_process as the c++ controller won't be available
                .put(MachineLearning.AUTODETECT_PROCESS.getKey(), false)
                .build();
    }

    @Override
    protected Path nodeConfigPath(final int nodeOrdinal) {
        if (!securityEnabled) {
            return null;
        }
        final Path conf = createTempDir().resolve("watcher_security");
        final Path xpackConf = conf.resolve(XPackPlugin.NAME);
        try {
            Files.createDirectories(xpackConf);
            writeFile(xpackConf, "users", SecuritySettings.USERS);
            writeFile(xpackConf, "users_roles", SecuritySettings.USER_ROLES);
            writeFile(xpackConf, "roles.yml", SecuritySettings.ROLES);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
        return conf;
    }

    private static void writeFile(final Path folder, final String name, final String content) throws IOException {
        final Path file = folder.resolve(name);
        try (BufferedWriter stream = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
            Streams.copy(content, stream);
        }
    }

    public static void writeFile(final Path folder, final String name, final byte[] content) throws IOException {
        final Path file = folder.resolve(name);
        try (OutputStream stream = Files.newOutputStream(file)) {
            Streams.copy(content, stream);
        }
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
                basicAuthHeaderValue(SecuritySettings.TEST_USERNAME, new SecureString(SecuritySettings.TEST_PASSWORD.toCharArray())));
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
        types.add(CommonAnalysisPlugin.class);
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

    @Before
    public void _setup() throws Exception {
        setupTimeWarp();
        startWatcherIfNodesExist();
        createWatcherIndicesOrAliases();
    }

    @After
    public void _cleanup() throws Exception {
        // Clear all internal watcher state for the next test method:
        logger.info("[#{}]: clearing watcher state", getTestName());
        stopWatcher();
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
                .put(Security.USER_SETTING.getKey(), "admin:" + SecuritySettingsSource.TEST_PASSWORD)
                .put(NetworkModule.TRANSPORT_TYPE_KEY, Security.NAME4)
                .put(NetworkModule.HTTP_TYPE_KEY, Security.NAME4)
                .build();
    }

    private void setupTimeWarp() throws Exception {
        if (timeWarped()) {
            timeWarp = new TimeWarp(internalCluster().getInstances(ScheduleTriggerEngineMock.class),
                    (ClockMock)getInstanceFromMaster(Clock.class));
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
            assertAcked(watcherClient().prepareWatchService().start().get());
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
    private void createWatcherIndicesOrAliases() throws Exception {
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
                            .addMapping("doc", (Map<String, Object>) allMappings.get("doc"))
                            .get();
                    assertAcked(response);
                    ensureGreen(newIndex);
                }
            } else {
                Settings.Builder builder = Settings.builder();
                if (randomBoolean()) {
                    builder.put("index.number_of_shards", scaledRandomIntBetween(1, 5));
                }
                if (randomBoolean()) {
                    // maximum number of replicas
                    ClusterState state = internalCluster().getDataNodeInstance(ClusterService.class).state();
                    int dataNodeCount = state.nodes().getDataNodes().size();
                    int replicas = scaledRandomIntBetween(0, dataNodeCount - 1);
                    builder.put("index.number_of_replicas", replicas);
                }
                assertAcked(client().admin().indices().prepareCreate(Watch.INDEX).setSettings(builder));
                ensureGreen(Watch.INDEX);
            }

            // alias for .triggered-watches, ensuring the index template is set appropriately
            if (rarely()) {
                String newIndex = ".triggered-watches-alias-index";
                BytesReference bytesReference = TemplateUtils.load("/triggered-watches.json");
                try (XContentParser parser = createParser(JsonXContent.jsonXContent, bytesReference.toBytesRef().bytes)) {
                    Map<String, Object> parserMap = parser.map();
                    Map<String, Object> allMappings = (Map<String, Object>) parserMap.get("mappings");

                    CreateIndexResponse response = client().admin().indices().prepareCreate(newIndex)
                            .setCause("Index to test aliases with .triggered-watches index")
                            .addAlias(new Alias(TriggeredWatchStore.INDEX_NAME))
                            .setSettings((Map<String, Object>) parserMap.get("settings"))
                            .addMapping("doc", (Map<String, Object>) allMappings.get("doc"))
                            .get();
                    assertAcked(response);
                    ensureGreen(newIndex);
                }
            } else {
                assertAcked(client().admin().indices().prepareCreate(TriggeredWatchStore.INDEX_NAME));
                ensureGreen(TriggeredWatchStore.INDEX_NAME);
            }

            String historyIndex = HistoryStore.getHistoryIndexNameForTime(DateTime.now(DateTimeZone.UTC));
            assertAcked(client().admin().indices().prepareCreate(historyIndex));
            ensureGreen(historyIndex);

            ensureWatcherStarted();
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

    protected IndexNameExpressionResolver indexNameExpressionResolver() {
        return internalCluster().getInstance(IndexNameExpressionResolver.class);
    }

    protected void assertValue(XContentSource source, String path, Matcher<?> matcher) {
        assertThat(source.getValue(path), (Matcher<Object>) matcher);
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

    protected SearchResponse searchWatchRecords(Consumer<SearchRequestBuilder> requestBuilderCallback) {
        SearchRequestBuilder builder =
                client().prepareSearch(HistoryStore.INDEX_PREFIX_WITH_TEMPLATE + "*").setTypes(HistoryStore.DOC_TYPE);
        requestBuilderCallback.accept(builder);
        return builder.get();
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

    private void ensureWatcherTemplatesAdded() throws Exception {
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

    protected void ensureWatcherStarted() throws Exception {
        ensureWatcherTemplatesAdded();
        assertBusy(() -> {
            WatcherStatsResponse watcherStatsResponse = watcherClient().prepareWatcherStats().get();
            assertThat(watcherStatsResponse.hasFailures(), is(false));
            List<Tuple<String, WatcherState>> currentStatesFromStatsRequest = watcherStatsResponse.getNodes().stream()
                    .map(response -> Tuple.tuple(response.getNode().getName(), response.getWatcherState()))
                    .collect(Collectors.toList());
            List<WatcherState> states = currentStatesFromStatsRequest.stream().map(Tuple::v2).collect(Collectors.toList());

            String message = String.format(Locale.ROOT, "Expected watcher to be started, but state was %s", currentStatesFromStatsRequest);
            assertThat(message, states, everyItem(is(WatcherState.STARTED)));
        });

    }

    protected void ensureLicenseEnabled() throws Exception {
        assertBusy(() -> {
            for (XPackLicenseState licenseState : internalCluster().getInstances(XPackLicenseState.class)) {
                assertThat(licenseState.isWatcherAllowed(), is(true));
            }
        });
    }

    protected void ensureWatcherStopped() throws Exception {
        assertBusy(() -> {
            WatcherStatsResponse watcherStatsResponse = watcherClient().prepareWatcherStats().get();
            assertThat(watcherStatsResponse.hasFailures(), is(false));
            List<Tuple<String, WatcherState>> currentStatesFromStatsRequest = watcherStatsResponse.getNodes().stream()
                    .map(response -> Tuple.tuple(response.getNode().getName(), response.getWatcherState()))
                    .collect(Collectors.toList());
            List<WatcherState> states = currentStatesFromStatsRequest.stream().map(Tuple::v2).collect(Collectors.toList());

            String message = String.format(Locale.ROOT, "Expected watcher to be stopped, but state was %s", currentStatesFromStatsRequest);
            assertThat(message, states, everyItem(is(WatcherState.STOPPED)));
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

        protected final Iterable<ScheduleTriggerEngineMock> schedulers;
        protected final ClockMock clock;

        public TimeWarp(Iterable<ScheduleTriggerEngineMock> schedulers, ClockMock clock) {
            this.schedulers = schedulers;
            this.clock = clock;
        }

        public void trigger(String jobName) {
            schedulers.forEach(scheduler -> scheduler.trigger(jobName));
        }

        public ClockMock clock() {
            return clock;
        }

        public void trigger(String id, int times, TimeValue timeValue) {
            schedulers.forEach(scheduler -> scheduler.trigger(id, times, timeValue));
        }
    }


    /** Security related settings */

    public static class SecuritySettings {

        public static final String TEST_USERNAME = "test";
        public static final String TEST_PASSWORD = SecuritySettingsSource.TEST_PASSWORD;
        private static final String TEST_PASSWORD_HASHED =  new String(Hasher.BCRYPT.hash(new SecureString(TEST_PASSWORD.toCharArray())));

        static boolean auditLogsEnabled = SystemPropertyUtil.getBoolean("tests.audit_logs", true);

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
                        " 'cluster:admin/script/put', 'cluster:monitor/task' ]\n" +
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
            builder.put("xpack.security.enabled", true)
                    .put("xpack.security.authc.realms.esusers.type", FileRealm.TYPE)
                    .put("xpack.security.authc.realms.esusers.order", 0)
                    .put("xpack.security.audit.enabled", auditLogsEnabled);
            /*
             * Security should always use one of its transports so if it is enabled explicitly declare one otherwise a local transport could
             * be used.
             */
            builder.put(NetworkModule.TRANSPORT_TYPE_KEY, Security.NAME4);
            builder.put(NetworkModule.HTTP_TYPE_KEY, Security.NAME4);
            return builder.build();
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
