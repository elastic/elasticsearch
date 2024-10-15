/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.admin.cluster.snapshots.features.ResetFeatureStateAction;
import org.elasticsearch.action.admin.cluster.snapshots.features.ResetFeatureStateRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.script.IngestScript;
import org.elasticsearch.script.MockDeterministicScript;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ExternalTestCluster;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.TestCluster;
import org.elasticsearch.transport.netty4.Netty4Plugin;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.autoscaling.Autoscaling;
import org.elasticsearch.xpack.autoscaling.AutoscalingMetadata;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderResult;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ilm.DeleteAction;
import org.elasticsearch.xpack.core.ilm.ForceMergeAction;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.LifecycleType;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.ShrinkAction;
import org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.MlMetaIndex;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.DeleteExpiredDataAction;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.PutFilterAction;
import org.elasticsearch.xpack.core.ml.action.SetUpgradeModeAction;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsTaskState;
import org.elasticsearch.xpack.core.ml.inference.ModelAliasMetadata;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelCacheMetadata;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentMetadata;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.job.config.JobTaskState;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndexFields;
import org.elasticsearch.xpack.core.ml.notifications.NotificationsIndex;
import org.elasticsearch.xpack.core.security.SecurityField;
import org.elasticsearch.xpack.core.security.authc.TokenMetadata;
import org.elasticsearch.xpack.esql.core.plugin.EsqlCorePlugin;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.ilm.IndexLifecycle;
import org.elasticsearch.xpack.ml.LocalStateMachineLearning;
import org.elasticsearch.xpack.ml.autoscaling.MlScalingReason;
import org.elasticsearch.xpack.slm.SnapshotLifecycle;
import org.elasticsearch.xpack.slm.history.SnapshotLifecycleTemplateRegistry;
import org.elasticsearch.xpack.transform.Transform;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.xpack.monitoring.MonitoringService.ELASTICSEARCH_COLLECTION_ENABLED;
import static org.elasticsearch.xpack.security.test.SecurityTestUtils.writeFile;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * Base class of ML integration tests that use a native autodetect process
 */
abstract class MlNativeIntegTestCase extends ESIntegTestCase {

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            LocalStateMachineLearning.class,
            Netty4Plugin.class,
            Autoscaling.class,
            ReindexPlugin.class,
            // The monitoring plugin requires script and gsub processors to be loaded
            IngestCommonPlugin.class,
            // The monitoring plugin script processor references painless. Include this for script compilation.
            // This is to reduce log spam
            MockPainlessScriptEngine.TestPlugin.class,
            // ILM is required for .ml-state template index settings
            IndexLifecycle.class,
            // SLM is required for the slm.history_index_enabled setting
            SnapshotLifecycle.class,
            // The feature reset API touches transform custom cluster state so we need this plugin to understand it
            Transform.class,
            DataStreamsPlugin.class,
            // ESQL and its dependency needed for node features
            EsqlCorePlugin.class,
            EsqlPlugin.class
        );
    }

    @Override
    protected Function<Client, Client> getClientWrapper() {
        final Map<String, String> headers = Map.of(
            "Authorization",
            basicAuthHeaderValue("x_pack_rest_user", SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)
        );
        // we need to wrap node clients because we do not specify a user for nodes and all requests will use the system
        // user. This is ok for internal n2n stuff but the test framework does other things like wiping indices, repositories, etc
        // that the system user cannot do. so we wrap the node client with a user that can do these things since the client() calls
        // return a node client
        return client -> asInstanceOf(NodeClient.class, client).filterWithHeader(headers);
    }

    private Settings externalClusterClientSettings() {
        final Path home = createTempDir();
        final Path xpackConf = home.resolve("config");
        try {
            Files.createDirectories(xpackConf);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        writeFile(xpackConf, "users", "x_pack_rest_user" + ":" + SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING + "\n");
        writeFile(xpackConf, "users_roles", SecuritySettingsSourceField.ES_TEST_ROOT_ROLE + ":x_pack_rest_user\n");
        writeFile(xpackConf, "roles.yml", SecuritySettingsSourceField.ES_TEST_ROOT_ROLE_YML);

        Path key;
        Path certificate;
        try {
            key = PathUtils.get(getClass().getResource("/testnode.pem").toURI());
            certificate = PathUtils.get(getClass().getResource("/testnode.crt").toURI());
        } catch (URISyntaxException e) {
            throw new IllegalStateException("error trying to get keystore path", e);
        }

        Settings.Builder builder = Settings.builder();
        builder.putList("node.roles", Collections.emptyList());
        builder.put(NetworkModule.TRANSPORT_TYPE_KEY, SecurityField.NAME4);
        builder.put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), true);
        builder.put(XPackSettings.SECURITY_ENABLED.getKey(), true);
        builder.put(MachineLearningField.AUTODETECT_PROCESS.getKey(), false);
        builder.put(XPackSettings.WATCHER_ENABLED.getKey(), false);
        builder.put(ELASTICSEARCH_COLLECTION_ENABLED.getKey(), false);
        builder.put(LifecycleSettings.LIFECYCLE_HISTORY_INDEX_ENABLED_SETTING.getKey(), false);
        builder.put(LifecycleSettings.SLM_HISTORY_INDEX_ENABLED_SETTING.getKey(), false);
        builder.put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial");
        builder.put(Environment.PATH_HOME_SETTING.getKey(), home);
        builder.put("xpack.security.transport.ssl.enabled", true);
        builder.put("xpack.security.transport.ssl.key", key.toAbsolutePath().toString());
        builder.put("xpack.security.transport.ssl.certificate", certificate.toAbsolutePath().toString());
        builder.put("xpack.security.transport.ssl.key_passphrase", "testnode");
        builder.put("xpack.security.transport.ssl.verification_mode", "certificate");
        return builder.build();
    }

    @Override
    protected TestCluster buildTestCluster(Scope scope, long seed) throws IOException {
        final String clusterAddresses = System.getProperty(TESTS_CLUSTER);
        assertTrue(TESTS_CLUSTER + " must be set", Strings.hasLength(clusterAddresses));
        if (scope == Scope.TEST) {
            throw new IllegalArgumentException("Cannot run TEST scope test with " + TESTS_CLUSTER);
        }
        final String clusterName = System.getProperty(TESTS_CLUSTER_NAME);
        if (Strings.isNullOrEmpty(clusterName)) {
            throw new IllegalArgumentException("External test cluster name must be provided");
        }
        final String[] stringAddresses = clusterAddresses.split(",");
        final TransportAddress[] transportAddresses = new TransportAddress[stringAddresses.length];
        int i = 0;
        for (String stringAddress : stringAddresses) {
            URL url = new URL("http://" + stringAddress);
            InetAddress inetAddress = InetAddress.getByName(url.getHost());
            transportAddresses[i++] = new TransportAddress(new InetSocketAddress(inetAddress, url.getPort()));
        }
        return new ExternalTestCluster(
            createTempDir(),
            externalClusterClientSettings(),
            nodePlugins(),
            getClientWrapper(),
            clusterName,
            transportAddresses
        );
    }

    protected void cleanUp() {
        setUpgradeModeTo(false);
        cleanUpResources();
    }

    @Override
    protected Set<String> excludeTemplates() {
        return new HashSet<>(
            Arrays.asList(
                NotificationsIndex.NOTIFICATIONS_INDEX,
                MlMetaIndex.indexName(),
                AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX,
                AnomalyDetectorsIndex.jobResultsIndexPrefix(),
                InferenceIndexConstants.LATEST_INDEX_NAME,
                SnapshotLifecycleTemplateRegistry.SLM_TEMPLATE_NAME,
                ".deprecation-indexing-template",
                ".deprecation-indexing-settings",
                ".deprecation-indexing-mappings"
            )
        );
    }

    protected void cleanUpResources() {
        client().execute(ResetFeatureStateAction.INSTANCE, new ResetFeatureStateRequest(TEST_REQUEST_TIMEOUT)).actionGet();
    }

    protected void setUpgradeModeTo(boolean enabled) {
        AcknowledgedResponse response = client().execute(SetUpgradeModeAction.INSTANCE, new SetUpgradeModeAction.Request(enabled))
            .actionGet();
        assertThat(response.isAcknowledged(), is(true));
        assertThat(upgradeMode(), is(enabled));
    }

    protected boolean upgradeMode() {
        ClusterState masterClusterState = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).all().get().getState();
        MlMetadata mlMetadata = MlMetadata.getMlMetadata(masterClusterState);
        return mlMetadata.isUpgradeMode();
    }

    protected DeleteExpiredDataAction.Response deleteExpiredData() throws Exception {
        DeleteExpiredDataAction.Response response = client().execute(
            DeleteExpiredDataAction.INSTANCE,
            new DeleteExpiredDataAction.Request()
        ).get();

        // We need to refresh to ensure the deletion is visible
        refresh("*");

        return response;
    }

    protected DeleteExpiredDataAction.Response deleteExpiredData(Float customThrottle) throws Exception {
        DeleteExpiredDataAction.Request request = new DeleteExpiredDataAction.Request();
        request.setRequestsPerSecond(customThrottle);
        DeleteExpiredDataAction.Response response = client().execute(DeleteExpiredDataAction.INSTANCE, request).get();
        // We need to refresh to ensure the deletion is visible
        refresh("*");

        return response;
    }

    protected PutFilterAction.Response putMlFilter(MlFilter filter) {
        return client().execute(PutFilterAction.INSTANCE, new PutFilterAction.Request(filter)).actionGet();
    }

    protected static List<String> fetchAllAuditMessages(String jobId) throws Exception {
        RefreshRequest refreshRequest = new RefreshRequest(NotificationsIndex.NOTIFICATIONS_INDEX);
        BroadcastResponse refreshResponse = client().execute(RefreshAction.INSTANCE, refreshRequest).actionGet();
        assertThat(refreshResponse.getStatus().getStatus(), anyOf(equalTo(200), equalTo(201)));

        SearchRequest searchRequest = new SearchRequestBuilder(client()).setIndices(NotificationsIndex.NOTIFICATIONS_INDEX)
            .addSort("timestamp", SortOrder.ASC)
            .setQuery(QueryBuilders.termQuery("job_id", jobId))
            .setSize(100)
            .request();
        List<String> messages = new ArrayList<>();
        assertResponse(
            client().execute(TransportSearchAction.TYPE, searchRequest),
            searchResponse -> Arrays.stream(searchResponse.getHits().getHits())
                .map(hit -> (String) hit.getSourceAsMap().get("message"))
                .forEach(messages::add)
        );
        return messages;
    }

    @Override
    protected void ensureClusterStateConsistency() throws IOException {
        if (cluster() != null && cluster().size() > 0) {
            List<NamedWriteableRegistry.Entry> entries = new ArrayList<>(ClusterModule.getNamedWriteables());
            entries.addAll(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedWriteables());
            entries.add(
                new NamedWriteableRegistry.Entry(
                    Metadata.Custom.class,
                    TrainedModelAssignmentMetadata.NAME,
                    TrainedModelAssignmentMetadata::fromStream
                )
            );
            entries.add(
                new NamedWriteableRegistry.Entry(
                    NamedDiff.class,
                    TrainedModelAssignmentMetadata.NAME,
                    TrainedModelAssignmentMetadata::readDiffFrom
                )
            );
            entries.add(new NamedWriteableRegistry.Entry(Metadata.Custom.class, ModelAliasMetadata.NAME, ModelAliasMetadata::new));
            entries.add(new NamedWriteableRegistry.Entry(NamedDiff.class, ModelAliasMetadata.NAME, ModelAliasMetadata::readDiffFrom));
            entries.add(
                new NamedWriteableRegistry.Entry(Metadata.Custom.class, TrainedModelCacheMetadata.NAME, TrainedModelCacheMetadata::new)
            );
            entries.add(
                new NamedWriteableRegistry.Entry(NamedDiff.class, TrainedModelCacheMetadata.NAME, TrainedModelCacheMetadata::readDiffFrom)
            );
            entries.add(new NamedWriteableRegistry.Entry(Metadata.Custom.class, "ml", MlMetadata::new));
            entries.add(new NamedWriteableRegistry.Entry(Metadata.Custom.class, IndexLifecycleMetadata.TYPE, IndexLifecycleMetadata::new));
            entries.add(
                new NamedWriteableRegistry.Entry(
                    LifecycleType.class,
                    TimeseriesLifecycleType.TYPE,
                    (in) -> TimeseriesLifecycleType.INSTANCE
                )
            );
            entries.add(new NamedWriteableRegistry.Entry(LifecycleAction.class, DeleteAction.NAME, DeleteAction::readFrom));
            entries.add(new NamedWriteableRegistry.Entry(LifecycleAction.class, ForceMergeAction.NAME, ForceMergeAction::new));
            entries.add(new NamedWriteableRegistry.Entry(LifecycleAction.class, RolloverAction.NAME, RolloverAction::read));
            entries.add(new NamedWriteableRegistry.Entry(LifecycleAction.class, ShrinkAction.NAME, ShrinkAction::new));
            entries.add(
                new NamedWriteableRegistry.Entry(
                    PersistentTaskParams.class,
                    MlTasks.DATAFEED_TASK_NAME,
                    StartDatafeedAction.DatafeedParams::new
                )
            );
            entries.add(
                new NamedWriteableRegistry.Entry(
                    PersistentTaskParams.class,
                    MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME,
                    StartDataFrameAnalyticsAction.TaskParams::new
                )
            );
            entries.add(new NamedWriteableRegistry.Entry(PersistentTaskParams.class, MlTasks.JOB_TASK_NAME, OpenJobAction.JobParams::new));
            entries.add(new NamedWriteableRegistry.Entry(PersistentTaskState.class, JobTaskState.NAME, JobTaskState::new));
            entries.add(new NamedWriteableRegistry.Entry(PersistentTaskState.class, DatafeedState.NAME, DatafeedState::fromStream));
            entries.add(
                new NamedWriteableRegistry.Entry(
                    PersistentTaskState.class,
                    DataFrameAnalyticsTaskState.NAME,
                    DataFrameAnalyticsTaskState::new
                )
            );
            entries.add(new NamedWriteableRegistry.Entry(ClusterState.Custom.class, TokenMetadata.TYPE, TokenMetadata::new));
            entries.add(new NamedWriteableRegistry.Entry(Metadata.Custom.class, AutoscalingMetadata.NAME, AutoscalingMetadata::new));
            entries.add(
                new NamedWriteableRegistry.Entry(
                    NamedDiff.class,
                    AutoscalingMetadata.NAME,
                    AutoscalingMetadata.AutoscalingMetadataDiff::new
                )
            );
            entries.add(
                new NamedWriteableRegistry.Entry(AutoscalingDeciderResult.Reason.class, MlScalingReason.NAME, MlScalingReason::new)
            );
            doEnsureClusterStateConsistency(new NamedWriteableRegistry(entries));
        }
    }

    protected static void createDataStreamAndTemplate(String dataStreamName, String mapping) throws IOException {
        client().execute(
            TransportPutComposableIndexTemplateAction.TYPE,
            new TransportPutComposableIndexTemplateAction.Request(dataStreamName + "_template").indexTemplate(
                ComposableIndexTemplate.builder()
                    .indexPatterns(Collections.singletonList(dataStreamName))
                    .template(new Template(null, new CompressedXContent(mapping), null))
                    .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                    .build()
            )
        ).actionGet();
        client().execute(
            CreateDataStreamAction.INSTANCE,
            new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, dataStreamName)
        ).actionGet();
    }

    public static class MockPainlessScriptEngine extends MockScriptEngine {

        public static final String NAME = "painless";

        public static class TestPlugin extends MockScriptPlugin {
            @Override
            public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
                return new MockPainlessScriptEngine();
            }

            @Override
            protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
                return Collections.emptyMap();
            }
        }

        @Override
        public String getType() {
            return NAME;
        }

        @Override
        public <T> T compile(String name, String script, ScriptContext<T> context, Map<String, String> options) {
            if (context.instanceClazz.equals(ScoreScript.class)) {
                return context.factoryClazz.cast(new MockScoreScript(MockDeterministicScript.asDeterministic(p -> 0.0)));
            }
            if (context.name.equals("ingest")) {
                IngestScript.Factory factory = (params, ctx) -> new IngestScript(params, ctx) {
                    @Override
                    public void execute() {}
                };
                return context.factoryClazz.cast(factory);
            }
            throw new IllegalArgumentException("mock painless does not know how to handle context [" + context.name + "]");
        }
    }
}
