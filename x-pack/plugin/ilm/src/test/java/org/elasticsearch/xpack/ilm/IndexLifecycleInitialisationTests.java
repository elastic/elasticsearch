/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ilm;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.Index;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ilm.ClusterStateWaitStep;
import org.elasticsearch.xpack.core.ilm.ExplainLifecycleRequest;
import org.elasticsearch.xpack.core.ilm.ExplainLifecycleResponse;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleExplainResponse;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecycleExecutionState;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.LifecycleType;
import org.elasticsearch.xpack.core.ilm.MockAction;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.PhaseExecutionInfo;
import org.elasticsearch.xpack.core.ilm.Step;
import org.elasticsearch.xpack.core.ilm.StopILMRequest;
import org.elasticsearch.xpack.core.ilm.TerminalPolicyStep;
import org.elasticsearch.xpack.core.ilm.action.ExplainLifecycleAction;
import org.elasticsearch.xpack.core.ilm.action.GetLifecycleAction;
import org.elasticsearch.xpack.core.ilm.action.GetStatusAction;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleAction;
import org.elasticsearch.xpack.core.ilm.action.StopILMAction;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.elasticsearch.client.Requests.clusterHealthRequest;
import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.core.ilm.LifecyclePolicyTestsUtils.newLockableLifecyclePolicy;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.core.CombinableMatcher.both;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;

@ESIntegTestCase.ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class IndexLifecycleInitialisationTests extends ESIntegTestCase {
    private Settings settings;
    private LifecyclePolicy lifecyclePolicy;
    private Phase mockPhase;
    private static final ObservableAction OBSERVABLE_ACTION;
    static {
        List<Step> steps = new ArrayList<>();
        Step.StepKey key = new Step.StepKey("mock", ObservableAction.NAME, ObservableClusterStateWaitStep.NAME);
        steps.add(new ObservableClusterStateWaitStep(key, TerminalPolicyStep.KEY));
        OBSERVABLE_ACTION = new ObservableAction(steps, true);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal));
        settings.put(XPackSettings.INDEX_LIFECYCLE_ENABLED.getKey(), true);
        settings.put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), false);
        settings.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        settings.put(XPackSettings.WATCHER_ENABLED.getKey(), false);
        settings.put(XPackSettings.MONITORING_ENABLED.getKey(), false);
        settings.put(XPackSettings.GRAPH_ENABLED.getKey(), false);
        settings.put(XPackSettings.LOGSTASH_ENABLED.getKey(), false);
        settings.put(LifecycleSettings.LIFECYCLE_POLL_INTERVAL, "1s");

        // This is necessary to prevent SLM installing a lifecycle policy, these tests assume a blank slate
        settings.put(LifecycleSettings.SLM_HISTORY_INDEX_ENABLED_SETTING.getKey(), false);
        return settings.build();
    }

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateCompositeXPackPlugin.class, IndexLifecycle.class, TestILMPlugin.class);
    }

    @Before
    public void init() {
        settings = Settings.builder().put(indexSettings()).put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 0).put(LifecycleSettings.LIFECYCLE_NAME, "test").build();
        List<Step> steps = new ArrayList<>();
        Step.StepKey key = new Step.StepKey("mock", ObservableAction.NAME, ObservableClusterStateWaitStep.NAME);
        steps.add(new ObservableClusterStateWaitStep(key, TerminalPolicyStep.KEY));
        Map<String, LifecycleAction> actions = Collections.singletonMap(ObservableAction.NAME, OBSERVABLE_ACTION);
        mockPhase = new Phase("mock", TimeValue.timeValueSeconds(0), actions);
        Map<String, Phase> phases = Collections.singletonMap("mock", mockPhase);
        lifecyclePolicy = newLockableLifecyclePolicy("test", phases);
    }

    public void testSingleNodeCluster() throws Exception {
        settings = Settings.builder().put(settings).put("index.lifecycle.test.complete", true).build();
        // start master node
        logger.info("Starting server1");
        final String server_1 = internalCluster().startNode();
        final String node1 = getLocalNodeId(server_1);

        // test get-lifecycle behavior when IndexLifecycleMetaData is null
        GetLifecycleAction.Response getUninitializedLifecycleResponse = client().execute(GetLifecycleAction.INSTANCE,
            new GetLifecycleAction.Request()).get();
        assertThat(getUninitializedLifecycleResponse.getPolicies().size(), equalTo(0));
        ExecutionException exception = expectThrows(ExecutionException.class,() -> client()
            .execute(GetLifecycleAction.INSTANCE, new GetLifecycleAction.Request("non-existent-policy")).get());
        assertThat(exception.getMessage(), containsString("Lifecycle policy not found: [non-existent-policy]"));

        logger.info("Creating lifecycle [test_lifecycle]");
        PutLifecycleAction.Request putLifecycleRequest = new PutLifecycleAction.Request(lifecyclePolicy);
        long lowerBoundModifiedDate = Instant.now().toEpochMilli();
        PutLifecycleAction.Response putLifecycleResponse = client().execute(PutLifecycleAction.INSTANCE, putLifecycleRequest).get();
        assertAcked(putLifecycleResponse);
        long upperBoundModifiedDate = Instant.now().toEpochMilli();

        // assert version and modified_date
        GetLifecycleAction.Response getLifecycleResponse = client().execute(GetLifecycleAction.INSTANCE,
            new GetLifecycleAction.Request()).get();
        assertThat(getLifecycleResponse.getPolicies().size(), equalTo(1));
        GetLifecycleAction.LifecyclePolicyResponseItem responseItem = getLifecycleResponse.getPolicies().get(0);
        assertThat(responseItem.getLifecyclePolicy(), equalTo(lifecyclePolicy));
        assertThat(responseItem.getVersion(), equalTo(1L));
        long actualModifiedDate = Instant.parse(responseItem.getModifiedDate()).toEpochMilli();
        assertThat(actualModifiedDate,
            is(both(greaterThanOrEqualTo(lowerBoundModifiedDate)).and(lessThanOrEqualTo(upperBoundModifiedDate))));

        logger.info("Creating index [test]");
        CreateIndexResponse createIndexResponse = client().admin().indices().create(createIndexRequest("test").settings(settings))
                .actionGet();
        assertAcked(createIndexResponse);
        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        RoutingNode routingNodeEntry1 = clusterState.getRoutingNodes().node(node1);
        assertThat(routingNodeEntry1.numberOfShardsWithState(STARTED), equalTo(1));
        assertBusy(() -> {
            assertTrue(indexExists("test"));
        });
        IndexLifecycleService indexLifecycleService = internalCluster().getInstance(IndexLifecycleService.class, server_1);
        assertThat(indexLifecycleService.getScheduler().jobCount(), equalTo(1));
        assertNotNull(indexLifecycleService.getScheduledJob());
        assertBusy(() -> {
            LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(client().admin().cluster()
                .prepareState().execute().actionGet().getState().getMetaData().index("test"));
            assertThat(lifecycleState.getStep(), equalTo(TerminalPolicyStep.KEY.getName()));
        });
    }

    public void testExplainExecution() throws Exception {
        // start node
        logger.info("Starting server1");
        final String server_1 = internalCluster().startNode();
        logger.info("Creating lifecycle [test_lifecycle]");
        PutLifecycleAction.Request putLifecycleRequest = new PutLifecycleAction.Request(lifecyclePolicy);
        PutLifecycleAction.Response putLifecycleResponse = client().execute(PutLifecycleAction.INSTANCE, putLifecycleRequest).get();
        assertAcked(putLifecycleResponse);

        GetLifecycleAction.Response getLifecycleResponse = client().execute(GetLifecycleAction.INSTANCE,
            new GetLifecycleAction.Request()).get();
        assertThat(getLifecycleResponse.getPolicies().size(), equalTo(1));
        GetLifecycleAction.LifecyclePolicyResponseItem responseItem = getLifecycleResponse.getPolicies().get(0);
        assertThat(responseItem.getLifecyclePolicy(), equalTo(lifecyclePolicy));
        assertThat(responseItem.getVersion(), equalTo(1L));
        long actualModifiedDate = Instant.parse(responseItem.getModifiedDate()).toEpochMilli();

        logger.info("Creating index [test]");
        CreateIndexResponse createIndexResponse = client().admin().indices().create(createIndexRequest("test").settings(settings))
            .actionGet();
        assertAcked(createIndexResponse);

        {
            PhaseExecutionInfo expectedExecutionInfo = new PhaseExecutionInfo(lifecyclePolicy.getName(), mockPhase, 1L, actualModifiedDate);
            assertBusy(() -> {
                ExplainLifecycleRequest explainRequest = new ExplainLifecycleRequest();
                ExplainLifecycleResponse explainResponse = client().execute(ExplainLifecycleAction.INSTANCE, explainRequest).get();
                assertThat(explainResponse.getIndexResponses().size(), equalTo(1));
                IndexLifecycleExplainResponse indexResponse = explainResponse.getIndexResponses().get("test");
                assertThat(indexResponse.getStep(), equalTo("observable_cluster_state_action"));
                assertThat(indexResponse.getPhaseExecutionInfo(), equalTo(expectedExecutionInfo));
            });
        }

        // complete the step
        client().admin().indices().prepareUpdateSettings("test")
            .setSettings(Collections.singletonMap("index.lifecycle.test.complete", true)).get();

        {
            PhaseExecutionInfo expectedExecutionInfo = new PhaseExecutionInfo(lifecyclePolicy.getName(), null, 1L, actualModifiedDate);
            assertBusy(() -> {
                ExplainLifecycleRequest explainRequest = new ExplainLifecycleRequest();
                ExplainLifecycleResponse explainResponse = client().execute(ExplainLifecycleAction.INSTANCE, explainRequest).get();
                assertThat(explainResponse.getIndexResponses().size(), equalTo(1));
                IndexLifecycleExplainResponse indexResponse = explainResponse.getIndexResponses().get("test");
                assertThat(indexResponse.getPhase(), equalTo(TerminalPolicyStep.COMPLETED_PHASE));
                assertThat(indexResponse.getStep(), equalTo(TerminalPolicyStep.KEY.getName()));
                assertThat(indexResponse.getPhaseExecutionInfo(), equalTo(expectedExecutionInfo));
            });
        }
    }

    public void testMasterDedicatedDataDedicated() throws Exception {
        settings = Settings.builder().put(settings).put("index.lifecycle.test.complete", true).build();
        // start master node
        logger.info("Starting master-only server1");
        final String server_1 = internalCluster().startMasterOnlyNode();
        // start data node
        logger.info("Starting data-only server2");
        final String server_2 = internalCluster().startDataOnlyNode();
        final String node2 = getLocalNodeId(server_2);

        // check that the scheduler was started on the appropriate node
        {
            IndexLifecycleService indexLifecycleService = internalCluster().getInstance(IndexLifecycleService.class, server_1);
            assertThat(indexLifecycleService.getScheduler().jobCount(), equalTo(1));
            assertNotNull(indexLifecycleService.getScheduledJob());
        }
        {
            IndexLifecycleService indexLifecycleService = internalCluster().getInstance(IndexLifecycleService.class, server_2);
            assertNull(indexLifecycleService.getScheduler());
            assertNull(indexLifecycleService.getScheduledJob());
        }

        logger.info("Creating lifecycle [test_lifecycle]");
        PutLifecycleAction.Request putLifecycleRequest = new PutLifecycleAction.Request(lifecyclePolicy);
        PutLifecycleAction.Response putLifecycleResponse = client().execute(PutLifecycleAction.INSTANCE, putLifecycleRequest).get();
        assertAcked(putLifecycleResponse);
        logger.info("Creating index [test]");
        CreateIndexResponse createIndexResponse = client().admin().indices().create(createIndexRequest("test").settings(settings))
                .actionGet();
        assertAcked(createIndexResponse);

        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        RoutingNode routingNodeEntry1 = clusterState.getRoutingNodes().node(node2);
        assertThat(routingNodeEntry1.numberOfShardsWithState(STARTED), equalTo(1));

        assertBusy(() -> assertTrue(indexExists("test")));
        assertBusy(() -> {
            LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(client().admin().cluster()
                .prepareState().execute().actionGet().getState().getMetaData().index("test"));
            assertThat(lifecycleState.getStep(), equalTo(TerminalPolicyStep.KEY.getName()));
        });
    }

    public void testMasterFailover() throws Exception {
        // start one server
        logger.info("Starting sever1");
        final String server_1 = internalCluster().startNode();
        final String node1 = getLocalNodeId(server_1);

        logger.info("Creating lifecycle [test_lifecycle]");
        PutLifecycleAction.Request putLifecycleRequest = new PutLifecycleAction.Request(lifecyclePolicy);
        PutLifecycleAction.Response putLifecycleResponse = client().execute(PutLifecycleAction.INSTANCE, putLifecycleRequest).get();
        assertAcked(putLifecycleResponse);

        logger.info("Creating index [test]");
        CreateIndexResponse createIndexResponse = client().admin().indices().create(createIndexRequest("test").settings(settings))
                .actionGet();
        assertAcked(createIndexResponse);

        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        RoutingNode routingNodeEntry1 = clusterState.getRoutingNodes().node(node1);
        assertThat(routingNodeEntry1.numberOfShardsWithState(STARTED), equalTo(1));

        logger.info("Starting server2");
        // start another server
        internalCluster().startNode();

        // first wait for 2 nodes in the cluster
        logger.info("Waiting for replicas to be assigned");
        ClusterHealthResponse clusterHealth = client().admin().cluster()
                .health(clusterHealthRequest().waitForGreenStatus().waitForNodes("2")).actionGet();
        logger.info("Done Cluster Health, status {}", clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        // check step in progress in lifecycle
        assertBusy(() -> {
            LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(client().admin().cluster()
                .prepareState().execute().actionGet().getState().getMetaData().index("test"));
            assertThat(lifecycleState.getStep(), equalTo(ObservableClusterStateWaitStep.NAME));
        });

        if (randomBoolean()) {
            // this checks that the phase execution is picked up from the phase definition settings
            logger.info("updating lifecycle [test_lifecycle] to be empty");
            PutLifecycleAction.Request updateLifecycleRequest = new PutLifecycleAction.Request
                (newLockableLifecyclePolicy(lifecyclePolicy.getName(), Collections.emptyMap()));
            PutLifecycleAction.Response updateLifecycleResponse = client()
                .execute(PutLifecycleAction.INSTANCE, updateLifecycleRequest).get();
            assertAcked(updateLifecycleResponse);
        }


        logger.info("Closing server1");
        // kill the first server
        internalCluster().stopCurrentMasterNode();

        // check that index lifecycle picked back up where it
        assertBusy(() -> {
            LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(client().admin().cluster()
                .prepareState().execute().actionGet().getState().getMetaData().index("test"));
            assertThat(lifecycleState.getStep(), equalTo(ObservableClusterStateWaitStep.NAME));
        });

        logger.info("new master is operation");
        // complete the step
        AcknowledgedResponse repsonse = client().admin().indices().prepareUpdateSettings("test")
            .setSettings(Collections.singletonMap("index.lifecycle.test.complete", true)).get();

        assertBusy(() -> {
            LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(client().admin().cluster()
                .prepareState().execute().actionGet().getState().getMetaData().index("test"));
            assertThat(lifecycleState.getStep(), equalTo(TerminalPolicyStep.KEY.getName()));
        });
    }

    public void testCreatePolicyWhenStopped() throws Exception {
        // start master node
        logger.info("Starting server1");
        final String server_1 = internalCluster().startNode();
        final String node1 = getLocalNodeId(server_1);

        assertAcked(client().execute(StopILMAction.INSTANCE, new StopILMRequest()).get());
        assertBusy(() -> assertThat(
            client().execute(GetStatusAction.INSTANCE, new GetStatusAction.Request()).get().getMode(),
            equalTo(OperationMode.STOPPED)));

        logger.info("Creating lifecycle [test_lifecycle]");
        PutLifecycleAction.Request putLifecycleRequest = new PutLifecycleAction.Request(lifecyclePolicy);
        long lowerBoundModifiedDate = Instant.now().toEpochMilli();
        PutLifecycleAction.Response putLifecycleResponse = client().execute(PutLifecycleAction.INSTANCE, putLifecycleRequest).get();
        assertAcked(putLifecycleResponse);
        long upperBoundModifiedDate = Instant.now().toEpochMilli();

        // assert version and modified_date
        GetLifecycleAction.Response getLifecycleResponse = client().execute(GetLifecycleAction.INSTANCE,
            new GetLifecycleAction.Request()).get();
        assertThat(getLifecycleResponse.getPolicies().size(), equalTo(1));
        GetLifecycleAction.LifecyclePolicyResponseItem responseItem = getLifecycleResponse.getPolicies().get(0);
        assertThat(responseItem.getLifecyclePolicy(), equalTo(lifecyclePolicy));
        assertThat(responseItem.getVersion(), equalTo(1L));
        long actualModifiedDate = Instant.parse(responseItem.getModifiedDate()).toEpochMilli();
        assertThat(actualModifiedDate,
            is(both(greaterThanOrEqualTo(lowerBoundModifiedDate)).and(lessThanOrEqualTo(upperBoundModifiedDate))));
        // assert ILM is still stopped
        GetStatusAction.Response statusResponse = client().execute(GetStatusAction.INSTANCE, new GetStatusAction.Request()).get();
        assertThat(statusResponse.getMode(), equalTo(OperationMode.STOPPED));
    }

    public void testPollIntervalUpdate() throws Exception {
        TimeValue pollInterval = TimeValue.timeValueSeconds(randomLongBetween(1, 5));
        final String server_1 = internalCluster().startMasterOnlyNode(
            Settings.builder().put(LifecycleSettings.LIFECYCLE_POLL_INTERVAL, pollInterval.getStringRep()).build());
        IndexLifecycleService indexLifecycleService = internalCluster().getInstance(IndexLifecycleService.class, server_1);
        assertBusy(() -> {
            assertNotNull(indexLifecycleService.getScheduler());
            assertThat(indexLifecycleService.getScheduler().jobCount(), equalTo(1));
        });
        {
            TimeValueSchedule schedule = (TimeValueSchedule) indexLifecycleService.getScheduledJob().getSchedule();
            assertThat(schedule.getInterval(), equalTo(pollInterval));
        }

        // update the poll interval
        TimeValue newPollInterval = TimeValue.timeValueHours(randomLongBetween(6, 1000));
        Settings newIntervalSettings = Settings.builder().put(LifecycleSettings.LIFECYCLE_POLL_INTERVAL,
            newPollInterval.getStringRep()).build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(newIntervalSettings));
        {
            TimeValueSchedule schedule = (TimeValueSchedule) indexLifecycleService.getScheduledJob().getSchedule();
            assertThat(schedule.getInterval(), equalTo(newPollInterval));
        }
    }

    private String getLocalNodeId(String name) {
        TransportService transportService = internalCluster().getInstance(TransportService.class, name);
        String nodeId = transportService.getLocalNode().getId();
        assertThat(nodeId, not(nullValue()));
        return nodeId;
    }

    public static class TestILMPlugin extends Plugin {
        public TestILMPlugin() {
        }

        public List<Setting<?>> getSettings() {
            final Setting<Boolean> COMPLETE_SETTING = Setting.boolSetting("index.lifecycle.test.complete", false,
                Setting.Property.Dynamic, Setting.Property.IndexScope);
            return Collections.singletonList(COMPLETE_SETTING);
        }

        @Override
        public List<NamedXContentRegistry.Entry> getNamedXContent() {
            return Arrays.asList(
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ObservableAction.NAME), (p) -> {
                    MockAction.parse(p);
                    return OBSERVABLE_ACTION;
                })
            );
        }

        @Override
        public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
            return Arrays.asList(new NamedWriteableRegistry.Entry(LifecycleType.class, LockableLifecycleType.TYPE,
                    (in) -> LockableLifecycleType.INSTANCE),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, ObservableAction.NAME, ObservableAction::readObservableAction),
                new NamedWriteableRegistry.Entry(ObservableClusterStateWaitStep.class, ObservableClusterStateWaitStep.NAME,
                    ObservableClusterStateWaitStep::new));
        }
    }

    public static class ObservableClusterStateWaitStep extends ClusterStateWaitStep implements NamedWriteable {
        public static final String NAME = "observable_cluster_state_action";

        public ObservableClusterStateWaitStep(StepKey current, StepKey next) {
            super(current, next);
        }

        public ObservableClusterStateWaitStep(StreamInput in) throws IOException {
            this(new StepKey(in.readString(), in.readString(), in.readString()), readOptionalNextStepKey(in));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(getKey().getPhase());
            out.writeString(getKey().getAction());
            out.writeString(getKey().getName());
            boolean hasNextStep = getNextStepKey() != null;
            out.writeBoolean(hasNextStep);
            if (hasNextStep) {
                out.writeString(getNextStepKey().getPhase());
                out.writeString(getNextStepKey().getAction());
                out.writeString(getNextStepKey().getName());
            }
        }

        private static StepKey readOptionalNextStepKey(StreamInput in) throws IOException {
            if (in.readBoolean()) {
                return new StepKey(in.readString(), in.readString(), in.readString());
            }
            return null;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public Result isConditionMet(Index index, ClusterState clusterState) {
            boolean complete = clusterState.metaData().index("test").getSettings()
                .getAsBoolean("index.lifecycle.test.complete", false);
            return new Result(complete, null);
        }
    }

    public static class ObservableAction extends MockAction {

        ObservableAction(List<Step> steps, boolean safe) {
            super(steps, safe);
        }

        public static ObservableAction readObservableAction(StreamInput in) throws IOException {
            List<Step> steps = in.readList(ObservableClusterStateWaitStep::new);
            boolean safe = in.readBoolean();
            return new ObservableAction(steps, safe);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeList(getSteps().stream().map(s -> (ObservableClusterStateWaitStep) s).collect(Collectors.toList()));
            out.writeBoolean(isSafeAction());
        }
    }
}
