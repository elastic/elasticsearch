/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ilm;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.scheduler.TimeValueSchedule;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ilm.ClusterStateWaitStep;
import org.elasticsearch.xpack.core.ilm.ExplainLifecycleRequest;
import org.elasticsearch.xpack.core.ilm.ExplainLifecycleResponse;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleExplainResponse;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.LifecycleType;
import org.elasticsearch.xpack.core.ilm.MockAction;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.PhaseCompleteStep;
import org.elasticsearch.xpack.core.ilm.PhaseExecutionInfo;
import org.elasticsearch.xpack.core.ilm.Step;
import org.elasticsearch.xpack.core.ilm.StopILMRequest;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static java.time.format.DateTimeFormatter.ISO_ZONED_DATE_TIME;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.ilm.LifecyclePolicyTestsUtils.newLockableLifecyclePolicy;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
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
        Step.StepKey compKey = new Step.StepKey("mock", "complete", "complete");
        steps.add(new ObservableClusterStateWaitStep(key, compKey));
        OBSERVABLE_ACTION = new ObservableAction(steps, true);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder nodeSettings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        nodeSettings.put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), false);
        nodeSettings.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        nodeSettings.put(XPackSettings.WATCHER_ENABLED.getKey(), false);
        nodeSettings.put(XPackSettings.GRAPH_ENABLED.getKey(), false);
        nodeSettings.put(LifecycleSettings.LIFECYCLE_POLL_INTERVAL, "1s");

        // This is necessary to prevent ILM and SLM installing a lifecycle policy, these tests assume a blank slate
        nodeSettings.put(LifecycleSettings.LIFECYCLE_HISTORY_INDEX_ENABLED, false);
        nodeSettings.put(LifecycleSettings.SLM_HISTORY_INDEX_ENABLED_SETTING.getKey(), false);
        return nodeSettings.build();
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
        settings = Settings.builder()
            .put(indexSettings())
            .put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 0)
            .put(LifecycleSettings.LIFECYCLE_NAME, "test")
            .build();
        List<Step> steps = new ArrayList<>();
        Step.StepKey key = new Step.StepKey("mock", ObservableAction.NAME, ObservableClusterStateWaitStep.NAME);
        Step.StepKey compKey = new Step.StepKey("mock", "complete", "complete");
        steps.add(new ObservableClusterStateWaitStep(key, compKey));
        steps.add(new PhaseCompleteStep(compKey, null));
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

        // test get-lifecycle behavior when IndexLifecycleMetadata is null
        GetLifecycleAction.Response getUninitializedLifecycleResponse = client().execute(
            GetLifecycleAction.INSTANCE,
            new GetLifecycleAction.Request()
        ).get();
        assertThat(getUninitializedLifecycleResponse.getPolicies().size(), equalTo(0));
        ExecutionException exception = expectThrows(
            ExecutionException.class,
            () -> client().execute(GetLifecycleAction.INSTANCE, new GetLifecycleAction.Request("non-existent-policy")).get()
        );
        assertThat(exception.getMessage(), containsString("Lifecycle policy not found: [non-existent-policy]"));

        logger.info("Creating lifecycle [test_lifecycle]");
        PutLifecycleAction.Request putLifecycleRequest = new PutLifecycleAction.Request(lifecyclePolicy);
        long lowerBoundModifiedDate = Instant.now().toEpochMilli();
        assertAcked(client().execute(PutLifecycleAction.INSTANCE, putLifecycleRequest).get());
        long upperBoundModifiedDate = Instant.now().toEpochMilli();

        // assert version and modified_date
        GetLifecycleAction.Response getLifecycleResponse = client().execute(GetLifecycleAction.INSTANCE, new GetLifecycleAction.Request())
            .get();
        assertThat(getLifecycleResponse.getPolicies().size(), equalTo(1));
        GetLifecycleAction.LifecyclePolicyResponseItem responseItem = getLifecycleResponse.getPolicies().get(0);
        assertThat(responseItem.getLifecyclePolicy(), equalTo(lifecyclePolicy));
        assertThat(responseItem.getVersion(), equalTo(1L));
        long actualModifiedDate = Instant.from(ISO_ZONED_DATE_TIME.parse(responseItem.getModifiedDate())).toEpochMilli();
        assertThat(
            actualModifiedDate,
            is(both(greaterThanOrEqualTo(lowerBoundModifiedDate)).and(lessThanOrEqualTo(upperBoundModifiedDate)))
        );

        logger.info("Creating index [test]");
        CreateIndexResponse createIndexResponse = indicesAdmin().create(new CreateIndexRequest("test").settings(settings)).actionGet();
        assertAcked(createIndexResponse);
        ClusterState clusterState = clusterAdmin().prepareState().get().getState();
        RoutingNode routingNodeEntry1 = clusterState.getRoutingNodes().node(node1);
        assertThat(routingNodeEntry1.numberOfShardsWithState(STARTED), equalTo(1));
        assertBusy(() -> { assertTrue(indexExists("test")); });
        IndexLifecycleService indexLifecycleService = internalCluster().getInstance(IndexLifecycleService.class, server_1);
        assertThat(indexLifecycleService.getScheduler().jobCount(), equalTo(1));
        assertNotNull(indexLifecycleService.getScheduledJob());
        assertBusy(() -> {
            LifecycleExecutionState lifecycleState = clusterAdmin().prepareState()
                .execute()
                .actionGet()
                .getState()
                .getMetadata()
                .index("test")
                .getLifecycleExecutionState();
            assertThat(lifecycleState.step(), equalTo("complete"));
        });
    }

    public void testNoOpPolicyUpdates() throws Exception {
        internalCluster().startNode();
        Map<String, Phase> phases = new HashMap<>();
        phases.put("hot", new Phase("hot", TimeValue.ZERO, Map.of()));
        LifecyclePolicy policy = new LifecyclePolicy("mypolicy", phases);

        PutLifecycleAction.Request putLifecycleRequest = new PutLifecycleAction.Request(policy);
        assertAcked(client().execute(PutLifecycleAction.INSTANCE, putLifecycleRequest).get());

        GetLifecycleAction.Response getLifecycleResponse = client().execute(GetLifecycleAction.INSTANCE, new GetLifecycleAction.Request())
            .get();
        assertThat(getLifecycleResponse.getPolicies().size(), equalTo(1));
        GetLifecycleAction.LifecyclePolicyResponseItem responseItem = getLifecycleResponse.getPolicies().get(0);
        assertThat(responseItem.getLifecyclePolicy(), equalTo(policy));
        assertThat(responseItem.getVersion(), equalTo(1L));

        // Put the same policy in place, which should be a no-op
        putLifecycleRequest = new PutLifecycleAction.Request(policy);
        assertAcked(client().execute(PutLifecycleAction.INSTANCE, putLifecycleRequest).get());

        getLifecycleResponse = client().execute(GetLifecycleAction.INSTANCE, new GetLifecycleAction.Request()).get();
        assertThat(getLifecycleResponse.getPolicies().size(), equalTo(1));
        responseItem = getLifecycleResponse.getPolicies().get(0);
        assertThat(responseItem.getLifecyclePolicy(), equalTo(policy));
        // Version should still be 1
        assertThat(responseItem.getVersion(), equalTo(1L));

        // Generate a brand new policy
        Map<String, Phase> newPhases = new HashMap<>(phases);
        newPhases.put("cold", new Phase("cold", TimeValue.timeValueDays(1), Map.of()));
        policy = new LifecyclePolicy("mypolicy", newPhases);

        putLifecycleRequest = new PutLifecycleAction.Request(policy);
        assertAcked(client().execute(PutLifecycleAction.INSTANCE, putLifecycleRequest).get());

        getLifecycleResponse = client().execute(GetLifecycleAction.INSTANCE, new GetLifecycleAction.Request()).get();
        assertThat(getLifecycleResponse.getPolicies().size(), equalTo(1));
        responseItem = getLifecycleResponse.getPolicies().get(0);
        assertThat(responseItem.getLifecyclePolicy(), equalTo(policy));
        // Version should now be 2
        assertThat(responseItem.getVersion(), equalTo(2L));
    }

    public void testExplainExecution() throws Exception {
        // start node
        logger.info("Starting server1");
        internalCluster().startNode();
        logger.info("Creating lifecycle [test_lifecycle]");
        PutLifecycleAction.Request putLifecycleRequest = new PutLifecycleAction.Request(lifecyclePolicy);
        assertAcked(client().execute(PutLifecycleAction.INSTANCE, putLifecycleRequest).get());

        GetLifecycleAction.Response getLifecycleResponse = client().execute(GetLifecycleAction.INSTANCE, new GetLifecycleAction.Request())
            .get();
        assertThat(getLifecycleResponse.getPolicies().size(), equalTo(1));
        GetLifecycleAction.LifecyclePolicyResponseItem responseItem = getLifecycleResponse.getPolicies().get(0);
        assertThat(responseItem.getLifecyclePolicy(), equalTo(lifecyclePolicy));
        assertThat(responseItem.getVersion(), equalTo(1L));
        long actualModifiedDate = Instant.from(ISO_ZONED_DATE_TIME.parse(responseItem.getModifiedDate())).toEpochMilli();

        logger.info("Creating index [test]");
        CreateIndexResponse createIndexResponse = indicesAdmin().create(new CreateIndexRequest("test").settings(settings)).actionGet();
        assertAcked(createIndexResponse);

        // using AtomicLong only to extract a value from a lambda rather than the more traditional atomic update use-case
        AtomicLong originalLifecycleDate = new AtomicLong();
        {
            PhaseExecutionInfo expectedExecutionInfo = new PhaseExecutionInfo(lifecyclePolicy.getName(), mockPhase, 1L, actualModifiedDate);
            assertBusy(() -> {
                IndexLifecycleExplainResponse indexResponse = executeExplainRequestAndGetTestIndexResponse("test");
                assertThat(indexResponse.getStep(), equalTo("observable_cluster_state_action"));
                assertThat(indexResponse.getPhaseExecutionInfo(), equalTo(expectedExecutionInfo));
                originalLifecycleDate.set(indexResponse.getLifecycleDate());
            });
        }

        // set the origination date setting to an older value
        updateIndexSettings(Settings.builder().put(IndexSettings.LIFECYCLE_ORIGINATION_DATE, 1000L), "test");

        {
            assertBusy(() -> {
                IndexLifecycleExplainResponse indexResponse = executeExplainRequestAndGetTestIndexResponse("test");
                assertThat("The configured origination date dictates the lifecycle date", indexResponse.getLifecycleDate(), equalTo(1000L));
            });
        }

        // set the origination date setting to null
        updateIndexSettings(Settings.builder().putNull(IndexSettings.LIFECYCLE_ORIGINATION_DATE), "test");

        {
            assertBusy(() -> {
                IndexLifecycleExplainResponse indexResponse = executeExplainRequestAndGetTestIndexResponse("test");
                assertThat(
                    "Without the origination date, the index create date should dictate the lifecycle date",
                    indexResponse.getLifecycleDate(),
                    equalTo(originalLifecycleDate.get())
                );
            });
        }

        // complete the step
        updateIndexSettings(Settings.builder().put("index.lifecycle.test.complete", true), "test");

        {
            Phase phase = new Phase("mock", TimeValue.ZERO, Collections.singletonMap("TEST_ACTION", OBSERVABLE_ACTION));
            PhaseExecutionInfo expectedExecutionInfo = new PhaseExecutionInfo(lifecyclePolicy.getName(), phase, 1L, actualModifiedDate);
            assertBusy(() -> {
                IndexLifecycleExplainResponse indexResponse = executeExplainRequestAndGetTestIndexResponse("test");
                assertThat("expected to be in the 'mock' phase", indexResponse.getPhase(), equalTo("mock"));
                assertThat("expected to be in the mock phase complete step", indexResponse.getStep(), equalTo("complete"));
                assertThat(indexResponse.getPhaseExecutionInfo(), equalTo(expectedExecutionInfo));
            });
        }
    }

    public void testExplainParseOriginationDate() throws Exception {
        // start node
        logger.info("Starting server1");
        internalCluster().startNode();
        logger.info("Starting server2");
        internalCluster().startNode();
        logger.info("Creating lifecycle [test_lifecycle]");
        PutLifecycleAction.Request putLifecycleRequest = new PutLifecycleAction.Request(lifecyclePolicy);
        assertAcked(client().execute(PutLifecycleAction.INSTANCE, putLifecycleRequest).get());

        GetLifecycleAction.Response getLifecycleResponse = client().execute(GetLifecycleAction.INSTANCE, new GetLifecycleAction.Request())
            .get();
        assertThat(getLifecycleResponse.getPolicies().size(), equalTo(1));
        GetLifecycleAction.LifecyclePolicyResponseItem responseItem = getLifecycleResponse.getPolicies().get(0);
        assertThat(responseItem.getLifecyclePolicy(), equalTo(lifecyclePolicy));
        assertThat(responseItem.getVersion(), equalTo(1L));

        String indexName = "test-2019.09.14";
        logger.info("Creating index [{}]", indexName);
        CreateIndexResponse createIndexResponse = indicesAdmin().create(
            new CreateIndexRequest(indexName).settings(
                Settings.builder().put(settings).put(IndexSettings.LIFECYCLE_PARSE_ORIGINATION_DATE, true)
            )
        ).actionGet();
        assertAcked(createIndexResponse);

        DateFormatter dateFormatter = DateFormatter.forPattern("yyyy.MM.dd");
        long expectedDate = dateFormatter.parseMillis("2019.09.14");
        assertBusy(() -> {
            IndexLifecycleExplainResponse indexResponse = executeExplainRequestAndGetTestIndexResponse(indexName);
            assertThat(indexResponse.getLifecycleDate(), is(expectedDate));
        });

        // disabling the lifecycle parsing would maintain the parsed value as that was set as the origination date
        updateIndexSettings(Settings.builder().put(IndexSettings.LIFECYCLE_PARSE_ORIGINATION_DATE, false), indexName);

        assertBusy(() -> {
            IndexLifecycleExplainResponse indexResponse = executeExplainRequestAndGetTestIndexResponse(indexName);
            assertThat(indexResponse.getLifecycleDate(), is(expectedDate));
        });

        // setting the lifecycle origination date setting to null should make the lifecyle date fallback on the index creation date
        updateIndexSettings(Settings.builder().putNull(IndexSettings.LIFECYCLE_ORIGINATION_DATE), indexName);

        assertBusy(() -> {
            IndexLifecycleExplainResponse indexResponse = executeExplainRequestAndGetTestIndexResponse(indexName);
            assertThat(indexResponse.getLifecycleDate(), is(greaterThan(expectedDate)));
        });

        // setting the lifecycle origination date to an explicit value overrides the date parsing
        long originationDate = 42L;
        updateIndexSettings(
            Settings.builder()
                .put(IndexSettings.LIFECYCLE_PARSE_ORIGINATION_DATE, true)
                .put(IndexSettings.LIFECYCLE_ORIGINATION_DATE, originationDate),
            indexName
        );

        assertBusy(() -> {
            IndexLifecycleExplainResponse indexResponse = executeExplainRequestAndGetTestIndexResponse(indexName);
            assertThat(indexResponse.getLifecycleDate(), is(originationDate));
        });
    }

    private IndexLifecycleExplainResponse executeExplainRequestAndGetTestIndexResponse(String indexName) throws ExecutionException,
        InterruptedException {
        ExplainLifecycleRequest explainRequest = new ExplainLifecycleRequest();
        ExplainLifecycleResponse explainResponse = client().execute(ExplainLifecycleAction.INSTANCE, explainRequest).get();
        assertThat(explainResponse.getIndexResponses().size(), equalTo(1));
        return explainResponse.getIndexResponses().get(indexName);
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
        assertAcked(client().execute(PutLifecycleAction.INSTANCE, putLifecycleRequest).get());
        logger.info("Creating index [test]");
        CreateIndexResponse createIndexResponse = indicesAdmin().create(new CreateIndexRequest("test").settings(settings)).actionGet();
        assertAcked(createIndexResponse);

        ClusterState clusterState = clusterAdmin().prepareState().get().getState();
        RoutingNode routingNodeEntry1 = clusterState.getRoutingNodes().node(node2);
        assertThat(routingNodeEntry1.numberOfShardsWithState(STARTED), equalTo(1));

        assertBusy(() -> assertTrue(indexExists("test")));
        assertBusy(() -> {
            LifecycleExecutionState lifecycleState = clusterAdmin().prepareState()
                .execute()
                .actionGet()
                .getState()
                .getMetadata()
                .index("test")
                .getLifecycleExecutionState();
            assertThat(lifecycleState.step(), equalTo("complete"));
        });
    }

    public void testCreatePolicyWhenStopped() throws Exception {
        // start master node
        logger.info("Starting server1");
        final String server_1 = internalCluster().startNode();
        final String node1 = getLocalNodeId(server_1);

        assertAcked(client().execute(StopILMAction.INSTANCE, new StopILMRequest()).get());
        assertBusy(() -> {
            OperationMode mode = client().execute(GetStatusAction.INSTANCE, new GetStatusAction.Request()).get().getMode();
            logger.info("--> waiting for STOPPED, currently: {}", mode);
            assertThat(mode, equalTo(OperationMode.STOPPED));
        });

        logger.info("Creating lifecycle [test_lifecycle]");
        PutLifecycleAction.Request putLifecycleRequest = new PutLifecycleAction.Request(lifecyclePolicy);
        long lowerBoundModifiedDate = Instant.now().toEpochMilli();
        assertAcked(client().execute(PutLifecycleAction.INSTANCE, putLifecycleRequest).get());
        long upperBoundModifiedDate = Instant.now().toEpochMilli();

        // assert version and modified_date
        GetLifecycleAction.Response getLifecycleResponse = client().execute(GetLifecycleAction.INSTANCE, new GetLifecycleAction.Request())
            .get();
        assertThat(getLifecycleResponse.getPolicies().size(), equalTo(1));
        GetLifecycleAction.LifecyclePolicyResponseItem responseItem = getLifecycleResponse.getPolicies().get(0);
        assertThat(responseItem.getLifecyclePolicy(), equalTo(lifecyclePolicy));
        assertThat(responseItem.getVersion(), equalTo(1L));
        long actualModifiedDate = Instant.from(ISO_ZONED_DATE_TIME.parse(responseItem.getModifiedDate())).toEpochMilli();
        assertThat(
            actualModifiedDate,
            is(both(greaterThanOrEqualTo(lowerBoundModifiedDate)).and(lessThanOrEqualTo(upperBoundModifiedDate)))
        );
        // assert ILM is still stopped
        GetStatusAction.Response statusResponse = client().execute(GetStatusAction.INSTANCE, new GetStatusAction.Request()).get();
        assertThat(statusResponse.getMode(), equalTo(OperationMode.STOPPED));
    }

    public void testPollIntervalUpdate() throws Exception {
        TimeValue pollInterval = TimeValue.timeValueSeconds(randomLongBetween(1, 5));
        final String server_1 = internalCluster().startMasterOnlyNode(
            Settings.builder().put(LifecycleSettings.LIFECYCLE_POLL_INTERVAL, pollInterval.getStringRep()).build()
        );
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
        updateClusterSettings(Settings.builder().put(LifecycleSettings.LIFECYCLE_POLL_INTERVAL, newPollInterval.getStringRep()));
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
        public TestILMPlugin() {}

        public List<Setting<?>> getSettings() {
            final Setting<Boolean> COMPLETE_SETTING = Setting.boolSetting(
                "index.lifecycle.test.complete",
                false,
                Setting.Property.Dynamic,
                Setting.Property.IndexScope
            );
            return Collections.singletonList(COMPLETE_SETTING);
        }

        @Override
        public List<NamedXContentRegistry.Entry> getNamedXContent() {
            return Arrays.asList(new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ObservableAction.NAME), (p) -> {
                MockAction.parse(p);
                return OBSERVABLE_ACTION;
            }));
        }

        @Override
        public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
            return Arrays.asList(
                new NamedWriteableRegistry.Entry(LifecycleType.class, LockableLifecycleType.TYPE, (in) -> LockableLifecycleType.INSTANCE),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, ObservableAction.NAME, ObservableAction::readObservableAction),
                new NamedWriteableRegistry.Entry(
                    ObservableClusterStateWaitStep.class,
                    ObservableClusterStateWaitStep.NAME,
                    ObservableClusterStateWaitStep::new
                )
            );
        }
    }

    public static class ObservableClusterStateWaitStep extends ClusterStateWaitStep implements NamedWriteable {
        public static final String NAME = "observable_cluster_state_action";

        public ObservableClusterStateWaitStep(StepKey current, StepKey next) {
            super(current, next);
        }

        @Override
        public boolean isRetryable() {
            return false;
        }

        public ObservableClusterStateWaitStep(StreamInput in) throws IOException {
            this(new StepKey(in.readString(), in.readString(), in.readString()), readOptionalNextStepKey(in));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(getKey().phase());
            out.writeString(getKey().action());
            out.writeString(getKey().name());
            boolean hasNextStep = getNextStepKey() != null;
            out.writeBoolean(hasNextStep);
            if (hasNextStep) {
                out.writeString(getNextStepKey().phase());
                out.writeString(getNextStepKey().action());
                out.writeString(getNextStepKey().name());
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
            boolean complete = clusterState.metadata().index("test").getSettings().getAsBoolean("index.lifecycle.test.complete", false);
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
