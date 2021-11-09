/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ilm;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ilm.ClusterStateWaitStep;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecycleExecutionState;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.LifecycleType;
import org.elasticsearch.xpack.core.ilm.MockAction;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.PhaseCompleteStep;
import org.elasticsearch.xpack.core.ilm.Step;
import org.elasticsearch.xpack.ilm.history.ILMHistoryItem;
import org.elasticsearch.xpack.ilm.history.ILMHistoryStore;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.xpack.ilm.LifecyclePolicyTestsUtils.newLockableLifecyclePolicy;

@ESIntegTestCase.ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class ILMHistoryStoreIT extends ESIntegTestCase {
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
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        settings.put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), false);
        settings.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        settings.put(XPackSettings.WATCHER_ENABLED.getKey(), false);
        settings.put(XPackSettings.GRAPH_ENABLED.getKey(), false);
        settings.put(LifecycleSettings.LIFECYCLE_POLL_INTERVAL, "1s");

        // This is necessary to prevent ILM and SLM installing a lifecycle policy, these tests assume a blank slate
        settings.put(LifecycleSettings.LIFECYCLE_HISTORY_INDEX_ENABLED, true);
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

    public void testPutAsyncStressTest() throws Exception {
        settings = Settings.builder().put(settings).put("index.lifecycle.test.complete", true).build();
        // start master node
        logger.info("Starting master-only server1");
        final String server_1 = internalCluster().startMasterOnlyNode();

        final String server_2 = internalCluster().startDataOnlyNode();

        // check that the scheduler was started on the appropriate node
        {
            IndexLifecycleService indexLifecycleService = internalCluster().getInstance(IndexLifecycleService.class, server_1);
            ILMHistoryStore ilmHistoryStore = indexLifecycleService.getIlmHistoryStore();
            assertNotNull(ilmHistoryStore);

            for (int i = 0; i < 8192; i++) {
                String index = randomAlphaOfLength(5);
                String policyId = randomAlphaOfLength(5);
                String phase = randomAlphaOfLength(5);
                final long timestamp = randomNonNegativeLong();
                ILMHistoryItem record = ILMHistoryItem.success(
                    index,
                    policyId,
                    timestamp,
                    10L,
                    LifecycleExecutionState.builder().setPhase(phase).build()
                );

                ilmHistoryStore.putAsync(record);
            }
            Thread.sleep(1000);

            ilmHistoryStore.close();
        }

        assertFalse(true);
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
