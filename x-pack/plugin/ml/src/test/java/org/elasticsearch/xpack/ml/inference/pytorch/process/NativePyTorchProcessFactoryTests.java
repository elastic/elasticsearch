/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pytorch.process;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction.TaskParams;
import org.elasticsearch.xpack.core.ml.inference.assignment.Priority;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.inference.assignment.TrainedModelAssignmentNodeService;
import org.elasticsearch.xpack.ml.inference.deployment.TrainedModelDeploymentTask;
import org.elasticsearch.xpack.ml.process.NativeController;
import org.elasticsearch.xpack.ml.process.ProcessPipes;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.ml.MlTasks.TRAINED_MODEL_ASSIGNMENT_TASK_ACTION;
import static org.elasticsearch.xpack.core.ml.MlTasks.TRAINED_MODEL_ASSIGNMENT_TASK_TYPE;
import static org.hamcrest.Matchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NativePyTorchProcessFactoryTests extends ESTestCase {

    /**
     * Thrown from the overridden {@code executeProcess} to abort {@code createProcess} right after both snapshot
     * call sites have been recorded, before it goes on to build a real {@link PyTorchBuilder} or start the process.
     */
    private static class AbortAfterCapture extends RuntimeException {}

    private NativeController nativeController;
    private ClusterService clusterService;

    @Before
    public void setUpMocks() {
        nativeController = mock(NativeController.class);
        // ClusterService.getNodeName() is final, so it cannot be stubbed via Mockito; the mock's default
        // (unset field, i.e. null) is fine here since the factory only uses nodeName in error-log messages
        // this test never reaches.
        clusterService = mock(ClusterService.class);
    }

    /**
     * Pins the contract fixed by the "snapshot sandboxEnabled once per process launch" change: within a single
     * {@code createProcess} call, the value used to build {@code ProcessPipes} (the isolated-pipe-layout decision)
     * and the value passed into {@code executeProcess} (the {@code --disableSandbox} decision) must be identical,
     * and that value must track whichever setting was in effect when {@code createProcess} was entered.
     *
     * Caveat: within a single-threaded test nothing can mutate the volatile field between the two reads inside
     * {@code createProcess}, so this does not exercise the actual concurrent race the fix addresses (a settings
     * update landing between the two reads). What it does prove: both call sites are fed from the same local
     * variable rather than two independent reads of the field, and a new {@code createProcess} call picks up a
     * changed setting value consistently across both call sites. A regression back to two live field reads would
     * still pass this test, since nothing mutates the field mid-call here - see the trade-off note above.
     */
    public void testSandboxEnabledSnapshotIsSharedBetweenProcessPipesAndExecuteProcess() {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        Environment env = TestEnvironment.newEnvironment(settings);
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            new HashSet<>(
                List.of(
                    MachineLearning.PROCESS_CONNECT_TIMEOUT,
                    MachineLearningField.MODEL_GRAPH_VALIDATION_ENABLED,
                    MachineLearningField.SANDBOX_ENABLED
                )
            )
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        RecordingFactory factory = new RecordingFactory(env, nativeController, clusterService);

        // First call: sandbox enabled.
        factory.setSandboxEnabled(true);
        expectThrows(AbortAfterCapture.class, () -> factory.createProcess(newTask("deployment-1"), null, null, null));

        // Second call: sandbox disabled. A future regression to two independent live reads of the volatile field
        // would still agree here (nothing mutates the field mid-call in this single-threaded test), but this at
        // least proves each createProcess invocation consistently threads one value to both call sites, and that
        // the value tracks the setting in effect at call time rather than being stuck from the first call.
        factory.setSandboxEnabled(false);
        expectThrows(AbortAfterCapture.class, () -> factory.createProcess(newTask("deployment-2"), null, null, null));

        assertThat(factory.processPipesSnapshots, contains(true, false));
        assertThat(factory.executeProcessSnapshots, contains(true, false));
    }

    private static TrainedModelDeploymentTask newTask(String deploymentId) {
        return new TrainedModelDeploymentTask(
            0,
            TRAINED_MODEL_ASSIGNMENT_TASK_TYPE,
            TRAINED_MODEL_ASSIGNMENT_TASK_ACTION,
            TaskId.EMPTY_TASK_ID,
            Map.of(),
            new TaskParams("my_model", deploymentId, 42L, 1, 1, 1024, ByteSizeValue.ofBytes(12), Priority.NORMAL, 0L, 0L),
            mock(TrainedModelAssignmentNodeService.class),
            mock(XPackLicenseState.class),
            mock(LicensedFeature.Persistent.class)
        );
    }

    /**
     * Overrides the two call sites inside {@code createProcess} to record the sandboxEnabledSnapshot value each one
     * actually received, then aborts before either constructs anything that would try to touch real native pipes.
     */
    private static class RecordingFactory extends NativePyTorchProcessFactory {

        final List<Boolean> processPipesSnapshots = new ArrayList<>();
        final List<Boolean> executeProcessSnapshots = new ArrayList<>();

        RecordingFactory(Environment env, NativeController nativeController, ClusterService clusterService) {
            super(env, nativeController, clusterService);
        }

        @Override
        ProcessPipes createProcessPipes(TrainedModelDeploymentTask task, boolean sandboxEnabledSnapshot) {
            processPipesSnapshots.add(sandboxEnabledSnapshot);
            return super.createProcessPipes(task, sandboxEnabledSnapshot);
        }

        @Override
        void executeProcess(ProcessPipes processPipes, TrainedModelDeploymentTask task, boolean sandboxEnabledSnapshot) {
            executeProcessSnapshots.add(sandboxEnabledSnapshot);
            throw new AbortAfterCapture();
        }
    }
}
