/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.deployment;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelPrefixStrings;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.ml.inference.pytorch.PriorityProcessWorkerExecutorService;
import org.elasticsearch.xpack.ml.inference.pytorch.process.PyTorchProcessFactory;
import org.elasticsearch.xpack.ml.inference.pytorch.process.PyTorchResultProcessor;
import org.elasticsearch.xpack.ml.notifications.InferenceAuditor;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.ml.MachineLearning.NATIVE_INFERENCE_COMMS_THREAD_POOL_NAME;
import static org.elasticsearch.xpack.ml.MachineLearning.UTILITY_THREAD_POOL_NAME;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DeploymentManagerTests extends ESTestCase {

    private ThreadPool tp;
    private InferenceAuditor inferenceAuditor;

    @Before
    public void managerSetup() {
        tp = new TestThreadPool(
            "DeploymentManagerTests",
            new ScalingExecutorBuilder(
                UTILITY_THREAD_POOL_NAME,
                1,
                4,
                TimeValue.timeValueMinutes(10),
                false,
                "xpack.ml.utility_thread_pool"
            ),
            new ScalingExecutorBuilder(
                NATIVE_INFERENCE_COMMS_THREAD_POOL_NAME,
                1,
                4,
                TimeValue.timeValueMinutes(10),
                false,
                "xpack.ml.native_inference_comms_thread_pool"
            )
        );
        inferenceAuditor = mock(InferenceAuditor.class);
    }

    @After
    public void shutdownThreadpool() {
        tp.shutdown();
    }

    public void testRejectedExecution() {
        TrainedModelDeploymentTask task = mock(TrainedModelDeploymentTask.class);
        Long taskId = 1L;
        when(task.getId()).thenReturn(taskId);
        when(task.isStopped()).thenReturn(Boolean.FALSE);
        when(task.getModelId()).thenReturn("test-rejected");
        when(task.getDeploymentId()).thenReturn("test-rejected-deployment");

        DeploymentManager deploymentManager = new DeploymentManager(
            mock(Client.class),
            mock(NamedXContentRegistry.class),
            tp,
            mock(PyTorchProcessFactory.class),
            10,
            inferenceAuditor
        );

        PriorityProcessWorkerExecutorService priorityExecutorService = new PriorityProcessWorkerExecutorService(
            tp.getThreadContext(),
            "test reject",
            10
        );
        priorityExecutorService.shutdown();

        AtomicInteger rejectedCount = new AtomicInteger();

        DeploymentManager.ProcessContext context = mock(DeploymentManager.ProcessContext.class);
        PyTorchResultProcessor resultProcessor = new PyTorchResultProcessor("1", threadSettings -> {});
        when(context.getResultProcessor()).thenReturn(resultProcessor);
        when(context.getPriorityProcessWorker()).thenReturn(priorityExecutorService);
        when(context.getRejectedExecutionCount()).thenReturn(rejectedCount);

        deploymentManager.addProcessContext(taskId, context);
        deploymentManager.infer(
            task,
            mock(InferenceConfig.class),
            NlpInferenceInput.fromText("foo"),
            false,
            TimeValue.timeValueMinutes(1),
            TrainedModelPrefixStrings.PrefixType.NONE,
            null,
            randomBoolean(),
            ActionListener.wrap(result -> fail("unexpected success"), e -> assertThat(e, instanceOf(EsRejectedExecutionException.class)))
        );

        assertThat(rejectedCount.intValue(), equalTo(1));
    }

    public void testStopWithoutProcessContextLogsAtDebugOnly() {
        TrainedModelDeploymentTask task = mock(TrainedModelDeploymentTask.class);
        when(task.getId()).thenReturn(1L);
        when(task.getDeploymentId()).thenReturn("no-process-deployment");
        DeploymentManager deploymentManager = new DeploymentManager(
            mock(Client.class),
            mock(NamedXContentRegistry.class),
            tp,
            mock(PyTorchProcessFactory.class),
            10,
            inferenceAuditor
        );
        String loggerName = DeploymentManager.class.getCanonicalName();
        var logger = LogManager.getLogger(DeploymentManager.class);
        Loggers.setLevel(logger, Level.DEBUG);
        try {
            MockLog.assertThatLogger(() -> {
                deploymentManager.stopDeployment(task);
                deploymentManager.stopAfterCompletingPendingWork(task, ActionListener.noop());
            },
                DeploymentManager.class,
                new MockLog.UnseenEventExpectation("no warn when stopping", loggerName, Level.WARN, "*No process context to stop*"),
                new MockLog.SeenEventExpectation(
                    "debug on forceful stop",
                    loggerName,
                    Level.DEBUG,
                    "[no-process-deployment] No process context to stop"
                ),
                new MockLog.SeenEventExpectation(
                    "debug on graceful stop",
                    loggerName,
                    Level.DEBUG,
                    "[no-process-deployment] No process context to stop gracefully"
                )
            );
        } finally {
            Loggers.setLevel(logger, (Level) null);
        }
    }

}
