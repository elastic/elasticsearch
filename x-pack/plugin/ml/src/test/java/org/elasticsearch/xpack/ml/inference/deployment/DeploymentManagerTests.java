/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.deployment;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.PassThroughConfig;
import org.elasticsearch.xpack.ml.inference.pytorch.process.PyTorchProcessFactory;
import org.elasticsearch.xpack.ml.inference.pytorch.process.PyTorchResultProcessor;
import org.junit.After;
import org.junit.Before;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.ml.MachineLearning.JOB_COMMS_THREAD_POOL_NAME;
import static org.elasticsearch.xpack.ml.MachineLearning.UTILITY_THREAD_POOL_NAME;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DeploymentManagerTests extends ESTestCase {

    private ThreadPool tp;

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
                JOB_COMMS_THREAD_POOL_NAME,
                1,
                4,
                TimeValue.timeValueMinutes(10),
                false,
                "xpack.ml.job_comms_thread_pool"
            )
        );
    }

    @After
    public void shutdownThreadpool() {
        tp.shutdown();
    }

    public void testInferListenerOnlyCalledOnce() {
        DeploymentManager.ProcessContext processContext = mock(DeploymentManager.ProcessContext.class);
        PyTorchResultProcessor resultProcessor = new PyTorchResultProcessor("1", threadSettings -> {});
        when(processContext.getResultProcessor()).thenReturn(resultProcessor);
        AtomicInteger timeoutCount = new AtomicInteger();
        when(processContext.getTimeoutCount()).thenReturn(timeoutCount);

        ListenerCounter listener = new ListenerCounter();
        DeploymentManager.InferenceAction action = new DeploymentManager.InferenceAction(
            "test-model",
            1,
            TimeValue.MAX_VALUE,
            processContext,
            new PassThroughConfig(null, null, null),
            Map.of(),
            tp,
            listener
        );

        action.onSuccess(new WarningInferenceResults("foo"));
        for (int i = 0; i < 10; i++) {
            action.onSuccess(new WarningInferenceResults("foo"));
            action.onFailure(new Exception("foo"));
            action.onTimeout();
        }
        assertThat(listener.failureCounts, equalTo(0));
        assertThat(listener.responseCounts, equalTo(1));

        action = new DeploymentManager.InferenceAction(
            "test-model",
            1,
            TimeValue.MAX_VALUE,
            processContext,
            new PassThroughConfig(null, null, null),
            Map.of(),
            tp,
            listener
        );

        action.onTimeout();
        for (int i = 0; i < 10; i++) {
            action.onSuccess(new WarningInferenceResults("foo"));
            action.onFailure(new Exception("foo"));
            action.onTimeout();
        }
        assertThat(listener.failureCounts, equalTo(1));
        assertThat(listener.responseCounts, equalTo(1));
        assertThat(timeoutCount.intValue(), equalTo(1));

        action = new DeploymentManager.InferenceAction(
            "test-model",
            1,
            TimeValue.MAX_VALUE,
            processContext,
            new PassThroughConfig(null, null, null),
            Map.of(),
            tp,
            listener
        );

        action.onFailure(new Exception("bar"));
        for (int i = 0; i < 10; i++) {
            action.onSuccess(new WarningInferenceResults("foo"));
            action.onFailure(new Exception("foo"));
            action.onTimeout();
        }
        assertThat(listener.failureCounts, equalTo(2));
        assertThat(listener.responseCounts, equalTo(1));
    }

    public void testRejectedExecution() {
        TrainedModelDeploymentTask task = mock(TrainedModelDeploymentTask.class);
        Long taskId = 1L;
        when(task.getId()).thenReturn(taskId);
        when(task.isStopped()).thenReturn(Boolean.FALSE);

        DeploymentManager deploymentManager = new DeploymentManager(
            mock(Client.class),
            mock(NamedXContentRegistry.class),
            tp,
            mock(PyTorchProcessFactory.class)
        );

        ExecutorService executorService = mock(ExecutorService.class);
        doThrow(new EsRejectedExecutionException("mock executor rejection")).when(executorService).execute(any(Runnable.class));

        AtomicInteger rejectedCount = new AtomicInteger();

        DeploymentManager.ProcessContext context = mock(DeploymentManager.ProcessContext.class);
        PyTorchResultProcessor resultProcessor = new PyTorchResultProcessor("1", threadSettings -> {});
        when(context.getResultProcessor()).thenReturn(resultProcessor);
        when(context.getExecutorService()).thenReturn(executorService);
        when(context.getRejectedExecutionCount()).thenReturn(rejectedCount);

        deploymentManager.addProcessContext(taskId, context);
        deploymentManager.infer(
            task,
            mock(InferenceConfig.class),
            Map.of(),
            TimeValue.timeValueMinutes(1),
            ActionListener.wrap(result -> fail("unexpected success"), e -> assertThat(e, instanceOf(EsRejectedExecutionException.class)))
        );

        assertThat(rejectedCount.intValue(), equalTo(1));
    }

    private static class ListenerCounter implements ActionListener<InferenceResults> {
        private int responseCounts;
        private int failureCounts;

        @Override
        public void onResponse(InferenceResults inferenceResults) {
            responseCounts++;
        }

        @Override
        public void onFailure(Exception e) {
            failureCounts++;
        }
    }

}
