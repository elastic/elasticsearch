/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.ModelPlotConfig;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.AutodetectParams;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.process.NativeController;
import org.elasticsearch.xpack.ml.process.ProcessPipes;
import org.elasticsearch.xpack.ml.process.logging.CppLogMessage;
import org.elasticsearch.xpack.ml.utils.persistence.ResultsPersisterService;
import org.junit.Before;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Set;
import java.util.function.BiConsumer;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class NativeAutodetectProcessFactoryTests extends ESTestCase {

    private static final String JOB_ID = "test_job";
    private static final TimeValue DEFAULT_PROCESS_CONNECT_TIMEOUT = MachineLearning.PROCESS_CONNECT_TIMEOUT.getDefault(null);

    private Settings settings;
    private Environment env;
    private NativeController nativeController;
    private ResultsPersisterService resultsPersisterService;
    private AnomalyDetectionAuditor anomalyDetectionAuditor;
    private BiConsumer<String, CppLogMessage> onCppLogMessageReceived;
    private ClusterService clusterService;
    private Job job;
    private AutodetectParams autodetectParams;
    private ProcessPipes processPipes;

    @Before
    @SuppressWarnings("unchecked")
    public void setUpMocks() {
        settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        env = TestEnvironment.newEnvironment(settings);
        nativeController = mock(NativeController.class);
        resultsPersisterService = mock(ResultsPersisterService.class);
        anomalyDetectionAuditor = mock(AnomalyDetectionAuditor.class);
        onCppLogMessageReceived = mock(BiConsumer.class);
        ClusterSettings clusterSettings = new ClusterSettings(settings,
            Set.of(MachineLearning.PROCESS_CONNECT_TIMEOUT, AutodetectBuilder.MAX_ANOMALY_RECORDS_SETTING_DYNAMIC));
        clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        job = mock(Job.class);
        when(job.getId()).thenReturn(JOB_ID);
        autodetectParams = mock(AutodetectParams.class);
        processPipes = mock(ProcessPipes.class);
    }

    public void testDefaultProcessConnectTimeout() throws IOException {
        NativeAutodetectProcessFactory nativeAutodetectProcessFactory = createNativeAutodetectProcessFactory();
        nativeAutodetectProcessFactory.createNativeProcess(job, autodetectParams, processPipes, Collections.emptyList());

        verify(processPipes, times(1)).connectStreams(eq(Duration.ofSeconds(DEFAULT_PROCESS_CONNECT_TIMEOUT.seconds())));
    }

    public void testSetProcessConnectTimeout() throws IOException {
        int timeoutSeconds = randomIntBetween(5, 100);
        NativeAutodetectProcessFactory nativeAutodetectProcessFactory = createNativeAutodetectProcessFactory();
        nativeAutodetectProcessFactory.setProcessConnectTimeout(TimeValue.timeValueSeconds(timeoutSeconds));
        nativeAutodetectProcessFactory.createNativeProcess(job, autodetectParams, processPipes, Collections.emptyList());

        verify(processPipes, times(1)).connectStreams(eq(Duration.ofSeconds(timeoutSeconds)));
    }

    public void testGetCppLogMessageHandlerForJob_ModelPlotEnabled() {
        when(job.getModelPlotConfig()).thenReturn(new ModelPlotConfig());

        CppLogMessage cppLogMessage = mock(CppLogMessage.class);

        NativeAutodetectProcessFactory nativeAutodetectProcessFactory = createNativeAutodetectProcessFactory();
        nativeAutodetectProcessFactory.getCppLogMessageReceivedHandlerForJob(job).accept(cppLogMessage);

        verify(onCppLogMessageReceived).accept(JOB_ID, cppLogMessage);
        verifyNoMoreInteractions(onCppLogMessageReceived);
    }

    public void testGetCppLogMessageHandlerForJob_ModelPlotNotEnabled() {
        when(job.getModelPlotConfig()).thenReturn(new ModelPlotConfig(false, null));

        CppLogMessage cppLogMessage = mock(CppLogMessage.class);

        NativeAutodetectProcessFactory nativeAutodetectProcessFactory = createNativeAutodetectProcessFactory();
        nativeAutodetectProcessFactory.getCppLogMessageReceivedHandlerForJob(job).accept(cppLogMessage);

        verifyZeroInteractions(onCppLogMessageReceived);
    }

    public void testGetCppLogMessageHandlerForJob_ModelPlotNull() {
        CppLogMessage cppLogMessage = mock(CppLogMessage.class);

        NativeAutodetectProcessFactory nativeAutodetectProcessFactory = createNativeAutodetectProcessFactory();
        nativeAutodetectProcessFactory.getCppLogMessageReceivedHandlerForJob(job).accept(cppLogMessage);

        verifyZeroInteractions(onCppLogMessageReceived);
    }

    private NativeAutodetectProcessFactory createNativeAutodetectProcessFactory() {
        return new NativeAutodetectProcessFactory(
            env, settings, nativeController, clusterService, resultsPersisterService, anomalyDetectionAuditor, onCppLogMessageReceived);
    }
}
