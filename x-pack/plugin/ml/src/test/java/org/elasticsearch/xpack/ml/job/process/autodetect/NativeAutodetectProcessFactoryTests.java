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
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.AutodetectParams;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.process.NativeController;
import org.elasticsearch.xpack.ml.process.ProcessPipes;
import org.elasticsearch.xpack.ml.utils.persistence.ResultsPersisterService;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Set;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NativeAutodetectProcessFactoryTests extends ESTestCase {

    public void testSetProcessConnectTimeout() throws IOException {

        int timeoutSeconds = randomIntBetween(5, 100);

        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        Environment env = TestEnvironment.newEnvironment(settings);
        NativeController nativeController = mock(NativeController.class);
        ResultsPersisterService resultsPersisterService = mock(ResultsPersisterService.class);
        AnomalyDetectionAuditor anomalyDetectionAuditor = mock(AnomalyDetectionAuditor.class);
        ClusterSettings clusterSettings = new ClusterSettings(settings,
            Set.of(MachineLearning.PROCESS_CONNECT_TIMEOUT, AutodetectBuilder.MAX_ANOMALY_RECORDS_SETTING_DYNAMIC));
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        Job job = mock(Job.class);
        when(job.getId()).thenReturn("set_process_connect_test_job");
        AutodetectParams autodetectParams = mock(AutodetectParams.class);
        ProcessPipes processPipes = mock(ProcessPipes.class);

        NativeAutodetectProcessFactory nativeAutodetectProcessFactory = new NativeAutodetectProcessFactory(
            env,
            settings,
            nativeController,
            clusterService,
            resultsPersisterService,
            anomalyDetectionAuditor);
        nativeAutodetectProcessFactory.setProcessConnectTimeout(TimeValue.timeValueSeconds(timeoutSeconds));
        nativeAutodetectProcessFactory.createNativeProcess(job, autodetectParams, processPipes, Collections.emptyList());

        verify(processPipes, times(1)).connectStreams(eq(Duration.ofSeconds(timeoutSeconds)));
    }
}
