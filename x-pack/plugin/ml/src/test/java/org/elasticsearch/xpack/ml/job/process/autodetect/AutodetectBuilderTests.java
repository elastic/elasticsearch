/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.PerPartitionCategorizationConfig;
import org.elasticsearch.xpack.ml.process.NativeController;
import org.elasticsearch.xpack.ml.process.ProcessPipes;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.core.ml.job.config.JobTests.buildJobBuilder;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.matchesPattern;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class AutodetectBuilderTests extends ESTestCase {

    private Logger logger;
    private List<Path> filesToDelete;
    private Environment env;
    private Settings settings;
    private NativeController nativeController;
    private ProcessPipes processPipes;
    private ArgumentCaptor<List<String>> commandCaptor;

    @SuppressWarnings("unchecked")
    @Before
    public void setUpTests() {
        logger = mock(Logger.class);
        filesToDelete = new ArrayList<>();
        commandCaptor = ArgumentCaptor.forClass((Class) List.class);
        settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        env = TestEnvironment.newEnvironment(settings);
        nativeController = mock(NativeController.class);
        processPipes = mock(ProcessPipes.class);
    }

    public void testBuildAutodetectCommand() {
        boolean isPerPartitionCategorization = randomBoolean();

        Job.Builder job = buildJobBuilder("unit-test-job");

        Detector.Builder detectorBuilder = new Detector.Builder("mean", "value");
        if (isPerPartitionCategorization) {
            detectorBuilder.setByFieldName("mlcategory");
        }
        detectorBuilder.setPartitionFieldName("foo");
        AnalysisConfig.Builder acBuilder = new AnalysisConfig.Builder(Collections.singletonList(detectorBuilder.build()));
        acBuilder.setBucketSpan(TimeValue.timeValueSeconds(120));
        acBuilder.setLatency(TimeValue.timeValueSeconds(360));
        acBuilder.setSummaryCountFieldName("summaryField");
        acBuilder.setMultivariateByFields(true);
        if (isPerPartitionCategorization) {
            acBuilder.setCategorizationFieldName("bar");
        }
        acBuilder.setPerPartitionCategorizationConfig(
            new PerPartitionCategorizationConfig(isPerPartitionCategorization, isPerPartitionCategorization)
        );

        job.setAnalysisConfig(acBuilder);

        DataDescription.Builder dd = new DataDescription.Builder();
        dd.setTimeField("tf");
        job.setDataDescription(dd);

        List<String> command = autodetectBuilder(job.build()).buildAutodetectCommand();
        assertTrue(command.contains(AutodetectBuilder.AUTODETECT_PATH));

        assertTrue(command.contains(AutodetectBuilder.LENGTH_ENCODED_INPUT_ARG));
        assertTrue(command.contains(AutodetectBuilder.maxAnomalyRecordsArg(settings)));
        assertTrue(command.contains(AutodetectBuilder.LICENSE_KEY_VALIDATED_ARG + true));

        assertEquals(4, command.size());
    }

    private AutodetectBuilder autodetectBuilder(Job job) {
        return new AutodetectBuilder(job, filesToDelete, logger, env, settings, nativeController, processPipes);
    }

    public void testBuildAutodetect() throws Exception {
        Job.Builder job = buildJobBuilder("unit-test-job");

        autodetectBuilder(job.build()).build();

        assertThat(filesToDelete, hasSize(1));

        verify(nativeController).startProcess(commandCaptor.capture());
        verifyNoMoreInteractions(nativeController);

        List<String> command = commandCaptor.getValue();
        assertThat(command, hasItem(matchesPattern("--config=.*\\.json")));
    }
}
