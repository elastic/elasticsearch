/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
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

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.core.ml.job.config.JobTests.buildJobBuilder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class AutodetectBuilderTests extends ESTestCase {

    private Logger logger;
    private List<Path> filesToDelete;
    private Environment env;
    private Settings settings;
    private NativeController nativeController;
    private ProcessPipes processPipes;

    @Before
    public void setUpTests() {
        logger = mock(Logger.class);
        filesToDelete = Collections.emptyList();
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
            new PerPartitionCategorizationConfig(isPerPartitionCategorization, isPerPartitionCategorization));

        job.setAnalysisConfig(acBuilder);

        DataDescription.Builder dd = new DataDescription.Builder();
        dd.setFormat(DataDescription.DataFormat.DELIMITED);
        dd.setFieldDelimiter('|');
        dd.setTimeField("tf");
        job.setDataDescription(dd);

        List<String> command = autodetectBuilder(job.build()).buildAutodetectCommand();
        assertTrue(command.contains(AutodetectBuilder.AUTODETECT_PATH));
        assertTrue(command.contains(AutodetectBuilder.BUCKET_SPAN_ARG + "120"));
        assertTrue(command.contains(AutodetectBuilder.LATENCY_ARG + "360"));
        assertTrue(command.contains(AutodetectBuilder.SUMMARY_COUNT_FIELD_ARG + "summaryField"));
        assertTrue(command.contains(AutodetectBuilder.MULTIVARIATE_BY_FIELDS_ARG));
        assertThat(command.contains(AutodetectBuilder.STOP_CATEGORIZATION_ON_WARN_ARG), is(isPerPartitionCategorization));

        assertTrue(command.contains(AutodetectBuilder.LENGTH_ENCODED_INPUT_ARG));
        assertTrue(command.contains(AutodetectBuilder.maxAnomalyRecordsArg(settings)));

        assertTrue(command.contains(AutodetectBuilder.TIME_FIELD_ARG + "tf"));
        assertTrue(command.contains(AutodetectBuilder.JOB_ID_ARG + "unit-test-job"));

        int expectedPersistInterval = 10800 + AutodetectBuilder.calculateStaggeringInterval(job.getId());
        assertTrue(command.contains(AutodetectBuilder.PERSIST_INTERVAL_ARG + expectedPersistInterval));
        int expectedMaxQuantileInterval = 21600 + AutodetectBuilder.calculateStaggeringInterval(job.getId());
        assertTrue(command.contains(AutodetectBuilder.MAX_QUANTILE_INTERVAL_ARG + expectedMaxQuantileInterval));

        assertEquals(isPerPartitionCategorization ? 12 : 11, command.size());
    }

    public void testBuildAutodetectCommand_defaultTimeField() {
        Job.Builder job = buildJobBuilder("unit-test-job");

        List<String> command = autodetectBuilder(job.build()).buildAutodetectCommand();

        assertTrue(command.contains(AutodetectBuilder.TIME_FIELD_ARG + "time"));
    }

    public void testBuildAutodetectCommand_givenPersistModelState() {
        settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put(AutodetectBuilder.DONT_PERSIST_MODEL_STATE_SETTING.getKey(), true).build();
        Job.Builder job = buildJobBuilder("unit-test-job");

        int expectedPersistInterval = 10800 + AutodetectBuilder.calculateStaggeringInterval(job.getId());

        List<String> command = autodetectBuilder(job.build()).buildAutodetectCommand();
        assertFalse(command.contains(AutodetectBuilder.PERSIST_INTERVAL_ARG + expectedPersistInterval));

        settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        env = TestEnvironment.newEnvironment(settings);

        command = autodetectBuilder(job.build()).buildAutodetectCommand();
        assertTrue(command.contains(AutodetectBuilder.PERSIST_INTERVAL_ARG + expectedPersistInterval));
    }

    private AutodetectBuilder autodetectBuilder(Job job) {
        return new AutodetectBuilder(job, filesToDelete, logger, env, settings, nativeController, processPipes);
    }
}
