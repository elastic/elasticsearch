/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.ml.job.config.DataDescription;
import org.elasticsearch.xpack.ml.job.config.Detector;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.mockito.Mockito;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.ml.job.config.JobTests.buildJobBuilder;

public class ProcessCtrlTests extends ESTestCase {

    private final Logger logger = Mockito.mock(Logger.class);
    // 4194304 is the maximum possible PID on Linux according to
    // http://web.archive.org/web/20111209081734/http://research.cs.wisc.edu/condor/condorg/linux_scalability.html
    private final long pid = randomIntBetween(2, 4194304);

    public void testBuildAutodetectCommand() {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        Environment env = new Environment(settings);
        Job.Builder job = buildJobBuilder("unit-test-job");

        Detector.Builder detectorBuilder = new Detector.Builder("metric", "value");
        detectorBuilder.setPartitionFieldName("foo");
        AnalysisConfig.Builder acBuilder = new AnalysisConfig.Builder(Collections.singletonList(detectorBuilder.build()));
        acBuilder.setBatchSpan(100L);
        acBuilder.setBucketSpan(120L);
        acBuilder.setLatency(360L);
        acBuilder.setPeriod(20L);
        acBuilder.setSummaryCountFieldName("summaryField");
        acBuilder.setOverlappingBuckets(true);
        acBuilder.setMultivariateByFields(true);
        acBuilder.setUsePerPartitionNormalization(true);
        job.setAnalysisConfig(acBuilder);

        DataDescription.Builder dd = new DataDescription.Builder();
        dd.setFormat(DataDescription.DataFormat.DELIMITED);
        dd.setFieldDelimiter('|');
        dd.setTimeField("tf");
        job.setDataDescription(dd);

        List<String> command = ProcessCtrl.buildAutodetectCommand(env, settings, job.build(), logger, true, pid);
        assertEquals(17, command.size());
        assertTrue(command.contains(ProcessCtrl.AUTODETECT_PATH));
        assertTrue(command.contains(ProcessCtrl.BATCH_SPAN_ARG + "100"));
        assertTrue(command.contains(ProcessCtrl.BUCKET_SPAN_ARG + "120"));
        assertTrue(command.contains(ProcessCtrl.LATENCY_ARG + "360"));
        assertTrue(command.contains(ProcessCtrl.PERIOD_ARG + "20"));
        assertTrue(command.contains(ProcessCtrl.SUMMARY_COUNT_FIELD_ARG + "summaryField"));
        assertTrue(command.contains(ProcessCtrl.RESULT_FINALIZATION_WINDOW_ARG + "2"));
        assertTrue(command.contains(ProcessCtrl.MULTIVARIATE_BY_FIELDS_ARG));

        assertTrue(command.contains(ProcessCtrl.LENGTH_ENCODED_INPUT_ARG));
        assertTrue(command.contains(ProcessCtrl.maxAnomalyRecordsArg(settings)));

        assertTrue(command.contains(ProcessCtrl.TIME_FIELD_ARG + "tf"));
        assertTrue(hasValidLicense(command));
        assertTrue(command.contains(ProcessCtrl.JOB_ID_ARG + "unit-test-job"));

        assertTrue(command.contains(ProcessCtrl.PER_PARTITION_NORMALIZATION));

        int expectedPersistInterval = 10800 + ProcessCtrl.calculateStaggeringInterval(job.getId());
        assertTrue(command.contains(ProcessCtrl.PERSIST_INTERVAL_ARG + expectedPersistInterval));
        int expectedMaxQuantileInterval = 21600 + ProcessCtrl.calculateStaggeringInterval(job.getId());
        assertTrue(command.contains(ProcessCtrl.MAX_QUANTILE_INTERVAL_ARG + expectedMaxQuantileInterval));
        assertTrue(command.contains(ProcessCtrl.IGNORE_DOWNTIME_ARG));
    }

    public void testBuildAutodetectCommand_defaultTimeField() {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        Environment env = new Environment(settings);
        Job.Builder job = buildJobBuilder("unit-test-job");

        List<String> command = ProcessCtrl.buildAutodetectCommand(env, settings, job.build(), logger, false, pid);

        assertTrue(command.contains(ProcessCtrl.TIME_FIELD_ARG + "time"));
    }

    public void testBuildAutodetectCommand_givenPersistModelState() {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put(ProcessCtrl.DONT_PERSIST_MODEL_STATE_SETTING.getKey(), true).build();
        Environment env = new Environment(settings);
        Job.Builder job = buildJobBuilder("unit-test-job");

        int expectedPersistInterval = 10800 + ProcessCtrl.calculateStaggeringInterval(job.getId());

        List<String> command = ProcessCtrl.buildAutodetectCommand(env, settings, job.build(), logger, false, pid);
        assertFalse(command.contains(ProcessCtrl.PERSIST_INTERVAL_ARG + expectedPersistInterval));

        settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        env = new Environment(settings);

        command = ProcessCtrl.buildAutodetectCommand(env, settings, job.build(), logger, false, pid);
        assertTrue(command.contains(ProcessCtrl.PERSIST_INTERVAL_ARG + expectedPersistInterval));
    }

    public void testBuildAutodetectCommand_GivenNoIgnoreDowntime() {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        Environment env = new Environment(
                settings);
        Job.Builder job = buildJobBuilder("foo");

        List<String> command = ProcessCtrl.buildAutodetectCommand(env, settings, job.build(), logger, false, pid);

        assertFalse(command.contains("--ignoreDowntime"));
    }

    public void testBuildAutodetectCommand_GivenIgnoreDowntimeParam() {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        Environment env = new Environment(
                settings);
        Job.Builder job = buildJobBuilder("foo");

        List<String> command = ProcessCtrl.buildAutodetectCommand(env, settings, job.build(), logger, true, pid);

        assertTrue(command.contains("--ignoreDowntime"));
    }

    public void testBuildNormalizerCommand() throws IOException {
        Environment env = new Environment(
                Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build());
        String jobId = "unit-test-job";

        List<String> command = ProcessCtrl.buildNormalizerCommand(env, jobId, null, 300, true, pid);
        assertEquals(5, command.size());
        assertTrue(command.contains(ProcessCtrl.NORMALIZE_PATH));
        assertTrue(command.contains(ProcessCtrl.BUCKET_SPAN_ARG + "300"));
        assertTrue(hasValidLicense(command));
        assertTrue(command.contains(ProcessCtrl.LENGTH_ENCODED_INPUT_ARG));
        assertTrue(command.contains(ProcessCtrl.PER_PARTITION_NORMALIZATION));
    }

    private boolean hasValidLicense(List<String> command) throws NumberFormatException {
        int matches = 0;
        for (String arg : command) {
            if (arg.startsWith(ProcessCtrl.LICENSE_VALIDATION_ARG)) {
                ++matches;
                String[] argAndVal = arg.split("=");
                if (argAndVal.length != 2) {
                    return false;
                }
                long val = Long.parseLong(argAndVal[1]);
                if ((val % ProcessCtrl.VALIDATION_NUMBER) != (pid % ProcessCtrl.VALIDATION_NUMBER)) {
                    return false;
                }
            }
        }
        return matches == 1;
    }
}
