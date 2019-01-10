/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.restart;

import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.upgrades.AbstractFullClusterRestartTestCase;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.test.rest.XPackRestTestHelper;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.isEmptyOrNullString;

public class MlMigrationFullClusterRestartIT extends AbstractFullClusterRestartTestCase {

    private static final String OLD_CLUSTER_CLOSED_JOB_ID = "migration-old-cluster-closed-job";
    private static final String OLD_CLUSTER_STOPPED_DATAFEED_ID = "migration-old-cluster-stopped-datafeed";

    @Override
    protected Settings restClientSettings() {
        String token = "Basic " + Base64.getEncoder().encodeToString("test_user:x-pack-test-password".getBytes(StandardCharsets.UTF_8));
        return Settings.builder()
                .put(ThreadContext.PREFIX + ".Authorization", token)
                .build();
    }

    @Before
    public void waitForMlTemplates() throws Exception {
        List<String> templatesToWaitFor = XPackRestTestHelper.ML_POST_V660_TEMPLATES;

        // If upgrading from a version prior to v6.6.0 the set of templates
        // to wait for is different
        if (isRunningAgainstOldCluster() && getOldClusterVersion().before(Version.V_6_6_0) ) {
                templatesToWaitFor = XPackRestTestHelper.ML_PRE_V660_TEMPLATES;
        }

        XPackRestTestHelper.waitForTemplates(client(), templatesToWaitFor);
    }

    private void createTestIndex() throws IOException {
        Request createTestIndex = new Request("PUT", "/airline-data");
        createTestIndex.setJsonEntity("{\"mappings\": { \"doc\": {\"properties\": {" +
                "\"time\": {\"type\": \"date\"}," +
                "\"airline\": {\"type\": \"keyword\"}," +
                "\"responsetime\": {\"type\": \"float\"}" +
                "}}}}");
        client().performRequest(createTestIndex);
    }

    public void testMigration() throws Exception {
        if (isRunningAgainstOldCluster()) {
            createTestIndex();
            oldClusterTests();
        } else {
            upgradedClusterTests();
        }
    }

    private void oldClusterTests() throws IOException {
        // create jobs and datafeeds
        Detector.Builder d = new Detector.Builder("metric", "responsetime");
        d.setByFieldName("airline");
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(d.build()));
        analysisConfig.setBucketSpan(TimeValue.timeValueMinutes(10));

        Job.Builder closedJob = new Job.Builder(OLD_CLUSTER_CLOSED_JOB_ID);
        closedJob.setAnalysisConfig(analysisConfig);
        closedJob.setDataDescription(new DataDescription.Builder());

        Request putClosedJob = new Request("PUT", "/_xpack/ml/anomaly_detectors/" + OLD_CLUSTER_CLOSED_JOB_ID);
        putClosedJob.setJsonEntity(Strings.toString(closedJob));
        client().performRequest(putClosedJob);

        DatafeedConfig.Builder stoppedDfBuilder = new DatafeedConfig.Builder(OLD_CLUSTER_STOPPED_DATAFEED_ID, OLD_CLUSTER_CLOSED_JOB_ID);
        if (getOldClusterVersion().before(Version.V_6_6_0)) {
            stoppedDfBuilder.setDelayedDataCheckConfig(null);
        }
        stoppedDfBuilder.setIndices(Collections.singletonList("airline-data"));

        Request putStoppedDatafeed = new Request("PUT", "/_xpack/ml/datafeeds/" + OLD_CLUSTER_STOPPED_DATAFEED_ID);
        putStoppedDatafeed.setJsonEntity(Strings.toString(stoppedDfBuilder.build()));
        client().performRequest(putStoppedDatafeed);
    }

    private void upgradedClusterTests() throws Exception {
        // wait for the closed job and datafeed to be migrated
        waitForMigration(Collections.singletonList(OLD_CLUSTER_CLOSED_JOB_ID),
                Collections.singletonList(OLD_CLUSTER_STOPPED_DATAFEED_ID),
                Collections.emptyList(), Collections.emptyList());

        // open the migrated job and datafeed
        Request openJob = new Request("POST", "_ml/anomaly_detectors/" + OLD_CLUSTER_CLOSED_JOB_ID + "/_open");
        client().performRequest(openJob);
        Request startDatafeed = new Request("POST", "_ml/datafeeds/" + OLD_CLUSTER_STOPPED_DATAFEED_ID + "/_start");
        client().performRequest(startDatafeed);

        waitForJobToBeAssigned(OLD_CLUSTER_CLOSED_JOB_ID);
        waitForDatafeedToBeAssigned(OLD_CLUSTER_STOPPED_DATAFEED_ID);
    }

    @SuppressWarnings("unchecked")
    private void waitForJobToBeAssigned(String jobId) throws Exception {
        assertBusy(() -> {
            Request getJobStats = new Request("GET", "_ml/anomaly_detectors/" + jobId + "/_stats");
            Response response = client().performRequest(getJobStats);

            Map<String, Object> stats = entityAsMap(response);
            List<Map<String, Object>> jobStats =
                    (List<Map<String, Object>>) XContentMapValues.extractValue("jobs", stats);

            assertEquals(jobId, XContentMapValues.extractValue("job_id", jobStats.get(0)));
            assertEquals("opened", XContentMapValues.extractValue("state", jobStats.get(0)));
            assertThat((String) XContentMapValues.extractValue("assignment_explanation", jobStats.get(0)), isEmptyOrNullString());
            assertNotNull(XContentMapValues.extractValue("node", jobStats.get(0)));
        }, 30, TimeUnit.SECONDS);
    }

    @SuppressWarnings("unchecked")
    private void waitForDatafeedToBeAssigned(String datafeedId) throws Exception {
        assertBusy(() -> {
            Request getDatafeedStats = new Request("GET", "_ml/datafeeds/" + datafeedId + "/_stats");
            Response response = client().performRequest(getDatafeedStats);
            Map<String, Object> stats = entityAsMap(response);
            List<Map<String, Object>> datafeedStats =
                    (List<Map<String, Object>>) XContentMapValues.extractValue("datafeeds", stats);

            assertEquals(datafeedId, XContentMapValues.extractValue("datafeed_id", datafeedStats.get(0)));
            assertEquals("started", XContentMapValues.extractValue("state", datafeedStats.get(0)));
            assertThat((String) XContentMapValues.extractValue("assignment_explanation", datafeedStats.get(0)), isEmptyOrNullString());
            assertNotNull(XContentMapValues.extractValue("node", datafeedStats.get(0)));
        }, 30, TimeUnit.SECONDS);
    }

    @SuppressWarnings("unchecked")
    private void waitForMigration(List<String> expectedMigratedJobs, List<String> expectedMigratedDatafeeds,
                                  List<String> unMigratedJobs, List<String> unMigratedDatafeeds) throws Exception {

        // After v6.6.0 jobs are created in the index so no migration will take place
        if (getOldClusterVersion().onOrAfter(Version.V_6_6_0)) {
            return;
        }

        assertBusy(() -> {
            // wait for the eligible configs to be moved from the clusterstate
            Request getClusterState = new Request("GET", "/_cluster/state/metadata");
            Response response = client().performRequest(getClusterState);
            Map<String, Object> responseMap = entityAsMap(response);

            List<Map<String, Object>> jobs =
                    (List<Map<String, Object>>) XContentMapValues.extractValue("metadata.ml.jobs", responseMap);
            assertNotNull(jobs);

            for (String jobId : expectedMigratedJobs) {
                assertJob(jobId, jobs, false);
            }

            for (String jobId : unMigratedJobs) {
                assertJob(jobId, jobs, true);
            }

            List<Map<String, Object>> datafeeds =
                    (List<Map<String, Object>>) XContentMapValues.extractValue("metadata.ml.datafeeds", responseMap);
            assertNotNull(datafeeds);

            for (String datafeedId : expectedMigratedDatafeeds) {
                assertDatafeed(datafeedId, datafeeds, false);
            }

            for (String datafeedId : unMigratedDatafeeds) {
                assertDatafeed(datafeedId, datafeeds, true);
            }

        }, 30, TimeUnit.SECONDS);
    }

    private void assertDatafeed(String datafeedId, List<Map<String, Object>> datafeeds, boolean expectedToBePresent) {
        Optional<Object> config = datafeeds.stream().map(map -> map.get("datafeed_id"))
                .filter(id -> id.equals(datafeedId)).findFirst();
        if (expectedToBePresent) {
            assertTrue(config.isPresent());
        } else {
            assertFalse(config.isPresent());
        }
    }

    private void assertJob(String jobId, List<Map<String, Object>> jobs, boolean expectedToBePresent) {
        Optional<Object> config = jobs.stream().map(map -> map.get("job_id"))
                .filter(id -> id.equals(jobId)).findFirst();
        if (expectedToBePresent) {
            assertTrue(config.isPresent());
        } else {
            assertFalse(config.isPresent());
        }
    }
}
