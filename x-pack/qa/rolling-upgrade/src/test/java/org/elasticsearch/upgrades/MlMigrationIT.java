/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.upgrades;

import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.test.rest.XPackRestTestHelper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;

public class MlMigrationIT extends AbstractUpgradeTestCase {

    private static final String PREFIX = "ml-migration-it-";
    private static final String OLD_CLUSTER_OPEN_JOB_ID = PREFIX + "old-cluster-open-job";
    private static final String OLD_CLUSTER_STARTED_DATAFEED_ID = PREFIX + "old-cluster-started-datafeed";
    private static final String OLD_CLUSTER_CLOSED_JOB_ID = PREFIX + "old-cluster-closed-job";
    private static final String OLD_CLUSTER_STOPPED_DATAFEED_ID = PREFIX + "old-cluster-stopped-datafeed";
    private static final String OLD_CLUSTER_CLOSED_JOB_EXTRA_ID = PREFIX + "old-cluster-closed-job-extra";
    private static final String OLD_CLUSTER_STOPPED_DATAFEED_EXTRA_ID = PREFIX + "old-cluster-stopped-datafeed-extra";

    @Override
    protected Collection<String> templatesToWaitFor() {
        List<String> templatesToWaitFor = XPackRestTestHelper.ML_POST_V660_TEMPLATES;

        // If upgrading from a version prior to v6.6.0 the set of templates
        // to wait for is different
        if (CLUSTER_TYPE == ClusterType.OLD) {
            if (UPGRADED_FROM_VERSION.before(Version.V_6_6_0)) {
                templatesToWaitFor = XPackRestTestHelper.ML_PRE_V660_TEMPLATES;
            }
        }

        return templatesToWaitFor;
    }

    private void waitForClusterHealth() throws IOException {
        switch (CLUSTER_TYPE) {
            case OLD:
            case MIXED:
                Request waitForYellow = new Request("GET", "/_cluster/health");
                waitForYellow.addParameter("wait_for_nodes", "3");
                waitForYellow.addParameter("wait_for_status", "yellow");
                client().performRequest(waitForYellow);
                break;
            case UPGRADED:
                Request waitForGreen = new Request("GET", "/_cluster/health");
                waitForGreen.addParameter("wait_for_nodes", "3");
                waitForGreen.addParameter("wait_for_status", "green");
                // wait for long enough that we give delayed unassigned shards to stop being delayed
                waitForGreen.addParameter("timeout", "70s");
                waitForGreen.addParameter("level", "shards");
                client().performRequest(waitForGreen);
                break;
            default:
                throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }
    }

    private void createTestIndex() throws IOException {
        Request createTestIndex = new Request("PUT", "/airline-responsetime-data");
        createTestIndex.setJsonEntity("{\"mappings\": { \"doc\": {\"properties\": {" +
                    "\"time\": {\"type\": \"date\"}," +
                    "\"airline\": {\"type\": \"keyword\"}," +
                    "\"responsetime\": {\"type\": \"float\"}" +
                "}}}}");
        client().performRequest(createTestIndex);
    }
    
    public void testConfigMigration() throws Exception {
        if (UPGRADED_FROM_VERSION.onOrAfter(Version.V_6_6_0)) {
            // We are testing migration of ml config defined in the clusterstate
            // in versions before V6.6.0. There is no point testing later versions
            // as the config will be written to index documents
            logger.info("Testing migration of ml config in version [" + UPGRADED_FROM_VERSION + "] is a no-op");
            return;
        }

        waitForClusterHealth();

        switch (CLUSTER_TYPE) {
            case OLD:
                createTestIndex();
                oldClusterTests();
                break;
            case MIXED:
                mixedClusterTests();
                break;
            case UPGRADED:
                upgradedClusterTests();
                break;
            default:
                throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }
    }

    private void oldClusterTests() throws IOException {
        // create jobs and datafeeds
        Detector.Builder d = new Detector.Builder("metric", "responsetime");
        d.setByFieldName("airline");
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(d.build()));
        analysisConfig.setBucketSpan(TimeValue.timeValueMinutes(10));
        Job.Builder openJob = new Job.Builder(OLD_CLUSTER_OPEN_JOB_ID);
        openJob.setAnalysisConfig(analysisConfig);
        openJob.setDataDescription(new DataDescription.Builder());

        Request putOpenJob = new Request("PUT", "_xpack/ml/anomaly_detectors/" + OLD_CLUSTER_OPEN_JOB_ID);
        putOpenJob.setJsonEntity(Strings.toString(openJob));
        client().performRequest(putOpenJob);

        Request openOpenJob = new Request("POST", "_xpack/ml/anomaly_detectors/" + OLD_CLUSTER_OPEN_JOB_ID + "/_open");
        client().performRequest(openOpenJob);

        DatafeedConfig.Builder dfBuilder = new DatafeedConfig.Builder(OLD_CLUSTER_STARTED_DATAFEED_ID, OLD_CLUSTER_OPEN_JOB_ID);
        if (UPGRADED_FROM_VERSION.before(Version.V_6_6_0)) {
            dfBuilder.setDelayedDataCheckConfig(null);
        }
        dfBuilder.setIndices(Collections.singletonList("airline-responsetime-data"));
        dfBuilder.setTypes(Collections.singletonList("doc"));

        Request putDatafeed = new Request("PUT", "_xpack/ml/datafeeds/" + OLD_CLUSTER_STARTED_DATAFEED_ID);
        putDatafeed.setJsonEntity(Strings.toString(dfBuilder.build()));
        client().performRequest(putDatafeed);

        Request startDatafeed = new Request("POST", "_xpack/ml/datafeeds/" + OLD_CLUSTER_STARTED_DATAFEED_ID + "/_start");
        client().performRequest(startDatafeed);

        Job.Builder closedJob = new Job.Builder(OLD_CLUSTER_CLOSED_JOB_ID);
        closedJob.setAnalysisConfig(analysisConfig);
        closedJob.setDataDescription(new DataDescription.Builder());

        Request putClosedJob = new Request("PUT", "_xpack/ml/anomaly_detectors/" + OLD_CLUSTER_CLOSED_JOB_ID);
        putClosedJob.setJsonEntity(Strings.toString(closedJob));
        client().performRequest(putClosedJob);

        DatafeedConfig.Builder stoppedDfBuilder = new DatafeedConfig.Builder(OLD_CLUSTER_STOPPED_DATAFEED_ID, OLD_CLUSTER_CLOSED_JOB_ID);
        if (UPGRADED_FROM_VERSION.before(Version.V_6_6_0)) {
            stoppedDfBuilder.setDelayedDataCheckConfig(null);
        }
        stoppedDfBuilder.setIndices(Collections.singletonList("airline-responsetime-data"));

        Request putStoppedDatafeed = new Request("PUT", "_xpack/ml/datafeeds/" + OLD_CLUSTER_STOPPED_DATAFEED_ID);
        putStoppedDatafeed.setJsonEntity(Strings.toString(stoppedDfBuilder.build()));
        client().performRequest(putStoppedDatafeed);

        Job.Builder extraJob = new Job.Builder(OLD_CLUSTER_CLOSED_JOB_EXTRA_ID);
        extraJob.setAnalysisConfig(analysisConfig);
        extraJob.setDataDescription(new DataDescription.Builder());

        Request putExtraJob = new Request("PUT", "_xpack/ml/anomaly_detectors/" + OLD_CLUSTER_CLOSED_JOB_EXTRA_ID);
        putExtraJob.setJsonEntity(Strings.toString(extraJob));
        client().performRequest(putExtraJob);

        putStoppedDatafeed = new Request("PUT", "_xpack/ml/datafeeds/" + OLD_CLUSTER_STOPPED_DATAFEED_EXTRA_ID);
        stoppedDfBuilder.setId(OLD_CLUSTER_STOPPED_DATAFEED_EXTRA_ID);
        stoppedDfBuilder.setJobId(OLD_CLUSTER_CLOSED_JOB_EXTRA_ID);
        putStoppedDatafeed.setJsonEntity(Strings.toString(stoppedDfBuilder.build()));
        client().performRequest(putStoppedDatafeed);

        assertConfigInClusterState();
    }

    private void mixedClusterTests() throws Exception {
        assertConfigInClusterState();
        checkJobs();
        checkDatafeeds();
    }

    private void upgradedClusterTests() throws Exception {
        tryUpdates();

        // These requests may fail because the configs have not been migrated
        // and open is disallowed prior to migration.
        boolean jobOpened = openMigratedJob(OLD_CLUSTER_CLOSED_JOB_ID);
        boolean datafeedStarted = false;
        if (jobOpened) {
            datafeedStarted = startMigratedDatafeed(OLD_CLUSTER_STOPPED_DATAFEED_ID);
        }

        // wait for the closed job and datafeed to be migrated
        waitForMigration(OLD_CLUSTER_CLOSED_JOB_ID, OLD_CLUSTER_STOPPED_DATAFEED_ID);

        // The open job and datafeed may or may not be migrated depending on how they were allocated.
        // Migration will only occur once all nodes in the cluster are v6.6.0 or higher
        // open jobs will only be migrated once they become unallocated. The open job
        // will only meet these conditions if it is running on the last node to be
        // upgraded
        waitForPossibleMigration(OLD_CLUSTER_OPEN_JOB_ID, OLD_CLUSTER_STARTED_DATAFEED_ID);

        // the job and datafeed left open during upgrade should
        // be assigned to a node
        waitForJobToBeAssigned(OLD_CLUSTER_OPEN_JOB_ID);
        waitForDatafeedToBeAssigned(OLD_CLUSTER_STARTED_DATAFEED_ID);

        // Now config is definitely migrated open job and datafeed
        // if the previous attempts failed
        if (jobOpened == false) {
            assertTrue(openMigratedJob(OLD_CLUSTER_CLOSED_JOB_ID));
        }
        if (datafeedStarted == false) {
            assertTrue(startMigratedDatafeed(OLD_CLUSTER_STOPPED_DATAFEED_ID));
        }
        waitForJobToBeAssigned(OLD_CLUSTER_CLOSED_JOB_ID);
        waitForDatafeedToBeAssigned(OLD_CLUSTER_STOPPED_DATAFEED_ID);

        // close the job left open during upgrade
        Request stopDatafeed = new Request("POST", "_xpack/ml/datafeeds/" + OLD_CLUSTER_STARTED_DATAFEED_ID + "/_stop");
        client().performRequest(stopDatafeed);

        Request closeJob = new Request("POST", "_xpack/ml/anomaly_detectors/" + OLD_CLUSTER_OPEN_JOB_ID + "/_close");
        client().performRequest(closeJob);

        // if the open job wasn't migrated previously it should be now after it has been closed
        waitForMigration(OLD_CLUSTER_OPEN_JOB_ID, OLD_CLUSTER_STARTED_DATAFEED_ID);
        checkJobsMarkedAsMigrated(Arrays.asList(OLD_CLUSTER_CLOSED_JOB_ID, OLD_CLUSTER_OPEN_JOB_ID));

        // and the job left open can be deleted
        Request deleteDatafeed = new Request("DELETE", "_xpack/ml/datafeeds/" + OLD_CLUSTER_STARTED_DATAFEED_ID);
        client().performRequest(deleteDatafeed);
        Request deleteJob = new Request("DELETE", "_xpack/ml/anomaly_detectors/" + OLD_CLUSTER_OPEN_JOB_ID);
        client().performRequest(deleteJob);
    }

    @SuppressWarnings("unchecked")
    private void checkJobs() throws IOException {
        // Wildcard expansion of jobs and datafeeds was added in 6.1.0
        if (UPGRADED_FROM_VERSION.before(Version.V_6_1_0) && CLUSTER_TYPE != ClusterType.UPGRADED) {
            return;
        }

        Request getJobs = new Request("GET", "_xpack/ml/anomaly_detectors/" + PREFIX + "*");
        Response response = client().performRequest(getJobs);

        Map<String, Object> jobs = entityAsMap(response);
        List<Map<String, Object>> jobConfigs =
                (List<Map<String, Object>>) XContentMapValues.extractValue("jobs", jobs);

        assertThat(jobConfigs, hasSize(3));
        assertEquals(OLD_CLUSTER_CLOSED_JOB_ID, jobConfigs.get(0).get("job_id"));
        assertEquals(OLD_CLUSTER_CLOSED_JOB_EXTRA_ID, jobConfigs.get(1).get("job_id"));
        assertEquals(OLD_CLUSTER_OPEN_JOB_ID, jobConfigs.get(2).get("job_id"));

        Map<String, Object> customSettings = (Map<String, Object>)jobConfigs.get(0).get("custom_settings");
        if (customSettings != null) {
            assertNull(customSettings.get("migrated from version"));
        }
        customSettings = (Map<String, Object>)jobConfigs.get(1).get("custom_settings");
        if (customSettings != null) {
            assertNull(customSettings.get("migrated from version"));
        }

        Request getJobStats = new Request("GET", "_xpack/ml/anomaly_detectors/"+ PREFIX + "*/_stats");
        response = client().performRequest(getJobStats);

        Map<String, Object> stats = entityAsMap(response);
        List<Map<String, Object>> jobStats =
                (List<Map<String, Object>>) XContentMapValues.extractValue("jobs", stats);
        assertThat(jobStats, hasSize(3));

        assertEquals(OLD_CLUSTER_CLOSED_JOB_ID, XContentMapValues.extractValue("job_id", jobStats.get(0)));
        assertEquals("closed", XContentMapValues.extractValue("state", jobStats.get(0)));
        assertThat((String)XContentMapValues.extractValue("assignment_explanation", jobStats.get(0)), isEmptyOrNullString());
    }

    @SuppressWarnings("unchecked")
    private void checkDatafeeds() throws IOException {
        // Wildcard expansion of jobs and datafeeds was added in 6.1.0
        if (UPGRADED_FROM_VERSION.before(Version.V_6_1_0) && CLUSTER_TYPE != ClusterType.UPGRADED) {
            return;
        }

        Request getDatafeeds = new Request("GET", "_xpack/ml/datafeeds/" + PREFIX + "*");
        Response response = client().performRequest(getDatafeeds);
        List<Map<String, Object>> configs =
                (List<Map<String, Object>>) XContentMapValues.extractValue("datafeeds", entityAsMap(response));
        assertThat(configs, hasSize(3));
        assertEquals(OLD_CLUSTER_STARTED_DATAFEED_ID, XContentMapValues.extractValue("datafeed_id", configs.get(0)));
        assertEquals(OLD_CLUSTER_STOPPED_DATAFEED_ID, XContentMapValues.extractValue("datafeed_id", configs.get(1)));
        assertEquals(OLD_CLUSTER_STOPPED_DATAFEED_EXTRA_ID, XContentMapValues.extractValue("datafeed_id", configs.get(2)));

        Request getDatafeedStats = new Request("GET", "_xpack/ml/datafeeds/" + PREFIX + "*/_stats");
        response = client().performRequest(getDatafeedStats);
        configs = (List<Map<String, Object>>) XContentMapValues.extractValue("datafeeds", entityAsMap(response));
        assertThat(configs, hasSize(3));
        assertEquals(OLD_CLUSTER_STARTED_DATAFEED_ID, XContentMapValues.extractValue("datafeed_id", configs.get(0)));
        assertEquals("started", XContentMapValues.extractValue("state", configs.get(0)));
        assertEquals(OLD_CLUSTER_STOPPED_DATAFEED_ID, XContentMapValues.extractValue("datafeed_id", configs.get(1)));
        assertEquals("stopped", XContentMapValues.extractValue("state", configs.get(1)));
        assertEquals(OLD_CLUSTER_STOPPED_DATAFEED_EXTRA_ID, XContentMapValues.extractValue("datafeed_id", configs.get(2)));
        assertEquals("stopped", XContentMapValues.extractValue("state", configs.get(2)));
    }

    @SuppressWarnings("unchecked")
    private void checkJobsMarkedAsMigrated(List<String> jobIds) throws IOException {
        String requestedIds = String.join(",", jobIds);
        Request getJobs = new Request("GET", "_xpack/ml/anomaly_detectors/" + requestedIds);
        Response response = client().performRequest(getJobs);
        List<Map<String, Object>> jobConfigs =
                (List<Map<String, Object>>) XContentMapValues.extractValue("jobs", entityAsMap(response));

        for (Map<String, Object> config : jobConfigs) {
            assertJobIsMarkedAsMigrated(config);
        }
    }

    @SuppressWarnings("unchecked")
    private void checkTaskParams(String jobId, String datafeedId, boolean expectedUpdated) throws Exception {
        Request getClusterState = new Request("GET", "/_cluster/state/metadata");
        Response response = client().performRequest(getClusterState);
        Map<String, Object> responseMap = entityAsMap(response);

        List<Map<String, Object>> tasks =
                (List<Map<String, Object>>) XContentMapValues.extractValue("metadata.persistent_tasks.tasks", responseMap);
        assertNotNull(tasks);
        for (Map<String, Object> task : tasks) {
            String id = (String) task.get("id");
            assertNotNull(id);
            if (id.equals(MlTasks.jobTaskId(jobId))) {
                Object jobParam = XContentMapValues.extractValue("task.xpack/ml/job.params.job", task);
                if (expectedUpdated) {
                    assertNotNull(jobParam);
                } else {
                    assertNull(jobParam);
                }
            } else if (id.equals(MlTasks.datafeedTaskId(datafeedId))) {
                Object jobIdParam = XContentMapValues.extractValue("task.xpack/ml/datafeed.params.job_id", task);
                Object indices = XContentMapValues.extractValue("task.xpack/ml/datafeed.params.indices", task);
                if (expectedUpdated) {
                    assertNotNull(jobIdParam);
                    assertNotNull(indices);
                } else {
                    assertNull(jobIdParam);
                    assertNull(indices);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void assertConfigInClusterState() throws IOException {
        Request getClusterState = new Request("GET", "/_cluster/state/metadata");
        Response response = client().performRequest(getClusterState);
        Map<String, Object> responseMap = entityAsMap(response);

        List<Map<String, Object>> jobs =
                (List<Map<String, Object>>) XContentMapValues.extractValue("metadata.ml.jobs", responseMap);
        assertThat(jobs, not(empty()));
        Optional<Object> job = jobs.stream().map(map -> map.get("job_id")).filter(id -> id.equals(OLD_CLUSTER_OPEN_JOB_ID)).findFirst();
        assertTrue(job.isPresent());
        job = jobs.stream().map(map -> map.get("job_id")).filter(id -> id.equals(OLD_CLUSTER_CLOSED_JOB_ID)).findFirst();
        assertTrue(job.isPresent());

        List<Map<String, Object>> datafeeds =
                (List<Map<String, Object>>) XContentMapValues.extractValue("metadata.ml.datafeeds", responseMap);
        assertNotNull(datafeeds);
        assertThat(datafeeds, not(empty()));
        Optional<Object> datafeed = datafeeds.stream().map(map -> map.get("datafeed_id"))
                .filter(id -> id.equals(OLD_CLUSTER_STARTED_DATAFEED_ID)).findFirst();
        assertTrue(datafeed.isPresent());
        datafeed = datafeeds.stream().map(map -> map.get("datafeed_id"))
                .filter(id -> id.equals(OLD_CLUSTER_STOPPED_DATAFEED_ID)).findFirst();
        assertTrue(datafeed.isPresent());
    }

    @SuppressWarnings("unchecked")
    private void waitForMigration(String expectedMigratedJobId, String expectedMigratedDatafeedId) throws Exception {
        assertBusy(() -> {
            // wait for the eligible configs to be moved from the clusterstate
            Request getClusterState = new Request("GET", "/_cluster/state/metadata");
            Response response = client().performRequest(getClusterState);
            Map<String, Object> responseMap = entityAsMap(response);

            List<Map<String, Object>> jobs =
                    (List<Map<String, Object>>) XContentMapValues.extractValue("metadata.ml.jobs", responseMap);

            if (jobs != null) {
                assertJobMigrated(expectedMigratedJobId, jobs);
            }

            List<Map<String, Object>> datafeeds =
                    (List<Map<String, Object>>) XContentMapValues.extractValue("metadata.ml.datafeeds", responseMap);

            if (datafeeds != null) {
                assertDatafeedMigrated(expectedMigratedDatafeedId, datafeeds);
            }

        }, 30, TimeUnit.SECONDS);
    }

    @SuppressWarnings("unchecked")
    private void waitForPossibleMigration(String perhapsMigratedJobId, String perhapsMigratedDatafeedId) throws Exception {
        assertBusy(() -> {
            Request getClusterState = new Request("GET", "/_cluster/state/metadata");
            Response response = client().performRequest(getClusterState);
            Map<String, Object> responseMap = entityAsMap(response);

            List<Map<String, Object>> jobs =
                    (List<Map<String, Object>>) XContentMapValues.extractValue("metadata.ml.jobs", responseMap);

            boolean jobMigrated = true;
            if (jobs != null) {
                jobMigrated = jobs.stream().map(map -> map.get("job_id"))
                        .noneMatch(id -> id.equals(perhapsMigratedJobId));
            }

            List<Map<String, Object>> datafeeds =
                    (List<Map<String, Object>>) XContentMapValues.extractValue("metadata.ml.datafeeds", responseMap);

            boolean datafeedMigrated = true;
            if (datafeeds != null) {
                datafeedMigrated = datafeeds.stream().map(map -> map.get("datafeed_id"))
                        .noneMatch(id -> id.equals(perhapsMigratedDatafeedId));
            }

            if (jobMigrated) {
                // if the job is migrated the datafeed should also be
                assertTrue(datafeedMigrated);
                checkJobsMarkedAsMigrated(Collections.singletonList(perhapsMigratedJobId));
            }

            // if migrated the persistent task params should have been updated
            checkTaskParams(perhapsMigratedJobId, perhapsMigratedDatafeedId, jobMigrated);
        });
    }

    @SuppressWarnings("unchecked")
    private void waitForJobToBeAssigned(String jobId) throws Exception {
        assertBusy(() -> {
            Request getJobStats = new Request("GET", "_xpack/ml/anomaly_detectors/" + jobId + "/_stats");
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
            Request getDatafeedStats = new Request("GET", "_xpack/ml/datafeeds/" + datafeedId + "/_stats");
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

    private boolean openMigratedJob(String jobId) throws IOException {
        // opening a job should be rejected prior to migration
        Request openJob = new Request("POST", "_xpack/ml/anomaly_detectors/" + jobId + "/_open");
        return updateJobExpectingSuccessOr503(jobId, openJob, "cannot open job as the configuration [" +
                jobId + "] is temporarily pending migration", false);
    }

    private boolean startMigratedDatafeed(String datafeedId) throws IOException {
        Request startDatafeed = new Request("POST", "_xpack/ml/datafeeds/" + datafeedId + "/_start");
        return updateDatafeedExpectingSuccessOr503(datafeedId, startDatafeed, "cannot start datafeed as the configuration [" +
                datafeedId + "] is temporarily pending migration", false);
    }

    private void tryUpdates() throws IOException {
        // in the upgraded cluster updates should be rejected prior
        // to migration. Either the config is migrated or the update
        // is rejected with the expected error

        // delete datafeed
        Request deleteDatafeed = new Request("DELETE", "_xpack/ml/datafeeds/" + OLD_CLUSTER_STOPPED_DATAFEED_EXTRA_ID);
        boolean datafeedDeleted = updateDatafeedExpectingSuccessOr503(OLD_CLUSTER_STOPPED_DATAFEED_EXTRA_ID, deleteDatafeed,
                "cannot delete datafeed as the configuration [" + OLD_CLUSTER_STOPPED_DATAFEED_EXTRA_ID
                        + "] is temporarily pending migration", true);

        if (datafeedDeleted && randomBoolean()) {
            // delete job if the datafeed that refers to it was deleted
            // otherwise the request is invalid
            Request deleteJob = new Request("DELETE", "_xpack/ml/anomaly_detectors/" + OLD_CLUSTER_CLOSED_JOB_EXTRA_ID);
            updateJobExpectingSuccessOr503(OLD_CLUSTER_CLOSED_JOB_EXTRA_ID, deleteJob, "cannot update job as the configuration ["
                    + OLD_CLUSTER_CLOSED_JOB_EXTRA_ID + "] is temporarily pending migration", true);
        } else {
            // update job
            Request updateJob = new Request("POST", "_xpack/ml/anomaly_detectors/" + OLD_CLUSTER_CLOSED_JOB_EXTRA_ID + "/_update");
            updateJob.setJsonEntity("{\"description\" : \"updated description\"}");
            updateJobExpectingSuccessOr503(OLD_CLUSTER_CLOSED_JOB_EXTRA_ID, updateJob, "cannot update job as the configuration ["
                    + OLD_CLUSTER_CLOSED_JOB_EXTRA_ID + "] is temporarily pending migration", false);
        }


    }

    @SuppressWarnings("unchecked")
    private boolean updateJobExpectingSuccessOr503(String jobId, Request request,
                                                   String expectedErrorMessage, boolean deleting) throws IOException {
        try {
            client().performRequest(request);

            // the request was successful so the job should have been migrated
            // ...unless it was deleted
            if (deleting) {
                return true;
            }

            Request getJob = new Request("GET", "_xpack/ml/anomaly_detectors/" + jobId);
            Response response = client().performRequest(getJob);
            List<Map<String, Object>> jobConfigs =
                    (List<Map<String, Object>>) XContentMapValues.extractValue("jobs", entityAsMap(response));
            assertJobIsMarkedAsMigrated(jobConfigs.get(0));
            return true;
        } catch (ResponseException e) {
            // a fail request is ok if the error was that the config has not been migrated
            assertThat(e.getMessage(), containsString(expectedErrorMessage));
            assertEquals(503, e.getResponse().getStatusLine().getStatusCode());
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    private boolean updateDatafeedExpectingSuccessOr503(String datafeedId, Request request,
                                                        String expectedErrorMessage, boolean deleting) throws IOException {
        // starting a datafeed should be rejected prior to migration
        try {
            client().performRequest(request);

            // the request was successful so the job should have been migrated
            // ...unless it was deleted
            if (deleting) {
                return true;
            }

            // if the request succeeded the config must have been migrated out of clusterstate
            Request getClusterState = new Request("GET", "/_cluster/state/metadata");
            Response response = client().performRequest(getClusterState);
            Map<String, Object> clusterStateMap = entityAsMap(response);
            List<Map<String, Object>> datafeeds =
                    (List<Map<String, Object>>) XContentMapValues.extractValue("metadata.ml.datafeeds", clusterStateMap);
            if (datafeeds != null) {
                assertDatafeedMigrated(datafeedId, datafeeds);
            }
            return true;
        } catch (ResponseException e) {
            // a fail request is ok if the error was that the config has not been migrated
            assertThat(e.getMessage(), containsString(expectedErrorMessage));
            assertEquals(503, e.getResponse().getStatusLine().getStatusCode());
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    private void assertJobIsMarkedAsMigrated(Map<String, Object> job) {
        Map<String, Object> customSettings = (Map<String, Object>)job.get("custom_settings");
        assertThat(customSettings.keySet(), contains("migrated from version"));
        assertEquals(UPGRADED_FROM_VERSION.toString(), customSettings.get("migrated from version").toString());
    }

    private void assertDatafeedMigrated(String datafeedId, List<Map<String, Object>> datafeeds) {
        Optional<Object> config = datafeeds.stream().map(map -> map.get("datafeed_id"))
                .filter(id -> id.equals(datafeedId)).findFirst();
        assertFalse("datafeed [" + datafeedId + "] has not been migrated", config.isPresent());
    }

    private void assertJobMigrated(String jobId, List<Map<String, Object>> jobs) {
        Optional<Object> config = jobs.stream().map(map -> map.get("job_id"))
                .filter(id -> id.equals(jobId)).findFirst();
        assertFalse("job [" + jobId + "] has not been migrated", config.isPresent());
    }
}
