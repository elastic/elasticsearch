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
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.test.rest.XPackRestTestHelper;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;

public class MlMigrationIT extends AbstractUpgradeTestCase {

    private static final String OLD_CLUSTER_OPEN_JOB_ID = "migration-old-cluster-open-job";
    private static final String OLD_CLUSTER_STARTED_DATAFEED_ID = "migration-old-cluster-started-datafeed";
    private static final String OLD_CLUSTER_CLOSED_JOB_ID = "migration-old-cluster-closed-job";
    private static final String OLD_CLUSTER_STOPPED_DATAFEED_ID = "migration-old-cluster-stopped-datafeed";


    @Override
    protected Collection<String> templatesToWaitFor() {
        List<String> templatesToWaitFor = XPackRestTestHelper.ML_POST_V660_TEMPLATES;

        // If upgrading from a version prior to v6.6.0 the set of templates
        // to wait for is different
        if (CLUSTER_TYPE == ClusterType.OLD) {
            String versionProperty = System.getProperty("tests.upgrade_from_version");
            if (versionProperty == null) {
                throw new IllegalStateException("System property 'tests.upgrade_from_version' not set, cannot start tests");
            }

            Version upgradeFromVersion = Version.fromString(versionProperty);
            if (upgradeFromVersion.before(Version.V_6_6_0)) {
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
        Request createTestIndex = new Request("PUT", "/airline-data");
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
        dfBuilder.setIndices(Collections.singletonList("airline-data"));
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
        stoppedDfBuilder.setIndices(Collections.singletonList("airline-data"));

        Request putStoppedDatafeed = new Request("PUT", "_xpack/ml/datafeeds/" + OLD_CLUSTER_STOPPED_DATAFEED_ID);
        putStoppedDatafeed.setJsonEntity(Strings.toString(stoppedDfBuilder.build()));
        client().performRequest(putStoppedDatafeed);

        assertConfigInClusterState();
    }

    private void mixedClusterTests() throws IOException {
        assertConfigInClusterState();
        checkJobs(false);
        checkDatafeeds();
    }

    private void upgradedClusterTests() throws Exception {
        tryUpdate();
        waitFromMigration();
        checkJobs(true);
        checkDatafeeds();
    }

    @SuppressWarnings("unchecked")
    private void checkJobs(boolean checkMigrated) throws IOException {
        // Wildcard expansion of jobs and datafeeds was added in 6.1.0
        if (UPGRADED_FROM_VERSION.before(Version.V_6_1_0) && CLUSTER_TYPE != ClusterType.UPGRADED) {
            return;
        }

        Request getJobs = new Request("GET", "_xpack/ml/anomaly_detectors/migration*");
        Response response = client().performRequest(getJobs);

        Map<String, Object> jobs = entityAsMap(response);
        List<Map<String, Object>> jobConfigs =
                (List<Map<String, Object>>) XContentMapValues.extractValue("jobs", jobs);

        assertThat(jobConfigs, hasSize(2));
        assertEquals(OLD_CLUSTER_CLOSED_JOB_ID, jobConfigs.get(0).get("job_id"));
        assertEquals(OLD_CLUSTER_OPEN_JOB_ID, jobConfigs.get(1).get("job_id"));
        if (checkMigrated) {
            // closed job will have been migrated
            assertJobIsMigrated(jobConfigs.get(0));

            // open job will not
            Map<String, Object> customSettings = (Map<String, Object>)jobConfigs.get(1).get("custom_settings");
            if (customSettings != null) {
                assertNull(customSettings.get("migrated from version"));
            }
        }


        Request getJobStats = new Request("GET", "_xpack/ml/anomaly_detectors/migration*/_stats");
        response = client().performRequest(getJobStats);

        Map<String, Object> stats = entityAsMap(response);
        List<Map<String, Object>> jobStats =
                (List<Map<String, Object>>) XContentMapValues.extractValue("jobs", stats);
        assertThat(jobStats, hasSize(2));

        assertEquals(OLD_CLUSTER_CLOSED_JOB_ID, XContentMapValues.extractValue("job_id", jobStats.get(0)));
        if (checkMigrated == false) {
            assertEquals("closed", XContentMapValues.extractValue("state", jobStats.get(0)));
        }
        assertThat((String)XContentMapValues.extractValue("assignment_explanation", jobStats.get(0)), isEmptyOrNullString());

        assertEquals(OLD_CLUSTER_OPEN_JOB_ID, XContentMapValues.extractValue("job_id", jobStats.get(1)));
        assertEquals("opened", XContentMapValues.extractValue("state", jobStats.get(1)));
        assertThat((String)XContentMapValues.extractValue("assignment_explanation", jobStats.get(1)), isEmptyOrNullString());
    }

    @SuppressWarnings("unchecked")
    private void checkDatafeeds() throws IOException {
        // Wildcard expansion of jobs and datafeeds was added in 6.1.0
        if (UPGRADED_FROM_VERSION.before(Version.V_6_1_0) && CLUSTER_TYPE != ClusterType.UPGRADED) {
            return;
        }

        Request getDatafeeds = new Request("GET", "_xpack/ml/datafeeds/migration*");
        Response response = client().performRequest(getDatafeeds);
        List<Map<String, Object>> configs =
                (List<Map<String, Object>>) XContentMapValues.extractValue("datafeeds", entityAsMap(response));
        assertThat(configs, hasSize(2));
        assertEquals(OLD_CLUSTER_STARTED_DATAFEED_ID, XContentMapValues.extractValue("datafeed_id", configs.get(0)));
        assertEquals(OLD_CLUSTER_STOPPED_DATAFEED_ID, XContentMapValues.extractValue("datafeed_id", configs.get(1)));

        Request getDatafeedStats = new Request("GET", "_xpack/ml/datafeeds/migration*/_stats");
        response = client().performRequest(getDatafeedStats);
        configs = (List<Map<String, Object>>) XContentMapValues.extractValue("datafeeds", entityAsMap(response));
        assertThat(configs, hasSize(2));
        assertEquals(OLD_CLUSTER_STARTED_DATAFEED_ID, XContentMapValues.extractValue("datafeed_id", configs.get(0)));
        assertEquals("started", XContentMapValues.extractValue("state", configs.get(0)));
        assertEquals(OLD_CLUSTER_STOPPED_DATAFEED_ID, XContentMapValues.extractValue("datafeed_id", configs.get(1)));
        assertEquals("stopped", XContentMapValues.extractValue("state", configs.get(1)));
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
    private void waitFromMigration() throws Exception {
        assertBusy(() -> {
            try {
                // wait for the eligible configs to be moved from the clusterstate
                Request getClusterState = new Request("GET", "/_cluster/state/metadata");
                Response response = client().performRequest(getClusterState);
                Map<String, Object> responseMap = entityAsMap(response);

                List<Map<String, Object>> jobs =
                        (List<Map<String, Object>>) XContentMapValues.extractValue("metadata.ml.jobs", responseMap);
                assertNotNull(jobs);
                // closed job should be migrated
                Optional<Object> config = jobs.stream().map(map -> map.get("job_id"))
                        .filter(id -> id.equals(OLD_CLUSTER_CLOSED_JOB_ID)).findFirst();
                assertFalse(config.isPresent());
                // open is not
                config = jobs.stream().map(map -> map.get("job_id"))
                        .filter(id -> id.equals(OLD_CLUSTER_OPEN_JOB_ID)).findFirst();
                assertTrue(config.isPresent());


                List<Map<String, Object>> datafeeds =
                        (List<Map<String, Object>>) XContentMapValues.extractValue("metadata.ml.datafeeds", responseMap);
                assertNotNull(datafeeds);
                // stopped datafeed should be migrated
                config = datafeeds.stream().map(map -> map.get("datafeed_id"))
                        .filter(id -> id.equals(OLD_CLUSTER_STOPPED_DATAFEED_ID)).findFirst();
                assertFalse(config.isPresent());
                // started is not
                config = datafeeds.stream().map(map -> map.get("datafeed_id"))
                        .filter(id -> id.equals(OLD_CLUSTER_STARTED_DATAFEED_ID)).findFirst();
                assertTrue(config.isPresent());

            } catch (IOException e) {

            }
        }, 30, TimeUnit.SECONDS);
    }

    @SuppressWarnings("unchecked")
    private void tryUpdate() throws IOException {
        // update to jobs and datafeeds should be rejected
        try {
            Request openJob = new Request("POST", "_xpack/ml/anomaly_detectors/" + OLD_CLUSTER_CLOSED_JOB_ID + "/_open");
            client().performRequest(openJob);
            // if the request was successful the job should have been migrated
            Request getJob = new Request("GET", "_xpack/ml/anomaly_detectors/" + OLD_CLUSTER_CLOSED_JOB_ID);
            Response response = client().performRequest(getJob);

            List<Map<String, Object>> jobConfigs =
                    (List<Map<String, Object>>) XContentMapValues.extractValue("jobs", entityAsMap(response));
            assertJobIsMigrated(jobConfigs.get(0));
        } catch (ResponseException e) {
            assertEquals(503, e.getResponse().getStatusLine().getStatusCode());
        }
    }

    @SuppressWarnings("unchecked")
    private void assertJobIsMigrated(Map<String, Object> job) {
        Map<String, Object> customSettings = (Map<String, Object>)job.get("custom_settings");
        assertThat(customSettings.keySet(), contains("migrated from version"));
        assertEquals(UPGRADED_FROM_VERSION.toString(), customSettings.get("migrated from version").toString());
    }

}
