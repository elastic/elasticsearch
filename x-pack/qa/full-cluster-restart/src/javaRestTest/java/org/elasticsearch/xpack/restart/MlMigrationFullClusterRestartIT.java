/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.restart;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.upgrades.FullClusterRestartUpgradeStatus;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.test.rest.XPackRestTestConstants;
import org.elasticsearch.xpack.test.rest.XPackRestTestHelper;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.is;

public class MlMigrationFullClusterRestartIT extends AbstractXpackFullClusterRestartTestCase {

    private static final String OLD_CLUSTER_OPEN_JOB_ID = "migration-old-cluster-open-job";
    private static final String OLD_CLUSTER_STARTED_DATAFEED_ID = "migration-old-cluster-started-datafeed";
    private static final String OLD_CLUSTER_CLOSED_JOB_ID = "migration-old-cluster-closed-job";
    private static final String OLD_CLUSTER_STOPPED_DATAFEED_ID = "migration-old-cluster-stopped-datafeed";

    public MlMigrationFullClusterRestartIT(@Name("cluster") FullClusterRestartUpgradeStatus upgradeStatus) {
        super(upgradeStatus);
    }

    @Override
    protected Settings restClientSettings() {
        String token = "Basic " + Base64.getEncoder().encodeToString("test_user:x-pack-test-password".getBytes(StandardCharsets.UTF_8));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Before
    public void waitForMlTemplates() throws Exception {
        // We shouldn't wait for ML templates during the upgrade - production won't
        if (isRunningAgainstOldCluster()) {
            XPackRestTestHelper.waitForTemplates(client(), XPackRestTestConstants.ML_POST_V7120_TEMPLATES);
        }
    }

    private void createTestIndex() throws IOException {
        Request createTestIndex = new Request("PUT", "/airline-data");
        createTestIndex.setJsonEntity("""
            {
              "mappings": {
                "doc": {
                  "properties": {
                    "time": {
                      "type": "date"
                    },
                    "airline": {
                      "type": "keyword"
                    },
                    "responsetime": {
                      "type": "float"
                    }
                  }
                }
              }
            }""");
        client().performRequest(createTestIndex);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/36816")
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
        putJob(OLD_CLUSTER_CLOSED_JOB_ID);

        DatafeedConfig.Builder stoppedDfBuilder = new DatafeedConfig.Builder(OLD_CLUSTER_STOPPED_DATAFEED_ID, OLD_CLUSTER_CLOSED_JOB_ID);
        stoppedDfBuilder.setIndices(Collections.singletonList("airline-data"));

        Request putStoppedDatafeed = new Request("PUT", "/_xpack/ml/datafeeds/" + OLD_CLUSTER_STOPPED_DATAFEED_ID);
        putStoppedDatafeed.setJsonEntity(Strings.toString(stoppedDfBuilder.build()));
        client().performRequest(putStoppedDatafeed);

        // open job and started datafeed
        putJob(OLD_CLUSTER_OPEN_JOB_ID);
        Request openOpenJob = new Request("POST", "_xpack/ml/anomaly_detectors/" + OLD_CLUSTER_OPEN_JOB_ID + "/_open");
        client().performRequest(openOpenJob);

        DatafeedConfig.Builder dfBuilder = new DatafeedConfig.Builder(OLD_CLUSTER_STARTED_DATAFEED_ID, OLD_CLUSTER_OPEN_JOB_ID);
        dfBuilder.setIndices(Collections.singletonList("airline-data"));
        addAggregations(dfBuilder);

        Request putDatafeed = new Request("PUT", "_xpack/ml/datafeeds/" + OLD_CLUSTER_STARTED_DATAFEED_ID);
        putDatafeed.setJsonEntity(Strings.toString(dfBuilder.build()));
        client().performRequest(putDatafeed);

        Request startDatafeed = new Request("POST", "_xpack/ml/datafeeds/" + OLD_CLUSTER_STARTED_DATAFEED_ID + "/_start");
        client().performRequest(startDatafeed);
    }

    private void upgradedClusterTests() throws Exception {
        waitForJobToBeAssigned(OLD_CLUSTER_OPEN_JOB_ID);
        waitForDatafeedToBeAssigned(OLD_CLUSTER_STARTED_DATAFEED_ID);
        // The persistent task params for the job & datafeed left open
        // during upgrade should be updated with new fields
        checkTaskParamsAreUpdated(OLD_CLUSTER_OPEN_JOB_ID, OLD_CLUSTER_STARTED_DATAFEED_ID);

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
            List<Map<String, Object>> jobStats = (List<Map<String, Object>>) XContentMapValues.extractValue("jobs", stats);

            assertEquals(jobId, XContentMapValues.extractValue("job_id", jobStats.get(0)));
            assertEquals("opened", XContentMapValues.extractValue("state", jobStats.get(0)));
            assertThat((String) XContentMapValues.extractValue("assignment_explanation", jobStats.get(0)), is(emptyOrNullString()));
            assertNotNull(XContentMapValues.extractValue("node", jobStats.get(0)));
        }, 30, TimeUnit.SECONDS);
    }

    @SuppressWarnings("unchecked")
    private void waitForDatafeedToBeAssigned(String datafeedId) throws Exception {
        assertBusy(() -> {
            Request getDatafeedStats = new Request("GET", "_ml/datafeeds/" + datafeedId + "/_stats");
            Response response = client().performRequest(getDatafeedStats);
            Map<String, Object> stats = entityAsMap(response);
            List<Map<String, Object>> datafeedStats = (List<Map<String, Object>>) XContentMapValues.extractValue("datafeeds", stats);

            assertEquals(datafeedId, XContentMapValues.extractValue("datafeed_id", datafeedStats.get(0)));
            assertEquals("started", XContentMapValues.extractValue("state", datafeedStats.get(0)));
            assertThat((String) XContentMapValues.extractValue("assignment_explanation", datafeedStats.get(0)), is(emptyOrNullString()));
            assertNotNull(XContentMapValues.extractValue("node", datafeedStats.get(0)));
        }, 30, TimeUnit.SECONDS);
    }

    @SuppressWarnings("unchecked")
    private void checkTaskParamsAreUpdated(String jobId, String datafeedId) throws Exception {
        Request getClusterState = new Request("GET", "/_cluster/state/metadata");
        Response response = client().performRequest(getClusterState);
        Map<String, Object> responseMap = entityAsMap(response);

        List<Map<String, Object>> tasks = (List<Map<String, Object>>) XContentMapValues.extractValue(
            "metadata.persistent_tasks.tasks",
            responseMap
        );
        assertNotNull(tasks);
        for (Map<String, Object> task : tasks) {
            String id = (String) task.get("id");
            assertNotNull(id);
            if (id.equals(MlTasks.jobTaskId(jobId))) {
                Object jobParam = XContentMapValues.extractValue("task.xpack/ml/job.params.job", task);
                assertNotNull(jobParam);
            } else if (id.equals(MlTasks.datafeedTaskId(datafeedId))) {
                Object jobIdParam = XContentMapValues.extractValue("task.xpack/ml/datafeed.params.job_id", task);
                assertNotNull(jobIdParam);
                Object indices = XContentMapValues.extractValue("task.xpack/ml/datafeed.params.indices", task);
                assertNotNull(indices);
            }
        }
    }

    private void addAggregations(DatafeedConfig.Builder dfBuilder) {
        TermsAggregationBuilder airline = AggregationBuilders.terms("airline");
        MaxAggregationBuilder maxTime = AggregationBuilders.max("time").field("time").subAggregation(airline);
        dfBuilder.setParsedAggregations(
            AggregatorFactories.builder()
                .addAggregator(AggregationBuilders.histogram("time").interval(300000).subAggregation(maxTime).field("time"))
        );
    }

    private void putJob(String jobId) throws IOException {
        String jobConfig = Strings.format("""
            {
                "job_id": "%s",
                "analysis_config": {
                    "bucket_span": "10m",
                    "detectors": [{
                        "function": "metric",
                        "by_field_name": "airline",
                        "field_name": "responsetime"
                    }]
                },
                "data_description": {}`
            }""", jobId);
        Request putClosedJob = new Request("PUT", "/_xpack/ml/anomaly_detectors/" + jobId);
        putClosedJob.setJsonEntity(jobConfig);
        client().performRequest(putClosedJob);
    }
}
