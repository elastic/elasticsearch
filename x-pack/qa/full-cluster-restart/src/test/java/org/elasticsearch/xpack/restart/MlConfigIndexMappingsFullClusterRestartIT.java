/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.restart;

import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.upgrades.AbstractFullClusterRestartTestCase;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.test.rest.XPackRestTestConstants;
import org.elasticsearch.xpack.test.rest.XPackRestTestHelper;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class MlConfigIndexMappingsFullClusterRestartIT extends AbstractFullClusterRestartTestCase {

    private static final String OLD_CLUSTER_JOB_ID = "ml-config-mappings-old-cluster-job";
    private static final String NEW_CLUSTER_JOB_ID = "ml-config-mappings-new-cluster-job";

    private static final Map<String, Object> EXPECTED_DATA_FRAME_ANALYSIS_MAPPINGS = getDataFrameAnalysisMappings();

    @SuppressWarnings("unchecked")
    private static Map<String, Object> getDataFrameAnalysisMappings() {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            ElasticsearchMappings.addDataFrameAnalyticsFields(builder);
            builder.endObject();

            Map<String, Object> asMap = builder.generator().contentType().xContent().createParser(
                NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, BytesReference.bytes(builder).streamInput()).map();
            return (Map<String, Object>) asMap.get(DataFrameAnalyticsConfig.ANALYSIS.getPreferredName());
        } catch (IOException e) {
            fail("Failed to initialize expected data frame analysis mappings");
        }
        return null;
    }

    @Override
    protected Settings restClientSettings() {
        String token = "Basic " + Base64.getEncoder().encodeToString("test_user:x-pack-test-password".getBytes(StandardCharsets.UTF_8));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .build();
    }

    @Before
    public void waitForMlTemplates() throws Exception {
        List<String> templatesToWaitFor = XPackRestTestConstants.ML_POST_V660_TEMPLATES;
        XPackRestTestHelper.waitForTemplates(client(), templatesToWaitFor);
    }

    public void testMlConfigIndexMappingsAfterMigration() throws Exception {
        if (isRunningAgainstOldCluster()) {
            assertThatMlConfigIndexDoesNotExist();
            // trigger .ml-config index creation
            createAnomalyDetectorJob(OLD_CLUSTER_JOB_ID);
            if (getOldClusterVersion().onOrAfter(Version.V_7_3_0)) {
                // .ml-config has mappings for analytics as the feature was introduced in 7.3.0
                assertThat(mappingsForDataFrameAnalysis(), is(notNullValue()));
            } else {
                // .ml-config does not yet have correct mappings, it will need an update after cluster is upgraded
                assertThat(mappingsForDataFrameAnalysis(), is(nullValue()));
            }
        } else {
            // trigger .ml-config index mappings update
            createAnomalyDetectorJob(NEW_CLUSTER_JOB_ID);
            // assert that the mappings are updated
            assertThat(mappingsForDataFrameAnalysis(), is(equalTo(EXPECTED_DATA_FRAME_ANALYSIS_MAPPINGS)));
        }
    }

    private void assertThatMlConfigIndexDoesNotExist() {
        Request getIndexRequest = new Request("GET", ".ml-config");
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(getIndexRequest));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(404));
    }

    private void createAnomalyDetectorJob(String jobId) throws IOException {
        Detector.Builder detector = new Detector.Builder("metric", "responsetime")
            .setByFieldName("airline");
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(detector.build()))
            .setBucketSpan(TimeValue.timeValueMinutes(10));
        Job.Builder job = new Job.Builder(jobId)
            .setAnalysisConfig(analysisConfig)
            .setDataDescription(new DataDescription.Builder());

        Request putJobRequest = new Request("PUT", "/_ml/anomaly_detectors/" + jobId);
        putJobRequest.setJsonEntity(Strings.toString(job));
        Response putJobResponse = client().performRequest(putJobRequest);
        assertThat(putJobResponse.getStatusLine().getStatusCode(), equalTo(200));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> mappingsForDataFrameAnalysis() throws Exception {
        Request getIndexMappingsRequest = new Request("GET", ".ml-config/_mappings");
        Response getIndexMappingsResponse = client().performRequest(getIndexMappingsRequest);
        assertThat(getIndexMappingsResponse.getStatusLine().getStatusCode(), equalTo(200));

        Map<String, Object> mappings = entityAsMap(getIndexMappingsResponse);
        mappings = (Map<String, Object>) XContentMapValues.extractValue(mappings, ".ml-config", "mappings");
        if (mappings.containsKey("doc")) {
            mappings = (Map<String, Object>) XContentMapValues.extractValue(mappings, "doc");
        }
        mappings = (Map<String, Object>) XContentMapValues.extractValue(mappings, "properties", "analysis");
        return mappings;
    }
}
