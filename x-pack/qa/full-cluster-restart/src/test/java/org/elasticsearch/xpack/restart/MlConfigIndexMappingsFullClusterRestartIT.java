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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.upgrades.AbstractFullClusterRestartTestCase;
import org.elasticsearch.xpack.core.ml.MlConfigIndex;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
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
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class MlConfigIndexMappingsFullClusterRestartIT extends AbstractFullClusterRestartTestCase {

    private static final String OLD_CLUSTER_JOB_ID = "ml-config-mappings-old-cluster-job";
    private static final String NEW_CLUSTER_JOB_ID = "ml-config-mappings-new-cluster-job";

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
        Map<String, Object> expectedConfigIndexMappings = loadConfigIndexMappings();
        if (isRunningAgainstOldCluster()) {
            assertThatMlConfigIndexDoesNotExist();
            // trigger .ml-config index creation
            createAnomalyDetectorJob(OLD_CLUSTER_JOB_ID);
            if (getOldClusterVersion().onOrAfter(Version.V_7_3_0)) {
                // .ml-config has mappings for analytics as the feature was introduced in 7.3.0
                assertThat(getDataFrameAnalysisMappings().keySet(), hasItem("outlier_detection"));
            } else {
                // .ml-config does not yet have correct mappings, it will need an update after cluster is upgraded
                assertThat(getDataFrameAnalysisMappings(), is(nullValue()));
            }
        } else {
            // trigger .ml-config index mappings update
            createAnomalyDetectorJob(NEW_CLUSTER_JOB_ID);
            // assert that the mappings are updated
            Map<String, Object> configIndexMappings = getConfigIndexMappings();

            // Remove renamed fields
            if (getOldClusterVersion().before(Version.V_8_0_0)) {
                configIndexMappings = XContentMapValues.filter(expectedConfigIndexMappings, null, new String[] {
                    "analysis.properties.*.properties.maximum_number_trees" // This was renamed to max_trees
                });
            }

            assertThat(configIndexMappings, equalTo(expectedConfigIndexMappings));
        }
    }

    private void assertThatMlConfigIndexDoesNotExist() {
        Request getIndexRequest = new Request("GET", ".ml-config");
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(getIndexRequest));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(404));
    }

    private void createAnomalyDetectorJob(String jobId) throws IOException {
        Detector.Builder detector = new Detector.Builder("metric", "responsetime");
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
    private Map<String, Object> getConfigIndexMappings() throws Exception {
        Request getIndexMappingsRequest = new Request("GET", ".ml-config/_mappings");
        Response getIndexMappingsResponse = client().performRequest(getIndexMappingsRequest);
        assertThat(getIndexMappingsResponse.getStatusLine().getStatusCode(), equalTo(200));

        Map<String, Object> mappings = entityAsMap(getIndexMappingsResponse);
        mappings = (Map<String, Object>) XContentMapValues.extractValue(mappings, ".ml-config", "mappings");
        if (mappings.containsKey("doc")) {
            mappings = (Map<String, Object>) XContentMapValues.extractValue(mappings, "doc");
        }
        mappings = (Map<String, Object>) XContentMapValues.extractValue(mappings, "properties");
        return mappings;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getDataFrameAnalysisMappings() throws Exception {
        Map<String, Object> mappings = getConfigIndexMappings();
        mappings = (Map<String, Object>) XContentMapValues.extractValue(mappings, "analysis", "properties");
        return mappings;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> loadConfigIndexMappings() throws IOException {
        String mapping = MlConfigIndex.mapping();
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, new BytesArray(mapping).streamInput())) {
            Map<String, Object> mappings = parser.map();
            mappings = (Map<String, Object>) XContentMapValues.extractValue(mappings, "_doc", "properties");
            return mappings;
        }
    }
}
