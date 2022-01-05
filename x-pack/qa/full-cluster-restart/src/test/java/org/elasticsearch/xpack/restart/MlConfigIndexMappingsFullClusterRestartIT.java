/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.restart;

import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.upgrades.AbstractFullClusterRestartTestCase;
import org.elasticsearch.xpack.test.rest.IndexMappingTemplateAsserter;
import org.elasticsearch.xpack.test.rest.XPackRestTestConstants;
import org.elasticsearch.xpack.test.rest.XPackRestTestHelper;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
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
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Before
    public void waitForMlTemplates() throws Exception {
        // We shouldn't wait for ML templates during the upgrade - production won't
        if (isRunningAgainstOldCluster()) {
            XPackRestTestHelper.waitForTemplates(
                client(),
                XPackRestTestConstants.ML_POST_V7120_TEMPLATES,
                getOldClusterVersion().onOrAfter(Version.V_7_8_0)
            );
        }
    }

    public void testMlConfigIndexMappingsAfterMigration() throws Exception {
        if (isRunningAgainstOldCluster()) {
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
            IndexMappingTemplateAsserter.assertMlMappingsMatchTemplates(client());
        }
    }

    private void createAnomalyDetectorJob(String jobId) throws IOException {
        String jobConfig = """
            {
                "job_id": "%s",
                "analysis_config": {
                    "bucket_span": "10m",
                    "detectors": [{
                        "function": "metric",
                        "field_name": "responsetime"
                    }]
                },
                "data_description": {}
            }""".formatted(jobId);

        Request putJobRequest = new Request("PUT", "/_ml/anomaly_detectors/" + jobId);
        putJobRequest.setJsonEntity(jobConfig);
        Response putJobResponse = client().performRequest(putJobRequest);
        assertThat(putJobResponse.getStatusLine().getStatusCode(), equalTo(200));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getConfigIndexMappings() throws Exception {
        Request getIndexMappingsRequest = new Request("GET", ".ml-config/_mappings");
        getIndexMappingsRequest.setOptions(expectVersionSpecificWarnings(v -> {
            final String systemIndexWarning = "this request accesses system indices: [.ml-config], but in a future major version, direct "
                + "access to system indices will be prevented by default";
            v.current(systemIndexWarning);
            v.compatible(systemIndexWarning);
        }));
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
}
