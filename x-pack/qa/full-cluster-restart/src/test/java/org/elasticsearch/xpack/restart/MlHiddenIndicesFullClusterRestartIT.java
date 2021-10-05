/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.restart;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.upgrades.AbstractFullClusterRestartTestCase;
import org.elasticsearch.xpack.core.ml.annotations.AnnotationIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndexFields;
import org.elasticsearch.xpack.test.rest.XPackRestTestConstants;
import org.elasticsearch.xpack.test.rest.XPackRestTestHelper;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class MlHiddenIndicesFullClusterRestartIT extends AbstractFullClusterRestartTestCase {

    private static final String OLD_CLUSTER_JOB_ID = "ml-hidden-indices-old-cluster-job";

    @Override
    protected Settings restClientSettings() {
        String token = "Basic " + Base64.getEncoder().encodeToString("test_user:x-pack-test-password".getBytes(StandardCharsets.UTF_8));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .build();
    }

    @Before
    public void waitForMlTemplates() throws Exception {
        List<String> templatesToWaitFor = (isRunningAgainstOldCluster() && getOldClusterVersion().before(Version.V_7_12_0))
            ? XPackRestTestConstants.ML_POST_V660_TEMPLATES
            : XPackRestTestConstants.ML_POST_V7120_TEMPLATES;
        boolean clusterUnderstandsComposableTemplates =
            isRunningAgainstOldCluster() == false || getOldClusterVersion().onOrAfter(Version.V_7_8_0);
        XPackRestTestHelper.waitForTemplates(client(), templatesToWaitFor, clusterUnderstandsComposableTemplates);
    }

    public void testMlIndicesBecomeHidden() throws Exception {
        if (isRunningAgainstOldCluster()) {
            // trigger ML indices creation
            createAnomalyDetectorJob(OLD_CLUSTER_JOB_ID);
            openAnomalyDetectorJob(OLD_CLUSTER_JOB_ID);

            if (getOldClusterVersion().before(Version.V_7_7_0)) {
                Map<String, Object> indexSettingsMap = contentAsMap(getMlIndicesSettings());
                Map<String, Object> aliasesMap = contentAsMap(getMlAliases());

                for (Map.Entry<String, Object> e : indexSettingsMap.entrySet()) {
                    String indexName = e.getKey();
                    @SuppressWarnings("unchecked")
                    Map<String, Object> settings = (Map<String, Object>) e.getValue();
                    assertThat(settings, is(notNullValue()));
                    assertThat("Index " + indexName + " expected not to be hidden but was",
                        XContentMapValues.extractValue(settings, "settings", "index", "hidden"),
                        is(nullValue()));
                }

                List<Tuple<String, String>> expected =
                    List.of(
                        Tuple.tuple(AnnotationIndex.INDEX_NAME, AnnotationIndex.READ_ALIAS_NAME),
                        Tuple.tuple(AnnotationIndex.INDEX_NAME, AnnotationIndex.WRITE_ALIAS_NAME),
                        Tuple.tuple(AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX, AnomalyDetectorsIndex.jobStateIndexWriteAlias())
                    );
                for (Tuple<String, String> indexAndAlias : expected) {
                    assertThat(
                        indexAndAlias + " should not be hidden",
                        XContentMapValues.extractValue(aliasesMap, indexAndAlias.v1(), "aliases", indexAndAlias.v2(), "is_hidden"),
                        is(nullValue()));
                }
            }
        } else {
            Map<String, Object> indexSettingsMap = contentAsMap(getMlIndicesSettings());
            Map<String, Object> aliasesMap = contentAsMap(getMlAliases());

            for (Map.Entry<String, Object> e : indexSettingsMap.entrySet()) {
                String indexName = e.getKey();
                // .ml-config is supposed to be a system index, *not* a hidden index
                if (".ml-config".equals(indexName)) {
                    continue;
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> settings = (Map<String, Object>) e.getValue();
                assertThat(settings, is(notNullValue()));
                assertThat("Index " + indexName + " expected to be hidden but wasn't",
                    XContentMapValues.extractValue(settings, "settings", "index", "hidden"),
                    is(equalTo("true")));
            }

            // TODO: Include all the ML aliases
            List<Tuple<String, String>> expected =
                List.of(
                    Tuple.tuple(AnnotationIndex.INDEX_NAME, AnnotationIndex.READ_ALIAS_NAME),
                    Tuple.tuple(AnnotationIndex.INDEX_NAME, AnnotationIndex.WRITE_ALIAS_NAME)
                );
            for (Tuple<String, String> indexAndAlias : expected) {
                assertThat(
                    indexAndAlias + " should be hidden, " + aliasesMap,
                    XContentMapValues.extractValue(aliasesMap, indexAndAlias.v1(), "aliases", indexAndAlias.v2(), "is_hidden"),
                    is(true));
            }
        }
    }

    private Response getMlIndicesSettings() throws IOException {
        Request getSettingsRequest = new Request("GET", ".ml-*/_settings");
        getSettingsRequest
            .setOptions(RequestOptions.DEFAULT.toBuilder()
                .setWarningsHandler(WarningsHandler.PERMISSIVE)
                .build());
        Response getSettingsResponse = client().performRequest(getSettingsRequest);
        assertThat(getSettingsResponse, is(notNullValue()));
        return getSettingsResponse;
    }

    private Response getMlAliases() throws IOException {
        Request getAliasesRequest =
            new Request("GET", ".ml-anomalies-*,.ml-state*,.ml-stats-*,.ml-notifications*,.ml-annotations*/_alias");
        getAliasesRequest
            .setOptions(RequestOptions.DEFAULT.toBuilder()
                .setWarningsHandler(WarningsHandler.PERMISSIVE)
                .build());
        Response getAliasesResponse = client().performRequest(getAliasesRequest);
        assertThat(getAliasesResponse, is(notNullValue()));
        return getAliasesResponse;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> contentAsMap(Response response) throws IOException {
        return new ObjectMapper().readValue(
            new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8), HashMap.class);
    }

    private void createAnomalyDetectorJob(String jobId) throws IOException {
        String jobConfig =
            "{\n" +
            "    \"job_id\": \"" + jobId + "\",\n" +
            "    \"analysis_config\": {\n" +
            "        \"bucket_span\": \"10m\",\n" +
            "        \"detectors\": [{\n" +
            "            \"function\": \"metric\",\n" +
            "            \"field_name\": \"responsetime\"\n" +
            "        }]\n" +
            "    },\n" +
            "    \"data_description\": {}\n" +
            "}";

        Request putJobRequest = new Request("PUT", "/_ml/anomaly_detectors/" + jobId);
        putJobRequest.setJsonEntity(jobConfig);
        Response putJobResponse = client().performRequest(putJobRequest);
        assertThat(putJobResponse.getStatusLine().getStatusCode(), equalTo(200));
    }

    private void openAnomalyDetectorJob(String jobId) throws IOException {
        Request openJobRequest = new Request("POST", "/_ml/anomaly_detectors/" + jobId + "/_open");
        Response openJobResponse = client().performRequest(openJobRequest);
        assertThat(openJobResponse.getStatusLine().getStatusCode(), equalTo(200));
    }
}
