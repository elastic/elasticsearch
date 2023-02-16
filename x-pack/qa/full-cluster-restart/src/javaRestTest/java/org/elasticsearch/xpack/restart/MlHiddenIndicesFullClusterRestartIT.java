/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.restart;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.upgrades.FullClusterRestartUpgradeStatus;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.spi.XContentProvider;
import org.elasticsearch.xpack.test.rest.XPackRestTestConstants;
import org.elasticsearch.xpack.test.rest.XPackRestTestHelper;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class MlHiddenIndicesFullClusterRestartIT extends AbstractXpackFullClusterRestartTestCase {

    private static final String JOB_ID = "ml-hidden-indices-old-cluster-job";
    private static final List<Tuple<List<String>, String>> EXPECTED_INDEX_ALIAS_PAIRS = List.of(
        Tuple.tuple(List.of(".ml-annotations-000001"), ".ml-annotations-read"),
        Tuple.tuple(List.of(".ml-annotations-000001"), ".ml-annotations-write"),
        Tuple.tuple(List.of(".ml-state", ".ml-state-000001"), ".ml-state-write"),
        Tuple.tuple(List.of(".ml-anomalies-shared"), ".ml-anomalies-" + JOB_ID),
        Tuple.tuple(List.of(".ml-anomalies-shared"), ".ml-anomalies-.write-" + JOB_ID)
    );

    public MlHiddenIndicesFullClusterRestartIT(@Name("cluster") FullClusterRestartUpgradeStatus upgradeStatus) {
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
            XPackRestTestHelper.waitForTemplates(
                client(),
                XPackRestTestConstants.ML_POST_V7120_TEMPLATES,
                getOldClusterVersion().onOrAfter(Version.V_7_8_0)
            );
        }
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/93521")
    public void testMlIndicesBecomeHidden() throws Exception {
        if (isRunningAgainstOldCluster()) {
            // trigger ML indices creation
            createAnomalyDetectorJob(JOB_ID);
            openAnomalyDetectorJob(JOB_ID);

            if (getOldClusterVersion().before(Version.V_7_7_0)) {
                Map<String, Object> indexSettingsMap = contentAsMap(getMlIndicesSettings());
                Map<String, Object> aliasesMap = contentAsMap(getMlAliases());

                assertThat("Index settings map was: " + indexSettingsMap, indexSettingsMap, is(aMapWithSize(greaterThanOrEqualTo(4))));
                for (Map.Entry<String, Object> e : indexSettingsMap.entrySet()) {
                    String indexName = e.getKey();
                    @SuppressWarnings("unchecked")
                    Map<String, Object> settings = (Map<String, Object>) e.getValue();
                    assertThat(settings, is(notNullValue()));
                    assertThat(
                        "Index " + indexName + " expected not to be hidden but was, settings = " + settings,
                        XContentMapValues.extractValue(settings, "settings", "index", "hidden"),
                        is(nullValue())
                    );
                }

                for (Tuple<List<String>, String> indexAndAlias : EXPECTED_INDEX_ALIAS_PAIRS) {
                    List<String> indices = indexAndAlias.v1();
                    String alias = indexAndAlias.v2();
                    for (String index : indices) {
                        assertThat(
                            indexAndAlias + " expected not be hidden but was, aliasesMap = " + aliasesMap,
                            XContentMapValues.extractValue(aliasesMap, index, "aliases", alias, "is_hidden"),
                            is(nullValue())
                        );
                    }
                }
            }
        } else {
            // The 5 operations in MlInitializationService.makeMlInternalIndicesHidden() run sequentially, so might
            // not all be finished when this test runs. The desired state should exist within a few seconds of startup,
            // hence the assertBusy().
            assertBusy(() -> {
                Map<String, Object> indexSettingsMap = contentAsMap(getMlIndicesSettings());
                Map<String, Object> aliasesMap = contentAsMap(getMlAliases());

                assertThat("Index settings map was: " + indexSettingsMap, indexSettingsMap, is(aMapWithSize(greaterThanOrEqualTo(4))));
                for (Map.Entry<String, Object> e : indexSettingsMap.entrySet()) {
                    String indexName = e.getKey();
                    @SuppressWarnings("unchecked")
                    Map<String, Object> settings = (Map<String, Object>) e.getValue();
                    assertThat(settings, is(notNullValue()));
                    assertThat(
                        "Index " + indexName + " expected to be hidden but wasn't, settings = " + settings,
                        XContentMapValues.extractValue(settings, "settings", "index", "hidden"),
                        is(equalTo("true"))
                    );
                }

                for (Tuple<List<String>, String> indexAndAlias : EXPECTED_INDEX_ALIAS_PAIRS) {
                    List<String> indices = indexAndAlias.v1();
                    String alias = indexAndAlias.v2();
                    assertThat(
                        indexAndAlias + " expected to be hidden but wasn't, aliasesMap = " + aliasesMap,
                        indices.stream()
                            .anyMatch(
                                index -> Boolean.TRUE.equals(
                                    XContentMapValues.extractValue(aliasesMap, index, "aliases", alias, "is_hidden")
                                )
                            ),
                        is(true)
                    );
                }
            });
        }
    }

    private Response getMlIndicesSettings() throws IOException {
        Request getSettingsRequest = new Request(
            "GET",
            ".ml-anomalies-*,.ml-state*,.ml-stats-*,.ml-notifications*,.ml-annotations*/_settings"
        );
        getSettingsRequest.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE).build());
        Response getSettingsResponse = client().performRequest(getSettingsRequest);
        assertThat(getSettingsResponse, is(notNullValue()));
        return getSettingsResponse;
    }

    private Response getMlAliases() throws IOException {
        Request getAliasesRequest = new Request("GET", ".ml-anomalies-*,.ml-state*,.ml-stats-*,.ml-notifications*,.ml-annotations*/_alias");
        getAliasesRequest.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE).build());
        Response getAliasesResponse = client().performRequest(getAliasesRequest);
        assertThat(getAliasesResponse, is(notNullValue()));
        return getAliasesResponse;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> contentAsMap(Response response) throws IOException {
        InputStreamReader reader = new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8);
        XContentParser parser = XContentProvider.provider()
            .getJsonXContent()
            .XContent()
            .createParser(XContentParserConfiguration.EMPTY, reader);
        return parser.map();
    }

    private void createAnomalyDetectorJob(String jobId) throws IOException {
        String jobConfig = Strings.format("""
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
            }""", jobId);

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
