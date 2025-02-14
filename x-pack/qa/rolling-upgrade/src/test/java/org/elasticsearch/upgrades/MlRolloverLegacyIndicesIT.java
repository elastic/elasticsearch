/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.upgrades;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.test.rest.XPackRestTestConstants;
import org.junit.BeforeClass;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.upgrades.MlJobSnapshotUpgradeIT.generateData;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class MlRolloverLegacyIndicesIT extends AbstractUpgradeTestCase {

    private static final String JOB_ID = "ml-rollover-legacy-job";
    private static final String CUSTOM_INDEX_JOB_ID = "ml-rollover-legacy-custom-job";
    private static final String CUSTOM_RESULTS_INDEX_NAME = "dedicated-results-index";
    private static final String UPGRADED_CLUSTER_JOB_ID = "ml-rollover-upgraded-job";
    private static final String UPGRADED_CUSTOM_INDEX_CLUSTER_JOB_ID = "ml-rollover-upgraded-custom-job";
    private static final int NUM_BUCKETS = 10;

    @BeforeClass
    public static void maybeSkip() {
        assumeFalse("Skip ML tests on unsupported glibc versions", SKIP_ML_TESTS);
    }

    @Override
    protected Collection<String> templatesToWaitFor() {
        // We shouldn't wait for ML templates during the upgrade - production won't
        if (CLUSTER_TYPE != ClusterType.OLD) {
            return super.templatesToWaitFor();
        }
        return Stream.concat(XPackRestTestConstants.ML_POST_V7120_TEMPLATES.stream(), super.templatesToWaitFor().stream())
            .collect(Collectors.toSet());
    }

    /**
     * Test rolling over v7 legacy indices and that the results index aliases are
     * updated to point to the new indices. The test covers both the shared and
     * custom results indices.
     */
    public void testRolloverLegacyIndices() throws Exception {

        switch (CLUSTER_TYPE) {
            case OLD:
                createAndRunJob(JOB_ID, false);
                createAndRunJob(CUSTOM_INDEX_JOB_ID, true);
                break;
            case MIXED:
                break;
            case UPGRADED:
                assertLegacyIndicesRollover();
                assertAnomalyIndicesRollover();
                assertNotificationsIndexAliasCreated();
                createAndRunJob(UPGRADED_CLUSTER_JOB_ID, false);
                closeJob(UPGRADED_CLUSTER_JOB_ID);
                createAndRunJob(UPGRADED_CUSTOM_INDEX_CLUSTER_JOB_ID, true);
                closeJob(UPGRADED_CUSTOM_INDEX_CLUSTER_JOB_ID);
                assertResultsInNewIndex(false);
                assertResultsInNewIndex(true);
                break;
            default:
                throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }
    }

    private void createAndRunJob(String jobId, boolean useCustomIndex) throws IOException {
        var resultsIndex = useCustomIndex ? "\"results_index_name\": \"" + CUSTOM_RESULTS_INDEX_NAME + "\"," : "";

        String jobConfig = Strings.format("""
                        {
                            %s
                            "analysis_config" : {
                                "bucket_span": "600s",
                                "detectors" :[{"function":"metric","field_name":"value","partition_field_name":"series"}]
                            },
                            "data_description" : {
                            }
                        }"
            """, resultsIndex);

        Request putJob = new Request("PUT", "_ml/anomaly_detectors/" + jobId);
        putJob.setJsonEntity(jobConfig);
        Response response = client().performRequest(putJob);
        assertEquals(200, response.getStatusLine().getStatusCode());

        Request openJob = new Request("POST", "_ml/anomaly_detectors/" + jobId + "/_open");
        response = client().performRequest(openJob);
        assertEquals(200, response.getStatusLine().getStatusCode());

        TimeValue bucketSpan = TimeValue.timeValueMinutes(10);
        long startTime = Instant.now().minus(24L, ChronoUnit.HOURS).toEpochMilli();

        var dataCounts = entityAsMap(
            postData(
                jobId,
                String.join(
                    "",
                    generateData(
                        startTime,
                        bucketSpan,
                        NUM_BUCKETS,
                        Collections.singletonList("foo"),
                        (bucketIndex, series) -> bucketIndex == 5 ? 100.0 : 10.0
                    )
                )
            )
        );
        assertThat(dataCounts.toString(), (Integer) dataCounts.get("bucket_count"), greaterThan(0));
        flushJob(jobId);
    }

    protected Response postData(String jobId, String data) throws IOException {
        // Post data is deprecated, so a deprecation warning is possible (depending on the old version)
        RequestOptions postDataOptions = RequestOptions.DEFAULT.toBuilder().setWarningsHandler(warnings -> {
            if (warnings.isEmpty()) {
                // No warning is OK - it means we hit an old node where post data is not deprecated
                return false;
            } else if (warnings.size() > 1) {
                return true;
            }
            return warnings.get(0)
                .equals(
                    "Posting data directly to anomaly detection jobs is deprecated, "
                        + "in a future major version it will be compulsory to use a datafeed"
                ) == false;
        }).build();

        Request postDataRequest = new Request("POST", "/_ml/anomaly_detectors/" + jobId + "/_data");
        // Post data is deprecated, so expect a deprecation warning
        postDataRequest.setOptions(postDataOptions);
        postDataRequest.setJsonEntity(data);
        return client().performRequest(postDataRequest);
    }

    protected void flushJob(String jobId) throws IOException {
        client().performRequest(new Request("POST", "/_ml/anomaly_detectors/" + jobId + "/_flush"));
    }

    private void closeJob(String jobId) throws IOException {
        Response closeResponse = client().performRequest(new Request("POST", "/_ml/anomaly_detectors/" + jobId + "/_close"));
        assertThat(entityAsMap(closeResponse), hasEntry("closed", true));
    }

    @SuppressWarnings("unchecked")
    private void assertLegacyIndicesRollover() throws Exception {
        if (isOriginalClusterVersionAtLeast(Version.V_8_0_0)) {
            // not a legacy index
            return;
        }

        assertBusy(() -> {
            RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
            builder.setWarningsHandler(WarningsHandler.PERMISSIVE); // ignore warnings about accessing system index
            Request getIndices = new Request("GET", ".ml*");
            getIndices.setOptions(builder);
            Response getIndicesResponse = client().performRequest(getIndices);
            assertOK(getIndicesResponse);
            var asString = EntityUtils.toString(getIndicesResponse.getEntity());
            // legacy -000001 index is rolled over creating -000002
            assertThat(asString, containsString(".ml-state-000002"));

            Request getAliases = new Request("GET", "_alias/.ml*");
            getAliases.setOptions(builder);
            Response getAliasesResponse = client().performRequest(getAliases);

            // Check the write alias points to the new index
            Map<String, Object> aliasesMap = entityAsMap(getAliasesResponse);
            var stateAlias = (Map<String, Object>) aliasesMap.get(".ml-state-000002");
            assertNotNull(stateAlias);
            var isHidden = XContentMapValues.extractValue(stateAlias, "aliases", ".ml-state-write", "is_hidden");
            assertEquals(Boolean.TRUE, isHidden);
        });
    }

    @SuppressWarnings("unchecked")
    private void assertAnomalyIndicesRollover() throws Exception {
        if (isOriginalClusterVersionAtLeast(Version.V_8_0_0)) {
            // not a legacy index
            return;
        }

        assertBusy(() -> {
            RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
            builder.setWarningsHandler(WarningsHandler.PERMISSIVE); // ignore warnings about accessing system index
            Request getIndices = new Request("GET", ".ml-anomalies*");
            getIndices.setOptions(builder);
            Response getIndicesResponse = client().performRequest(getIndices);
            assertOK(getIndicesResponse);
            var asString = EntityUtils.toString(getIndicesResponse.getEntity());
            assertThat(asString, containsString(".ml-anomalies-custom-" + CUSTOM_RESULTS_INDEX_NAME));
            assertThat(asString, containsString(".ml-anomalies-custom-" + CUSTOM_RESULTS_INDEX_NAME + "-000001"));
            assertThat(asString, containsString(".ml-anomalies-shared"));
            assertThat(asString, containsString(".ml-anomalies-shared-000001"));

            Request getAliases = new Request("GET", "_alias/.ml*");
            getAliases.setOptions(builder);
            Response getAliasesResponse = client().performRequest(getAliases);

            // Check the write alias points to the new index
            Map<String, Object> aliasesResponseMap = entityAsMap(getAliasesResponse);

            String expectedReadAlias = ".ml-anomalies-" + CUSTOM_INDEX_JOB_ID;
            String expectedWriteAlias = ".ml-anomalies-.write-" + CUSTOM_INDEX_JOB_ID;

            {
                var rolledCustomResultsIndex = (Map<String, Object>) aliasesResponseMap.get(
                    ".ml-anomalies-custom-" + CUSTOM_RESULTS_INDEX_NAME + "-000001"
                );
                assertNotNull(aliasesResponseMap.toString(), rolledCustomResultsIndex);

                var aliases = (Map<String, Object>) rolledCustomResultsIndex.get("aliases");
                assertThat(aliasesResponseMap.toString(), aliases.entrySet(), hasSize(2));
                assertThat(aliasesResponseMap.toString(), aliases.keySet(), containsInAnyOrder(expectedReadAlias, expectedWriteAlias));

                // Read alias
                var isHidden = XContentMapValues.extractValue(rolledCustomResultsIndex, "aliases", expectedReadAlias, "is_hidden");
                assertEquals(Boolean.TRUE, isHidden);
                var isWrite = XContentMapValues.extractValue(rolledCustomResultsIndex, "aliases", expectedReadAlias, "is_write_index");
                assertNull(isWrite); // not a write index
                var filter = XContentMapValues.extractValue(rolledCustomResultsIndex, "aliases", expectedReadAlias, "filter");
                assertNotNull(filter);

                // Write alias
                isHidden = XContentMapValues.extractValue(rolledCustomResultsIndex, "aliases", expectedWriteAlias, "is_hidden");
                assertEquals(Boolean.TRUE, isHidden);
                isWrite = XContentMapValues.extractValue(rolledCustomResultsIndex, "aliases", expectedWriteAlias, "is_write_index");
                assertEquals(Boolean.TRUE, isWrite);
                filter = XContentMapValues.extractValue(rolledCustomResultsIndex, "aliases", expectedReadAlias, "filter");
                assertNotNull(filter);
            }

            {
                var olcustomResultsIndex = (Map<String, Object>) aliasesResponseMap.get(
                    ".ml-anomalies-custom-" + CUSTOM_RESULTS_INDEX_NAME
                );
                assertNotNull(aliasesResponseMap.toString(), olcustomResultsIndex);
                var aliases = (Map<String, Object>) olcustomResultsIndex.get("aliases");
                assertThat(aliasesResponseMap.toString(), aliases.entrySet(), hasSize(1));
                assertThat(aliasesResponseMap.toString(), aliases.keySet(), containsInAnyOrder(expectedReadAlias));

                // Read alias
                var isHidden = XContentMapValues.extractValue(olcustomResultsIndex, "aliases", expectedReadAlias, "is_hidden");
                assertEquals(Boolean.TRUE, isHidden);
                var isWrite = XContentMapValues.extractValue(olcustomResultsIndex, "aliases", expectedReadAlias, "is_write_index");
                assertNull(isWrite); // not a write index
                var filter = XContentMapValues.extractValue(olcustomResultsIndex, "aliases", expectedReadAlias, "filter");
                assertNotNull(filter);
            }
        });
    }

    @SuppressWarnings("unchecked")
    public void assertResultsInNewIndex(boolean checkCustomIndex) throws Exception {
        if (isOriginalClusterVersionAtLeast(Version.V_8_0_0)) {
            // not a legacy index
            return;
        }

        var searchUrl = checkCustomIndex
            ? ".ml-anomalies-custom-" + CUSTOM_RESULTS_INDEX_NAME + "-000001/_search"
            : ".ml-anomalies-shared-000001/_search";

        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        builder.setWarningsHandler(WarningsHandler.PERMISSIVE); // ignore warnings about accessing hidden index
        Request getIndices = new Request("GET", searchUrl);
        getIndices.setOptions(builder);
        Response searchResponse = client().performRequest(getIndices);
        assertOK(searchResponse);

        final Map<String, Object> responseMap = responseAsMap(searchResponse);
        Map<String, Object> hits = ((Map<String, Object>) responseMap.get("hits"));
        assertEquals(responseMap.toString(), NUM_BUCKETS, ((List<Object>) hits.get("hits")).size());
    }

    @SuppressWarnings("unchecked")
    private void assertNotificationsIndexAliasCreated() throws Exception {
        assertBusy(() -> {
            Request getMappings = new Request("GET", "_alias/.ml-notifications-write");
            Response response = client().performRequest(getMappings);
            Map<String, Object> responseMap = entityAsMap(response);
            assertThat(responseMap.entrySet(), hasSize(1));
            var aliases = (Map<String, Object>) responseMap.get(".ml-notifications-000002");
            assertThat(aliases.entrySet(), hasSize(1));
            var allAliases = (Map<String, Object>) aliases.get("aliases");
            var writeAlias = (Map<String, Object>) allAliases.get(".ml-notifications-write");

            assertThat(writeAlias, hasEntry("is_hidden", Boolean.TRUE));
            var isWriteIndex = (Boolean) writeAlias.get("is_write_index");
            assertThat(isWriteIndex, anyOf(is(Boolean.TRUE), nullValue()));
        });
    }
}
