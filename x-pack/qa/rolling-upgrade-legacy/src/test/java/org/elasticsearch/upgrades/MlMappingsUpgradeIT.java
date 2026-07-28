/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.upgrades;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.xpack.core.ml.MlConfigIndex;
import org.elasticsearch.xpack.core.ml.MlStatsIndex;
import org.elasticsearch.xpack.core.ml.annotations.AnnotationIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.notifications.NotificationsIndex;
import org.elasticsearch.xpack.test.rest.IndexMappingTemplateAsserter;
import org.elasticsearch.xpack.test.rest.XPackRestTestConstants;
import org.elasticsearch.xpack.test.rest.XPackRestTestHelper;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractValue;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class MlMappingsUpgradeIT extends AbstractUpgradeTestCase {

    private static final String JOB_ID = "ml-mappings-upgrade-job";

    /**
     * Mirrors {@code MlIndexTemplateRegistry#ML_INDEX_TEMPLATE_VERSION}; kept here because rolling-upgrade
     * tests cannot depend on the ML plugin module.
     */
    private static final int ML_INDEX_TEMPLATE_VERSION = 10000003 + AnomalyDetectorsIndex.RESULTS_INDEX_MAPPINGS_VERSION
        + NotificationsIndex.NOTIFICATIONS_INDEX_MAPPINGS_VERSION + MlStatsIndex.STATS_INDEX_MAPPINGS_VERSION
        + NotificationsIndex.NOTIFICATIONS_INDEX_TEMPLATE_VERSION;

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
     * The purpose of this test is to ensure that when a job is open through a rolling upgrade we upgrade the results
     * index mappings when it is assigned to an upgraded node even if no other ML endpoint is called after the upgrade
     */
    public void testMappingsUpgrade() throws Exception {

        switch (CLUSTER_TYPE) {
            case OLD:
                createAndOpenTestJob();
                break;
            case MIXED:
                // We don't know whether the job is on an old or upgraded node, so cannot assert that the mappings have been upgraded
                break;
            case UPGRADED:
                assertUpgradedResultsMappings();
                assertUpgradedAnnotationsMappings();
                closeAndReopenTestJob();
                assertUpgradedConfigMappings();
                assertMlLegacyTemplatesDeleted();
                IndexMappingTemplateAsserter.assertMlMappingsMatchTemplates(client());
                assertNotificationsIndexAliasCreated();
                assertBusy(
                    () -> IndexMappingTemplateAsserter.assertTemplateVersionAndPattern(
                        client(),
                        ".ml-anomalies-",
                        ML_INDEX_TEMPLATE_VERSION,
                        List.of(".ml-anomalies-*", ".reindexed-v7-ml-anomalies-*", ".reindexed-v8-ml-anomalies-*")
                    )
                );
                assertBusy(
                    () -> IndexMappingTemplateAsserter.assertTemplateVersionAndPattern(
                        client(),
                        ".ml-state",
                        ML_INDEX_TEMPLATE_VERSION,
                        Arrays.asList(AnomalyDetectorsIndex.jobStateIndexPatterns())
                    )
                );
                break;
            default:
                throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }
    }

    private void createAndOpenTestJob() throws IOException {
        // Use a custom index because other rolling upgrade tests meddle with the shared index
        String jobConfig = """
                        {
                            "results_index_name":"mappings-upgrade-test",
                            "analysis_config" : {
                                "bucket_span": "600s",
                                "detectors" :[{"function":"metric","field_name":"responsetime","by_field_name":"airline"}]
                            },
                            "data_description" : {
                            }
                        }"
            """;

        Request putJob = new Request("PUT", "_ml/anomaly_detectors/" + JOB_ID);
        putJob.setJsonEntity(jobConfig);
        Response response = client().performRequest(putJob);
        assertEquals(200, response.getStatusLine().getStatusCode());

        Request openJob = new Request("POST", "_ml/anomaly_detectors/" + JOB_ID + "/_open");
        response = client().performRequest(openJob);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    /**
     * Opens the test job if it was closed during rolling upgrade so results write aliases exist.
     * The job must already exist from the {@code OLD} cluster phase.
     */
    private void ensureTestJobIsOpen() throws IOException {
        Request getJob = new Request("GET", "_ml/anomaly_detectors/" + JOB_ID);
        Response response = client().performRequest(getJob);
        assertEquals(200, response.getStatusLine().getStatusCode());
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> jobs = (List<Map<String, Object>>) entityAsMap(response).get("jobs");
        assertThat(jobs, hasSize(1));
        if ("closed".equals(jobs.get(0).get("state"))) {
            openTestJob();
        }
    }

    // Doing this should force the config index mappings to be upgraded,
    // when the finished time is cleared on reopening the job
    private void openTestJob() throws IOException {
        Request openJob = new Request("POST", "_ml/anomaly_detectors/" + JOB_ID + "/_open");
        Response response = client().performRequest(openJob);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    private void closeAndReopenTestJob() throws IOException {

        Request closeJob = new Request("POST", "_ml/anomaly_detectors/" + JOB_ID + "/_close");
        Response response = client().performRequest(closeJob);
        assertEquals(200, response.getStatusLine().getStatusCode());

        openTestJob();
    }

    @SuppressWarnings("unchecked")
    private void assertUpgradedResultsMappings() throws Exception {

        assertBusy(() -> {
            ensureTestJobIsOpen();
            Request getMappings = new Request("GET", XPackRestTestHelper.resultsWriteAlias(JOB_ID) + "/_mappings");
            Response response = client().performRequest(getMappings);

            Map<String, Object> responseLevel = entityAsMap(response);
            assertNotNull(responseLevel);
            Map<String, Object> indexLevel = null;
            // The name of the concrete index underlying the results index alias may or may not have been changed
            // by the upgrade process (depending on what other tests are being run and the order they're run in),
            // so navigating to the next level of the tree must account for both cases
            for (Map.Entry<String, Object> entry : responseLevel.entrySet()) {
                if (entry.getKey().startsWith(".ml-anomalies-") && entry.getKey().contains("mappings-upgrade-test")) {
                    indexLevel = (Map<String, Object>) entry.getValue();
                    break;
                }
            }
            assertNotNull(indexLevel);

            assertEquals(
                AnomalyDetectorsIndex.RESULTS_INDEX_MAPPINGS_VERSION,
                extractValue("mappings._meta.managed_index_mappings_version", indexLevel)
            );

            // TODO: as the years go by, the field we assert on here should be changed
            // to the most recent field we've added that is NOT of type "keyword"
            assertEquals(
                "Incorrect type for peak_model_bytes in " + responseLevel,
                "long",
                extractValue("mappings.properties.model_size_stats.properties.peak_model_bytes.type", indexLevel)
            );
        });
    }

    @SuppressWarnings("unchecked")
    private void assertUpgradedAnnotationsMappings() throws Exception {

        assertBusy(() -> {
            Request getMappings = new Request("GET", ".ml-annotations-write/_mappings");
            Response response = client().performRequest(getMappings);

            Map<String, Object> responseLevel = entityAsMap(response);
            assertNotNull(responseLevel);
            Map<String, Object> indexLevel = null;
            // The name of the concrete index underlying the annotations index write alias may or may not have been
            // changed by the upgrade process (depending on what other tests are being run and the order they're run
            // in), so navigating to the next level of the tree must account for both cases
            for (Map.Entry<String, Object> entry : responseLevel.entrySet()) {
                if (entry.getKey().startsWith(".ml-annotations-")) {
                    indexLevel = (Map<String, Object>) entry.getValue();
                    break;
                }
            }
            assertNotNull(indexLevel);

            assertEquals(
                AnnotationIndex.ANNOTATION_INDEX_MAPPINGS_VERSION,
                extractValue("mappings._meta.managed_index_mappings_version", indexLevel)
            );

            // TODO: as the years go by, the field we assert on here should be changed
            // to the most recent field we've added that would be incorrectly mapped by dynamic
            // mappings, for example a field we want to be "keyword" incorrectly mapped as "text"
            assertEquals(
                "Incorrect type for event in " + responseLevel,
                "keyword",
                extractValue("mappings.properties.event.type", indexLevel)
            );
        });
    }

    private void assertMlLegacyTemplatesDeleted() throws Exception {

        // All the legacy ML templates we created over the years should be deleted now they're no longer needed
        assertBusy(() -> {
            Request request = new Request("GET", "/_template/.ml*");
            try {
                Response response = client().performRequest(request);
                Map<String, Object> responseLevel = entityAsMap(response);
                assertNotNull(responseLevel);
                // If we get here the test has failed, but it's critical that we find out which templates
                // existed, hence not using expectThrows() above
                assertThat(responseLevel.keySet(), empty());
            } catch (ResponseException e) {
                // Not found is fine
                assertThat(
                    "Unexpected failure getting ML templates: " + e.getResponse().getStatusLine(),
                    e.getResponse().getStatusLine().getStatusCode(),
                    is(404)
                );
            }
        });
    }

    @SuppressWarnings("unchecked")
    private void assertUpgradedConfigMappings() throws Exception {

        assertBusy(() -> {
            Request getMappings = new Request("GET", ".ml-config/_mappings");
            getMappings.setOptions(
                expectWarnings(
                    "this request accesses system indices: [.ml-config], but in a future major "
                        + "version, direct access to system indices will be prevented by default"
                )
            );
            Response response = client().performRequest(getMappings);

            Map<String, Object> responseLevel = entityAsMap(response);
            assertNotNull(responseLevel);
            Map<String, Object> indexLevel = (Map<String, Object>) responseLevel.get(".ml-config");
            assertNotNull(indexLevel);

            assertEquals(
                MlConfigIndex.CONFIG_INDEX_MAPPINGS_VERSION,
                extractValue("mappings._meta.managed_index_mappings_version", indexLevel)
            );

            // TODO: as the years go by, the field we assert on here should be changed
            // to the most recent field we've added that is NOT of type "keyword"
            assertEquals(
                "Incorrect type for annotations_enabled in " + responseLevel,
                "boolean",
                extractValue("mappings.properties.model_plot_config.properties.annotations_enabled.type", indexLevel)
            );
        });
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
