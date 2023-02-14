/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.upgrades;

import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.xpack.test.rest.IndexMappingTemplateAsserter;
import org.elasticsearch.xpack.test.rest.XPackRestTestConstants;
import org.elasticsearch.xpack.test.rest.XPackRestTestHelper;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractValue;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

public class MlMappingsUpgradeIT extends AbstractUpgradeTestCase {

    private static final String JOB_ID = "ml-mappings-upgrade-job";

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

    // Doing this should force the config index mappings to be upgraded,
    // when the finished time is cleared on reopening the job
    private void closeAndReopenTestJob() throws IOException {

        Request closeJob = new Request("POST", "_ml/anomaly_detectors/" + JOB_ID + "/_close");
        Response response = client().performRequest(closeJob);
        assertEquals(200, response.getStatusLine().getStatusCode());

        Request openJob = new Request("POST", "_ml/anomaly_detectors/" + JOB_ID + "/_open");
        response = client().performRequest(openJob);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    @SuppressWarnings("unchecked")
    private void assertUpgradedResultsMappings() throws Exception {

        assertBusy(() -> {
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

            assertEquals(Version.CURRENT.toString(), extractValue("mappings._meta.version", indexLevel));

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

            assertEquals(Version.CURRENT.toString(), extractValue("mappings._meta.version", indexLevel));

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

            assertEquals(Version.CURRENT.toString(), extractValue("mappings._meta.version", indexLevel));

            // TODO: as the years go by, the field we assert on here should be changed
            // to the most recent field we've added that is NOT of type "keyword"
            assertEquals(
                "Incorrect type for annotations_enabled in " + responseLevel,
                "boolean",
                extractValue("mappings.properties.model_plot_config.properties.annotations_enabled.type", indexLevel)
            );
        });
    }
}
