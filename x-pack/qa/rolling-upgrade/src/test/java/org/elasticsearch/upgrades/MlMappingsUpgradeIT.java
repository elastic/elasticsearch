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
import org.elasticsearch.client.ml.job.config.AnalysisConfig;
import org.elasticsearch.client.ml.job.config.DataDescription;
import org.elasticsearch.client.ml.job.config.Detector;
import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.test.rest.IndexMappingTemplateAsserter;
import org.elasticsearch.xpack.test.rest.XPackRestTestConstants;
import org.elasticsearch.xpack.test.rest.XPackRestTestHelper;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractValue;

public class MlMappingsUpgradeIT extends AbstractUpgradeTestCase {

    private static final String JOB_ID = "ml-mappings-upgrade-job";

    @Override
    protected Collection<String> templatesToWaitFor() {
        List<String> templatesToWaitFor = UPGRADE_FROM_VERSION.onOrAfter(Version.V_7_12_0)
            ? XPackRestTestConstants.ML_POST_V7120_TEMPLATES
            : XPackRestTestConstants.ML_POST_V660_TEMPLATES;
        return Stream.concat(templatesToWaitFor.stream(),
            super.templatesToWaitFor().stream()).collect(Collectors.toSet());
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
                IndexMappingTemplateAsserter.assertMlMappingsMatchTemplates(client());
                break;
            default:
                throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }
    }

    private void createAndOpenTestJob() throws IOException {

        Detector.Builder d = new Detector.Builder("metric", "responsetime");
        d.setByFieldName("airline");
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(d.build()));
        analysisConfig.setBucketSpan(TimeValue.timeValueMinutes(10));
        Job.Builder job = new Job.Builder(JOB_ID);
        job.setAnalysisConfig(analysisConfig);
        job.setDataDescription(new DataDescription.Builder());
        // Use a custom index because other rolling upgrade tests meddle with the shared index
        job.setResultsIndexName("mappings-upgrade-test");

        Request putJob = new Request("PUT", "_ml/anomaly_detectors/" + JOB_ID);
        putJob.setJsonEntity(Strings.toString(job.build()));
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
            assertEquals("Incorrect type for peak_model_bytes in " + responseLevel, "long",
                extractValue("mappings.properties.model_size_stats.properties.peak_model_bytes.type", indexLevel));
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
            assertEquals("Incorrect type for event in " + responseLevel, "keyword",
                extractValue("mappings.properties.event.type", indexLevel));
        });
    }

    @SuppressWarnings("unchecked")
    private void assertUpgradedConfigMappings() throws Exception {

        assertBusy(() -> {
            Request getMappings = new Request("GET", ".ml-config/_mappings");
            getMappings.setOptions(expectWarnings("this request accesses system indices: [.ml-config], but in a future major " +
                "version, direct access to system indices will be prevented by default"));
            Response response = client().performRequest(getMappings);

            Map<String, Object> responseLevel = entityAsMap(response);
            assertNotNull(responseLevel);
            Map<String, Object> indexLevel = (Map<String, Object>) responseLevel.get(".ml-config");
            assertNotNull(indexLevel);

            assertEquals(Version.CURRENT.toString(), extractValue("mappings._meta.version", indexLevel));

            // TODO: as the years go by, the field we assert on here should be changed
            // to the most recent field we've added that is NOT of type "keyword"
            assertEquals("Incorrect type for annotations_enabled in " + responseLevel, "boolean",
                extractValue("mappings.properties.model_plot_config.properties.annotations_enabled.type", indexLevel));
        });
    }
}
