/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.application;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.upgrades.FullClusterRestartUpgradeStatus;
import org.elasticsearch.upgrades.ParameterizedFullClusterRestartTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.Version.V_8_12_0;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class FullClusterRestartIT extends ParameterizedFullClusterRestartTestCase {
    // DSL was introduced with version 8.12.0 of ES.
    private static final Version DSL_DEFAULT_RETENTION_VERSION = V_8_12_0;

    // DSL was introduced with the version 3 of the registry.
    private static final int DSL_REGISTRY_VERSION = 3;

    // Legacy name we used for ILM policy configuration in versions prior to 8.12.0.
    private static final String EVENT_DATA_STREAM_LEGACY_TEMPLATE_NAME = "behavioral_analytics-events-default";

    // Event data streams template name.
    private static final String EVENT_DATA_STREAM_LEGACY_ILM_POLICY_NAME = "behavioral_analytics-events-default_policy";

    @ClassRule
    public static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .version(org.elasticsearch.test.cluster.util.Version.fromString(OLD_CLUSTER_VERSION))
        .nodes(2)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .module("x-pack-ent-search")
        .build();

    public FullClusterRestartIT(@Name("cluster") FullClusterRestartUpgradeStatus upgradeStatus) {
        super(upgradeStatus);
    }

    @Override
    protected ElasticsearchCluster getUpgradeCluster() {
        return cluster;
    }

    public void testBehavioralAnalyticsDataRetention() throws Exception {
        assumeTrue(
            "Data retention changed by default to DSL in " + DSL_DEFAULT_RETENTION_VERSION,
            getOldClusterTestVersion().before(DSL_DEFAULT_RETENTION_VERSION.toString())
        );

        String legacyAnalyticsCollectionName = "oldstuff";
        String newAnalyticsCollectionName = "newstuff";

        // Wait for the cluster to finish initialization
        waitForClusterReady();

        if (isRunningAgainstOldCluster()) {
            // Create an analytics collection
            Request legacyPutRequest = new Request("PUT", "_application/analytics/" + legacyAnalyticsCollectionName);
            assertOK(client().performRequest(legacyPutRequest));

            // Validate that ILM lifecycle is in place
            assertBusy(() -> assertUsingLegacyDataRetentionPolicy(legacyAnalyticsCollectionName));
        } else {
            // Create a new analytics collection
            Request putRequest = new Request("PUT", "_application/analytics/" + newAnalyticsCollectionName);
            assertOK(client().performRequest(putRequest));

            // Validate that NO ILM lifecycle is in place and we are using DLS instead.
            assertBusy(() -> assertUsingDslDataRetention(newAnalyticsCollectionName));

            // Validate that the existing analytics collection created with an older version is still using ILM
            assertBusy(() -> assertUsingLegacyDataRetentionPolicy(legacyAnalyticsCollectionName));
        }
    }

    private void assertUsingLegacyDataRetentionPolicy(String analyticsCollectionName) throws IOException {
        String dataStreamName = "behavioral_analytics-events-" + analyticsCollectionName;
        Request getDataStreamRequest = new Request("GET", "_data_stream/" + dataStreamName);
        Response response = client().performRequest(getDataStreamRequest);
        assertOK(response);
        ObjectPath dataStream = ObjectPath.createFromResponse(response);
        String pathToIlmPolicy = "data_streams.0.ilm_policy";
        assertNotNull(dataStream.evaluate(pathToIlmPolicy));
        assertEquals(EVENT_DATA_STREAM_LEGACY_ILM_POLICY_NAME, dataStream.evaluate(pathToIlmPolicy));

        Request policyRequest = new Request("GET", "_ilm/policy/" + EVENT_DATA_STREAM_LEGACY_ILM_POLICY_NAME);
        ObjectPath policy = ObjectPath.createFromResponse(client().performRequest(policyRequest));
        assertNotNull(policy.evaluate(EVENT_DATA_STREAM_LEGACY_ILM_POLICY_NAME));
    }

    private void assertUsingDslDataRetention(String analyticsCollectionName) throws IOException {
        String dataStreamName = "behavioral_analytics-events-" + analyticsCollectionName;
        Request getDataStreamRequest = new Request("GET", "_data_stream/" + dataStreamName);
        Response response = client().performRequest(getDataStreamRequest);
        assertOK(response);
        ObjectPath dataStreamResponse = ObjectPath.createFromResponse(response);

        List<Object> dataStreams = dataStreamResponse.evaluate("data_streams");
        boolean evaluatedNewDataStream = false;
        for (Object dataStreamObj : dataStreams) {
            ObjectPath dataStream = new ObjectPath(dataStreamObj);
            if (dataStreamName.equals(dataStream.evaluate("name"))) {
                assertEquals(true, dataStream.evaluate("lifecycle.enabled"));
                assertEquals("180d", dataStream.evaluate("lifecycle.data_retention"));
                assertEquals("Data stream lifecycle", dataStream.evaluate("next_generation_managed_by"));
                assertEquals(false, dataStream.evaluate("prefer_ilm"));
                evaluatedNewDataStream = true;
            }
        }
        assertTrue(evaluatedNewDataStream);
    }

    private void waitForClusterReady() throws Exception {
        // Ensure index template is installed with the right version before executing the tests.
        if (isRunningAgainstOldCluster()) {
            // No minimum version of the registry required when running on old clusters.
            assertBusy(() -> assertDataStreamTemplateExists(EVENT_DATA_STREAM_LEGACY_TEMPLATE_NAME));

            // When running on old cluster, wait for the ILM policy to be installed.
            assertBusy(() -> assertILMPolicyExists(EVENT_DATA_STREAM_LEGACY_ILM_POLICY_NAME));
        } else {
            // DSL has been introduced with the version 3 of the registry.
            // Wait for this version to be deployed.
            assertBusy(() -> assertDataStreamTemplateExists(EVENT_DATA_STREAM_LEGACY_TEMPLATE_NAME, DSL_REGISTRY_VERSION));
        }
    }

    private void assertDataStreamTemplateExists(String templateName) throws IOException {
        assertDataStreamTemplateExists(templateName, null);
    }

    private void assertDataStreamTemplateExists(String templateName, Integer minVersion) throws IOException {
        try {
            Request getIndexTemplateRequest = new Request("GET", "_index_template/" + templateName);
            Response response = client().performRequest(getIndexTemplateRequest);
            assertOK(response);

            if (minVersion != null) {
                String pathToVersion = "index_templates.0.index_template.version";
                ObjectPath indexTemplatesResponse = ObjectPath.createFromResponse(response);
                assertThat(indexTemplatesResponse.evaluate(pathToVersion), greaterThanOrEqualTo(minVersion));
            }
        } catch (ResponseException e) {
            int status = e.getResponse().getStatusLine().getStatusCode();
            if (status == 404) {
                throw new AssertionError("Waiting for the template to be created");
            }
            throw e;
        }
    }

    private void assertILMPolicyExists(String policyName) throws IOException {
        try {
            Request getILMPolicyRequest = new Request("GET", "_ilm/policy/" + policyName);
            Response response = client().performRequest(getILMPolicyRequest);
            assertOK(response);

            assertNotNull(ObjectPath.createFromResponse(response).evaluate(policyName));
        } catch (ResponseException e) {
            int status = e.getResponse().getStatusLine().getStatusCode();
            if (status == 404) {
                throw new AssertionError("Waiting for the policy to be created");
            }
            throw e;
        }
    }
}
