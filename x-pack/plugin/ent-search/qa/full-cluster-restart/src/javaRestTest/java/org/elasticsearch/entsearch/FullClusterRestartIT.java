/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.entsearch;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.upgrades.FullClusterRestartUpgradeStatus;
import org.elasticsearch.upgrades.ParameterizedFullClusterRestartTestCase;
import org.junit.ClassRule;

import java.io.IOException;

public class FullClusterRestartIT extends ParameterizedFullClusterRestartTestCase {

    // Legacy name we used for ILM policy configuration in versions prior to 8.11.
    private static final String EVENT_DATA_STREAM_LEGACY_ILM_POLICY_NAME = "behavioral_analytics-events-default_policy";

    @ClassRule
    public static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .version(getOldClusterTestVersion())
        .nodes(2)
        .setting("xpack.security.enabled", "false")
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

        String legacyAnalyticsCollectionName = "oldstuff";
        String newAnalyticsCollectionName = "newstuff";

        try (RestClient restClient = client()) {
            if (isRunningAgainstOldCluster()) {
                // Create an analytics collection
                Request legacyPutRequest = new Request("PUT", "_application/analytics/" + legacyAnalyticsCollectionName);
                assertOK(restClient.performRequest(legacyPutRequest));

                // Validate that ILM lifecycle is in place
                assertBusy(() -> testLegacyDataRetentionPolicy(legacyAnalyticsCollectionName));
            } else {

                // Validate that the existing analytics collection is still using ILM
                assertBusy(() -> testLegacyDataRetentionPolicy(legacyAnalyticsCollectionName));

                // Create a new analytics collection
                Request putRequest = new Request("PUT", "_application/analytics/" + newAnalyticsCollectionName);
                assertOK(restClient.performRequest(putRequest));

                // Validate that NO ILM lifecycle is in place and we are using DLM instead.
                assertBusy(() -> testDlmDataRetentionPolicy(newAnalyticsCollectionName));
            }
        }
    }

    private void testLegacyDataRetentionPolicy(String analyticsCollectionName) throws IOException {
        String dataStreamName = "behavioral_analytics-events-" + analyticsCollectionName;
        Request getDataStreamRequest = new Request("GET", "_data_stream/" + dataStreamName);
        ObjectPath dataStream = ObjectPath.createFromResponse(client().performRequest(getDataStreamRequest));
        String pathToIlmPolicy = "data_streams.0.ilm_policy";
        assertNotNull(dataStream.evaluate(pathToIlmPolicy));
        assertEquals("behavioral_analytics-events-default_policy", dataStream.evaluate(pathToIlmPolicy));

        Request policyRequest = new Request("GET", "_ilm/policy/" + EVENT_DATA_STREAM_LEGACY_ILM_POLICY_NAME);
        ObjectPath policy = ObjectPath.createFromResponse(client().performRequest(policyRequest));
        assertNotNull(policy.evaluate(EVENT_DATA_STREAM_LEGACY_ILM_POLICY_NAME));
    }

    private void testDlmDataRetentionPolicy(String analyticsCollectionName) throws IOException {
        String dataStreamName = "behavioral_analytics-events-" + analyticsCollectionName;
        Request getDataStreamRequest = new Request("GET", "_data_stream/" + dataStreamName);
        ObjectPath dataStream = ObjectPath.createFromResponse(client().performRequest(getDataStreamRequest));
        assertNull(dataStream.evaluate("data_streams.0.ilm_policy"));
        assertEquals(true, dataStream.evaluate("data_streams.0.lifecycle.enabled"));
        assertEquals("180d", dataStream.evaluate("data_streams.0.lifecycle.retention"));
    }
}
