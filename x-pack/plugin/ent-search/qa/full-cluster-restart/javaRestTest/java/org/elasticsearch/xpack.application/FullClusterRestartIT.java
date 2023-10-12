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
package org.elasticsearch.xpack.application;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.upgrades.FullClusterRestartUpgradeStatus;
import org.elasticsearch.upgrades.ParameterizedFullClusterRestartTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;

public class FullClusterRestartIT extends ParameterizedFullClusterRestartTestCase {

    private static final Version DSL_DEFAULT_RETENTION_VERSION = V_8_12_0;

    // Legacy name we used for ILM policy configuration in versions prior to 8.12.0.
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

        assumeTrue(
            "Data retention changed by default to DSL in " + DSL_DEFAULT_RETENTION_VERSION,
            getOldClusterTestVersion().before(DSL_DEFAULT_RETENTION_VERSION)
        );

        String legacyAnalyticsCollectionName = "oldstuff";
        String newAnalyticsCollectionName = "newstuff";

        if (isRunningAgainstOldCluster()) {
            // Create an analytics collection
            Request legacyPutRequest = new Request("PUT", "_application/analytics/" + legacyAnalyticsCollectionName);
            assertOK(client().performRequest(legacyPutRequest));

            // Validate that ILM lifecycle is in place
            assertBusy(() -> assertLegacyDataRetentionPolicy(legacyAnalyticsCollectionName));
        } else {
            // Create a new analytics collection
            Request putRequest = new Request("PUT", "_application/analytics/" + newAnalyticsCollectionName);
            assertOK(client().performRequest(putRequest));

            // Validate that NO ILM lifecycle is in place and we are using DLS instead.
            assertBusy(() -> assertDslDataRetention(newAnalyticsCollectionName));

            // Validate that the existing analytics collection created with an older version is still using ILM
            assertBusy(() -> assertLegacyDataRetentionPolicy(legacyAnalyticsCollectionName));
        }
    }

    private void assertLegacyDataRetentionPolicy(String analyticsCollectionName) throws IOException {
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

    private void assertDslDataRetention(String analyticsCollectionName) throws IOException {
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
                assertNull(dataStream.evaluate("ilm_policy"));
                assertEquals(true, dataStream.evaluate("lifecycle.enabled"));
                assertEquals("180d", dataStream.evaluate("lifecycle.data_retention"));
                evaluatedNewDataStream = true;
            }
        }
        assertTrue(evaluatedNewDataStream);

    }
}
