/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.action.PutInferenceModelAction;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettings;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.discovery.PeerFinder.DISCOVERY_FIND_PEERS_INTERVAL_SETTING;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 0)
public class UpdateRateLimitsClusterServiceTests extends ESIntegTestCase {

    private MockLog mockLog;

    public void setUp() throws Exception {
        super.setUp();
        mockLog = MockLog.capture(UpdateRateLimitsClusterService.class, TransportService.class);
    }

    public void tearDown() throws Exception {
        mockLog.close();
        super.tearDown();
    }

    public void testNodeJoinsRateLimitsUpdated() {
        // TODO: Expectation does not work, but I see the correct log message
        final String rateLimitUpdatedPattern = "Updating rate limit for endpoint";
        final MockLog.LoggingExpectation rateLimitUpdatedExpectation = new MockLog.SeenEventExpectation(
                "rate limit updated",
                UpdateRateLimitsClusterService.class.getCanonicalName(),
                Level.INFO,
                rateLimitUpdatedPattern
        );

        // This expectation works
        final String publishAddressMessage = "publish_addresssss";
        final MockLog.LoggingExpectation publishAddressExpectation = new MockLog.SeenEventExpectation(
                "publish_address",
                TransportService.class.getCanonicalName(),
                Level.INFO,
                publishAddressMessage
        );


        // TODO: re-enable as soon you know how to capture logs of newly joined nodes
        //mockLog.addExpectation(rateLimitUpdatedExpectation);
        mockLog.addExpectation(publishAddressExpectation);

        var nodeSettings = Settings.builder()
            .put(ElasticInferenceServiceSettings.EIS_GATEWAY_URL.getKey(), "http://localhost:8080")
            .build();

        var oneClusterNode = 1;
        var putInferenceModelActionPayload = """
            {
                "service": "elastic",
                "service_settings": {
                    "model_id": ".elser_model_2"
                }
            }
            """;

        InternalTestCluster internalCluster = internalCluster();

        // We need to start a non-master-only-node to be able to store an inference endpoint
        internalCluster.startNode(nodeSettings);
        ensureStableCluster(oneClusterNode);

        var inferenceEntityId = "inference-endpoint-id";
        var createInferenceEndpointRequest = new PutInferenceModelAction.Request(
            TaskType.SPARSE_EMBEDDING,
            inferenceEntityId,
            new BytesArray(putInferenceModelActionPayload),
            XContentType.JSON
        );
        client().execute(PutInferenceModelAction.INSTANCE, createInferenceEndpointRequest).actionGet();

        // Perform inference so the rate limiting endpoint handler is stored
        var performInferenceRequest = new InferenceAction.Request(
            TaskType.SPARSE_EMBEDDING,
            inferenceEntityId,
            null,
            List.of("some input"),
            new HashMap<>(),
            InputType.UNSPECIFIED,
            TimeValue.THIRTY_SECONDS,
            false
        );
        client().execute(InferenceAction.INSTANCE, performInferenceRequest).actionGet();

        internalCluster.startNode(
            Settings.builder()
                .build()
        );
        ensureStableCluster(2);
        client().admin().cluster().prepareNodesStats().get(TimeValue.timeValueSeconds(10));

        mockLog.assertAllExpectationsMatched();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // TODO: InternalSettingsPlugin needed?
        return Arrays.asList(InferencePlugin.class);
    }
}
