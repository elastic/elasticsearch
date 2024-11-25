/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.RequestExecutorService;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;

public class UpdateRateLimitsClusterService implements ClusterStateListener {

    private static final Logger LOGGER = LogManager.getLogger(UpdateRateLimitsClusterService.class);

    private final InferenceServiceRegistry inferenceServiceRegistry;

    public UpdateRateLimitsClusterService(ClusterService clusterService, InferenceServiceRegistry inferenceServiceRegistry) {
        this.inferenceServiceRegistry = inferenceServiceRegistry;
        clusterService.addListener(this);
        LOGGER.info("Added UpdateRateLimitsClusterService as a ClusterStateListener");
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        LOGGER.info("Received cluster changed event {}", event.source());

        // TODO: check what this actually does and if it's necessary
        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }

        // TODO: check if the cluster is ready?

        // TODO: other sanity checks?

        if (event.nodesAdded() || event.nodesRemoved()) {
            LOGGER.info("Received nodesAdded or nodesRemoved event");

            var numNodes = event.state().nodes().getSize();

            LOGGER.info("Number of nodes in the cluster: {}", numNodes);
            var elasticInferenceServiceOptional = inferenceServiceRegistry.getService(ElasticInferenceService.NAME);

            if (elasticInferenceServiceOptional.isPresent() == false) {
                // TODO: adapt
                LOGGER.info("ElasticInferenceService is not present");
                return;
            }

            ElasticInferenceService elasticInferenceService = (ElasticInferenceService) elasticInferenceServiceOptional.get();
            var sender = elasticInferenceService.getSender();

            if (sender instanceof HttpRequestSender == false) {
                // TODO: adapt
                LOGGER.warn("sender is not type HttpRequestSender");
                return;
            }

            HttpRequestSender httpRequestSender = (HttpRequestSender) sender;

            LOGGER.info("Updating rate limits for {} endpoints", httpRequestSender.rateLimitingEndpointHandlers().size());
            for (RequestExecutorService.RateLimitingEndpointHandler rateLimitingEndpointHandler : httpRequestSender
                .rateLimitingEndpointHandlers()) {
                // TODO: keep the original rate limit and always divide by the number of nodes to handle node leaving/joining correctly
                var oldRequestsPerTimeUnit = rateLimitingEndpointHandler.requestsPerTimeUnit();
                var clusterAwareTokenLimit = oldRequestsPerTimeUnit / numNodes;
                LOGGER.info(
                    "Updating rate limit for endpoint {} from {} to {} tokens per time unit",
                    rateLimitingEndpointHandler.id(),
                    oldRequestsPerTimeUnit,
                    clusterAwareTokenLimit
                );

                rateLimitingEndpointHandler.updateTokensPerTimeUnit(clusterAwareTokenLimit);
            }
        }
    }
}
