/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.env.Environment;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class TransportNodeEnrollmentAction extends HandledTransportAction<NodeEnrollmentRequest, NodeEnrollmentResponse> {
    private final Environment environment;
    private final ClusterService clusterService;

    @Inject public TransportNodeEnrollmentAction(
        TransportService transportService, ClusterService clusterService, ActionFilters actionFilters, Environment environment) {
        super(NodeEnrollmentAction.NAME, transportService, actionFilters, NodeEnrollmentRequest::new);
        this.environment = environment;
        this.clusterService = clusterService;
    }

    @Override protected void doExecute(
        Task task, NodeEnrollmentRequest request, ActionListener<NodeEnrollmentResponse> listener) {
        try {
            final String httpCaKeystore = "httpCa.p12";
            final String transportKeystore = "transport.p12";
            final Path httpCaKeystorePath = environment.configFile().resolve(httpCaKeystore);
            final Path transportKeystorePath = environment.configFile().resolve(transportKeystore);
            if (Files.exists(httpCaKeystorePath) == false) {
                listener.onFailure(new IllegalStateException("HTTP layer CA keystore " + httpCaKeystore + " does not exist"));
            } else if (Files.exists(transportKeystorePath) == false) {
                listener.onFailure(new IllegalStateException("Transport layer keystore " + httpCaKeystore + " does not exist"));
            } else {
                final String httpCa = Base64.getUrlEncoder().encodeToString(Files.readAllBytes(httpCaKeystorePath));
                final String transport = Base64.getUrlEncoder().encodeToString(Files.readAllBytes(transportKeystorePath));
                final List<String> nodeList = new ArrayList<>();
                for (DiscoveryNode node : clusterService.state().getNodes()) {
                    nodeList.add(node.getAddress().toString());
                }
                listener.onResponse(new NodeEnrollmentResponse(httpCa, transport, clusterService.getClusterName().value(), nodeList));
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
