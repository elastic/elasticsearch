/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.enrollment;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.xpack.core.ssl.StoreKeyConfig;
import org.elasticsearch.env.Environment;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.enrollment.NodeEnrollmentAction;
import org.elasticsearch.xpack.core.security.action.enrollment.NodeEnrollmentRequest;
import org.elasticsearch.xpack.core.security.action.enrollment.NodeEnrollmentResponse;
import org.elasticsearch.xpack.core.ssl.KeyConfig;
import org.elasticsearch.xpack.core.ssl.SSLService;

import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

public class TransportNodeEnrollmentAction extends HandledTransportAction<NodeEnrollmentRequest, NodeEnrollmentResponse> {
    private final Environment environment;
    private final ClusterService clusterService;
    private final SSLService sslService;

    @Inject
    public TransportNodeEnrollmentAction(TransportService transportService, ClusterService clusterService, SSLService sslService,
                                         ActionFilters actionFilters, Environment environment) {
        super(NodeEnrollmentAction.NAME, transportService, actionFilters, NodeEnrollmentRequest::new);
        this.environment = environment;
        this.clusterService = clusterService;
        this.sslService = sslService;
    }

    @Override protected void doExecute(
        Task task, NodeEnrollmentRequest request, ActionListener<NodeEnrollmentResponse> listener) {
        try {
            final KeyConfig transportKeyConfig = sslService.getTransportSSLConfiguration().keyConfig();
            final KeyConfig httpKeyConfig = sslService.getHttpTransportSSLConfiguration().keyConfig();
            if (transportKeyConfig instanceof StoreKeyConfig == false) {
                listener.onFailure(new IllegalStateException(
                    "Unable to enroll client. Elasticsearch node transport layer SSL configuration is not configured with a keystore"));
                return;
            }
            if (httpKeyConfig instanceof StoreKeyConfig == false) {
                listener.onFailure(new IllegalStateException(
                    "Unable to enroll client. Elasticsearch node HTTP layer SSL configuration is not configured with a keystore"));
                return;
            }
            final List<Tuple<PrivateKey, X509Certificate>> transportKeysAndCertificates =
                ((StoreKeyConfig) transportKeyConfig).getPrivateKeyEntries(environment);
            final List<Tuple<PrivateKey, X509Certificate>> httpCaKeysAndCertificates =
                ((StoreKeyConfig) httpKeyConfig).getPrivateKeyEntries(environment).stream()
                    .filter(t -> t.v2().getBasicConstraints() != -1).collect(Collectors.toList());
            if (transportKeysAndCertificates.isEmpty()) {
                listener.onFailure(new IllegalStateException(
                    "Unable to enroll node. Elasticsearch node transport layer SSL configuration doesn't contain any keys"));
                return;
            } else if (transportKeysAndCertificates.size() > 1) {
                listener.onFailure(new IllegalStateException(
                    "Unable to enroll node. Elasticsearch node transport layer SSL configuration contains multiple keys"));
                return;
            }
            if (httpCaKeysAndCertificates.isEmpty()) {
                listener.onFailure(new IllegalStateException(
                    "Unable to enroll client. Elasticsearch node HTTP layer SSL configuration Keystore doesn't contain any " +
                        "PrivateKey entries where the associated certificate is a CA certificate"));
                return;
            } else if (httpCaKeysAndCertificates.size() > 1) {
                listener.onFailure(new IllegalStateException(
                    "Unable to enroll client. Elasticsearch node HTTP layer SSL configuration Keystore contain multiple " +
                        "PrivateKey entries where the associated certificate is a CA certificate"));
                return;
            }

            final String httpCaKey = Base64.getUrlEncoder().encodeToString(httpCaKeysAndCertificates.get(0).v1().getEncoded());
            final String httpCaCert = Base64.getUrlEncoder().encodeToString(httpCaKeysAndCertificates.get(0).v2().getEncoded());
            final String transportKey = Base64.getUrlEncoder().encodeToString(transportKeysAndCertificates.get(0).v1().getEncoded());
            final String transportCert = Base64.getUrlEncoder().encodeToString(transportKeysAndCertificates.get(0).v2().getEncoded());
            final List<String> nodeList = new ArrayList<>();
            for (DiscoveryNode node : clusterService.state().getNodes()) {
                nodeList.add(node.getAddress().toString());
            }
            listener.onResponse(new NodeEnrollmentResponse(httpCaKey,
                httpCaCert,
                transportKey,
                transportCert,
                clusterService.getClusterName().value(),
                nodeList));

        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
