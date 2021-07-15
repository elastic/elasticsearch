/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.enrollment;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoAction;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.transport.TransportInfo;
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
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportNodeEnrollmentAction extends HandledTransportAction<NodeEnrollmentRequest, NodeEnrollmentResponse> {
    private final Environment environment;
    private final SSLService sslService;
    private final Client client;

    @Inject
    public TransportNodeEnrollmentAction(TransportService transportService, SSLService sslService, Client client,
                                         ActionFilters actionFilters, Environment environment) {
        super(NodeEnrollmentAction.NAME, transportService, actionFilters, NodeEnrollmentRequest::new);
        this.environment = environment;
        this.sslService = sslService;
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, NodeEnrollmentRequest request, ActionListener<NodeEnrollmentResponse> listener) {

        final KeyConfig transportKeyConfig = sslService.getTransportSSLConfiguration().keyConfig();
        final KeyConfig httpKeyConfig = sslService.getHttpTransportSSLConfiguration().keyConfig();
        if (transportKeyConfig instanceof StoreKeyConfig == false) {
            listener.onFailure(new IllegalStateException(
                "Unable to enroll node. Elasticsearch node transport layer SSL configuration is not configured with a keystore"));
            return;
        }
        if (httpKeyConfig instanceof StoreKeyConfig == false) {
            listener.onFailure(new IllegalStateException(
                "Unable to enroll node. Elasticsearch node HTTP layer SSL configuration is not configured with a keystore"));
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
                "Unable to enroll node. Elasticsearch node HTTP layer SSL configuration Keystore doesn't contain any " +
                    "PrivateKey entries where the associated certificate is a CA certificate"));
            return;
        } else if (httpCaKeysAndCertificates.size() > 1) {
            listener.onFailure(new IllegalStateException(
                "Unable to enroll node. Elasticsearch node HTTP layer SSL configuration Keystore contain multiple " +
                    "PrivateKey entries where the associated certificate is a CA certificate"));
            return;
        }

        final List<String> nodeList = new ArrayList<>();
        final NodesInfoRequest nodesInfoRequest = new NodesInfoRequest().addMetric(NodesInfoRequest.Metric.TRANSPORT.metricName());
        executeAsyncWithOrigin(client, SECURITY_ORIGIN, NodesInfoAction.INSTANCE, nodesInfoRequest, ActionListener.wrap(
            response -> {
                for (NodeInfo nodeInfo : response.getNodes()) {
                    nodeList.add(nodeInfo.getInfo(TransportInfo.class).getAddress().publishAddress().toString());
                }
                try {
                    final String httpCaKey = Base64.getUrlEncoder().encodeToString(httpCaKeysAndCertificates.get(0).v1().getEncoded());
                    final String httpCaCert = Base64.getUrlEncoder().encodeToString(httpCaKeysAndCertificates.get(0).v2().getEncoded());
                    final String transportKey =
                        Base64.getUrlEncoder().encodeToString(transportKeysAndCertificates.get(0).v1().getEncoded());
                    final String transportCert =
                        Base64.getUrlEncoder().encodeToString(transportKeysAndCertificates.get(0).v2().getEncoded());
                    listener.onResponse(new NodeEnrollmentResponse(httpCaKey,
                        httpCaCert,
                        transportKey,
                        transportCert,
                        nodeList));
                } catch (CertificateEncodingException e) {
                    listener.onFailure(new ElasticsearchException("Unable to enroll node", e));
                }
            }, listener::onFailure
        ));
    }
}
