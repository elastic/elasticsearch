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
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoMetrics;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.TransportNodesInfoAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.ssl.SslKeyConfig;
import org.elasticsearch.common.ssl.StoreKeyConfig;
import org.elasticsearch.common.ssl.StoredCertificate;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportInfo;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.enrollment.NodeEnrollmentAction;
import org.elasticsearch.xpack.core.security.action.enrollment.NodeEnrollmentRequest;
import org.elasticsearch.xpack.core.security.action.enrollment.NodeEnrollmentResponse;
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
    private final SSLService sslService;
    private final Client client;

    @Inject
    public TransportNodeEnrollmentAction(
        TransportService transportService,
        SSLService sslService,
        Client client,
        ActionFilters actionFilters
    ) {
        super(NodeEnrollmentAction.NAME, transportService, actionFilters, NodeEnrollmentRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.sslService = sslService;
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, NodeEnrollmentRequest request, ActionListener<NodeEnrollmentResponse> listener) {

        final SslKeyConfig transportKeyConfig = sslService.getTransportSSLConfiguration().keyConfig();
        final SslKeyConfig httpKeyConfig = sslService.getHttpTransportSSLConfiguration().keyConfig();
        if (transportKeyConfig instanceof StoreKeyConfig == false) {
            listener.onFailure(
                new IllegalStateException(
                    "Unable to enroll node. Elasticsearch node transport layer SSL configuration is not configured with a keystore"
                )
            );
            return;
        }
        if (httpKeyConfig instanceof StoreKeyConfig == false) {
            listener.onFailure(
                new IllegalStateException(
                    "Unable to enroll node. Elasticsearch node HTTP layer SSL configuration is not configured with a keystore"
                )
            );
            return;
        }

        final List<Tuple<PrivateKey, X509Certificate>> transportKeysAndCertificates = transportKeyConfig.getKeys();
        final List<Tuple<PrivateKey, X509Certificate>> httpCaKeysAndCertificates = httpKeyConfig.getKeys()
            .stream()
            .filter(t -> t.v2().getBasicConstraints() != -1)
            .toList();

        if (transportKeysAndCertificates.isEmpty()) {
            listener.onFailure(
                new IllegalStateException(
                    "Unable to enroll node. Elasticsearch node transport layer SSL configuration doesn't contain any keys"
                )
            );
            return;
        } else if (transportKeysAndCertificates.size() > 1) {
            listener.onFailure(
                new IllegalStateException(
                    "Unable to enroll node. Elasticsearch node transport layer SSL configuration contains multiple keys"
                )
            );
            return;
        }
        final List<X509Certificate> transportCaCertificates;
        try {
            transportCaCertificates = ((StoreKeyConfig) transportKeyConfig).getConfiguredCertificates()
                .stream()
                .map(StoredCertificate::certificate)
                .filter(x509Certificate -> x509Certificate.getBasicConstraints() != -1)
                .collect(Collectors.toList());
        } catch (Exception e) {
            listener.onFailure(
                new ElasticsearchException(
                    "Unable to enroll node. Cannot retrieve CA certificate " + "for the transport layer of the Elasticsearch node.",
                    e
                )
            );
            return;
        }
        if (transportCaCertificates.size() != 1) {
            listener.onFailure(
                new ElasticsearchException(
                    "Unable to enroll Elasticsearch node. Elasticsearch node transport layer SSL configuration Keystore "
                        + "[xpack.security.transport.ssl.keystore] doesn't contain a single CA certificate"
                )
            );
        }

        if (httpCaKeysAndCertificates.isEmpty()) {
            listener.onFailure(
                new IllegalStateException(
                    "Unable to enroll node. Elasticsearch node HTTP layer SSL configuration Keystore doesn't contain any "
                        + "PrivateKey entries where the associated certificate is a CA certificate"
                )
            );
            return;
        } else if (httpCaKeysAndCertificates.size() > 1) {
            listener.onFailure(
                new IllegalStateException(
                    "Unable to enroll node. Elasticsearch node HTTP layer SSL configuration Keystore contain multiple "
                        + "PrivateKey entries where the associated certificate is a CA certificate"
                )
            );
            return;
        }

        final List<String> nodeList = new ArrayList<>();
        final NodesInfoRequest nodesInfoRequest = new NodesInfoRequest().addMetric(NodesInfoMetrics.Metric.TRANSPORT.metricName());
        executeAsyncWithOrigin(client, SECURITY_ORIGIN, TransportNodesInfoAction.TYPE, nodesInfoRequest, ActionListener.wrap(response -> {
            for (NodeInfo nodeInfo : response.getNodes()) {
                nodeList.add(nodeInfo.getInfo(TransportInfo.class).getAddress().publishAddress().toString());
            }
            try {
                final String httpCaKey = Base64.getEncoder().encodeToString(httpCaKeysAndCertificates.get(0).v1().getEncoded());
                final String httpCaCert = Base64.getEncoder().encodeToString(httpCaKeysAndCertificates.get(0).v2().getEncoded());
                final String transportCaCert = Base64.getEncoder().encodeToString(transportCaCertificates.get(0).getEncoded());
                final String transportKey = Base64.getEncoder().encodeToString(transportKeysAndCertificates.get(0).v1().getEncoded());
                final String transportCert = Base64.getEncoder().encodeToString(transportKeysAndCertificates.get(0).v2().getEncoded());
                listener.onResponse(
                    new NodeEnrollmentResponse(httpCaKey, httpCaCert, transportCaCert, transportKey, transportCert, nodeList)
                );
            } catch (CertificateEncodingException e) {
                listener.onFailure(new ElasticsearchException("Unable to enroll node", e));
            }
        }, listener::onFailure));
    }
}
