/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.enrollment;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoAction;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.enrollment.ClientEnrollmentAction;
import org.elasticsearch.xpack.core.security.action.enrollment.ClientEnrollmentRequest;
import org.elasticsearch.xpack.core.security.action.enrollment.ClientEnrollmentResponse;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordAction;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordRequest;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordRequestBuilder;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.ssl.KeyConfig;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.ssl.StoreKeyConfig;

import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;

import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

public class TransportClientEnrollmentAction extends HandledTransportAction<ClientEnrollmentRequest, ClientEnrollmentResponse> {

    private static final Logger logger = LogManager.getLogger(TransportClientEnrollmentAction.class);

    private final Environment environment;
    private final Client client;
    private final Settings settings;
    private final SSLService sslService;

    @Inject
    public TransportClientEnrollmentAction(TransportService transportService, Client client, Settings settings,
                                                   SSLService sslService, Environment environment, ActionFilters actionFilters) {
        super(ClientEnrollmentAction.NAME, transportService, actionFilters, ClientEnrollmentRequest::new);
        this.environment = environment;
        this.client = new OriginSettingClient(client, SECURITY_ORIGIN);
        this.settings = settings;
        this.sslService = sslService;
    }

    @Override
    protected void doExecute(
        Task task, ClientEnrollmentRequest request, ActionListener<ClientEnrollmentResponse> listener) {
        try {
            final KeyConfig keyConfig = sslService.getHttpTransportSSLConfiguration().keyConfig();
            if (keyConfig instanceof StoreKeyConfig == false) {
                listener.onFailure(new IllegalStateException(
                        "Unable to enroll client. Elasticsearch node HTTP layer SSL configuration is not configured with a keystore"));
                return;
            }
            final List<X509Certificate> caCertificates = ((StoreKeyConfig) keyConfig).x509Certificates(environment).stream()
                    .filter(x509Certificate -> x509Certificate.getBasicConstraints() != -1).collect(Collectors.toList());
            if (caCertificates.size() != 1) {
                    listener.onFailure(new IllegalStateException(
                        "Unable to enroll client. Elasticsearch node HTTP layer SSL configuration Keystore doesn't contain a single " +
                            "PrivateKey entry where the associated certificate is a CA certificate"));

            } else {
                final String httpCa = Base64.getUrlEncoder().encodeToString(caCertificates.get(0).getEncoded());
                final NodesInfoRequest nodesInfoRequest = new NodesInfoRequest().addMetric(NodesInfoRequest.Metric.HTTP.metricName());
                client.execute(NodesInfoAction.INSTANCE, nodesInfoRequest, ActionListener.wrap(nodesInfoResponse -> {
                    final List<String> nodeList = new ArrayList<>();
                    for (NodeInfo nodeInfo : nodesInfoResponse.getNodes()) {
                        nodeList.add(nodeInfo.getInfo(HttpInfo.class).getAddress().publishAddress().toString());
                    }
                    if (shouldSetPassword(request)) {
                        final String user = userPasswordToSet(request);
                        assert user != null;
                        final ChangePasswordRequest changePasswordRequest =
                            new ChangePasswordRequestBuilder(client).username(user).password(request.getClientPassword().getChars(),
                                Hasher.resolve(XPackSettings.PASSWORD_HASHING_ALGORITHM.get(settings))).request();
                        client.execute(ChangePasswordAction.INSTANCE, changePasswordRequest,
                            ActionListener.wrap(response -> {
                                logger.debug("Successfully set the password for user [{}] during client [{}] enrollment",
                                    user, request.getClientType());
                                listener.onResponse(new ClientEnrollmentResponse(httpCa, nodeList));
                                },
                                e -> listener.onFailure(new ElasticsearchException("Failed to set the password for user " + user, e))));
                    } else {
                        listener.onResponse(new ClientEnrollmentResponse(httpCa, nodeList));
                    }
                }, e -> {
                    logger.debug("Failed to enroll client [{}]. Error was [{}]", request.getClientType(), e.getMessage());
                    listener.onFailure(e);
                }));
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private boolean shouldSetPassword(ClientEnrollmentRequest request) {
        return request.getClientType().equals(ClientEnrollmentRequest.ClientType.KIBANA.getValue()) && request.getClientPassword() != null;
    }

    private String userPasswordToSet(ClientEnrollmentRequest request) {
        if (request.getClientType().equals(ClientEnrollmentRequest.ClientType.KIBANA.getValue())) {
            return "kibana_system";
        } else {
            return null;
        }
    }
}
