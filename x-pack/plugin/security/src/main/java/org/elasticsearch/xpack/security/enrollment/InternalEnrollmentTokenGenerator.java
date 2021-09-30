/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.enrollment;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoAction;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.EnrollmentToken;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.enrollment.KibanaEnrollmentAction;
import org.elasticsearch.xpack.core.security.action.enrollment.NodeEnrollmentAction;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.ssl.SSLService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;

public class InternalEnrollmentTokenGenerator extends BaseEnrollmentTokenGenerator {
    protected static final long ENROLL_API_KEY_EXPIRATION = 30L;

    private static final Logger LOGGER = LogManager.getLogger(InternalEnrollmentTokenGenerator.class);
    private final Environment environment;
    private final SSLService sslService;
    private final Client client;

    public InternalEnrollmentTokenGenerator(Environment environment, SSLService sslService, Client client) {
        this.environment = environment;
        this.sslService = sslService;
        this.client = new OriginSettingClient(client, SECURITY_ORIGIN);
    }

    /**
     * Creates an enrollment token for an elasticsearch node
     * @param tokenListener The listener to be notified with the result. It will be passed the {@link EnrollmentToken} if creation was
     *                      successful, null otherwise.
     */
    public void createNodeEnrollmentToken(ActionListener<EnrollmentToken> tokenListener) {
        create(NodeEnrollmentAction.NAME, tokenListener);
    }

    /**
     * Creates an enrollment token for a kibana instance
     * @param tokenListener The listener to be notified with the result. It will be passed the {@link EnrollmentToken} if creation was
     *                      successful, null otherwise.
     */
    public void createKibanaEnrollmentToken(ActionListener<EnrollmentToken> tokenListener) {
        create(KibanaEnrollmentAction.NAME, tokenListener);
    }

    protected void create(String action, ActionListener<EnrollmentToken> listener) {
        if (XPackSettings.ENROLLMENT_ENABLED.get(environment.settings()) != true) {
            LOGGER.warn("[xpack.security.enrollment.enabled] must be set to `true` to create an enrollment token");
            listener.onResponse(null);
            return;
        }
        final String fingerprint;
        try {
            fingerprint = getCaFingerprint(sslService);
        } catch (Exception e) {
            LOGGER.warn(
                "Error creating an Enrollment Token.Failed to get the fingerprint of the CA Certificate for the HTTP layer of elasticsearch"
            );
            listener.onResponse(null);
            return;
        }
        final CreateApiKeyRequest apiKeyRequest = new CreateApiKeyRequest(
            "enrollment_token_API_key_" + UUIDs.base64UUID(),
            List.of(new RoleDescriptor("create_enrollment_token", new String[] { action }, null, null)),
            TimeValue.timeValueMinutes(ENROLL_API_KEY_EXPIRATION)
        );

        client.execute(CreateApiKeyAction.INSTANCE, apiKeyRequest, ActionListener.wrap(createApiKeyResponse -> {
            final String apiKey = createApiKeyResponse.getId() + ":" + createApiKeyResponse.getKey().toString();
            final NodesInfoRequest nodesInfoRequest = new NodesInfoRequest().nodesIds("_local")
                .addMetric(NodesInfoRequest.Metric.HTTP.metricName());
            final List<String> httpAddressesList = new ArrayList<>();
            client.execute(NodesInfoAction.INSTANCE, nodesInfoRequest, ActionListener.wrap(response -> {
                for (NodeInfo nodeInfo : response.getNodes()) {
                    httpAddressesList.addAll(
                        Arrays.stream(nodeInfo.getInfo(HttpInfo.class).getAddress().boundAddresses())
                            .map(TransportAddress::toString)
                            .collect(Collectors.toList())
                    );
                }
                final EnrollmentToken enrollmentToken = new EnrollmentToken(
                    apiKey,
                    fingerprint,
                    Version.CURRENT.toString(),
                    httpAddressesList
                );
                listener.onResponse(enrollmentToken);
            }, e -> {
                LOGGER.warn("Error creating an Enrollment Token. Failed to get HTTP info from the Nodes Info API", e);
                listener.onResponse(null);
            }));
        }, e -> {
            LOGGER.warn("Error creating an Enrollment Token. Failed to generate API key", e);
            listener.onResponse(null);
        }));
    }

}
