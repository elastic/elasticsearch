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
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.http.HttpInfo;
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

import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.XPackSettings.ENROLLMENT_ENABLED;

public class InternalEnrollmentTokenGenerator extends BaseEnrollmentTokenGenerator {
    protected static final long ENROLL_API_KEY_EXPIRATION_MINUTES = 30L;

    private static final Logger LOGGER = LogManager.getLogger(InternalEnrollmentTokenGenerator.class);
    private final Environment environment;
    private final SSLService sslService;
    private final Client client;
    private final NodesInfoRequest nodesInfoRequest;

    public InternalEnrollmentTokenGenerator(Environment environment, SSLService sslService, Client client) {
        this.environment = environment;
        this.sslService = sslService;
        // enrollment token API keys will be owned by the "_xpack_security" system user ("superuser" role)
        this.client = new OriginSettingClient(client, SECURITY_ORIGIN);
        // the enrollment token can only be used against the node that generates it
        this.nodesInfoRequest = new NodesInfoRequest().nodesIds("_local").addMetric(NodesInfoRequest.Metric.HTTP.metricName());
    }

    /**
     * Creates an enrollment token for an elasticsearch node
     * @param listener The listener to be notified with the result. It will be passed the {@link EnrollmentToken} if creation was
     *                      successful, {@code null} otherwise.
     */
    public void createNodeEnrollmentToken(ActionListener<EnrollmentToken> listener) {
        client.execute(NodesInfoAction.INSTANCE, nodesInfoRequest, ActionListener.wrap(response -> {
            assert response.getNodes().size() == 1;
            NodeInfo nodeInfo = response.getNodes().get(0);
            assembleToken(EnrollTokenType.NODE, nodeInfo.getInfo(HttpInfo.class), listener);
        }, e -> {
            LOGGER.error("Failed to create enrollment token when retrieving local nodes HTTP info", e);
            listener.onResponse(null);
        }));
    }

    /**
     * Creates an enrollment token for a kibana instance
     * @param listener The listener to be notified with the result. It will be passed the {@link EnrollmentToken} if creation was
     *                      successful, {@code null} otherwise.
     */
    public void createKibanaEnrollmentToken(ActionListener<EnrollmentToken> listener) {
        client.execute(NodesInfoAction.INSTANCE, nodesInfoRequest, ActionListener.wrap(response -> {
            assert response.getNodes().size() == 1;
            NodeInfo nodeInfo = response.getNodes().get(0);
            assembleToken(EnrollTokenType.KIBANA, nodeInfo.getInfo(HttpInfo.class), listener);
        }, e -> {
            LOGGER.error("Failed to create enrollment token when retrieving local nodes HTTP info", e);
            listener.onResponse(null);
        }));
    }

    private void assembleToken(EnrollTokenType enrollTokenType, HttpInfo httpInfo, ActionListener<EnrollmentToken> listener) {
        if (false == ENROLLMENT_ENABLED.get(environment.settings())) {
            LOGGER.error("Cannot create enrollment token [" + enrollTokenType + "] because enrollment is disabled " +
                "with setting [" + ENROLLMENT_ENABLED.getKey() + "] set to [false]");
            listener.onResponse(null);
            return;
        }
        final String fingerprint;
        try {
            fingerprint = getCaFingerprint(sslService);
        } catch (Exception e) {
            LOGGER.error("Failed to create enrollment token when computing HTTPS CA fingerprint, possibly the certs are not auto-generated",
                e);
            listener.onResponse(null);
            return;
        }
        final List<String> tokenAddresses;
        try {
            final List<String> httpAddressesList = getAllHttpAddresses(httpInfo);
            final Tuple<List<String>, List<String>> splitAddresses = splitAddresses(httpAddressesList);
            if (enrollTokenType == EnrollTokenType.NODE) {
                // do not generate node enrollment tokens, on node startup, if the node is only bound to localhost (empty non-local
                // addresses list)
                if (splitAddresses.v2().isEmpty()) {
                    LOGGER.info("Will not generate node enrollment token if HTTPS is bound on localhost only");
                    listener.onResponse(null);
                    return;
                }
                tokenAddresses = splitAddresses.v2();
            } else {
                // Kibana enrollment tokens are generated even if HTTPS is bound on localhost only
                assert enrollTokenType == EnrollTokenType.KIBANA;
                tokenAddresses = getFilteredAddresses(splitAddresses);
            }
        } catch (Exception e) {
            LOGGER.error("Failed to create enrollment when extracting HTTPS bound addresses", e);
            listener.onResponse(null);
            return;
        }
        final CreateApiKeyRequest apiKeyRequest = new CreateApiKeyRequest(
            "enrollment_token_API_key_" + UUIDs.base64UUID(),
            List.of(new RoleDescriptor("create_enrollment_token", new String[] { enrollTokenType.toString() }, null, null)),
            TimeValue.timeValueMinutes(ENROLL_API_KEY_EXPIRATION_MINUTES)
        );
        client.execute(CreateApiKeyAction.INSTANCE, apiKeyRequest, ActionListener.wrap(createApiKeyResponse -> {
            final String apiKey = createApiKeyResponse.getId() + ":" + createApiKeyResponse.getKey().toString();
            final EnrollmentToken enrollmentToken = new EnrollmentToken(
                apiKey,
                fingerprint,
                Version.CURRENT.toString(),
                tokenAddresses
            );
            listener.onResponse(enrollmentToken);
        }, e -> {
            LOGGER.error("Failed to create enrollment token when generating API key", e);
            listener.onResponse(null);
        }));
    }

    private static List<String> getAllHttpAddresses(HttpInfo httpInfo) {
        final List<String> httpAddressesList = new ArrayList<>();
        httpAddressesList.add(httpInfo.getAddress().publishAddress().toString());
        Arrays.stream(httpInfo.getAddress().boundAddresses())
            .map(TransportAddress::toString)
            .forEach(httpAddressesList::add);
        return  httpAddressesList;
    }

    private enum EnrollTokenType {
        KIBANA {
            @Override
            public String toString() {
                return KibanaEnrollmentAction.NAME;
            }
        },
        NODE {
            @Override
            public String toString() {
                return NodeEnrollmentAction.NAME;
            }
        }
    }

}
