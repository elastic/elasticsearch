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
import java.util.function.Consumer;

import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.XPackSettings.ENROLLMENT_ENABLED;

public class InternalEnrollmentTokenGenerator extends BaseEnrollmentTokenGenerator {

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
     * Creates an enrollment token for Elasticsearch nodes enrolling to the current node.
     * In case of errors, including due to issues with the node's configuration, a {@code null} token is returned, and the exception
     * is logged but no exception is thrown.
     */
    public void createNodeEnrollmentToken(Consumer<EnrollmentToken> consumer) {
        client.execute(NodesInfoAction.INSTANCE, nodesInfoRequest, ActionListener.wrap(response -> {
            assert response.getNodes().size() == 1;
            NodeInfo nodeInfo = response.getNodes().get(0);
            assembleToken(EnrollTokenType.NODE, nodeInfo.getInfo(HttpInfo.class), consumer);
        }, e -> {
            LOGGER.error("Failed to create node enrollment token when retrieving local nodes HTTP info", e);
            consumer.accept(null);
        }));
    }

    /**
     * Creates an enrollment token for Kibana instances enrolling to the current node.
     * In case of errors, including due to issues with the node's configuration, a {@code null} token is returned, and the exception
     * is logged but no exception is thrown.
     */
    public void createKibanaEnrollmentToken(Consumer<EnrollmentToken> consumer) {
        client.execute(NodesInfoAction.INSTANCE, nodesInfoRequest, ActionListener.wrap(response -> {
            assert response.getNodes().size() == 1;
            NodeInfo nodeInfo = response.getNodes().get(0);
            assembleToken(EnrollTokenType.KIBANA, nodeInfo.getInfo(HttpInfo.class), consumer);
        }, e -> {
            LOGGER.error("Failed to create kibana enrollment token when retrieving local nodes HTTP info", e);
            consumer.accept(null);
        }));
    }

    private void assembleToken(EnrollTokenType enrollTokenType, HttpInfo httpInfo, Consumer<EnrollmentToken> consumer) {
        if (false == ENROLLMENT_ENABLED.get(environment.settings())) {
            LOGGER.error("Cannot create enrollment token [" + enrollTokenType + "] because enrollment is disabled " +
                "with setting [" + ENROLLMENT_ENABLED.getKey() + "] set to [false]");
            consumer.accept(null);
            return;
        }
        final String fingerprint;
        try {
            fingerprint = getHttpsCaFingerprint();
        } catch (Exception e) {
            LOGGER.error("Failed to create enrollment token when computing HTTPS CA fingerprint, possibly the certs are not auto-generated",
                e);
            consumer.accept(null);
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
                    consumer.accept(null);
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
            consumer.accept(null);
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
            consumer.accept(enrollmentToken);
        }, e -> {
            LOGGER.error("Failed to create enrollment token when generating API key", e);
            consumer.accept(null);
        }));
    }

    public String getHttpsCaFingerprint() throws Exception {
        return getHttpsCaFingerprint(sslService);
    }

    private static List<String> getAllHttpAddresses(HttpInfo httpInfo) {
        final List<String> httpAddressesList = new ArrayList<>();
        httpAddressesList.add(httpInfo.getAddress().publishAddress().toString());
        Arrays.stream(httpInfo.getAddress().boundAddresses())
            .map(TransportAddress::toString)
            .forEach(httpAddressesList::add);
        return httpAddressesList;
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
