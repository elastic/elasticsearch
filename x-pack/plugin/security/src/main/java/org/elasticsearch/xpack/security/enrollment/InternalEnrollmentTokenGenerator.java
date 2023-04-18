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
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.transport.TransportInfo;
import org.elasticsearch.xpack.core.security.EnrollmentToken;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.enrollment.KibanaEnrollmentAction;
import org.elasticsearch.xpack.core.security.action.enrollment.NodeEnrollmentAction;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.ssl.SSLService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import static org.elasticsearch.threadpool.ThreadPool.Names.GENERIC;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.XPackSettings.ENROLLMENT_ENABLED;

public class InternalEnrollmentTokenGenerator extends BaseEnrollmentTokenGenerator {

    private static final Logger LOGGER = LogManager.getLogger(InternalEnrollmentTokenGenerator.class);
    private final Environment environment;
    private final SSLService sslService;
    private final Client client;

    public InternalEnrollmentTokenGenerator(Environment environment, SSLService sslService, Client client) {
        this.environment = environment;
        this.sslService = sslService;
        // enrollment tokens API keys will be owned by the "_xpack_security" system user ("_xpack_security" role)
        this.client = new OriginSettingClient(client, SECURITY_ORIGIN);
    }

    /**
     * Tries to create an enrollment token for Elasticsearch nodes enrolling to the current node.
     * If node is bound only on localhost for either the transport or the HTTPS interface, no token is generated,
     * in which case an empty string token is returned.
     * In case of errors, including due to issues with the node's configuration, a {@code null} token is returned, and the exception
     * is logged but no exception is thrown.
     */
    public void maybeCreateNodeEnrollmentToken(Consumer<String> consumer, Iterator<TimeValue> backoff) {
        // the enrollment token can only be used against the node that generated it
        final NodesInfoRequest nodesInfoRequest = new NodesInfoRequest().nodesIds("_local")
            .addMetrics(NodesInfoRequest.Metric.HTTP.metricName(), NodesInfoRequest.Metric.TRANSPORT.metricName());

        client.execute(NodesInfoAction.INSTANCE, nodesInfoRequest, ActionListener.wrap(response -> {
            assert response.getNodes().size() == 1;
            NodeInfo nodeInfo = response.getNodes().get(0);
            TransportInfo transportInfo = nodeInfo.getInfo(TransportInfo.class);
            HttpInfo httpInfo = nodeInfo.getInfo(HttpInfo.class);
            if (null == transportInfo || null == httpInfo) {
                if (backoff.hasNext()) {
                    LOGGER.debug("Local node's HTTP/transport info is not yet available, will retry...");
                    client.threadPool().schedule(() -> maybeCreateNodeEnrollmentToken(consumer, backoff), backoff.next(), GENERIC);
                } else {
                    LOGGER.warn("Unable to get local node's HTTP/transport info after all retries.");
                    consumer.accept(null);
                }
            } else {
                boolean noNonLocalTransportAddresses = splitAddresses(getAllTransportAddresses(transportInfo)).v2().isEmpty();
                if (noNonLocalTransportAddresses) {
                    LOGGER.info(
                        "Will not generate node enrollment token because node is only bound on localhost for transport "
                            + "and cannot connect to nodes from other hosts"
                    );
                    // empty enrollment token when skip
                    consumer.accept("");
                    return;
                }
                boolean noNonLocalHttpAddresses = splitAddresses(getAllHttpAddresses(httpInfo)).v2().isEmpty();
                if (noNonLocalHttpAddresses) {
                    LOGGER.info(
                        "Will not generate node enrollment token because node is only bound on localhost for HTTPS "
                            + "and cannot enroll nodes from other hosts"
                    );
                    // empty enrollment token when skip
                    consumer.accept("");
                    return;
                }
                LOGGER.debug("Attempting to generate the node enrollment token");
                assembleToken(EnrollmentTokenType.NODE, httpInfo, token -> {
                    if (null == token) {
                        consumer.accept(null);
                    } else {
                        try {
                            LOGGER.debug("Successfully generated the node enrollment token");
                            consumer.accept(token.getEncoded());
                        } catch (Exception e) {
                            LOGGER.error("Failed to encode node enrollment token", e);
                            // null enrollment token when error
                            consumer.accept(null);
                        }
                    }
                });
            }
        }, e -> {
            LOGGER.error("Failed to create node enrollment token when retrieving local node's HTTP/transport info", e);
            // null enrollment token when error
            consumer.accept(null);
        }));
    }

    /**
     * Creates an enrollment token for Kibana instances enrolling to the current node.
     * In case of errors, including due to issues with the node's configuration, a {@code null} token is returned, and the exception
     * is logged but no exception is thrown.
     */
    public void createKibanaEnrollmentToken(Consumer<EnrollmentToken> consumer, Iterator<TimeValue> backoff) {
        // the enrollment token can only be used against the node that generated it
        final NodesInfoRequest nodesInfoRequest = new NodesInfoRequest().nodesIds("_local")
            .addMetric(NodesInfoRequest.Metric.HTTP.metricName());
        client.execute(NodesInfoAction.INSTANCE, nodesInfoRequest, ActionListener.wrap(response -> {
            assert response.getNodes().size() == 1;
            NodeInfo nodeInfo = response.getNodes().get(0);
            HttpInfo httpInfo = nodeInfo.getInfo(HttpInfo.class);
            if (null == httpInfo) {
                if (backoff.hasNext()) {
                    LOGGER.info("Local node's HTTP info is not yet available, will retry...");
                    client.threadPool().schedule(() -> createKibanaEnrollmentToken(consumer, backoff), backoff.next(), GENERIC);
                } else {
                    LOGGER.warn("Unable to get local node's HTTP info after all retries.");
                    consumer.accept(null);
                }
            } else {
                assembleToken(EnrollmentTokenType.KIBANA, httpInfo, consumer);
            }
        }, e -> {
            LOGGER.error("Failed to create kibana enrollment token when retrieving local nodes HTTP info", e);
            consumer.accept(null);
        }));
    }

    private void assembleToken(EnrollmentTokenType enrollTokenType, HttpInfo httpInfo, Consumer<EnrollmentToken> consumer) {
        if (false == ENROLLMENT_ENABLED.get(environment.settings())) {
            LOGGER.error(
                "Cannot create enrollment token ["
                    + enrollTokenType
                    + "] because enrollment is disabled "
                    + "with setting ["
                    + ENROLLMENT_ENABLED.getKey()
                    + "] set to [false]"
            );
            consumer.accept(null);
            return;
        }
        final String fingerprint;
        try {
            fingerprint = getHttpsCaFingerprint();
        } catch (Exception e) {
            LOGGER.error(
                "Failed to create enrollment token when computing HTTPS CA fingerprint, possibly the certs are not auto-generated",
                e
            );
            consumer.accept(null);
            return;
        }
        final List<String> tokenAddresses;
        try {
            final List<String> httpAddressesList = getAllHttpAddresses(httpInfo);
            tokenAddresses = getFilteredAddresses(httpAddressesList);
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
            final EnrollmentToken enrollmentToken = new EnrollmentToken(apiKey, fingerprint, Version.CURRENT.toString(), tokenAddresses);
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
        collectAllAddresses(httpInfo.getAddress(), httpAddressesList);
        return httpAddressesList;
    }

    private static List<String> getAllTransportAddresses(TransportInfo transportInfo) {
        final List<String> transportAddressesList = new ArrayList<>();
        collectAllAddresses(transportInfo.getAddress(), transportAddressesList);
        transportInfo.getProfileAddresses().values().forEach(profileAddress -> collectAllAddresses(profileAddress, transportAddressesList));
        return transportAddressesList;
    }

    private static void collectAllAddresses(BoundTransportAddress address, List<String> collectedAddressesList) {
        collectedAddressesList.add(address.publishAddress().toString());
        Arrays.stream(address.boundAddresses()).map(TransportAddress::toString).forEach(collectedAddressesList::add);
    }

    private enum EnrollmentTokenType {
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
