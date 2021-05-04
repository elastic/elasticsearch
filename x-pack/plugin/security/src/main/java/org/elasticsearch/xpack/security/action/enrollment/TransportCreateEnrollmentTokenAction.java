/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.enrollment;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.ssl.SslUtil;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.env.Environment;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.node.NodeService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.enrollment.CreateEnrollmentTokenAction;
import org.elasticsearch.xpack.core.security.action.enrollment.CreateEnrollmentTokenRequest;
import org.elasticsearch.xpack.core.security.action.enrollment.CreateEnrollmentTokenResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.ssl.KeyConfig;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.ssl.StoreKeyConfig;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authc.support.ApiKeyGenerator;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;

import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Transport action responsible for creating an enrollment token based on a request.
 */

public class TransportCreateEnrollmentTokenAction
    extends HandledTransportAction<CreateEnrollmentTokenRequest, CreateEnrollmentTokenResponse> {

    public static final long ENROLL_API_KEY_EXPIRATION_SEC = 30*60;

    private final ApiKeyGenerator generator;
    private final SecurityContext securityContext;
    private final Environment environment;
    private final NodeService nodeService;
    private final SSLService sslService;

    @Inject
    public TransportCreateEnrollmentTokenAction(TransportService transportService, ActionFilters actionFilters, ApiKeyService apiKeyService,
                                                SecurityContext context, CompositeRolesStore rolesStore,
                                                NamedXContentRegistry xContentRegistry, Environment environment, NodeService nodeService,
                                                SSLService sslService) {
        super(CreateEnrollmentTokenAction.NAME, transportService, actionFilters, CreateEnrollmentTokenRequest::new);
        this.generator = new ApiKeyGenerator(apiKeyService, rolesStore, xContentRegistry);
        this.securityContext = context;
        this.environment = environment;
        this.nodeService = nodeService;
        this.sslService = sslService;
    }

    @Override
    protected void doExecute(Task task, CreateEnrollmentTokenRequest request,
                             ActionListener<CreateEnrollmentTokenResponse> listener) {
        try {
            final KeyConfig keyConfig = sslService.getHttpTransportSSLConfiguration().keyConfig();
            if (keyConfig instanceof StoreKeyConfig == false) {
                listener.onFailure(new IllegalStateException(
                    "Unable to create an enrollment token. Elasticsearch node HTTP layer SSL configuration is not configured with a " +
                        "keystore"));
                return;
            }
            final List<X509Certificate> caCertificates = ((StoreKeyConfig) keyConfig).x509Certificates(environment).stream()
                .filter(x509Certificate -> x509Certificate.getBasicConstraints() != -1).collect(Collectors.toList());
            if (caCertificates.size() != 1) {
                listener.onFailure(new IllegalStateException(
                    "Unable to create an enrollment token. Elasticsearch node HTTP layer SSL configuration Keystore doesn't contain a " +
                    "single PrivateKey entry where the associated certificate is a CA certificate"));

            } else {
                final TimeValue expiration = TimeValue.timeValueSeconds(ENROLL_API_KEY_EXPIRATION_SEC);
                final List<RoleDescriptor> roleDescriptors = new ArrayList<>(1);
                final String[] clusterPrivileges = { "enroll" };
                final RoleDescriptor roleDescriptor = new RoleDescriptor("create_enrollment_token", clusterPrivileges, null, null);
                roleDescriptors.add(roleDescriptor);
                CreateApiKeyRequest apiRequest = new CreateApiKeyRequest("enrollment_token_API_key_" + UUIDs.base64UUID(),
                    roleDescriptors, expiration);
                final String fingerprint = SslUtil.calculateFingerprint(caCertificates.get(0));
                createEnrolmentToken(fingerprint, apiRequest, listener);
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void createEnrolmentToken(String fingerprint, CreateApiKeyRequest apiRequest,
                                      ActionListener<CreateEnrollmentTokenResponse> listener) {
        try {
            generator.generateApiKey(securityContext.getAuthentication(), apiRequest,
                ActionListener.wrap(
                    CreateApiKeyResponse -> {
                        final NodeInfo nodeInfo = nodeService.info(false, false, false, false, false, false,
                            true, false, false, false, false);
                        final HttpInfo httpInfo = nodeInfo.getInfo(HttpInfo.class);
                        final String address = httpInfo.getAddress().publishAddress().toString();

                        final XContentBuilder builder = JsonXContent.contentBuilder();
                        builder.startObject();
                        builder.field("adr", address);
                        builder.field("fgr", fingerprint);
                        builder.field("key", CreateApiKeyResponse.getKey().toString());
                        builder.endObject();
                        final String jsonString = Strings.toString(builder);
                        final String token = Base64.getEncoder().encodeToString(jsonString.getBytes(StandardCharsets.UTF_8));

                        final CreateEnrollmentTokenResponse response = new CreateEnrollmentTokenResponse(token);
                        listener.onResponse(response);
                    },
                    listener::onFailure
                )
            );
        } catch (Exception e) {
            logger.error(() -> new ParameterizedMessage("Error generating enrollment token"), e);
            listener.onFailure(e);
        }
    }
}
