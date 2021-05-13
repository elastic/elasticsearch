/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.enrollment;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.Tuple;
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
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Transport action responsible for creating an enrollment token based on a request.
 */

public class TransportCreateEnrollmentTokenAction
    extends HandledTransportAction<CreateEnrollmentTokenRequest, CreateEnrollmentTokenResponse> {

    protected static final long ENROLL_API_KEY_EXPIRATION_SEC = 30*60;

    private static final Logger logger = LogManager.getLogger(TransportCreateEnrollmentTokenAction.class);
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
        this(transportService, actionFilters, context,
            environment, nodeService, sslService, new ApiKeyGenerator(apiKeyService, rolesStore, xContentRegistry));
    }

    // Constructor for testing
    TransportCreateEnrollmentTokenAction(TransportService transportService, ActionFilters actionFilters, SecurityContext context,
                                         Environment environment, NodeService nodeService, SSLService sslService,
                                         ApiKeyGenerator generator) {
        super(CreateEnrollmentTokenAction.NAME, transportService, actionFilters, CreateEnrollmentTokenRequest::new);
        this.generator = generator;
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
            final List<Tuple<PrivateKey, X509Certificate>> httpCaKeysAndCertificates =
                ((StoreKeyConfig) keyConfig).getPrivateKeyEntries(environment).stream()
                    .filter(t -> t.v2().getBasicConstraints() != -1).collect(Collectors.toList());
            if (httpCaKeysAndCertificates.isEmpty()) {
                listener.onFailure(new IllegalStateException(
                    "Unable to create an enrollment token. Elasticsearch node HTTP layer SSL configuration Keystore doesn't contain any " +
                        "PrivateKey entries where the associated certificate is a CA certificate"));
                return;
            } else if (httpCaKeysAndCertificates.size() > 1) {
                listener.onFailure(new IllegalStateException(
                    "Unable to create an enrollment token. Elasticsearch node HTTP layer SSL configuration Keystore contain multiple " +
                        "PrivateKey entries where the associated certificate is a CA certificate"));
                return;
            } else {
                final TimeValue expiration = TimeValue.timeValueSeconds(ENROLL_API_KEY_EXPIRATION_SEC);
                final String[] clusterPrivileges = { "enroll" };
                final List<RoleDescriptor> roleDescriptors = List.of(new RoleDescriptor("create_enrollment_token", clusterPrivileges,
                    null, null));
                CreateApiKeyRequest apiRequest = new CreateApiKeyRequest("enrollment_token_API_key_" + UUIDs.base64UUID(),
                    roleDescriptors, expiration);
                final String fingerprint = SslUtil.calculateFingerprint(httpCaKeysAndCertificates.get(0).v2());
                generator.generateApiKey(securityContext.getAuthentication(), apiRequest,
                    ActionListener.wrap(
                        CreateApiKeyResponse -> {
                            try {
                                final String address = nodeService.info(false, false, false, false, false, false,
                                    true, false, false, false, false).getInfo(HttpInfo.class).getAddress().publishAddress().toString();
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
                            } catch (Exception e) {
                                logger.error(("Error generating enrollment token"), e);
                                listener.onFailure(e);
                            }
                        },
                        listener::onFailure
                    )
                );
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
