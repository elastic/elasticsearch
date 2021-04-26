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
import org.elasticsearch.xpack.core.enrollment.CreateEnrollmentTokenAction;
import org.elasticsearch.xpack.core.enrollment.CreateEnrollmentTokenRequest;
import org.elasticsearch.xpack.core.enrollment.CreateEnrollmentTokenResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authc.support.ApiKeyGenerator;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

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

    @Inject
    public TransportCreateEnrollmentTokenAction(TransportService transportService, ActionFilters actionFilters, ApiKeyService apiKeyService,
                                                SecurityContext context, CompositeRolesStore rolesStore,
                                                NamedXContentRegistry xContentRegistry, Environment environment, NodeService nodeService) {
        super(CreateEnrollmentTokenAction.NAME, transportService, actionFilters, CreateEnrollmentTokenRequest::new);
        this.generator = new ApiKeyGenerator(apiKeyService, rolesStore, xContentRegistry);
        this.securityContext = context;
        this.environment = environment;
        this.nodeService = nodeService;
    }

    @Override
    protected void doExecute(Task task, CreateEnrollmentTokenRequest request,
                             ActionListener<CreateEnrollmentTokenResponse> listener) {
        createEnrolmentToken(listener);
    }

    private void createEnrolmentToken(ActionListener<CreateEnrollmentTokenResponse> listener) {
        try {
            final TimeValue expiration = TimeValue.timeValueSeconds(ENROLL_API_KEY_EXPIRATION_SEC);
            final List<RoleDescriptor> roleDescriptors = new ArrayList<>(1);
            final String[] clusterPrivileges = { "enroll" };
            final RoleDescriptor roleDescriptor = new RoleDescriptor("create_enrollment_token", clusterPrivileges, null, null);
            roleDescriptors.add(roleDescriptor);
            CreateApiKeyRequest apiRequest = new CreateApiKeyRequest("enrollment_token_API_key_" + UUIDs.base64UUID(),
                roleDescriptors, expiration);
            generator.generateApiKey(securityContext.getAuthentication(), apiRequest,
                ActionListener.wrap(
                    CreateApiKeyResponse -> {;
                        final String httpCaCert = "httpCa.pem";
                        final Path httpCaCertPath = environment.configFile().resolve(httpCaCert);
                        if (Files.exists(httpCaCertPath) == false) {
                            listener.onFailure(new IllegalStateException("HTTP layer CA certificate " + httpCaCert + " does not exist"));
                            return;
                        }
                        final NodeInfo nodeInfo = nodeService.info(false, false, false, false, false, false,
                            true, false, false, false, false);
                        final HttpInfo httpInfo = nodeInfo.getInfo(HttpInfo.class);
                        final String address = httpInfo.getAddress().publishAddress().toString();

                        final X509Certificate[] certificates = CertParsingUtils.readX509Certificates(List.of(httpCaCertPath));
                        final X509Certificate cert = certificates[0];
                        final String fingerprint = SslUtil.calculateFingerprint(cert);

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
