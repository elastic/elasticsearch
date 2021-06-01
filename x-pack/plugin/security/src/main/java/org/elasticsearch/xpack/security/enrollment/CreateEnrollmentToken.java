/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.enrollment;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.ssl.SslUtil;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.env.Environment;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.node.NodeService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
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

public class CreateEnrollmentToken {
    protected static final long ENROLL_API_KEY_EXPIRATION_SEC = 30*60;

    private static final Logger logger = LogManager.getLogger(CreateEnrollmentToken.class);
    private final ApiKeyGenerator generator;
    private final SecurityContext securityContext;
    private final Environment environment;
    private final NodeService nodeService;
    private final SSLService sslService;

    public CreateEnrollmentToken(ApiKeyService apiKeyService,
                                 SecurityContext context, CompositeRolesStore rolesStore,
                                 NamedXContentRegistry xContentRegistry, Environment environment, NodeService nodeService,
                                 SSLService sslService) {
        this(new ApiKeyGenerator(apiKeyService, rolesStore, xContentRegistry), context, environment, nodeService,
            sslService);
    }

    // protected for testing
    protected CreateEnrollmentToken(ApiKeyGenerator generator,
                                 SecurityContext context, Environment environment, NodeService nodeService,
                                 SSLService sslService) {
        this.generator = generator;
        this.securityContext = context;
        this.environment = environment;
        this.nodeService = nodeService;
        this.sslService = sslService;
    }

    protected String create() throws Exception {
        final KeyConfig keyConfig = sslService.getHttpTransportSSLConfiguration().keyConfig();
        if (keyConfig instanceof StoreKeyConfig == false) {
            throw new IllegalStateException(
                "Unable to create an enrollment token. Elasticsearch node HTTP layer SSL configuration is not configured with a keystore");
        }
        final List<Tuple<PrivateKey, X509Certificate>> httpCaKeysAndCertificates =
            ((StoreKeyConfig) keyConfig).getPrivateKeyEntries(environment).stream()
                .filter(t -> t.v2().getBasicConstraints() != -1).collect(Collectors.toList());
        if (httpCaKeysAndCertificates.isEmpty()) {
            throw new IllegalStateException(
                "Unable to create an enrollment token. Elasticsearch node HTTP layer SSL configuration Keystore doesn't contain any " +
                    "PrivateKey entries where the associated certificate is a CA certificate");
        } else if (httpCaKeysAndCertificates.size() > 1) {
            throw new IllegalStateException(
                "Unable to create an enrollment token. Elasticsearch node HTTP layer SSL configuration Keystore contain multiple " +
                    "PrivateKey entries where the associated certificate is a CA certificate");
        } else {
            final TimeValue expiration = TimeValue.timeValueSeconds(ENROLL_API_KEY_EXPIRATION_SEC);
            final String[] clusterPrivileges = {ClusterPrivilegeResolver.ENROLL_NODE.name()};
            final List<RoleDescriptor> roleDescriptors = List.of(new RoleDescriptor("create_enrollment_token", clusterPrivileges,
                null, null));
            CreateApiKeyRequest apiRequest = new CreateApiKeyRequest("enrollment_token_API_key_" + UUIDs.base64UUID(),
                roleDescriptors, expiration);
            final String fingerprint = SslUtil.calculateFingerprint(httpCaKeysAndCertificates.get(0).v2());
            PlainActionFuture<CreateApiKeyResponse> future = new PlainActionFuture<>();
            generator.generateApiKey(securityContext.getAuthentication(), apiRequest, future);

            try {
                final TransportAddress address = nodeService.info(false, false, false, false, false, false,
                    true, false, false, false, false).getInfo(HttpInfo.class).getAddress().publishAddress();
                final String address_formatted = NetworkAddress.format(address.address());
                final XContentBuilder builder = JsonXContent.contentBuilder();
                builder.startObject();
                builder.field("v", 0);
                builder.field("adr", address_formatted);
                builder.field("fgr", fingerprint);
                builder.field("key", future.actionGet().getKey().toString());//  CreateApiKeyResponse.getKey().toString());
                builder.endObject();
                final String jsonString = Strings.toString(builder);
                final String token = Base64.getUrlEncoder().encodeToString(jsonString.getBytes(StandardCharsets.UTF_8));
                return token;
            } catch (Exception e) {
                logger.error(("Error generating enrollment token"), e);
                throw new IllegalStateException(("Error generating enrollment token"), e);
            }
        }
    }
}
