/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;
import org.elasticsearch.xpack.core.security.authz.store.RoleReference;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.transport.RemoteClusterSecurityClusterCredential;
import org.elasticsearch.xpack.security.transport.RemoteClusterSecuritySubjectAccess;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.API_KEY_ID_KEY;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.RCS_CLUSTER_CREDENTIAL_HEADER_KEY;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.RCS_FC_API_KEY_ID_KEY;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.RCS_FC_ROLE_DESCRIPTORS_SETS_KEY;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.RCS_QC_ROLE_DESCRIPTORS_SETS_KEY;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.RCS_QC_SUBJECT_AUTHENTICATION_KEY;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.RCS_SUBJECT_ACCESS_HEADER_KEY;

public class RemoteClusterSecurityService {
    private static final Logger logger = LogManager.getLogger(RemoteClusterSecurityService.class);

    private final ApiKeyService apiKeyService;

    public RemoteClusterSecurityService(final ApiKeyService apiKeyService) {
        this.apiKeyService = apiKeyService;
    }

    // public record RemoteClusterSecurityClusterCredential(ApiKeyService.ApiKeyCredentials fcApiKeyCredentials) {
    //
    // private static ApiKeyService.ApiKeyCredentials decode(final String fcApiKey) {
    // return decode(new SecureString(fcApiKey.toCharArray()));
    // }
    //
    // private static ApiKeyService.ApiKeyCredentials decode(final SecureString fcApiKeySecureString) {
    // return ApiKeyUtil.toApiKeyCredentials(fcApiKeySecureString);
    // }
    //
    // private static String encode(final ApiKeyService.ApiKeyCredentials fcApiKey) {
    // return fcApiKey.toString();
    // }
    // final ApiKeyService.ApiKeyCredentials apiKeyCredentials = ApiKeyUtil.toApiKeyCredentials(fcClusterCredentials);

    RemoteClusterSecurityAuthenticationToken getRemoteClusterSecurityAuthenticationTokenFromThreadContextHeader(
        final ThreadContext threadContext
    ) {
        // Header contains an FC API Key from the QC, to be validated by the FC for cluster-level authentication and authorization
        final RemoteClusterSecurityClusterCredential fc = RemoteClusterSecurityClusterCredential.readFromContextHeader(threadContext);
        if (fc == null) {
            logger.debug("Header [{}] is absent", RCS_CLUSTER_CREDENTIAL_HEADER_KEY);
            return null;
        }
        logger.trace("Header [{}] is present", RCS_CLUSTER_CREDENTIAL_HEADER_KEY);
        // Header contains QC user access computed by the QC (i.e. Authentication and Authorization), only trusted if FC API Key is valid
        final RemoteClusterSecuritySubjectAccess qc;
        try {
            qc = RemoteClusterSecuritySubjectAccess.readFromThreadContextHeader(threadContext);
            if (qc == null) {
                logger.debug("Header [{}] is absent", RCS_SUBJECT_ACCESS_HEADER_KEY);
                return null;
            }
        } catch (IOException ex) {
            logger.warn("Header [{}] parsing failed", RCS_SUBJECT_ACCESS_HEADER_KEY, ex);
            return null;
        }
        logger.trace(
            "Header [{}] is present, QC Subject: [{}]",
            RCS_SUBJECT_ACCESS_HEADER_KEY,
            qc.authentication().getEffectiveSubject().toString()
        );
        return new RemoteClusterSecurityAuthenticationToken(qc, fc);
    }

    void tryAuthenticate(
        ThreadContext ctx,
        RemoteClusterSecurityAuthenticationToken remoteClusterSecurityAuthenticationToken,
        ActionListener<AuthenticationResult<User>> listener
    ) {
        // QC Authentication and Authorization
        final RemoteClusterSecuritySubjectAccess remoteClusterSecuritySubjectAccess = remoteClusterSecurityAuthenticationToken
            .getRemoteClusterSecuritySubjectAccess();
        if (remoteClusterSecuritySubjectAccess == null) {
            listener.onResponse(AuthenticationResult.unsuccessful("Missing QC RemoteClusterSecuritySubjectAccess", null));
            return;
        }
        final Authentication qcAuthentication = remoteClusterSecuritySubjectAccess.authentication();
        if (qcAuthentication == null) {
            listener.onResponse(AuthenticationResult.unsuccessful("Missing QC Authentication", null));
            return;
        }
        final RoleDescriptorsIntersection qcAuthorization = remoteClusterSecuritySubjectAccess.authorization();
        if (qcAuthorization == null) {
            listener.onResponse(AuthenticationResult.unsuccessful("Missing QC Authentication", null));
            return;
        }
        // FC credential Scheme (i.e. APIKey) and Value (i.e. Base64(id:secret))
        final RemoteClusterSecurityClusterCredential remoteClusterSecurityClusterCredential = remoteClusterSecurityAuthenticationToken
            .getRemoteClusterSecurityClusterCredential();
        if (remoteClusterSecurityClusterCredential == null) {
            listener.onResponse(AuthenticationResult.unsuccessful("Missing FC RemoteClusterSecurityClusterCredential", null));
            return;
        }
        final String scheme = remoteClusterSecurityClusterCredential.scheme();
        final SecureString value = remoteClusterSecurityClusterCredential.value();
        if (Strings.isEmpty(value)) {
            listener.onResponse(AuthenticationResult.unsuccessful("Missing FC cluster credentials value", null));
            return;
        }
        if (Strings.isEmpty(scheme)) {
            listener.onResponse(AuthenticationResult.unsuccessful("Missing FC cluster credentials scheme", null));
            return;
        }
        assert ApiKeyService.API_KEY_SCHEME.equals(scheme) : "Only ApiKey scheme is supported";
        final ApiKeyService.ApiKeyCredentials fcApiKeyCredentials = ApiKeyUtil.toApiKeyCredentials(value);
        if (fcApiKeyCredentials == null) {
            listener.onResponse(AuthenticationResult.unsuccessful("Missing FC ApiKeyService.ApiKeyCredentials", null));
            return;
        }
        // Try to authenticate FC API Key. If successful, use it to construct an FC user
        this.apiKeyService.loadApiKeyAndValidateCredentials(ctx, fcApiKeyCredentials, ActionListener.wrap(fcApiKeyAuthcResult -> {
            // TODO Success path only, handle auth failure
            final User rcsFcUser = buildRemoteClusterSecurityUser(qcAuthentication, qcAuthorization, fcApiKeyAuthcResult.getValue());
            listener.onResponse(AuthenticationResult.success(rcsFcUser, Collections.emptyMap()));
        }, listener::onFailure));
    }

    public static final class RemoteClusterSecurityAuthenticationToken implements AuthenticationToken, Closeable {
        private final RemoteClusterSecuritySubjectAccess qcRemoteClusterSecuritySubjectAccess;
        private final RemoteClusterSecurityClusterCredential fcRemoteClusterSecurityClusterCredential;

        public RemoteClusterSecurityAuthenticationToken(
            RemoteClusterSecuritySubjectAccess qcRemoteClusterSecuritySubjectAccess,
            RemoteClusterSecurityClusterCredential fcRemoteClusterSecurityClusterCredential
        ) {
            this.qcRemoteClusterSecuritySubjectAccess = qcRemoteClusterSecuritySubjectAccess;
            this.fcRemoteClusterSecurityClusterCredential = fcRemoteClusterSecurityClusterCredential;
        }

        RemoteClusterSecuritySubjectAccess getRemoteClusterSecuritySubjectAccess() {
            return this.qcRemoteClusterSecuritySubjectAccess;
        }

        RemoteClusterSecurityClusterCredential getRemoteClusterSecurityClusterCredential() {
            return this.fcRemoteClusterSecurityClusterCredential;
        }

        @Override
        public void clearCredentials() {
            close();
        }

        @Override
        public void close() {
            this.fcRemoteClusterSecurityClusterCredential.value();
        }

        @Override
        public String principal() {
            // TODO API Key ID????
            return this.qcRemoteClusterSecuritySubjectAccess.authentication().getEffectiveSubject().getUser().principal()
                + "/"
                + this.fcRemoteClusterSecurityClusterCredential.scheme();
        }

        @Override
        public Object credentials() {
            return this.fcRemoteClusterSecurityClusterCredential.value();
        }
    }

    public static List<RoleDescriptor> parseRoleDescriptorsBytes(final BytesReference bytesReference) {
        if (bytesReference == null) {
            return Collections.emptyList();
        }
        // Empty config is OK. No deprecationLogger needed like in ApiKeyService.parseRoleDescriptorsBytes.
        return RoleDescriptor.parseRoleDescriptorsBytes(XContentParserConfiguration.EMPTY, bytesReference);
    }

    public User buildRemoteClusterSecurityUser(
        final Authentication qcAuthentication,
        final RoleDescriptorsIntersection qcAuthorization,
        final User fcApiKeyUser
    ) throws IOException {
        final Map<String, Object> fcApiKeyMetadata = fcApiKeyUser.metadata();
        final String fcApiKeyId = (String) fcApiKeyMetadata.get(API_KEY_ID_KEY);
        final BytesReference fcRoleDescriptorsIntersectionBytes = getFcRoleDescriptorsIntersectionBytes(fcApiKeyMetadata);

        final User qcUser = qcAuthentication.getEffectiveSubject().getUser();
        final BytesReference qcRoleDescriptorsSetsBytes = (BytesReference) qcUser.metadata().get(RCS_QC_ROLE_DESCRIPTORS_SETS_KEY);
        assert qcRoleDescriptorsSetsBytes != null;

        final Map<String, Object> rcsFcMetadata = Map.of(
            RCS_QC_SUBJECT_AUTHENTICATION_KEY,
            qcAuthentication,
            RCS_FC_API_KEY_ID_KEY,
            fcApiKeyId,
            RCS_FC_ROLE_DESCRIPTORS_SETS_KEY,
            fcRoleDescriptorsIntersectionBytes,
            RCS_QC_ROLE_DESCRIPTORS_SETS_KEY,
            qcRoleDescriptorsSetsBytes
        );
        return new User(qcUser.principal(), Strings.EMPTY_ARRAY, qcUser.email(), qcUser.email(), rcsFcMetadata, true);
    }

    private BytesReference getFcRoleDescriptorsIntersectionBytes(final Map<String, Object> fcApiKeyMetadata) throws IOException {
        final String fcApiKeyId = (String) fcApiKeyMetadata.get(API_KEY_ID_KEY);
        assert fcApiKeyId != null;
        final BytesReference fcSubjectRoleDescriptorsBytes = (BytesReference) fcApiKeyMetadata.get(API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY);
        assert fcSubjectRoleDescriptorsBytes != null;
        final BytesReference fcApiKeyRoleDescriptorsBytes = (BytesReference) fcApiKeyMetadata.get(API_KEY_ROLE_DESCRIPTORS_KEY);

        final List<Set<RoleDescriptor>> roleDescriptorsIntersectionList = new ArrayList<>();
        roleDescriptorsIntersectionList.add(
            new LinkedHashSet<>(
                apiKeyService.parseRoleDescriptorsBytes(fcApiKeyId, fcSubjectRoleDescriptorsBytes, RoleReference.ApiKeyRoleType.ASSIGNED)
            )
        );
        if (fcApiKeyRoleDescriptorsBytes != null) {
            roleDescriptorsIntersectionList.add(
                new LinkedHashSet<>(
                    apiKeyService.parseRoleDescriptorsBytes(
                        fcApiKeyId,
                        fcApiKeyRoleDescriptorsBytes,
                        RoleReference.ApiKeyRoleType.LIMITED_BY
                    )
                )
            );
        }
        final RoleDescriptorsIntersection fcRoleDescriptorsIntersection = new RoleDescriptorsIntersection(roleDescriptorsIntersectionList);

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            fcRoleDescriptorsIntersection.writeTo(output);
            return output.bytes();
        }
    }
}
