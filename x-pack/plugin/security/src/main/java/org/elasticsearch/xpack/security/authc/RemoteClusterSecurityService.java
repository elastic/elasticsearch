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

    RemoteAccessAuthenticationToken getRemoteAccessAuthenticationTokenFromThreadContextHeader(final ThreadContext threadContext) {
        // Header contains FC API Key to be authenticated and authorized
        final RemoteClusterSecurityClusterCredential fc = RemoteClusterSecurityClusterCredential.readFromThreadContextHeader(threadContext);
        if (fc == null) {
            logger.debug("Header [{}] is absent", RCS_CLUSTER_CREDENTIAL_HEADER_KEY);
            return null;
        }
        logger.trace("Header [{}] is present, FC API Key ID: [{}]", RCS_CLUSTER_CREDENTIAL_HEADER_KEY, fc.fcApiKeyCredentials().getId());
        // Header contains QC user access (i.e. Authentication and Authorization)
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
        return new RemoteAccessAuthenticationToken(qc, fc);
    }

    void tryAuthenticate(
        ThreadContext ctx,
        RemoteAccessAuthenticationToken remoteAccessAuthenticationToken,
        ActionListener<AuthenticationResult<User>> listener
    ) {
        final RemoteClusterSecuritySubjectAccess remoteClusterSecuritySubjectAccess = remoteAccessAuthenticationToken
            .getRemoteClusterSecuritySubjectAccess();
        if (remoteClusterSecuritySubjectAccess == null) {
            listener.onResponse(AuthenticationResult.unsuccessful("Missing QC RemoteClusterSecuritySubjectAccess", null));
        } else {
            final Authentication qcAuthentication = remoteClusterSecuritySubjectAccess.authentication();
            if (remoteClusterSecuritySubjectAccess.authentication() == null) {
                listener.onResponse(AuthenticationResult.unsuccessful("Missing QC Authentication", null));
            } else {
                final RemoteClusterSecurityClusterCredential remoteClusterSecurityClusterCredential = remoteAccessAuthenticationToken
                    .getRemoteClusterSecurityClusterCredential();
                final ApiKeyService.ApiKeyCredentials fcApiKeyCredentials = remoteClusterSecurityClusterCredential.fcApiKeyCredentials();
                this.apiKeyService.loadApiKeyAndValidateCredentials(ctx, fcApiKeyCredentials, ActionListener.wrap(fcApiKeyAuthcResult -> {
                    // TODO Success path only, handle auth failure
                    final User rcsFcUser = buildRemoteClusterSecurityUser(qcAuthentication, fcApiKeyAuthcResult.getValue());
                    listener.onResponse(AuthenticationResult.success(rcsFcUser, Collections.emptyMap()));
                }, listener::onFailure));
            }
        }
    }

    public static final class RemoteAccessAuthenticationToken implements AuthenticationToken, Closeable {
        private final RemoteClusterSecuritySubjectAccess qcRemoteClusterSecuritySubjectAccess;
        private final RemoteClusterSecurityClusterCredential fcRemoteClusterSecurityClusterCredential;

        public RemoteAccessAuthenticationToken(
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
            this.fcRemoteClusterSecurityClusterCredential.fcApiKeyCredentials().close();
        }

        @Override
        public String principal() {
            return this.qcRemoteClusterSecuritySubjectAccess.authentication().getEffectiveSubject().getUser().principal()
                + "/"
                + this.fcRemoteClusterSecurityClusterCredential.fcApiKeyCredentials().getId();
        }

        @Override
        public Object credentials() {
            return this.fcRemoteClusterSecurityClusterCredential.fcApiKeyCredentials();
        }
    }

    public static List<RoleDescriptor> parseRoleDescriptorsBytes(final BytesReference bytesReference) {
        if (bytesReference == null) {
            return Collections.emptyList();
        }
        // Empty config is OK. No deprecationLogger needed like in ApiKeyService.parseRoleDescriptorsBytes.
        return RoleDescriptor.parseRoleDescriptorsBytes(XContentParserConfiguration.EMPTY, bytesReference);
    }

    public User buildRemoteClusterSecurityUser(final Authentication qcAuthentication, final User fcApiKeyUser) throws IOException {
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
