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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.transport.RemoteClusterSecurityClusterCredential;
import org.elasticsearch.xpack.security.transport.RemoteClusterSecuritySubjectAccess;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.RCS_CLUSTER_CREDENTIAL_HEADER_KEY;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.RCS_ROLE_DESCRIPTORS_INTERSECTION_KEY;
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
                    final User rcsFcUser = Subject.buildRemoteClusterSecurityUser(qcAuthentication, fcApiKeyAuthcResult.getValue());

                    final Map<String, Object> rcsAuthenticationResultMetadata = new HashMap<>(fcApiKeyAuthcResult.getMetadata());
                    rcsAuthenticationResultMetadata.put(
                        RCS_ROLE_DESCRIPTORS_INTERSECTION_KEY,
                        Subject.buildRoleReferencesForRemoteClusterSecurityStatic(rcsFcUser.metadata())
                    );

                    listener.onResponse(AuthenticationResult.success(rcsFcUser, rcsAuthenticationResultMetadata));
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
        // Note: Empty config is OK. No deprecationLogger needed like in ApiKeyService.parseRoleDescriptorsBytes.
        return RoleDescriptor.parseRoleDescriptorsBytes(XContentParserConfiguration.EMPTY, bytesReference);
    }
}
