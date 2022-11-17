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
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.transport.RemoteClusterSecurityClusterCredential;
import org.elasticsearch.xpack.security.transport.RemoteClusterSecuritySubjectAccess;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class RemoteClusterSecurityService {
    private static final Logger logger = LogManager.getLogger(RemoteClusterSecurityService.class);

    private final ApiKeyService apiKeyService;

    public RemoteClusterSecurityService(final ApiKeyService apiKeyService) {
        this.apiKeyService = apiKeyService;
    }

    RemoteAccessAuthenticationToken getCredentialsFromHeader(final ThreadContext threadContext) {
        // Header `_remote_access_cluster_credential` contains FC API Key to be authenticated and authorized
        final RemoteClusterSecurityClusterCredential fc = RemoteClusterSecurityClusterCredential.readFromContext(threadContext);
        if (fc == null) {
            logger.debug("Header [{}] is absent", AuthenticationField.REMOTE_CLUSTER_SECURITY_CLUSTER_CREDENTIAL_HEADER_KEY);
            return null;
        } else {
            logger.trace(
                "Header [{}] is present [{}]",
                AuthenticationField.REMOTE_CLUSTER_SECURITY_CLUSTER_CREDENTIAL_HEADER_KEY,
                fc.fcApiKeyCredentials().getId()
            );
        }
        // Header `_remote_access_authentication` contains QC user access (i.e. Authentication and Authorization)
        final RemoteClusterSecuritySubjectAccess qc;
        try {
            qc = RemoteClusterSecuritySubjectAccess.readFromContext(threadContext);
            if (qc == null) {
                logger.debug("Header [{}] is absent", AuthenticationField.REMOTE_CLUSTER_SECURITY_SUBJECT_ACCESS_HEADER_KEY);
                return null;
            }
            logger.trace(
                "Header [{}] is present [{}]",
                AuthenticationField.REMOTE_CLUSTER_SECURITY_SUBJECT_ACCESS_HEADER_KEY,
                qc.authentication().getEffectiveSubject()
            );
        } catch (IOException ex) {
            logger.warn("Header [{}] parsing failed", AuthenticationField.REMOTE_CLUSTER_SECURITY_SUBJECT_ACCESS_HEADER_KEY, ex);
            return null;
        }
        return new RemoteAccessAuthenticationToken(qc, fc);
    }

    void tryAuthenticate(
        ThreadContext ctx,
        RemoteAccessAuthenticationToken remoteAccessAuthenticationToken,
        ActionListener<AuthenticationResult<User>> listener
    ) {
        final RemoteClusterSecuritySubjectAccess remoteClusterSecuritySubjectAccess = remoteAccessAuthenticationToken
            .getRemoteAccessControls();
        if (remoteClusterSecuritySubjectAccess == null) {
            listener.onResponse(AuthenticationResult.unsuccessful("Missing RemoteAccessUserAccess instance", null));
        } else if (remoteClusterSecuritySubjectAccess.authentication() == null) {
            listener.onResponse(AuthenticationResult.unsuccessful("Missing Authentication instance", null));
        } else if (remoteClusterSecuritySubjectAccess.authorization() == null) {
            listener.onResponse(AuthenticationResult.unsuccessful("Missing RoleDescriptorsBytesIntersection instance", null));
        } else {
            final RemoteClusterSecurityClusterCredential remoteClusterSecurityClusterCredential = remoteAccessAuthenticationToken
                .getRemoteAccessClusterCredential();
            final ApiKeyService.ApiKeyCredentials credentials = remoteClusterSecurityClusterCredential.fcApiKeyCredentials();
            this.apiKeyService.loadApiKeyAndValidateCredentials(ctx, credentials, ActionListener.wrap(authRes -> {
                // TODO handle auth failure
                final User apiKeyUser = authRes.getValue();
                final User user = new User(apiKeyUser.principal(), Strings.EMPTY_ARRAY, "fullName", "email", apiKeyUser.metadata(), true);
                final Map<String, Object> authResultMetadata = new HashMap<>(authRes.getMetadata());
                authResultMetadata.put(
                    AuthenticationField.REMOTE_CLUSTER_SECURITY_ROLE_DESCRIPTORS_INTERSECTION_KEY,
                    remoteClusterSecuritySubjectAccess.authorization()
                );
                listener.onResponse(AuthenticationResult.success(user, authResultMetadata));
            }, listener::onFailure));
        }
    }

    public static final class RemoteAccessAuthenticationToken implements AuthenticationToken, Closeable {
        private final RemoteClusterSecuritySubjectAccess remoteClusterSecuritySubjectAccess;
        private final RemoteClusterSecurityClusterCredential fcRemoteClusterSecurityClusterCredential;

        public RemoteAccessAuthenticationToken(
            RemoteClusterSecuritySubjectAccess remoteClusterSecuritySubjectAccess,
            RemoteClusterSecurityClusterCredential fcRemoteClusterSecurityClusterCredential
        ) {
            this.remoteClusterSecuritySubjectAccess = remoteClusterSecuritySubjectAccess;
            this.fcRemoteClusterSecurityClusterCredential = fcRemoteClusterSecurityClusterCredential;
        }

        RemoteClusterSecuritySubjectAccess getRemoteAccessControls() {
            return this.remoteClusterSecuritySubjectAccess;
        }

        RemoteClusterSecurityClusterCredential getRemoteAccessClusterCredential() {
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
            return this.remoteClusterSecuritySubjectAccess.authentication().getEffectiveSubject().getUser().principal()
                + "/"
                + this.fcRemoteClusterSecurityClusterCredential.fcApiKeyCredentials().getId();
        }

        @Override
        public Object credentials() {
            return this.fcRemoteClusterSecurityClusterCredential.fcApiKeyCredentials();
        }
    }
}
