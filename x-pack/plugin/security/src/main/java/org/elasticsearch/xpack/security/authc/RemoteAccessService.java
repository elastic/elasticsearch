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
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.transport.RemoteAccessClusterCredential;
import org.elasticsearch.xpack.security.transport.RemoteAccessUserAccess;

import java.io.Closeable;
import java.io.IOException;

public class RemoteAccessService {
    private static final Logger logger = LogManager.getLogger(RemoteAccessService.class);

    private final ApiKeyService apiKeyService;

    public RemoteAccessService(final ApiKeyService apiKeyService) {
        this.apiKeyService = apiKeyService;
    }

    RemoteAccessAuthenticationToken getCredentialsFromHeader(final ThreadContext threadContext) {
        // Header `_remote_access_authentication` contains QC remote access controls (i.e. Authentication and Authorization instances)
        final RemoteAccessUserAccess remoteAccessUserAccess = getRemoteAccessControls(threadContext);
        if (remoteAccessUserAccess == null) {
            return null;
        }
        // Header `_remote_access_cluster_credential` contains an unverified FC API Key to be used for FC local access control
        final RemoteAccessClusterCredential remoteAccessClusterCredential = RemoteAccessClusterCredential.readFromContext(threadContext);
        if (remoteAccessClusterCredential == null) {
            return null;
        }
        return new RemoteAccessAuthenticationToken(remoteAccessUserAccess, remoteAccessClusterCredential);
    }

    private static RemoteAccessUserAccess getRemoteAccessControls(ThreadContext threadContext) {
        final RemoteAccessUserAccess remoteAccessUserAccess;
        try {
            remoteAccessUserAccess = RemoteAccessUserAccess.readFromContext(threadContext);
            if (remoteAccessUserAccess == null) {
                logger.debug("Remote access authentication header is absent");
                return null;
            }
            logger.trace("Remote access authentication present [{}]", remoteAccessUserAccess);
        } catch (IOException ex) {
            logger.warn("Remote access authentication parse failed", ex);
            return null;
        }
        return remoteAccessUserAccess;
    }

    void tryAuthenticate(
        ThreadContext ctx,
        RemoteAccessAuthenticationToken remoteAccessAuthenticationToken,
        ActionListener<AuthenticationResult<User>> listener
    ) {
        final RemoteAccessUserAccess remoteAccessUserAccess = remoteAccessAuthenticationToken.getRemoteAccessControls();
        if (remoteAccessUserAccess == null) {
            listener.onResponse(AuthenticationResult.unsuccessful("Missing RemoteAccessUserAccess instance", null));
        } else if (remoteAccessUserAccess.authentication() == null) {
            listener.onResponse(AuthenticationResult.unsuccessful("Missing Authentication instance", null));
        } else if (remoteAccessUserAccess.authorization() == null) {
            listener.onResponse(AuthenticationResult.unsuccessful("Missing RoleDescriptorsBytesIntersection instance", null));
        } else {
            final RemoteAccessClusterCredential remoteAccessClusterCredential = remoteAccessAuthenticationToken
                .getRemoteAccessClusterCredential();
            final ApiKeyService.ApiKeyCredentials credentials = remoteAccessClusterCredential.fcApiKeyCredentials();
            this.apiKeyService.loadApiKeyAndValidateCredentials(ctx, credentials, listener);
        }
    }

    public static final class RemoteAccessAuthenticationToken implements AuthenticationToken, Closeable {
        private final RemoteAccessUserAccess remoteAccessUserAccess;
        private final RemoteAccessClusterCredential fcRemoteAccessClusterCredential;

        public RemoteAccessAuthenticationToken(
            RemoteAccessUserAccess remoteAccessUserAccess,
            RemoteAccessClusterCredential fcRemoteAccessClusterCredential
        ) {
            this.remoteAccessUserAccess = remoteAccessUserAccess;
            this.fcRemoteAccessClusterCredential = fcRemoteAccessClusterCredential;
        }

        RemoteAccessUserAccess getRemoteAccessControls() {
            return this.remoteAccessUserAccess;
        }

        RemoteAccessClusterCredential getRemoteAccessClusterCredential() {
            return this.fcRemoteAccessClusterCredential;
        }

        @Override
        public void clearCredentials() {
            close();
        }

        @Override
        public void close() {
            this.fcRemoteAccessClusterCredential.fcApiKeyCredentials().close();
        }

        @Override
        public String principal() {
            return this.remoteAccessUserAccess.authentication().getEffectiveSubject().getUser().principal()
                + "/"
                + this.fcRemoteAccessClusterCredential.fcApiKeyCredentials().getId();
        }

        @Override
        public Object credentials() {
            return this.fcRemoteAccessClusterCredential.fcApiKeyCredentials();
        }
    }
}
