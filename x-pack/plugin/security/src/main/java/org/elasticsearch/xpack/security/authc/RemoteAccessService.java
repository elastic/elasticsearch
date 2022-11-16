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
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.transport.RemoteAccessControls;

import java.io.Closeable;
import java.io.IOException;

public class RemoteAccessService {
    private static final Logger logger = LogManager.getLogger(RemoteAccessService.class);

    private final ApiKeyService apiKeyService;

    public RemoteAccessService(final ApiKeyService apiKeyService) {
        this.apiKeyService = apiKeyService;
    }

    RemoteAccessCredentials getCredentialsFromHeader(final ThreadContext threadContext) {
        // Header `_remote_access_authentication` contains QC remote access controls (i.e. Authentication and Authorization instances)
        final RemoteAccessControls remoteAccessControls;
        try {
            remoteAccessControls = RemoteAccessControls.readFromContext(threadContext);
            if (remoteAccessControls == null) {
                logger.debug("Remote access authentication header is absent");
                return null;
            }
            logger.trace("Remote access authentication present [{}]", remoteAccessControls);
        } catch (IOException ex) {
            logger.warn("Remote access authentication parse failed", ex);
            return null;
        }
        // TODO Header `_remote_access_cluster_credential` contains an unverified FC API Key to be used for FC local access control
        final ApiKeyService.ApiKeyCredentials fcApiKeyCredentials = new ApiKeyService.ApiKeyCredentials(
            new String(""),
            new SecureString(new char[] {})
        );
        // Combine
        return new RemoteAccessCredentials(remoteAccessControls, fcApiKeyCredentials);
    }

    void tryAuthenticate(
        ThreadContext ctx,
        RemoteAccessCredentials remoteAccessCredentials,
        ActionListener<AuthenticationResult<User>> listener
    ) {
        this.apiKeyService.loadApiKeyAndValidateCredentials(ctx, remoteAccessCredentials.getApiKeyCredentials(), listener);
    }

    public static final class RemoteAccessCredentials implements AuthenticationToken, Closeable {
        private final RemoteAccessControls remoteAccessControls;
        private final ApiKeyService.ApiKeyCredentials fcApiKeyCredentials;

        public RemoteAccessCredentials(RemoteAccessControls remoteAccessControls, ApiKeyService.ApiKeyCredentials fcApiKeyCredentials) {
            this.remoteAccessControls = remoteAccessControls;
            this.fcApiKeyCredentials = fcApiKeyCredentials;
        }

        RemoteAccessControls getRemoteAccessQcControls() {
            return this.remoteAccessControls;
        }

        ApiKeyService.ApiKeyCredentials getApiKeyCredentials() {
            return this.fcApiKeyCredentials;
        }

        @Override
        public void clearCredentials() {
            close();
        }

        @Override
        public void close() {
            this.fcApiKeyCredentials.close();
        }

        @Override
        public String principal() {
            return this.remoteAccessControls.authentication().getEffectiveSubject().getUser().principal()
                + "/"
                + this.fcApiKeyCredentials.getId();
        }

        @Override
        public Object credentials() {
            return this.fcApiKeyCredentials;
        }
    }
}
