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
import org.elasticsearch.xpack.security.transport.RemoteAccessQcControls;

import java.io.Closeable;
import java.io.IOException;

public class CrossClusterService {
    private static final Logger logger = LogManager.getLogger(CrossClusterService.class);

    private final ApiKeyService apiKeyService;

    public CrossClusterService(final ApiKeyService apiKeyService) {
        this.apiKeyService = apiKeyService;
    }

    CrossClusterCredentials getCredentialsFromHeader(ThreadContext threadContext) {
        // Header _remote_access_authentication
        final RemoteAccessQcControls remoteAccessQcControls; // Successful QC Authentication and Authorization details
        try {
            remoteAccessQcControls = RemoteAccessQcControls.readFromContext(threadContext);
            if (remoteAccessQcControls == null) {
                logger.info("Remote access authentication header is absent");
                return null;
            }
            logger.info("Remote access authentication present [{}]", remoteAccessQcControls);
        } catch (IOException ex) {
            logger.error("Remote access authentication parse failed", ex);
            return null;
        }
        final String qcPrincipal = remoteAccessQcControls.authentication().getEffectiveSubject().getUser().principal();
        // TODO Header _remote_access_cluster_credential
        final ApiKeyService.ApiKeyCredentials fcApiKeyCredentials = new ApiKeyService.ApiKeyCredentials(
            new String(""),
            new SecureString(new char[] {})
        );
        // Combine
        return new CrossClusterCredentials(qcPrincipal, fcApiKeyCredentials);
    }

    void tryAuthenticate(
        ThreadContext ctx,
        CrossClusterCredentials crossClusterCredentials,
        ActionListener<AuthenticationResult<User>> listener
    ) {
        this.apiKeyService.loadApiKeyAndValidateCredentials(ctx, crossClusterCredentials.getApiKeyCredentials(), listener);
    }

    public static final class CrossClusterCredentials implements AuthenticationToken, Closeable {
        private final String qcPrincipal;
        private final ApiKeyService.ApiKeyCredentials fcApiKeyCredentials;

        public CrossClusterCredentials(String qcPrincipal, ApiKeyService.ApiKeyCredentials fcApiKeyCredentials) {
            this.qcPrincipal = qcPrincipal;
            this.fcApiKeyCredentials = fcApiKeyCredentials;
        }

        String getQcPrincipal() {
            return this.qcPrincipal;
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
            return this.qcPrincipal;
        }

        @Override
        public Object credentials() {
            return this.fcApiKeyCredentials;
        }
    }
}
