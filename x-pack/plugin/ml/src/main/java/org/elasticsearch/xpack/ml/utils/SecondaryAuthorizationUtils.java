/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.utils;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.support.SecondaryAuthentication;

import java.io.IOException;
import java.util.Map;

public final class SecondaryAuthorizationUtils {

    private SecondaryAuthorizationUtils() {}

    /**
     * Returns security headers preferring secondary auth if it exists.
     * For cloud API keys, this extracts the serverless authenticating token
     * and embeds it in the authentication metadata to support cross-project search.
     */
    public static Map<String, String> getSecurityHeadersPreferringSecondary(
        ThreadPool threadPool,
        SecurityContext securityContext,
        ClusterState clusterState
    ) {
        SetOnce<Map<String, String>> filteredHeadersHolder = new SetOnce<>();
        useSecondaryAuthIfAvailable(securityContext, () -> {
            final ThreadContext threadContext = threadPool.getThreadContext();
            final Authentication authentication = securityContext.getAuthentication();
            if (authentication != null && authentication.isCloudApiKey()) {
                Object t = threadContext.getTransient("_security_serverless_authenticating_token");
                if (t instanceof AuthenticationToken tok) {
                    Object creds = tok.credentials();
                    if (creds instanceof SecureString secure) {
                        String rawCredential = secure.toString();
                        try {
                            Map<String, String> filteredHeaders = Map.of(
                                AuthenticationField.AUTHENTICATION_KEY,
                                authentication.copyWithMetadataField(
                                    AuthenticationField.SECURITY_TASK_AUTHENTICATING_TOKEN_KEY,
                                    rawCredential
                                ).encode()
                            );
                            filteredHeadersHolder.set(filteredHeaders);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            } else {
                Map<String, String> filteredHeaders = ClientHelper.getPersistableSafeSecurityHeaders(threadContext, clusterState);
                filteredHeadersHolder.set(filteredHeaders);
            }
        });
        return filteredHeadersHolder.get();
    }

    /**
     * This executes the supplied runnable inside the secondary auth context if it exists;
     */
    public static void useSecondaryAuthIfAvailable(SecurityContext securityContext, Runnable runnable) {
        if (securityContext == null) {
            runnable.run();
            return;
        }
        SecondaryAuthentication secondaryAuth = securityContext.getSecondaryAuthentication();
        if (secondaryAuth != null) {
            runnable = secondaryAuth.wrap(runnable);
        }
        runnable.run();
    }

}
