/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.support.SecondaryAuthentication;

import java.util.Map;

public final class SecondaryAuthorizationUtils {

    private static final Logger logger = LogManager.getLogger(SecondaryAuthorizationUtils.class);

    private SecondaryAuthorizationUtils() {}

    /**
     * Returns security headers preferring secondary auth if it exists.
     */
    public static Map<String, String> getSecurityHeadersPreferringSecondary(
        ThreadPool threadPool,
        SecurityContext securityContext,
        ClusterState clusterState
    ) {
        SetOnce<Map<String, String>> filteredHeadersHolder = new SetOnce<>();
        useSecondaryAuthIfAvailable(securityContext, () -> {
            final ThreadContext threadContext = threadPool.getThreadContext();
            Map<String, String> filteredHeaders = ClientHelper.getPersistableSafeSecurityHeaders(threadContext, clusterState);
            filteredHeadersHolder.set(filteredHeaders);
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
        if (secondaryAuth == null) {
            runnable.run();
            return;
        }
        secondaryAuth.wrap(runnable).run();
    }
}
