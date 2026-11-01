/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.utils;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.support.SecondaryAuthentication;

import java.util.Map;
import java.util.function.Supplier;

public final class SecondaryAuthorizationUtils {

    private SecondaryAuthorizationUtils() {}

    /**
     * Returns security headers preferring secondary auth if it exists.
     */
    public static Map<String, String> getSecurityHeadersPreferringSecondary(
        ThreadPool threadPool,
        SecurityContext securityContext,
        ClusterState clusterState
    ) {
        return useSecondaryAuthIfAvailable(
            securityContext,
            () -> ClientHelper.getPersistableSafeSecurityHeaders(threadPool.getThreadContext(), clusterState)
        );
    }

    /**
     * This executes the supplied runnable inside the secondary auth context if it exists;
     */
    public static void useSecondaryAuthIfAvailable(SecurityContext securityContext, Runnable runnable) {
        useSecondaryAuthIfAvailable(securityContext, () -> {
            runnable.run();
            return null;
        });
    }

    /**
     * Value-returning variant: executes {@code body} inside the secondary auth context when one is
     * present (and security is enabled), otherwise runs it on the current context, and returns its
     * result. Mirrors {@link SecondaryAuthentication#execute(java.util.function.Function)} so callers
     * can extract a value out of the swapped context without a {@code SetOnce} holder.
     */
    public static <T> T useSecondaryAuthIfAvailable(SecurityContext securityContext, Supplier<T> body) {
        if (securityContext == null) {
            return body.get();
        }
        SecondaryAuthentication secondaryAuth = securityContext.getSecondaryAuthentication();
        if (secondaryAuth == null) {
            return body.get();
        }
        return secondaryAuth.execute(ignore -> body.get());
    }
}
