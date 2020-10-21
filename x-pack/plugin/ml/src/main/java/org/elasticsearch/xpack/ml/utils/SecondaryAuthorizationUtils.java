/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.utils;

import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.support.SecondaryAuthentication;

public final class SecondaryAuthorizationUtils {

    private SecondaryAuthorizationUtils() {}

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
