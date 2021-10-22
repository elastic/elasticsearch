/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.mockito;

import org.elasticsearch.cli.SuppressForbidden;

import java.net.URL;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.DomainCombiner;
import java.security.PrivilegedAction;
import java.security.ProtectionDomain;
import java.util.Arrays;
import java.util.function.Supplier;

class SecureMockUtil {

    private static final AccessControlContext context;
    static {
        // This combiner extracts the protection domain of mockito if it exists and
        // uses only that. Without it, the mock maker delegated calls would run with
        // the Elasticsearch test framework on the call stack. This effectively removes
        // the test framework from the call stack, so that we only need to grant privileges
        // to mockito itself.
        DomainCombiner combiner = (current, assigned) -> Arrays.stream(current)
            .filter(pd -> pd.getCodeSource() != null && getFilePath(pd.getCodeSource().getLocation()).contains("mockito-core"))
            .findFirst()
            .map(pd -> new ProtectionDomain[]{ pd })
            .orElse(current);

        AccessControlContext acc = new AccessControlContext(AccessController.getContext(), combiner);

        // getContext must be called with the new acc so that a combined context will be created
        context = AccessController.doPrivileged((PrivilegedAction<AccessControlContext>) AccessController::getContext, acc);
    }

    @SuppressForbidden(reason = "needed to get file path for comparison")
    private static String getFilePath(URL url) {
        return url.getFile();
    }

    // forces static init to run
    public static void init() {}

    // wrap the given call to play nice with SecurityManager
    static <T> T wrap(Supplier<T> call) {
        return AccessController.doPrivileged((PrivilegedAction<T>) call::get, context);
    }

    // no construction
    private SecureMockUtil() {}
}
