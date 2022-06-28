/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.mockito;

import org.mockito.plugins.MockMaker;

import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.DomainCombiner;
import java.security.PrivilegedAction;
import java.security.ProtectionDomain;
import java.util.function.Supplier;

class SecureMockUtil {

    // we use the protection domain of mockito for wrapped calls so that
    // Elasticsearch server jar does not need additional permissions
    private static final AccessControlContext context = getContext();

    private static AccessControlContext getContext() {
        ProtectionDomain[] pda = new ProtectionDomain[] { wrap(MockMaker.class::getProtectionDomain) };
        DomainCombiner combiner = (current, assigned) -> pda;
        AccessControlContext acc = new AccessControlContext(AccessController.getContext(), combiner);
        // getContext must be called with the new acc so that a combined context will be created
        return AccessController.doPrivileged((PrivilegedAction<AccessControlContext>) AccessController::getContext, acc);
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
