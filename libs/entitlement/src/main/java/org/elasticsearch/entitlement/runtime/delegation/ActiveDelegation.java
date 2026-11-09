/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.delegation;

/**
 * While it is active on a thread, entitlement checks whose caller belongs to {@code delegatingClass} use the entitlements of
 * {@code requestingClass}.
 * <p>
 * A class belongs to {@code delegatingClass} if its nest host is {@code delegatingClass}: that is the class itself, the
 * classes declared inside it, and the classes the JVM generates for their lambdas.
 *
 * @param delegatingClass the code that delegates; always its own nest host
 * @param requestingClass the class whose entitlements the checks use
 */
public record ActiveDelegation(Class<?> delegatingClass, Class<?> requestingClass) {

    public boolean appliesTo(Class<?> callerClass) {
        return callerClass == delegatingClass || callerClass.getNestHost() == delegatingClass;
    }
}
