/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.delegation;

import org.elasticsearch.core.CheckedSupplier;

/**
 * Holds the {@link ActiveDelegation} of the current thread. {@link #call} sets it while an operation runs, and entitlement
 * checks read it with {@link #current()}. The thread that called {@link #call} sees it, and so do the threads that a
 * {@code StructuredTaskScope} opened inside {@link #call} forks.
 * <p>
 * This version, used on Java 25 and later, uses a {@link ScopedValue}. Earlier Java versions use a version based on
 * {@link ThreadLocal}.
 */
public final class DelegationHolder {
    private static final ScopedValue<ActiveDelegation> CURRENT = ScopedValue.newInstance();

    private DelegationHolder() {}

    /**
     * @return the delegation passed to the innermost {@link #call} running on this thread, or {@code null} if there is none
     */
    public static ActiveDelegation current() {
        return CURRENT.isBound() ? CURRENT.get() : null;
    }

    /**
     * Runs {@code op}. Until {@code op} finishes, {@link #current()} returns {@code delegation} on this thread.
     */
    public static <T, E extends Exception> T call(ActiveDelegation delegation, CheckedSupplier<T, E> op) throws E {
        return ScopedValue.where(CURRENT, delegation).call(op::get);
    }
}
