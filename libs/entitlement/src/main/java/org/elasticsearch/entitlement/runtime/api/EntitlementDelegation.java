/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.api;

import org.elasticsearch.core.CheckedSupplier;
import org.elasticsearch.core.Strings;
import org.elasticsearch.entitlement.runtime.delegation.ActiveDelegation;
import org.elasticsearch.entitlement.runtime.delegation.DelegationHolder;
import org.elasticsearch.entitlement.runtime.delegation.DelegationPolicy;
import org.elasticsearch.entitlement.runtime.delegation.DelegationRuntime;

import java.util.Iterator;

import static java.lang.StackWalker.Option.RETAIN_CLASS_REFERENCE;

/**
 * Delegates entitlement checks: code in {@code server} called from another component uses the entitlements of that component.
 * <p>
 * An entitlement check normally uses the entitlements of the class that calls the instrumented JDK method. When a server utility
 * does a sensitive operation on behalf of a plugin, the check uses the entitlements of server, not those of the plugin. An
 * {@code EntitlementDelegation} changes that: the checks made while {@link #call} runs use the entitlements of caller
 * class ({@link #asCaller()}), or of the class that created an instance of it ({@link #asCreator()}).
 * <p>
 * Checks are delegated for the class that obtains the {@code EntitlementDelegation} and for the classes declared
 * inside it and their lambdas. Only classes in the server layer may delegate (see {@code PolicyManager#canDelegate}). In
 * production this package is exported only to the server module, so no other code can use this API.
 * <p>
 * Delegation is transitive: if the class whose entitlements would be used is itself running under delegation, the
 * entitlements of that delegation are used instead.
 */
public abstract sealed class EntitlementDelegation permits EntitlementDelegation.AsCaller, EntitlementDelegation.AsCreator {

    private static final StackWalker WALKER = StackWalker.getInstance(RETAIN_CLASS_REFERENCE);

    final Class<?> delegatingClass;

    private EntitlementDelegation(Class<?> delegatingClass) {
        this.delegatingClass = delegatingClass;
    }

    /**
     * Creates a delegation to the caller: on each {@link #call}, the checks use the entitlements of the class that calls the
     * delegating class. Usually stored in a {@code private static final} field.
     *
     * @throws IllegalCallerException if the calling class is not in the server layer. Possible only when Java modules are
     * not active (e.g. in tests): when this code runs modularized, the module export already prevents other classes
     * from calling this method.
     */
    public static EntitlementDelegation asCaller() {
        Class<?> delegatingClass = WALKER.getCallerClass().getNestHost();
        DelegationPolicy policy = DelegationRuntime.policy();
        if (policy != null) {
            requireCanDelegate(policy, delegatingClass);
        }
        return new AsCaller(delegatingClass);
    }

    /**
     * Creates a delegation to the creator: on each {@link #call}, the checks use the entitlements of the class that created the instance.
     * It must be stored in an instance field, initialized in a constructor or a field initializer.
     * <p>
     * The creator is the first class on the stack outside the delegating class when the instance is constructed.
     * Typically, this is the class calling the constructor.
     * Beware of subclasses: when a subclass declared outside the delegating class is constructed, its own constructor
     * is that first frame, so the subclass is the creator. Avoid using this mechanism with complex class hierarchies.
     *
     * @throws IllegalStateException if it is not called from a constructor
     * @throws IllegalCallerException if the calling class is not in the server layer. Possible only when Java modules are
     * not active (e.g. in tests): when this code runs modularized, the module export already prevents other classes
     * from calling this method.
     */
    public static EntitlementDelegation asCreator() {
        return WALKER.walk(frames -> {
            Iterator<StackWalker.StackFrame> callers = frames.skip(1).iterator();
            StackWalker.StackFrame constructor = callers.next();
            if (constructor.getMethodName().equals("<init>") == false) {
                throw new IllegalStateException(
                    Strings.format(
                        "EntitlementDelegation.asCreator() must be called from a constructor or an instance field initializer, "
                            + "not from [%s.%s]",
                        constructor.getClassName(),
                        constructor.getMethodName()
                    )
                );
            }
            Class<?> delegatingClass = constructor.getDeclaringClass().getNestHost();
            DelegationPolicy policy = DelegationRuntime.policy();
            if (policy == null) {
                return new AsCreator(delegatingClass, delegatingClass);
            }
            requireCanDelegate(policy, delegatingClass);
            return new AsCreator(delegatingClass, findRequestingClass(policy, delegatingClass, callers));
        });
    }

    /**
     * Runs {@code op}. While it runs, the entitlement checks made by the code of the delegating class on this thread use the
     * entitlements of the class this delegation was created for.
     */
    public final <T, E extends Exception> T call(CheckedSupplier<T, E> op) throws E {
        DelegationPolicy policy = DelegationRuntime.policy();
        if (policy == null) {
            return op.get();
        }
        return DelegationHolder.call(new ActiveDelegation(delegatingClass, requestingClass(policy)), op);
    }

    abstract Class<?> requestingClass(DelegationPolicy policy);

    static final class AsCaller extends EntitlementDelegation {
        private AsCaller(Class<?> delegatingClass) {
            super(delegatingClass);
        }

        @Override
        Class<?> requestingClass(DelegationPolicy policy) {
            return WALKER.walk(frames -> findRequestingClass(policy, delegatingClass, frames.iterator()));
        }
    }

    static final class AsCreator extends EntitlementDelegation {
        private final Class<?> creator;

        private AsCreator(Class<?> delegatingClass, Class<?> creator) {
            super(delegatingClass);
            this.creator = creator;
        }

        @Override
        Class<?> requestingClass(DelegationPolicy policy) {
            return creator;
        }
    }

    /**
     * The first class on the stack that is not part of the delegating class, of the JDK or of the entitlement library.
     * If there is no such class, it returns the class itself.
     * If that class is running under a delegation, it returns the requesting class of that delegation.
     */
    private static Class<?> findRequestingClass(
        DelegationPolicy policy,
        Class<?> delegatingClass,
        Iterator<StackWalker.StackFrame> frames
    ) {
        while (frames.hasNext()) {
            Class<?> type = frames.next().getDeclaringClass();
            if (type.getNestHost() != delegatingClass && policy.isSkippedFrame(type) == false) {
                ActiveDelegation active = DelegationHolder.current();
                return active != null && active.appliesTo(type) ? active.requestingClass() : type;
            }
        }
        return delegatingClass;
    }

    private static void requireCanDelegate(DelegationPolicy policy, Class<?> delegatingClass) {
        if (policy.canDelegate(delegatingClass) == false) {
            throw new IllegalCallerException(Strings.format("[%s] may not delegate entitlement checks", delegatingClass.getName()));
        }
    }
}
