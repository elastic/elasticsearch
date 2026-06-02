/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.bridge;

import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Walks the call stack to identify the class that triggered an instrumented JDK method.
 * <p>
 * On Java 17, the JVM creates lambda class implementations via
 * {@link java.lang.invoke.MethodHandles.Lookup#defineHiddenClass}, which is itself
 * instrumented by the entitlement system.  This creates a dangerous re-entrancy:
 * <ol>
 *   <li>ES code creates a lambda &rarr; JVM calls {@code defineHiddenClass}</li>
 *   <li>The instrumented {@code defineHiddenClass} calls {@link Util#getCallerClass()}</li>
 *   <li>If anything inside {@code getCallerClass()} creates a lambda or loads a class
 *       that triggers {@code defineHiddenClass}, the cycle repeats: infinite recursion or
 *       {@link ClassCircularityError}.</li>
 * </ol>
 * <p>
 * This class lives in its own source file so that:
 * <ul>
 *   <li>It is <strong>not</strong> a nested class of {@link Util}.  Java 17's eager bytecode
 *       verifier loads all classes referenced from a class file at link time; a nested class
 *       would carry an enclosing-class reference back to {@link Util}, causing a
 *       {@link ClassCircularityError} when the verifier loads this helper while {@link Util}
 *       itself is still being loaded.</li>
 *   <li>It does <strong>not</strong> create a {@link StackWalker} in its own static initializer.
 *       On Java 17, {@link StackWalker#getInstance} internally creates a lambda, which triggers
 *       {@code defineHiddenClass}, which calls {@link Util#getCallerClass()}, which in turn
 *       tries to load this class again — causing a {@link ClassCircularityError}.  Instead,
 *       the walker is created in {@link Util}'s static initializer (which runs before the
 *       entitlement instrumentation is active) and passed in via {@link #findCaller(StackWalker)}.</li>
 *   <li>It contains no lambdas or method references of its own, and calls only
 *       {@link Spliterator#tryAdvance} (never {@link Stream#skip}) to avoid
 *       {@link java.util.stream.StreamSpliterators.WrappingSpliterator#initPartialTraversalState},
 *       which uses a {@code buf::accept} method reference that would itself trigger
 *       {@code defineHiddenClass} and restart the cycle.</li>
 * </ul>
 */
class CallerFinder implements Function<Stream<StackWalker.StackFrame>, Class<?>>,
    Consumer<StackWalker.StackFrame> {

    /** Packages whose frames we skip when searching for the "real" caller. */
    static final Set<String> SKIP_INTERNAL_PACKAGES = Set.of(
        "java.lang.invoke",
        "java.lang.reflect",
        "jdk.internal.reflect"
    );

    /** Number of frames to skip unconditionally at the top of the stack. */
    private int skipRemaining;

    /** The result; {@code null} means "not yet found". */
    private Class<?> result;

    /**
     * Walks the current call stack and returns the first frame whose package is not in
     * {@link #SKIP_INTERNAL_PACKAGES}, after skipping {@code Util.getCallerClass()} and
     * this method itself.
     *
     * @param walker the pre-initialized walker (owned by {@link Util} to avoid re-entrant initialization)
     * @return the calling class, or {@code null} if every frame was internal
     */
    static Class<?> findCaller(StackWalker walker) {
        return walker.walk(new CallerFinder());
    }

    @Override
    public Class<?> apply(Stream<StackWalker.StackFrame> frames) {
        skipRemaining = 3; // skip findCaller(), getCallerClass(), and the instrumented method's own frame
        result = null;
        Spliterator<StackWalker.StackFrame> sp = frames.spliterator();
        while (result == null && sp.tryAdvance(this)) {
            // keep walking until we find the first non-internal caller
        }
        return result;
    }

    @Override
    public void accept(StackWalker.StackFrame frame) {
        if (skipRemaining > 0) {
            skipRemaining--;
            return;
        }
        if (SKIP_INTERNAL_PACKAGES.contains(frame.getDeclaringClass().getPackageName()) == false) {
            result = frame.getDeclaringClass();
        }
    }
}
