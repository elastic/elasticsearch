/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.bridge;

import java.util.Iterator;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.lang.StackWalker.Option.RETAIN_CLASS_REFERENCE;
import static java.lang.StackWalker.Option.SHOW_HIDDEN_FRAMES;

public class Util {
    /**
     * A special value representing the case where a method <em>has no caller</em>.
     * This can occur if it's called directly from the JVM.
     *
     * @see StackWalker#getCallerClass()
     */
    public static final Class<?> NO_CLASS = new Object() {}.getClass();

    /**
     * Why would we write this instead of using {@link StackWalker#getCallerClass()}?
     * Because that method throws {@link IllegalCallerException} if called from the "outermost frame",
     * which includes at least some cases of a method called from a native frame.
     *
     * @return the class that called the method which called this; or {@link #NO_CLASS} from the outermost frame.
     */
    @SuppressWarnings("unused") // Called reflectively from InstrumenterImpl
    public static Class<?> getCallerClass() {
        return WALKER.walk(CALLER_FINDER);
    }

    private static final Set<String> skipInternalPackages = Set.of("java.lang.invoke", "java.lang.reflect", "jdk.internal.reflect");

    /**
     * Walks the stack without any lambda or method-reference so that no {@code invokedynamic}
     * call site needs to be linked during this method's execution.
     * <p>
     * On Java 17, the JVM creates lambda class implementations via
     * {@link java.lang.invoke.MethodHandles.Lookup#defineHiddenClass}, which is itself an
     * instrumented method. Any lambda defined inside {@link #getCallerClass()} would therefore
     * trigger a re-entrant call to {@link #getCallerClass()} the first time that lambda was
     * initialised, causing infinite recursion. Using an anonymous class and a plain
     * {@link Iterator} avoids all {@code invokedynamic} call sites in the critical path.
     */
    private static final Function<Stream<StackWalker.StackFrame>, Class<?>> CALLER_FINDER = new Function<
        Stream<StackWalker.StackFrame>,
        Class<?>>() {
        @Override
        public Class<?> apply(Stream<StackWalker.StackFrame> frames) {
            Iterator<StackWalker.StackFrame> iter = frames.skip(2).iterator(); // skip getCallerClass and its caller
            while (iter.hasNext()) {
                StackWalker.StackFrame frame = iter.next();
                if (skipInternalPackages.contains(frame.getDeclaringClass().getPackageName()) == false) {
                    return frame.getDeclaringClass();
                }
            }
            return NO_CLASS;
        }
    };

    private static final StackWalker WALKER = StackWalker.getInstance(Set.of(RETAIN_CLASS_REFERENCE, SHOW_HIDDEN_FRAMES));

}
