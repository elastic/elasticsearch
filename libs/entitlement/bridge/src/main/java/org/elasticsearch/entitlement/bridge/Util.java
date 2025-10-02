/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.bridge;

import java.util.Optional;
import java.util.Set;

import static java.lang.StackWalker.Option.RETAIN_CLASS_REFERENCE;
import static java.lang.StackWalker.Option.SHOW_HIDDEN_FRAMES;

public class Util {
    /**
     * A special value representing the case where a method <em>has no caller</em>.
     * This can occur if it's called directly from the JVM.
     *
     * @see StackWalker#getCallerClass()
     */
    public static final Class<?> NO_CLASS = new Object() {
    }.getClass();

    private static final Set<String> skipInternalPackages = Set.of("java.lang.invoke", "java.lang.reflect", "jdk.internal.reflect");

    /**
     * Why would we write this instead of using {@link StackWalker#getCallerClass()}?
     * Because that method throws {@link IllegalCallerException} if called from the "outermost frame",
     * which includes at least some cases of a method called from a native frame.
     *
     * @return the class that called the method which called this; or {@link #NO_CLASS} from the outermost frame.
     */
    @SuppressWarnings("unused") // Called reflectively from InstrumenterImpl
    public static Class<?> getCallerClass() {
        Optional<Class<?>> callerClassIfAny = StackWalker.getInstance(Set.of(RETAIN_CLASS_REFERENCE, SHOW_HIDDEN_FRAMES))
            .walk(
                frames -> frames.skip(2) // Skip this method and its caller
                    .filter(frame -> skipInternalPackages.contains(frame.getDeclaringClass().getPackageName()) == false)
                    .findFirst()
                    .map(StackWalker.StackFrame::getDeclaringClass)
            );
        return callerClassIfAny.orElse(NO_CLASS);
    }

}
