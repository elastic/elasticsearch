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
     * Initialized here (in {@code Util}, which is loaded early as part of the {@code java.base} patch,
     * before {@code defineHiddenClass} is instrumented) so that creating the {@link StackWalker} cannot
     * re-enter the entitlement system.  If this field lived in {@link CallerFinder}, its lazy
     * initialization would occur during the first entitlement check, when the instrumented
     * {@code defineHiddenClass} might recursively trigger another check before {@link CallerFinder}
     * finishes loading — causing a {@link ClassCircularityError}.
     */
    static final StackWalker WALKER = StackWalker.getInstance(Set.of(RETAIN_CLASS_REFERENCE, SHOW_HIDDEN_FRAMES));

    /**
     * Why would we write this instead of using {@link StackWalker#getCallerClass()}?
     * Because that method throws {@link IllegalCallerException} if called from the "outermost frame",
     * which includes at least some cases of a method called from a native frame.
     * <p>
     * The actual stack walking is delegated to {@link CallerFinder} to avoid class-loading
     * circularity; see that class's Javadoc for the full explanation.
     *
     * @return the class that called the method which called this; or {@link #NO_CLASS} from the outermost frame.
     */
    @SuppressWarnings("unused") // Called reflectively from InstrumenterImpl
    public static Class<?> getCallerClass() {
        Class<?> result = CallerFinder.findCaller(WALKER);
        return result != null ? result : NO_CLASS;
    }

}
