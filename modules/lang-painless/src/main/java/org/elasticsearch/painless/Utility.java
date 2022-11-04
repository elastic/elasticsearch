/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import java.util.Objects;

/**
 * A set of methods for non-native boxing and non-native
 * exact math operations used at both compile-time and runtime.
 */
public class Utility {

    public static String charToString(final char value) {
        return String.valueOf(value);
    }

    public static char StringTochar(final String value) {
        if (value == null) {
            throw new ClassCastException(
                "cannot cast " + "null " + String.class.getCanonicalName() + " to " + char.class.getCanonicalName()
            );
        }

        if (value.length() != 1) {
            throw new ClassCastException(
                "cannot cast " + String.class.getCanonicalName() + " with length not equal to one to " + char.class.getCanonicalName()
            );
        }

        return value.charAt(0);
    }

    /**
     * Used by {@link org.elasticsearch.painless.phase.DefaultEqualityMethodOptimizationPhase} to call {@code b.equals(a)}
     * without changing execution order
     */
    public static boolean equalsRHS(Object a, Object b) {
        return Objects.equals(b, a);
    }

    private Utility() {}
}
