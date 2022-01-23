/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common;

/**
 * Holds a value that is either:
 * a) set implicitly e.g. through some default value
 * b) set explicitly e.g. from a user selection
 * <p>
 * When merging conflicting configuration settings such as
 * field mapping settings it is preferable to preserve an explicit
 * choice rather than a choice made only made implicitly by defaults.
 */
public record Explicit<T> (T value, boolean explicit) {

    public static final Explicit<Boolean> EXPLICIT_TRUE = new Explicit<>(true, true);
    public static final Explicit<Boolean> EXPLICIT_FALSE = new Explicit<>(false, true);
    public static final Explicit<Boolean> IMPLICIT_TRUE = new Explicit<>(true, false);
    public static final Explicit<Boolean> IMPLICIT_FALSE = new Explicit<>(false, false);

    public static Explicit<Boolean> explicitBoolean(boolean value) {
        return value ? EXPLICIT_TRUE : EXPLICIT_FALSE;
    }
}
