/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.spi.annotation;

/**
 * Use an augmented annotation to augment a class to have available
 * a static final field from a different class.
 *
 * Example:
 * {@code
 * // Whitelist
 *
 *...
 * class java.lang.Integer {
 *     ...
 * }
 *
 * class Augmented {
 *     int MAX_VALUE @augmented[augmented_canonical_class_name="java.lang.Integer"]
 *     int MIN_VALUE @augmented[augmented_canonical_class_name="java.lang.Integer"]
 * }
 * ...
 *
 * // Script
 * ...
 * long value = <some_value>;
 * int augmented = 0;
 * if (value < Augmented.MAX_VALUE && value > Augmented.MIN_VALUE) {
 *     augmented = (int)value;
 * }
 * ...
 * }
 */
public record AugmentedAnnotation(String augmentedCanonicalClassName) {
    public static final String NAME = "augmented";
}
