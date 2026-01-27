/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Tests that extend {@link AbstractFunctionTestCase} can use this annotation to specify the name of the function
 * to use when generating documentation files while running tests.
 * If this is not used, the name will be deduced from the test class name, by removing the "Test" suffix, and converting
 * the class name to snake case. This annotation can be used to override that behavior, for cases where the deduced name
 * is not correct. For example, in Elasticsearch the class name for `GeoPoint` capitalizes the `P` in `Point`, but the
 * function name is `to_geopoint`, not `to_geo_point`. In some cases, even when compatible class names are used,
 * like `StX` for the function `st_x`, the annotation is needed because the name deduction does not allow only a single
 * character after the underscore.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface FunctionName {
    /** The function name to use in generating documentation files while running tests */
    String value();
}
