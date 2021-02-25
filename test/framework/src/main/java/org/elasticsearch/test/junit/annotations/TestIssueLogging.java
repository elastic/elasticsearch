/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.junit.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PACKAGE;
import static java.lang.annotation.ElementType.TYPE;

/**
 * Annotation used to set a custom log level when investigating test failures. Do not use this annotation to explicitly
 * control the logging level in tests; instead, use {@link TestLogging}.
 *
 * It supports multiple logger:level comma-separated key-value pairs of logger:level (e.g.,
 * org.elasticsearch.cluster.metadata:TRACE). Use the _root keyword to set the root logger level.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({PACKAGE, TYPE, METHOD})
public @interface TestIssueLogging {

    /**
     * A comma-separated list of key-value pairs of logger:level. For each key-value pair of logger:level, the test
     * framework will set the logging level of the specified logger to the specified level.
     *
     * @return the logger:level pairs
     */
    String value();

    /**
     * This property is used to link to the open test issue under investigation.
     *
     * @return the issue link
     */
    String issueUrl();

}
