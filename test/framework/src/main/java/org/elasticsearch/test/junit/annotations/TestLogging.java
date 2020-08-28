/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.test.junit.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PACKAGE;
import static java.lang.annotation.ElementType.TYPE;

/**
 * Annotation used to set a custom log level for controlling logging behavior in tests. Do not use this annotation when
 * investigating test failures; instead, use {@link TestIssueLogging}.
 *
 * It supports multiple logger:level comma-separated key-value pairs of logger:level (e.g.,
 * org.elasticsearch.cluster.metadata:TRACE). Use the _root keyword to set the root logger level.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({PACKAGE, TYPE, METHOD})
public @interface TestLogging {

    /**
     * A comma-separated list of key-value pairs of logger:level. For each key-value pair of logger:level, the test
     * framework will set the logging level of the specified logger to the specified level.
     *
     * @return the logger:level pairs
     */
    String value();

    /**
     * The reason this annotation is used to control logger behavior during a test.
     *
     * @return the reason for adding the annotation
     */
    String reason();

}
