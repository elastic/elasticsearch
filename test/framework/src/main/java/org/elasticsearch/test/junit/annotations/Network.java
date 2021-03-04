/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.junit.annotations;

import com.carrotsearch.randomizedtesting.annotations.TestGroup;

import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Annotation used to set if internet network connectivity is required to run the test.
 * By default, tests annotated with @Network won't be executed.
 * Set -Dtests.network=true when running test to launch network tests
 */
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@TestGroup(enabled = false, sysProperty = "tests.network")
public @interface Network {
}
