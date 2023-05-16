/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 *
 * This project is based on a modification of https://github.com/tdunning/t-digest which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.tdigest;

import org.elasticsearch.test.ESTestCase;
import org.junit.Ignore;
import org.junit.runner.RunWith;

import com.carrotsearch.randomizedtesting.JUnit3MethodProvider;
import com.carrotsearch.randomizedtesting.JUnit4MethodProvider;
import com.carrotsearch.randomizedtesting.annotations.Listeners;
import com.carrotsearch.randomizedtesting.annotations.TestMethodProviders;

@Ignore
@Listeners({
        ReproduceInfoPrinterRunListener.class
})
@TestMethodProviders({
        JUnit3MethodProvider.class, // test names starting with test*
        JUnit4MethodProvider.class  // test methods annotated with @Test
})
@RunWith(value = com.carrotsearch.randomizedtesting.RandomizedRunner.class)
/**
 * Base test case, all other test cases must inherit this one.
 */
public abstract class AbstractTest extends ESTestCase {

}
