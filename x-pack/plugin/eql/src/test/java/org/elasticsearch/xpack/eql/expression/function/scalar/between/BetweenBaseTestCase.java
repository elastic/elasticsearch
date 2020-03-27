/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.between;

import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.Callable;

public abstract class BetweenBaseTestCase extends ESTestCase {

    protected static final int NUMBER_OF_TEST_RUNS = 20;

    protected static void run(Callable<Void> callable) throws Exception {
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            callable.call();
        }
    }
}
