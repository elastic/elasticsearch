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

import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

import com.carrotsearch.randomizedtesting.RandomizedContext;

public final class ReproduceInfoPrinterRunListener extends RunListener {

    private boolean failed = false;

    @Override
    public void testFailure(Failure failure) {
        failed = true;
    }

    @Override
    public void testRunFinished(Result result) {
        if (failed) {
            printReproLine();
        }
        failed = false;
    }

    private void printReproLine() {
        final StringBuilder b = new StringBuilder();
        b.append("NOTE: reproduce with: mvn test -Dtests.seed=").append(RandomizedContext.current().getRunnerSeedAsString());
        if (System.getProperty("runSlowTests") != null) {
            b.append(" -DrunSlowTests=").append(System.getProperty("runSlowTests"));
        }
        b.append(" -Dtests.class=").append(RandomizedContext.current().getTargetClass().getName());
        System.out.println(b.toString());
    }

}
