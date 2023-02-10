/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.junit;

import org.elasticsearch.core.CheckedRunnable;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class RunnableTestRuleAdapter implements TestRule {

    private CheckedRunnable<Exception> runnable;

    public RunnableTestRuleAdapter(CheckedRunnable<Exception> runnable) {
        this.runnable = runnable;
    }

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                runnable.run();
                base.evaluate();
            }
        };
    }
}
