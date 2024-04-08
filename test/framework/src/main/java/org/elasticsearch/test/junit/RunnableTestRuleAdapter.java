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

/**
 * Adapts a {@link CheckedRunnable} to a JUnit {@link TestRule}.
 * The runnable is executed ({@link CheckedRunnable#run()}) when the {@link Statement} is evaluated, before the test is executed.
 * That is, the supplied runnable represents a set-up step for the test.
 * This is useful when a class needs some set-up code to be executed as part of a TestRule rather than a {@link org.junit.Before}
 * or {@link org.junit.BeforeClass} annotated method. For example, if a test must run some set-up code <i>before</i> launching an
 * {@code ElasticsearchCluster} that set-up code must be contained within a {@link TestRule} and chained to the cluster via a
 * {@link org.junit.rules.RuleChain}. By using this adapter, the setup code can be expressed as a method reference as in the example below:
 * <pre>{@code
 * @ClassRule
 * public static TestRule ruleChain = RuleChain.outerRule(new RunnableTestRuleAdapter(MyTests::generateTestFiles)).around(cluster);
 * }</pre>
 */
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
