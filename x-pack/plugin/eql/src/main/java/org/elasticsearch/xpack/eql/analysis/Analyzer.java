/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.analysis;

import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.rule.RuleExecutor;

import java.util.Collection;

import static java.util.Arrays.asList;

public class Analyzer extends RuleExecutor<LogicalPlan> {

    private final FunctionRegistry functionRegistry;
    private final Verifier verifier;

    public Analyzer(FunctionRegistry functionRegistry, Verifier verifier) {
        this.functionRegistry = functionRegistry;
        this.verifier = verifier;
    }

    @Override
    protected Iterable<RuleExecutor<LogicalPlan>.Batch> batches() {
        Batch resolution = new Batch("Resolution");
        
        return asList(resolution);
    }

    public LogicalPlan analyze(LogicalPlan plan) {
        return verify(execute(plan));
    }

    private LogicalPlan verify(LogicalPlan plan) {
        Collection<Failure> failures = verifier.verify(plan);
        if (!failures.isEmpty()) {
            throw new VerificationException(failures);
        }
        return plan;
    }
}
