/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.function.math;

import org.elasticsearch.xpack.esql.optimizer.promql.AbstractPromqlPlanOptimizerTests;

import static org.hamcrest.Matchers.equalTo;

public class MathPromqlFunctionCallTests extends AbstractPromqlPlanOptimizerTests {
    public void testAbsConstantResult() {
        assertConstantResult("abs(vector(-1))", equalTo(1.0));
    }
}
