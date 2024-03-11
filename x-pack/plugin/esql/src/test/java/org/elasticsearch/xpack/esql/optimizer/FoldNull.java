/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.qlcore.expression.Expression;

public class FoldNull extends LogicalPlanOptimizer.FoldNull {
    @Override
    public Expression rule(Expression e) {
        return super.rule(e);
    }
}
