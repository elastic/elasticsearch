/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.optimizer;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules;

public class FoldNull extends OptimizerRules.FoldNull {
    @Override
    public Expression rule(Expression e) {
        return super.rule(e);
    }
}
