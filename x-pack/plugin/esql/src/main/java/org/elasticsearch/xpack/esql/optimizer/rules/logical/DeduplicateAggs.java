/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

/**
 * This rule handles duplicate aggregate functions to avoid duplicate compute
 * stats a = min(x), b = min(x), c = count(*), d = count() by g
 * becomes
 * stats a = min(x), c = count(*) by g | eval b = a, d = c | keep a, b, c, d, g
 */
public final class DeduplicateAggs extends ReplaceAggregateAggExpressionWithEval implements OptimizerRules.CoordinatorOnly {

    public DeduplicateAggs() {
        super(false);
    }
}
