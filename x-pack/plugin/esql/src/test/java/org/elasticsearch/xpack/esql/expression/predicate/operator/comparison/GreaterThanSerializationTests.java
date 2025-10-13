/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;

public class GreaterThanSerializationTests extends AbstractComparisonSerializationTests<GreaterThan> {
    @Override
    protected GreaterThan create(Source source, Expression left, Expression right) {
        return new GreaterThan(source, left, right);
    }
}
