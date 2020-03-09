/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression.predicate.operator.comparison;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.predicate.Negatable;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparisonProcessor.BinaryComparisonOperation;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

public class NotEquals extends BinaryComparison implements Negatable<BinaryComparison> {

    public NotEquals(Source source, Expression left, Expression right) {
        super(source, left, right, BinaryComparisonOperation.NEQ);
    }

    @Override
    protected NodeInfo<NotEquals> info() {
        return NodeInfo.create(this, NotEquals::new, left(), right());
    }

    @Override
    protected NotEquals replaceChildren(Expression newLeft, Expression newRight) {
        return new NotEquals(source(), newLeft, newRight);
    }

    @Override
    public NotEquals swapLeftAndRight() {
        return new NotEquals(source(), right(), left());
    }

    @Override
    public BinaryComparison negate() {
        return new Equals(source(), left(), right());
    }
}
