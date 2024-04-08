/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.TypeResolutions;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.time.ZoneId;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.DEFAULT;

public class NullEquals extends org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.NullEquals {
    public NullEquals(Source source, Expression left, Expression right, ZoneId zoneId) {
        super(source, left, right, zoneId);
    }

    @Override
    protected TypeResolution resolveInputType(Expression e, TypeResolutions.ParamOrdinal paramOrdinal) {
        return EsqlTypeResolutions.isExact(e, sourceText(), DEFAULT);
    }

    @Override
    protected NodeInfo<org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.NullEquals> info() {
        return NodeInfo.create(this, NullEquals::new, left(), right(), zoneId());
    }

    @Override
    protected org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.NullEquals replaceChildren(
        Expression newLeft,
        Expression newRight
    ) {
        return new NullEquals(source(), newLeft, newRight, zoneId());
    }

    @Override
    public org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.NullEquals swapLeftAndRight() {
        return new NullEquals(source(), right(), left(), zoneId());
    }

}
