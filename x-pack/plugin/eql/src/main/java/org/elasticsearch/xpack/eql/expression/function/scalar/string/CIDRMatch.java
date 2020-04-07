/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.Expressions.ParamOrdinal;
import org.elasticsearch.xpack.ql.expression.function.scalar.BaseSurrogateFunction;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.CollectionUtils;

import java.util.List;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isIPAndExact;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isStringAndExact;

/**
 * EQL specific cidrMatch function
 * Returns true if the source address matches any of the provided CIDR blocks.
 * Refer to: https://eql.readthedocs.io/en/latest/query-guide/functions.html#cidrMatch
 */
public class CIDRMatch extends BaseSurrogateFunction {

    private final Expression field;
    private final List<Expression> addresses;

    public CIDRMatch(Source source, Expression field, List<Expression> addresses) {
        super(source, CollectionUtils.combine(singletonList(field), addresses));
        this.field = field;
        this.addresses = addresses;
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, CIDRMatch::new, field, addresses);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() < 2) {
            throw new IllegalArgumentException("expected at least [2] children but received [" + newChildren.size() + "]");
        }
        return new CIDRMatch(source(), newChildren.get(0), newChildren.subList(1, newChildren.size()));
    }

    @Override
    public DataType dataType() {
        return DataTypes.BOOLEAN;
    }

    @Override
    protected TypeResolution resolveType() {
        if (!childrenResolved()) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isIPAndExact(field, sourceText(), Expressions.ParamOrdinal.FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }

        for (Expression addr : addresses) {
            // Currently we have limited enum for ordinal numbers
            // So just using default here for error messaging
            resolution = isStringAndExact(addr, sourceText(), ParamOrdinal.DEFAULT);
            if (resolution.unresolved()) {
                return resolution;
            }
        }

        int index = 1;

        for (Expression addr : addresses) {

            resolution = isFoldable(addr, sourceText(), ParamOrdinal.fromIndex(index));
            if (resolution.unresolved()) {
                break;
            }

            resolution = isStringAndExact(addr, sourceText(), ParamOrdinal.fromIndex(index));
            if (resolution.unresolved()) {
                break;
            }

            index++;
        }

        return resolution;
    }

    @Override
    public ScalarFunction makeSubstitute() {
        ScalarFunction func = null;

        for (Expression address : addresses) {
            final Equals eq = new Equals(source(), field, address);
            func = (func == null) ? eq : new Or(source(), func, eq);
        }

        return func;
    }
}
