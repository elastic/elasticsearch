/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.xpack.eql.util.StringUtils;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions.ParamOrdinal;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.function.scalar.BaseSurrogateFunction;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.ql.expression.predicate.regex.Like;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.CollectionUtils;

import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isStringAndExact;

/**
 * EQL wildcard function. Matches the form:
 *     wildcard(field, "*wildcard*pattern*", ...)
 */
public class Wildcard extends BaseSurrogateFunction {

    private final Expression field;
    private final List<Expression> patterns;

    public Wildcard(Source source, Expression field, List<Expression> patterns) {
        super(source, CollectionUtils.combine(Collections.singletonList(field), patterns));
        this.field = field;
        this.patterns = patterns;
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Wildcard::new, field, patterns);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() < 2) {
            throw new IllegalArgumentException("expected at least [2] children but received [" + newChildren.size() + "]");
        }

        return new Wildcard(source(), newChildren.get(0), newChildren.subList(1, newChildren.size()));
    }

    @Override
    public DataType dataType() {
        return DataTypes.BOOLEAN;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution lastResolution = isStringAndExact(field, sourceText(), ParamOrdinal.FIRST);
        if (lastResolution.unresolved()) {
            return lastResolution;
        }

        int index = 1;

        for (Expression p: patterns) {

            lastResolution = isFoldable(p, sourceText(), ParamOrdinal.fromIndex(index));
            if (lastResolution.unresolved()) {
                break;
            }

            lastResolution = isString(p, sourceText(), ParamOrdinal.fromIndex(index));
            if (lastResolution.unresolved()) {
                break;
            }

            index++;
        }

        return lastResolution;
    }

    @Override
    public ScalarFunction makeSubstitute() {
        ScalarFunction result = null;

        for (Expression pattern: patterns) {
            String wcString = pattern.fold().toString();
            Like like = new Like(source(), field, StringUtils.toLikePattern(wcString));
            result = result == null ? like : new Or(source(), result, like);
        }

        return result;
    }
}