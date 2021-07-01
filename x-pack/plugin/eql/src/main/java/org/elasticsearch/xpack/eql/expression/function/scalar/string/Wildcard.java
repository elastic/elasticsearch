/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.xpack.eql.util.StringUtils;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.TypeResolutions;
import org.elasticsearch.xpack.ql.expression.function.scalar.BaseSurrogateFunction;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.predicate.Predicates;
import org.elasticsearch.xpack.ql.expression.predicate.regex.Like;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.CollectionUtils;

import java.util.Collections;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
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
    private final boolean caseInsensitive;

    public Wildcard(Source source, Expression field, List<Expression> patterns, boolean caseInsensitive) {
        super(source, CollectionUtils.combine(Collections.singletonList(field), patterns));
        this.field = field;
        this.patterns = patterns;
        this.caseInsensitive = caseInsensitive;
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Wildcard::new, field, patterns, caseInsensitive);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Wildcard(source(), newChildren.get(0), newChildren.subList(1, newChildren.size()), caseInsensitive);
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

        TypeResolution lastResolution = isStringAndExact(field, sourceText(), FIRST);
        if (lastResolution.unresolved()) {
            return lastResolution;
        }

        int index = 1;

        for (Expression p: patterns) {

            lastResolution = isFoldable(p, sourceText(), TypeResolutions.ParamOrdinal.fromIndex(index));
            if (lastResolution.unresolved()) {
                break;
            }

            lastResolution = isString(p, sourceText(), TypeResolutions.ParamOrdinal.fromIndex(index));
            if (lastResolution.unresolved()) {
                break;
            }

            index++;
        }

        return lastResolution;
    }

    @Override
    public ScalarFunction makeSubstitute() {
        return (ScalarFunction) Predicates.combineOr(
            patterns.stream()
                .map(e -> new Like(source(), field, StringUtils.toLikePattern(e.fold().toString()), caseInsensitive))
                .collect(toList()));
    }
}
