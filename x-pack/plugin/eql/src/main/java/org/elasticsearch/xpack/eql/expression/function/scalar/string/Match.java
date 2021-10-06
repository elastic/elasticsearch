/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.TypeResolutions;
import org.elasticsearch.xpack.ql.expression.function.scalar.BaseSurrogateFunction;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.predicate.regex.RLike;
import org.elasticsearch.xpack.ql.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isStringAndExact;

/**
 * EQL specific match function
 * Returns true if the source field matches any of the provided regular expressions
 * Refer to: https://eql.readthedocs.io/en/latest/query-guide/functions.html#match
 */
public class Match extends BaseSurrogateFunction {

    private final Expression field;
    private final List<Expression> patterns;
    private final boolean caseInsensitive;

    public Match(Source source, Expression field, List<Expression> patterns, boolean caseInsensitive) {
        this(source, CollectionUtils.combine(singletonList(field), patterns), caseInsensitive);
    }

    private Match(Source source, List<Expression> children, boolean caseInsensitive) {
        super(source, children);
        this.field = children().get(0);
        this.patterns = children().subList(1, children().size());
        this.caseInsensitive = caseInsensitive;
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Match::new, field, patterns, caseInsensitive);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Match(source(), newChildren, caseInsensitive);
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

        TypeResolution resolution = isStringAndExact(field, sourceText(), FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }

        int index = 1;
        for (Expression regex : patterns) {
            // Currently we have limited enum for ordinal numbers
            // So just using default here for error messaging
            resolution = isStringAndExact(regex, sourceText(), TypeResolutions.ParamOrdinal.fromIndex(index));
            if (resolution.unresolved()) {
                return resolution;
            }

            resolution = isFoldable(regex, sourceText(), TypeResolutions.ParamOrdinal.fromIndex(index));
            if (resolution.unresolved()) {
                break;
            }

            index++;
        }

        return resolution;
    }

    @Override
    public ScalarFunction makeSubstitute() {
        List<String> patternStrings = new ArrayList<>(patterns.size());

        for (Expression regex : patterns) {
            patternStrings.add(regex.fold().toString());
        }

        return new RLike(source(), field, new RLikePattern(String.join("|", patternStrings)), caseInsensitive);
    }
}
