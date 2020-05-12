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
import org.elasticsearch.xpack.ql.expression.predicate.regex.RLike;
import org.elasticsearch.xpack.ql.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static java.util.Collections.singletonList;
import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
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

    public Match(Source source, Expression field, List<Expression> patterns) {
        this(source, CollectionUtils.combine(singletonList(field), patterns));
    }

    private Match(Source source, List<Expression> children) {
        super(source, children);
        this.field = children().get(0);
        this.patterns = children().subList(1, children().size());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Match::new, field, patterns);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() < 2) {
            throw new IllegalArgumentException("expected at least [2] children but received [" + newChildren.size() + "]");
        }
        return new Match(source(), newChildren);
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

        TypeResolution resolution;
        int index = 0;
        List<Integer> nonFoldables = new ArrayList<>(2);
        for (Expression e : children()) {
            // Currently we have limited enum for ordinal numbers
            // So just using default here for error messaging
            ParamOrdinal paramOrd = ParamOrdinal.fromIndex(index);
            resolution = isStringAndExact(e, sourceText(), paramOrd);
            if (resolution.unresolved()) {
                return resolution;
            }
            resolution = isFoldable(e, sourceText(), paramOrd);
            if (resolution.unresolved()) {
                nonFoldables.add(index);
            }
            index++;
        }

        if (nonFoldables.size() == 2) {
            StringBuilder sb = new StringBuilder(format(null, "only one argument of [{}] must be non-constant but multiple found: ",
                sourceText()));
            for (Integer i : nonFoldables) {
                ParamOrdinal pOrd = ParamOrdinal.fromIndex(i);
                sb.append(format(null, "{}argument: [{}]",
                    pOrd == ParamOrdinal.DEFAULT ? "" : pOrd.name().toLowerCase(Locale.ROOT) + " ",
                    Expressions.name(children().get(i))));
                sb.append(", ");
            }
            sb.delete(sb.length() - 2, sb.length());
            return new TypeResolution(sb.toString());
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public ScalarFunction makeSubstitute() {
        List<String> patternStrings = new ArrayList<>(patterns.size());

        for (Expression regex : patterns) {
            patternStrings.add(regex.fold().toString());
        }

        return new RLike(source(), field, new RLikePattern(String.join("|", patternStrings)));
    }
}
