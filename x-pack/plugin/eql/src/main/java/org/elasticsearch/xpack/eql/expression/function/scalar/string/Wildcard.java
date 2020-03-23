/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.eql.utils.StringUtils;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.Expressions.ParamOrdinal;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.ql.expression.predicate.regex.Like;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isStringAndExact;

/**
 * EQL wildcard function. Matches the form:
 *     wildcard(field, "*wildcard*pattern*", "*wildcard*pattern*")
 */

public class Wildcard extends ScalarFunction {

    private final Expression field;
    private final List<Expression> patterns;

    public Wildcard(Source source, Expression field, List<Expression> patterns) {
        super(source, toArguments(field, patterns));
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
        if (!childrenResolved()) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution lastResolution = isStringAndExact(field, sourceText(), ParamOrdinal.FIRST);
        if (lastResolution.unresolved()) {
            return lastResolution;
        }

        for (Expression p: patterns) {
            lastResolution = isStringAndExact(p, sourceText(), ParamOrdinal.DEFAULT);
            if (lastResolution.unresolved()) {
                break;
            }

            if (p.foldable() == false) {
                return new TypeResolution(format(null, "wildcard against variables are not (currently) supported; offender [{}] in [{}]",
                    Expressions.name(p),
                    sourceText()));
            }
        }

        return lastResolution;
    }

    @Override
    public boolean foldable() {
        return Expressions.foldable(arguments());
    }

    @Override
    public Object fold() {
        return asLikes().fold();
    }

    @Override
    protected Pipe makePipe() {
        throw new EqlIllegalArgumentException("Wildcard.makePipe() should not be called directly");
    }

    @Override
    public ScriptTemplate asScript() {
        throw new EqlIllegalArgumentException("Wildcard.asScript() should not be called directly");
    }

    public Expression asLikes() {
        Expression result = null;

        for (Expression pattern: patterns) {
            String wcString = pattern.fold().toString();
            Like like = new Like(source(), field, StringUtils.toLikePattern(wcString));
            result = result == null ? like : new Or(source(), result, like);
        }

        return result;
    }

    private static List<Expression> toArguments(Expression src, List<Expression> patterns) {
        ArrayList<Expression> arguments = new ArrayList<>();
        arguments.add(src);
        arguments.addAll(patterns);
        return arguments;
    }
}
