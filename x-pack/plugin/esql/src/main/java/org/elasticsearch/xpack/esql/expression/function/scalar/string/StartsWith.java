/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.planner.Mappable;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isStringAndExact;

public class StartsWith extends ScalarFunction implements Mappable {

    private final Expression str;
    private final Expression prefix;

    public StartsWith(Source source, Expression str, Expression prefix) {
        super(source, Arrays.asList(str, prefix));
        this.str = str;
        this.prefix = prefix;
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

        TypeResolution resolution = isStringAndExact(str, sourceText(), FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }
        return isStringAndExact(prefix, sourceText(), SECOND);
    }

    @Override
    public boolean foldable() {
        return str.foldable() && prefix.foldable();
    }

    @Override
    public Object fold() {
        return Mappable.super.fold();
    }

    @Evaluator
    static boolean process(BytesRef str, BytesRef prefix) {
        if (str.length < prefix.length) {
            return false;
        }
        return Arrays.equals(str.bytes, str.offset, str.offset + prefix.length, prefix.bytes, prefix.offset, prefix.offset + prefix.length);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new StartsWith(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, StartsWith::new, str, prefix);
    }

    @Override
    public ScriptTemplate asScript() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Supplier<EvalOperator.ExpressionEvaluator> toEvaluator(
        Function<Expression, Supplier<EvalOperator.ExpressionEvaluator>> toEvaluator
    ) {
        Supplier<EvalOperator.ExpressionEvaluator> strEval = toEvaluator.apply(str);
        Supplier<EvalOperator.ExpressionEvaluator> prefixEval = toEvaluator.apply(prefix);
        return () -> new StartsWithEvaluator(strEval.get(), prefixEval.get());
    }
}
