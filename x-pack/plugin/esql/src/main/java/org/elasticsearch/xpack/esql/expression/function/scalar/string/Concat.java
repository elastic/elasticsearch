/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.planner.Mappable;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
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
import java.util.stream.Stream;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isStringAndExact;

/**
 * Join strings.
 */
public class Concat extends ScalarFunction implements Mappable {
    public Concat(Source source, Expression first, List<Expression> rest) {
        super(source, Stream.concat(Stream.of(first), rest.stream()).toList());
    }

    @Override
    public DataType dataType() {
        return DataTypes.KEYWORD;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = TypeResolution.TYPE_RESOLVED;
        for (Expression value : children()) {
            resolution = isStringAndExact(value, sourceText(), DEFAULT);

            if (resolution.unresolved()) {
                return resolution;
            }
        }

        return resolution;
    }

    @Override
    public boolean foldable() {
        return Expressions.foldable(children());
    }

    @Override
    public BytesRef fold() {
        BytesRefBuilder result = new BytesRefBuilder();
        for (Expression v : children()) {
            BytesRef val = (BytesRef) v.fold();
            if (val == null) {
                return null;
            }
            result.append(val);
        }
        return result.get();
    }

    @Override
    public Supplier<EvalOperator.ExpressionEvaluator> toEvaluator(
        Function<Expression, Supplier<EvalOperator.ExpressionEvaluator>> toEvaluator
    ) {
        return () -> new Evaluator(
            children().stream().map(toEvaluator).map(Supplier::get).toArray(EvalOperator.ExpressionEvaluator[]::new)
        );
    }

    private class Evaluator implements EvalOperator.ExpressionEvaluator {
        private final BytesRefBuilder evaluated = new BytesRefBuilder();
        private final EvalOperator.ExpressionEvaluator[] values;

        Evaluator(EvalOperator.ExpressionEvaluator[] values) {
            this.values = values;
        }

        @Override
        public BytesRef computeRow(Page page, int position) {
            evaluated.clear();
            for (int i = 0; i < values.length; i++) {
                BytesRef val = (BytesRef) values[i].computeRow(page, position);
                if (val == null) {
                    return null;
                }
                evaluated.append(val);
            }
            return evaluated.get();
        }

        @Override
        public String toString() {
            return "Evaluator{values=" + Arrays.toString(values) + '}';
        }
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Concat(source(), newChildren.get(0), newChildren.subList(1, newChildren.size()));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Concat::new, children().get(0), children().subList(1, children().size()));
    }

    @Override
    public ScriptTemplate asScript() {
        throw new UnsupportedOperationException();
    }
}
