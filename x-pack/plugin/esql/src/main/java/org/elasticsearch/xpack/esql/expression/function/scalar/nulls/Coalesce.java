/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.nulls;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner;
import org.elasticsearch.xpack.esql.planner.Mappable;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.Nullability;
import org.elasticsearch.xpack.ql.expression.TypeResolutions;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.ql.type.DataTypes.NULL;

/**
 * Function returning the first non-null value.
 */
public class Coalesce extends ScalarFunction implements Mappable {
    private DataType dataType;

    public Coalesce(Source source, List<Expression> expressions) {
        super(source, expressions);
    }

    @Override
    public DataType dataType() {
        if (dataType == null) {
            resolveType();
        }
        return dataType;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        for (int position = 0; position < children().size(); position++) {
            if (dataType == null || dataType == NULL) {
                dataType = children().get(position).dataType();
                continue;
            }
            TypeResolution resolution = TypeResolutions.isType(
                children().get(position),
                t -> t == dataType,
                sourceText(),
                TypeResolutions.ParamOrdinal.fromIndex(position),
                dataType.typeName()
            );
            if (resolution.unresolved()) {
                return resolution;
            }
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public Nullability nullable() {
        // If any of the children aren't nullable then this isn't.
        for (Expression c : children()) {
            if (c.nullable() == Nullability.FALSE) {
                return Nullability.FALSE;
            }
        }
        /*
         * Otherwise let's call this one "unknown". If we returned TRUE here
         * an optimizer rule would replace this with null if any of our children
         * fold to null. We don't want that at all.
         */
        return Nullability.UNKNOWN;
    }

    @Override
    public ScriptTemplate asScript() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Coalesce(source(), newChildren);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Coalesce::new, children());
    }

    @Override
    public boolean foldable() {
        return Expressions.foldable(children());
    }

    @Override
    public Object fold() {
        return Mappable.super.fold();
    }

    @Override
    public Supplier<EvalOperator.ExpressionEvaluator> toEvaluator(
        Function<Expression, Supplier<EvalOperator.ExpressionEvaluator>> toEvaluator
    ) {
        List<Supplier<EvalOperator.ExpressionEvaluator>> evaluatorSuppliers = children().stream().map(toEvaluator).toList();
        return () -> new CoalesceEvaluator(
            LocalExecutionPlanner.toElementType(dataType()),
            evaluatorSuppliers.stream().map(Supplier::get).toList()
        );
    }

    private record CoalesceEvaluator(ElementType resultType, List<EvalOperator.ExpressionEvaluator> evaluators)
        implements
            EvalOperator.ExpressionEvaluator {
        @Override
        public Block eval(Page page) {
            /*
             * We have to evaluate lazily so any errors or warnings that would be
             * produced by the right hand side are avoided. And so if anything
             * on the right hand side is slow we skip it.
             *
             * And it'd be good if that lazy evaluation were fast. But this
             * implementation isn't. It's fairly simple - running position at
             * a time - but it's not at all fast.
             */
            int positionCount = page.getPositionCount();
            Block.Builder result = resultType.newBlockBuilder(positionCount);
            position: for (int p = 0; p < positionCount; p++) {
                int[] positions = new int[] { p };
                Page limited = new Page(
                    1,
                    IntStream.range(0, page.getBlockCount()).mapToObj(b -> page.getBlock(b).filter(positions)).toArray(Block[]::new)
                );
                for (EvalOperator.ExpressionEvaluator eval : evaluators) {
                    Block e = eval.eval(limited);
                    if (false == e.isNull(0)) {
                        result.copyFrom(e, 0, 1);
                        continue position;
                    }
                }
                result.appendNull();
            }
            return result.build();
        }
    }
}
