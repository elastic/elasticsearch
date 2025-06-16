/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.predicate.nulls;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.esql.core.expression.predicate.Negatable;
import org.elasticsearch.xpack.esql.core.querydsl.query.ExistsQuery;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;

import java.io.IOException;

public class IsNotNull extends UnaryScalarFunction implements EvaluatorMapper, Negatable<UnaryScalarFunction>, TranslationAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "IsNotNull",
        IsNotNull::new
    );

    public IsNotNull(Source source, Expression field) {
        super(source, field);
    }

    private IsNotNull(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<IsNotNull> info() {
        return NodeInfo.create(this, IsNotNull::new, field());
    }

    @Override
    protected IsNotNull replaceChild(Expression newChild) {
        return new IsNotNull(source(), newChild);
    }

    @Override
    public Object fold(FoldContext ctx) {
        return DataType.isNull(field().dataType()) == false && field().fold(ctx) != null;
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return new IsNotNullEvaluatorFactory(toEvaluator.apply(field()));

    }

    @Override
    public Nullability nullable() {
        return Nullability.FALSE;
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    public UnaryScalarFunction negate() {
        return new IsNull(source(), field());
    }

    @Override
    public Translatable translatable(LucenePushdownPredicates pushdownPredicates) {
        return IsNull.isTranslatable(field(), pushdownPredicates);
    }

    @Override
    public Query asQuery(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        return new ExistsQuery(source(), handler.nameOf(field()));
    }

    record IsNotNullEvaluatorFactory(EvalOperator.ExpressionEvaluator.Factory field) implements EvalOperator.ExpressionEvaluator.Factory {
        @Override
        public EvalOperator.ExpressionEvaluator get(DriverContext context) {
            return new IsNotNullEvaluator(context, field.get(context));
        }

        @Override
        public String toString() {
            return "IsNotNullEvaluator[field=" + field + ']';
        }
    }

    record IsNotNullEvaluator(DriverContext driverContext, EvalOperator.ExpressionEvaluator field)
        implements
            EvalOperator.ExpressionEvaluator {
        @Override
        public Block eval(Page page) {
            try (Block fieldBlock = field.eval(page)) {
                if (fieldBlock.asVector() != null) {
                    return driverContext.blockFactory().newConstantBooleanBlockWith(true, page.getPositionCount());
                }
                try (var builder = driverContext.blockFactory().newBooleanVectorFixedBuilder(page.getPositionCount())) {
                    for (int p = 0; p < page.getPositionCount(); p++) {
                        builder.appendBoolean(p, fieldBlock.isNull(p) == false);
                    }
                    return builder.build().asBlock();
                }
            }
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(field);
        }

        @Override
        public String toString() {
            return "IsNotNullEvaluator[field=" + field + ']';
        }
    }
}
