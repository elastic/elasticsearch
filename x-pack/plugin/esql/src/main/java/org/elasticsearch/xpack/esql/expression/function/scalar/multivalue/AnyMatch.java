/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Lambda;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RegexMatch;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class AnyMatch extends EsqlScalarFunction implements TranslationAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "AnyMatch", AnyMatch::new);

    private final Lambda lambda;
    private final Expression field;

    @FunctionInfo(
        returnType = {
            "boolean",
            "cartesian_point",
            "cartesian_shape",
            "date",
            "date_nanos",
            "double",
            "geo_point",
            "geo_shape",
            "integer",
            "ip",
            "keyword",
            "long",
            "unsigned_long",
            "version" },
        description = "TODO"
    )
    public AnyMatch(Source source, Expression field, Expression lambda) {
        super(source, List.of(field, lambda));
        this.field = field;
        this.lambda = (Lambda) lambda;
    }

    @Override
    public List<Expression> children() {
        return List.of(field, lambda);
    }

    public AnyMatch(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        Layout.Builder newLayout = toEvaluator.layout().builder().append(lambda.getFields().get(0));
        ToEvaluator subEvaluator = toEvaluator.withLayout(newLayout.build());
        return new AnyMatchEvaluator.Factory(toEvaluator.apply(field), subEvaluator.apply(lambda.condition()));
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new AnyMatch(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, AnyMatch::new, field, lambda);
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(field);
        out.writeNamedWriteable(lambda);
    }

    @Override
    protected TypeResolution resolveType() {
        return super.resolveType();
    }

    @Override
    public List<? extends Attribute> resolveLambdaInputs(Lambda lambda, List<Attribute> previousInputs) {
        Attribute f = lambda.getFields().get(0);
        if (field.resolved()) {
            return List.of(new ReferenceAttribute(f.source(), f.name(), field.dataType()));
        }
        return previousInputs;
    }

    @Override
    public Object fold(Source source, FoldContext ctx) {
        return super.fold(source, ctx);
    }

    public Lambda lambda() {
        return lambda;
    }

    @Override
    public boolean translatable(LucenePushdownPredicates pushdownPredicates) {
        Expression translatedLambda = translateLambdaForPushdown();
        if (translatedLambda instanceof TranslationAware ta) {
            AttributeSet references = translatedLambda.references();
            if (references.size() == 1 && references.contains(field)) {
                boolean[] unsupported = { false };
                translatedLambda.forEachDown(x -> { // TODO can we do better? Probably...
                    if ((x instanceof FieldAttribute
                        || x instanceof Literal
                        || x instanceof EsqlBinaryComparison
                        || x instanceof RegexMatch) == false) {
                        unsupported[0] = true;
                    }
                });
                return unsupported[0] == false && ta.translatable(pushdownPredicates);
            }
        }
        return false;
    }

    private Expression translateLambdaForPushdown() {
        Expression translatedLambda = lambda.condition()
            .transformDown(ReferenceAttribute.class, x -> x.equals(lambda.getFields().get(0)) ? field : x);
        return translatedLambda;
    }

    @Override
    public Query asQuery(TranslatorHandler handler) {
        return ((TranslationAware) translateLambdaForPushdown()).asQuery(handler);
    }

    static class AnyMatchEvaluator implements EvalOperator.ExpressionEvaluator {
        private final EvalOperator.ExpressionEvaluator evaluator;
        private final EvalOperator.ExpressionEvaluator lambdaEvaluator;

        AnyMatchEvaluator(EvalOperator.ExpressionEvaluator expressionEvaluator, EvalOperator.ExpressionEvaluator lambdaEvaluator) {
            this.evaluator = expressionEvaluator;
            this.lambdaEvaluator = lambdaEvaluator;
        }

        @Override
        public Block eval(Page page) {
            try (Block fieldValue = evaluator.eval(page)) {
                Block expandedField = fieldValue.expand();
                int[] expandingFilter = expandingFilter(fieldValue);
                Block[] blocks = new Block[page.getBlockCount() + 1];
                for (int i = 0; i < page.getBlockCount(); i++) {
                    blocks[i] = page.getBlock(i).filter(expandingFilter);
                }
                blocks[blocks.length - 1] = expandedField;
                BooleanBlock.Builder result;
                Page newPage = new Page(blocks);
                try {
                    try (BooleanBlock evaluated = (BooleanBlock) lambdaEvaluator.eval(newPage)) {
                        result = fieldValue.blockFactory().newBooleanBlockBuilder(fieldValue.getPositionCount());
                        for (int i = 0; i < fieldValue.getPositionCount(); i++) {
                            var firstVAlueIndex = fieldValue.getFirstValueIndex(i);
                            boolean found = false;
                            for (int i1 = 0; i1 < fieldValue.getValueCount(i); i1++) {
                                if (evaluated.getBoolean(firstVAlueIndex + i1)) {
                                    found = true;
                                    break;
                                }
                            }
                            result.appendBoolean(found);
                        }
                    }
                } finally {
                    newPage.releaseBlocks();
                }
                return result.build();
            }
        }

        private int[] expandingFilter(Block expandingBlock) {
            int totalNulls = 0;
            for (int i = 0; i < expandingBlock.getPositionCount(); i++) {
                if (expandingBlock.isNull(i)) {
                    totalNulls++;
                }
            }
            int[] duplicateFilter = new int[expandingBlock.getTotalValueCount() + totalNulls];
            int n = 0;
            for (int i = 0; i < expandingBlock.getPositionCount(); i++) {
                int toAdd = expandingBlock.getValueCount(i);
                if (toAdd == 0) {
                    toAdd = 1; // null values
                }
                Arrays.fill(duplicateFilter, n, n + toAdd, i);
                n += toAdd;
            }
            return duplicateFilter;
        }

        @Override
        public void close() {

        }

        static class Factory implements EvalOperator.ExpressionEvaluator.Factory {
            private final EvalOperator.ExpressionEvaluator.Factory fieldEval;
            private final EvalOperator.ExpressionEvaluator.Factory lambdaEval;

            Factory(EvalOperator.ExpressionEvaluator.Factory fieldEval, EvalOperator.ExpressionEvaluator.Factory lambdaEval) {
                this.fieldEval = fieldEval;
                this.lambdaEval = lambdaEval;
            }

            @Override
            public EvalOperator.ExpressionEvaluator get(DriverContext context) {

                return new AnyMatchEvaluator(fieldEval.get(context), lambdaEval.get(context));
            }

            @Override
            public boolean eagerEvalSafeInLazy() {
                return false;
            }
        }
    }
}
