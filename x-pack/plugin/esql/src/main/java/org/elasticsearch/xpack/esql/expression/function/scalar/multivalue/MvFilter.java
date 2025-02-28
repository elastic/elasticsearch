/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisPlanVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Lambda;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.planner.Layout;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

public class MvFilter extends EsqlScalarFunction implements PostAnalysisPlanVerificationAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "MvFilter", MvFilter::new);

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
    public MvFilter(Source source, Expression field, Expression lambda) {
        super(source, List.of(field, lambda));
        this.field = field;
        this.lambda = (Lambda) lambda;
    }

    @Override
    public List<Expression> children() {
        return List.of(field, lambda);
    }

    public MvFilter(StreamInput in) throws IOException {
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
        return new MvFilterEvaluator.Factory(toEvaluator.apply(field), subEvaluator.apply(lambda.condition()));
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new MvFilter(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvFilter::new, field, lambda);
    }

    @Override
    public DataType dataType() {
        return field.dataType();
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
    public BiConsumer<LogicalPlan, Failures> postAnalysisPlanVerification() {
        return (plan, failures) -> {
            if (lambda.condition().dataType() != DataType.BOOLEAN) {
                failures.add(fail(field, "mv_filter expects a lambda that returns a boolean value []", lambda.sourceText()));
            }
        };
    }

    static class MvFilterEvaluator implements EvalOperator.ExpressionEvaluator {
        private final EvalOperator.ExpressionEvaluator evaluator;
        private final EvalOperator.ExpressionEvaluator lambdaEvaluator;
        private Lambda lambda;

        MvFilterEvaluator(EvalOperator.ExpressionEvaluator expressionEvaluator, EvalOperator.ExpressionEvaluator lambdaEvaluator) {
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
                Block result;
                Page newPage = new Page(blocks);
                try {
                    try (BooleanBlock evaluated = (BooleanBlock) lambdaEvaluator.eval(newPage)) {
                        result = collapse(evaluated, expandedField, expandingFilter);
                    }
                } finally {
                    newPage.releaseBlocks();
                }
                return result;
            }
        }

        private Block collapse(BooleanBlock condition, Block input, int[] expandingFilter) {
            if (input instanceof LongBlock) {
                LongBlock inputTyped = (LongBlock) input;
                LongBlock.Builder result = inputTyped.blockFactory().newLongBlockBuilder(expandingFilter.length);
                int nextPosition = 0;

                boolean open = false;
                boolean nullToAppend = false;
                if (input.getPositionCount() > 0) {
                    for (int i = 0; i < inputTyped.getPositionCount(); i++) {
                        if (nextPosition < expandingFilter[i]) {
                            if (open) {
                                result.endPositionEntry();
                                open = false;
                            }
                            if (nullToAppend) {
                                result.appendNull();
                            }
                            nextPosition++;
                        }
                        if (inputTyped.isNull(i) == false && condition.isNull(i) == false && condition.getBoolean(i)) {
                            if (open == false) {
                                result.beginPositionEntry();
                                open = true;
                            }
                            result.appendLong(inputTyped.getLong(i));
                            nullToAppend = false;
                        } else {
                            if (open == false) {
                                nullToAppend = true;
                            }
                        }
                    }
                    if (open) {
                        result.endPositionEntry();
                    }
                    if (nullToAppend) {
                        result.appendNull();
                    }
                }
                return result.build();
            } else if (input instanceof IntBlock) {
                IntBlock inputTyped = (IntBlock) input;
                IntBlock.Builder result = inputTyped.blockFactory().newIntBlockBuilder(expandingFilter.length);
                int nextPosition = 0;

                boolean open = false;
                boolean nullToAppend = false;
                if (input.getPositionCount() > 0) {
                    for (int i = 0; i < inputTyped.getPositionCount(); i++) {
                        if (nextPosition < expandingFilter[i]) {
                            if (open) {
                                result.endPositionEntry();
                                open = false;
                            }
                            if (nullToAppend) {
                                result.appendNull();
                            }
                            nextPosition++;
                        }
                        if (inputTyped.isNull(i) == false && condition.isNull(i) == false && condition.getBoolean(i)) {
                            if (open == false) {
                                result.beginPositionEntry();
                                open = true;
                            }
                            result.appendInt(inputTyped.getInt(i));
                            nullToAppend = false;
                        } else {
                            if (open == false) {
                                nullToAppend = true;
                            }
                        }
                    }
                    if (open) {
                        result.endPositionEntry();
                    }
                    if (nullToAppend) {
                        result.appendNull();
                    }
                }
                return result.build();
            } else if (input instanceof BooleanBlock) {
                BooleanBlock inputTyped = (BooleanBlock) input;
                BooleanBlock.Builder result = inputTyped.blockFactory().newBooleanBlockBuilder(expandingFilter.length);
                int nextPosition = 0;

                boolean open = false;
                boolean nullToAppend = false;
                if (input.getPositionCount() > 0) {
                    for (int i = 0; i < inputTyped.getPositionCount(); i++) {
                        if (nextPosition < expandingFilter[i]) {
                            if (open) {
                                result.endPositionEntry();
                                open = false;
                            }
                            if (nullToAppend) {
                                result.appendNull();
                            }
                            nextPosition++;
                        }
                        if (inputTyped.isNull(i) == false && condition.isNull(i) == false && condition.getBoolean(i)) {
                            if (open == false) {
                                result.beginPositionEntry();
                                open = true;
                            }
                            result.appendBoolean(inputTyped.getBoolean(i));
                            nullToAppend = false;
                        } else {
                            if (open == false) {
                                nullToAppend = true;
                            }
                        }
                    }
                    if (open) {
                        result.endPositionEntry();
                    }
                    if (nullToAppend) {
                        result.appendNull();
                    }
                }
                return result.build();
            } else if (input instanceof DoubleBlock) {
                DoubleBlock inputTyped = (DoubleBlock) input;
                DoubleBlock.Builder result = inputTyped.blockFactory().newDoubleBlockBuilder(expandingFilter.length);
                int nextPosition = 0;

                boolean open = false;
                boolean nullToAppend = false;
                if (input.getPositionCount() > 0) {
                    for (int i = 0; i < inputTyped.getPositionCount(); i++) {
                        if (nextPosition < expandingFilter[i]) {
                            if (open) {
                                result.endPositionEntry();
                                open = false;
                            }
                            if (nullToAppend) {
                                result.appendNull();
                            }
                            nextPosition++;
                        }
                        if (inputTyped.isNull(i) == false && condition.isNull(i) == false && condition.getBoolean(i)) {
                            if (open == false) {
                                result.beginPositionEntry();
                                open = true;
                            }
                            result.appendDouble(inputTyped.getDouble(i));
                            nullToAppend = false;
                        } else {
                            if (open == false) {
                                nullToAppend = true;
                            }
                        }
                    }
                    if (open) {
                        result.endPositionEntry();
                    }
                    if (nullToAppend) {
                        result.appendNull();
                    }
                }
                return result.build();
            } else if (input instanceof BytesRefBlock) {
                BytesRefBlock inputTyped = (BytesRefBlock) input;
                BytesRefBlock.Builder result = inputTyped.blockFactory().newBytesRefBlockBuilder(expandingFilter.length);
                int nextPosition = 0;

                boolean open = false;
                boolean nullToAppend = false;
                if (input.getPositionCount() > 0) {
                    BytesRef spare = new BytesRef();
                    for (int i = 0; i < inputTyped.getPositionCount(); i++) {
                        if (nextPosition < expandingFilter[i]) {
                            if (open) {
                                result.endPositionEntry();
                                open = false;
                            }
                            if (nullToAppend) {
                                result.appendNull();
                            }
                            nextPosition++;
                        }
                        if (inputTyped.isNull(i) == false && condition.isNull(i) == false && condition.getBoolean(i)) {
                            if (open == false) {
                                result.beginPositionEntry();
                                open = true;
                            }
                            result.appendBytesRef(inputTyped.getBytesRef(i, spare));
                            nullToAppend = false;
                        } else {
                            if (open == false) {
                                nullToAppend = true;
                            }
                        }
                    }
                    if (open) {
                        result.endPositionEntry();
                    }
                    if (nullToAppend) {
                        result.appendNull();
                    }
                }
                return result.build();
            } else {
                throw new UnsupportedOperationException();
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

                return new MvFilterEvaluator(fieldEval.get(context), lambdaEval.get(context));
            }

            @Override
            public boolean eagerEvalSafeInLazy() {
                return false;
            }
        }
    }
}
