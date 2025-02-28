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
import org.elasticsearch.xpack.esql.planner.Layout;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class MvMap extends EsqlScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "MvMap", MvMap::new);

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
    public MvMap(Source source, Expression field, Expression lambda) {
        super(source, List.of(field, lambda));
        this.field = field;
        this.lambda = (Lambda) lambda;
    }

    @Override
    public List<Expression> children() {
        return List.of(field, lambda);
    }

    public MvMap(StreamInput in) throws IOException {
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
        return new MvMapEvaluator.Factory(toEvaluator.apply(field), subEvaluator.apply(lambda.condition()));
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new MvMap(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvMap::new, field, lambda);
    }

    @Override
    public DataType dataType() {
        return lambda().dataType();
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

    static class MvMapEvaluator implements EvalOperator.ExpressionEvaluator {
        private final EvalOperator.ExpressionEvaluator evaluator;
        private final EvalOperator.ExpressionEvaluator lambdaEvaluator;
        private Lambda lambda;

        MvMapEvaluator(EvalOperator.ExpressionEvaluator expressionEvaluator, EvalOperator.ExpressionEvaluator lambdaEvaluator) {
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
                    try (Block evaluated = lambdaEvaluator.eval(newPage)) {
                        result = collapse(evaluated, expandingFilter);
                    }
                } finally {
                    newPage.releaseBlocks();
                }
                return result;
            }
        }

        private Block collapse(Block input, int[] expandingFilter) {
            if (input instanceof LongBlock) {
                LongBlock inputBytesRef = (LongBlock) input;
                LongBlock.Builder result = inputBytesRef.blockFactory().newLongBlockBuilder(expandingFilter.length);
                int nextPosition = 0;

                boolean open = false;
                boolean nullToAppend = false;
                if (input.getPositionCount() > 0) {
                    for (int i = 0; i < inputBytesRef.getPositionCount(); i++) {
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
                        if (inputBytesRef.isNull(i) == false) {
                            if (open == false) {
                                result.beginPositionEntry();
                                open = true;
                            }
                            result.appendLong(inputBytesRef.getLong(i));
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
                IntBlock inputBytesRef = (IntBlock) input;
                IntBlock.Builder result = inputBytesRef.blockFactory().newIntBlockBuilder(expandingFilter.length);
                int nextPosition = 0;

                boolean open = false;
                boolean nullToAppend = false;
                if (input.getPositionCount() > 0) {
                    for (int i = 0; i < inputBytesRef.getPositionCount(); i++) {
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
                        if (inputBytesRef.isNull(i) == false) {
                            if (open == false) {
                                result.beginPositionEntry();
                                open = true;
                            }
                            result.appendInt(inputBytesRef.getInt(i));
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
                BooleanBlock inputBytesRef = (BooleanBlock) input;
                BooleanBlock.Builder result = inputBytesRef.blockFactory().newBooleanBlockBuilder(expandingFilter.length);
                int nextPosition = 0;

                boolean open = false;
                boolean nullToAppend = false;
                if (input.getPositionCount() > 0) {
                    for (int i = 0; i < inputBytesRef.getPositionCount(); i++) {
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
                        if (inputBytesRef.isNull(i) == false) {
                            if (open == false) {
                                result.beginPositionEntry();
                                open = true;
                            }
                            result.appendBoolean(inputBytesRef.getBoolean(i));
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
                DoubleBlock inputBytesRef = (DoubleBlock) input;
                DoubleBlock.Builder result = inputBytesRef.blockFactory().newDoubleBlockBuilder(expandingFilter.length);
                int nextPosition = 0;

                boolean open = false;
                boolean nullToAppend = false;
                if (input.getPositionCount() > 0) {
                    for (int i = 0; i < inputBytesRef.getPositionCount(); i++) {
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
                        if (inputBytesRef.isNull(i) == false) {
                            if (open == false) {
                                result.beginPositionEntry();
                                open = true;
                            }
                            result.appendDouble(inputBytesRef.getDouble(i));
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
                BytesRefBlock inputBytesRef = (BytesRefBlock) input;
                BytesRefBlock.Builder result = inputBytesRef.blockFactory().newBytesRefBlockBuilder(expandingFilter.length);
                int nextPosition = 0;

                boolean open = false;
                boolean nullToAppend = false;
                if (input.getPositionCount() > 0) {
                    BytesRef spare = new BytesRef();
                    for (int i = 0; i < inputBytesRef.getPositionCount(); i++) {
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
                        if (inputBytesRef.isNull(i) == false) {
                            if (open == false) {
                                result.beginPositionEntry();
                                open = true;
                            }
                            result.appendBytesRef(inputBytesRef.getBytesRef(i, spare));
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

                return new MvMapEvaluator(fieldEval.get(context), lambdaEval.get(context));
            }

            @Override
            public boolean eagerEvalSafeInLazy() {
                return false;
            }
        }
    }
}
