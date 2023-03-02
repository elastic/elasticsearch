/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;

import java.util.function.Supplier;

@Experimental
public class EvalOperator implements Operator {

    public record EvalOperatorFactory(Supplier<ExpressionEvaluator> evaluator, ElementType elementType) implements OperatorFactory {

        @Override
        public Operator get() {
            return new EvalOperator(evaluator.get(), elementType);
        }

        @Override
        public String describe() {
            return "EvalOperator[elementType=" + elementType + ", evaluator=" + evaluator.get() + "]";
        }
    }

    private final ExpressionEvaluator evaluator;
    private final ElementType elementType;

    boolean finished;

    Page lastInput;

    public EvalOperator(ExpressionEvaluator evaluator, ElementType elementType) {
        this.evaluator = evaluator;
        this.elementType = elementType;
    }

    @Override
    public Page getOutput() {
        if (lastInput == null) {
            return null;
        }
        int rowsCount = lastInput.getPositionCount();
        Page lastPage = lastInput.appendBlock(switch (elementType) {
            case LONG -> {
                var blockBuilder = LongBlock.newBlockBuilder(rowsCount);
                for (int i = 0; i < rowsCount; i++) {
                    Number result = (Number) evaluator.computeRow(lastInput, i);
                    if (result == null) {
                        blockBuilder.appendNull();
                    } else {
                        blockBuilder.appendLong(result.longValue());
                    }
                }
                yield blockBuilder.build();
            }
            case INT -> {
                var blockBuilder = IntBlock.newBlockBuilder(rowsCount);
                for (int i = 0; i < lastInput.getPositionCount(); i++) {
                    Number result = (Number) evaluator.computeRow(lastInput, i);
                    if (result == null) {
                        blockBuilder.appendNull();
                    } else {
                        blockBuilder.appendInt(result.intValue());
                    }
                }
                yield blockBuilder.build();
            }
            case BYTES_REF -> {
                var blockBuilder = BytesRefBlock.newBlockBuilder(rowsCount);
                for (int i = 0; i < lastInput.getPositionCount(); i++) {
                    BytesRef result = (BytesRef) evaluator.computeRow(lastInput, i);
                    if (result == null) {
                        blockBuilder.appendNull();
                    } else {
                        blockBuilder.appendBytesRef(result);
                    }
                }
                yield blockBuilder.build();
            }
            case DOUBLE -> {
                var blockBuilder = DoubleBlock.newBlockBuilder(rowsCount);
                for (int i = 0; i < lastInput.getPositionCount(); i++) {
                    Number result = (Number) evaluator.computeRow(lastInput, i);
                    if (result == null) {
                        blockBuilder.appendNull();
                    } else {
                        blockBuilder.appendDouble(result.doubleValue());
                    }
                }
                yield blockBuilder.build();
            }
            case BOOLEAN -> {
                var blockBuilder = BooleanBlock.newBlockBuilder(rowsCount);
                for (int i = 0; i < lastInput.getPositionCount(); i++) {
                    Boolean result = (Boolean) evaluator.computeRow(lastInput, i);
                    if (result == null) {
                        blockBuilder.appendNull();
                    } else {
                        blockBuilder.appendBoolean(result);
                    }
                }
                yield blockBuilder.build();
            }
            case NULL -> Block.constantNullBlock(rowsCount);
            default -> throw new UnsupportedOperationException("unsupported element type [" + elementType + "]");
        });

        lastInput = null;
        return lastPage;
    }

    @Override
    public boolean isFinished() {
        return lastInput == null && finished;
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean needsInput() {
        return lastInput == null && finished == false;
    }

    @Override
    public void addInput(Page page) {
        lastInput = page;
    }

    @Override
    public void close() {

    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName()).append("[");
        sb.append("elementType=").append(elementType).append(", ");
        sb.append("evaluator=").append(evaluator);
        sb.append("]");
        return sb.toString();
    }

    public interface ExpressionEvaluator {
        Object computeRow(Page page, int position);
    }
}
