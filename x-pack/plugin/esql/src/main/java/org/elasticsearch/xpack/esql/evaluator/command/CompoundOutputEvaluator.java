/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator.command;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.operator.ColumnExtractOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.util.ArrayList;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.IntPredicate;
import java.util.function.ObjIntConsumer;
import java.util.function.Supplier;

import static org.elasticsearch.common.lucene.BytesRefs.toBytesRef;

/**
 * The base evaluator that extracts compound output. Subclasses should implement the actual evaluation logic.
 */
public abstract class CompoundOutputEvaluator<T extends CompoundOutputEvaluator.OutputFieldsCollector>
    implements
        ColumnExtractOperator.Evaluator {

    private final T outputFieldsCollector;
    private final DataType inputType;
    private final Warnings warnings;

    protected CompoundOutputEvaluator(DataType inputType, Warnings warnings, T outputFieldsCollector) {
        this.inputType = inputType;
        this.warnings = warnings;
        this.outputFieldsCollector = outputFieldsCollector;
    }

    /**
     * Executes the evaluation of the corresponding function on the provided input.
     * The {@code target} output array must have the same size as the {@code functionOutputFields} list that was provided in construction,
     * and its elements must match this list's entries in type and order. Otherwise, this method will throw an exception.
     * If an expected output field is missing from the actual output of the function, a null value will be appended to the corresponding
     * target block. If the actual output of the function contains an entry that is not expected, it will be ignored.
     * @param input the input to evaluate the function on
     * @param row row index in the input
     * @param target the output column blocks
     * @param spare the {@link BytesRef} to use for value retrieval
     * @throws EsqlIllegalArgumentException if the {@code target} array does not have the correct size or its elements do not match the
     *                                      expected output fields
     */
    @Override
    public void computeRow(BytesRefBlock input, int row, Block.Builder[] target, BytesRef spare) {
        boolean evaluated = false;
        if (input.isNull(row) == false) {
            try {
                BytesRef bytes = input.getBytesRef(input.getFirstValueIndex(row), spare);
                String inputAsString = getInputAsString(bytes, inputType);
                evaluated = outputFieldsCollector.evaluate(inputAsString, target);
            } catch (Exception e) {
                warnings.registerException(e);
            }
        }

        // if the input is null or invalid, we must return nulls for all output fields
        if (evaluated == false) {
            for (Block.Builder builder : target) {
                builder.appendNull();
            }
        }
    }

    private static String getInputAsString(BytesRef input, DataType inputType) {
        if (inputType == DataType.IP) {
            return EsqlDataTypeConverter.ipToString(input);
        } else if (DataType.isString(inputType)) {
            return input.utf8ToString();
        } else {
            throw new IllegalArgumentException("Unsupported input type [" + inputType + "]");
        }
    }

    /**
     * The base class for output fields collectors.
     * Concrete collectors would implement interfaces that correspond to their corresponding evaluating function, in addition to
     * extending this class.
     */
    abstract static class OutputFieldsCollector {
        /**
         * A {@link Block.Builder[]} holder that is being set before each row evaluation.
         */
        protected final BlocksBearer blocksBearer;

        /**
         * Subclasses must fill this list with a null value collector for each unknown requested output field.
         * Normally, we shouldn't encounter unknown fields. This should only happen in rare cases where the cluster contains nodes
         * of different versions and the coordinating node's version supports a field that is not supported by the executing node.
         */
        protected final ArrayList<Consumer<Block.Builder[]>> unknownFieldCollectors;

        protected OutputFieldsCollector(BlocksBearer blocksBearer) {
            this.blocksBearer = blocksBearer;
            this.unknownFieldCollectors = new ArrayList<>();
        }

        /**
         * The main evaluation logic, dispatching actual evaluation to subclasses.
         * The subclass {@link #evaluate(String)} method would apply the evaluation logic and fill the target blocks accordingly.
         * @param input the input string to evaluate the function on
         * @param target the output column blocks
         * @return {@code true} means that ALL fields were evaluated; {@code false} means that NONE were evaluated
         * @throws Exception if thrown by the evaluation logic, the implementation must guarantee that NO field was evaluated
         */
        boolean evaluate(final String input, final Block.Builder[] target) throws Exception {
            boolean evaluated;
            try {
                blocksBearer.accept(target);
                evaluated = evaluate(input);
            } finally {
                blocksBearer.accept(null);
            }
            if (evaluated && unknownFieldCollectors.isEmpty() == false) {
                // noinspection ForLoopReplaceableByForEach
                for (int i = 0; i < unknownFieldCollectors.size(); i++) {
                    unknownFieldCollectors.get(i).accept(target);
                }
            }
            return evaluated;
        }

        /**
         * IMPORTANT: the implementing method must ensure that the entire evaluation is completed in full before writing values
         * to the output fields. The returned value should indicate whether ALL fields were evaluated or NONE were evaluated.
         * The best practice is to accumulate all output values in local variables/structures, and only write to the output fields at the
         * end of the method.
         * @param input the input string to evaluate the function on
         * @return {@code true} means that ALL fields were evaluated; {@code false} means that NONE were evaluated
         * @throws Exception if thrown by the evaluation logic, the implementation must guarantee that NO field was evaluated
         */
        protected abstract boolean evaluate(String input) throws Exception;
    }

    /**
     * A {@link Block.Builder[]} holder that is being set before each row evaluation.
     */
    public static final class BlocksBearer implements Consumer<Block.Builder[]>, Supplier<Block.Builder[]> {
        private Block.Builder[] blocks;

        @Override
        public void accept(Block.Builder[] blocks) {
            this.blocks = blocks;
        }

        @Override
        public Block.Builder[] get() {
            return blocks;
        }
    }

    protected static final BiConsumer<Block.Builder[], String> NOOP_STRING_COLLECTOR = (blocks, value) -> {/*no-op*/};
    protected static final ObjIntConsumer<Block.Builder[]> NOOP_INT_COLLECTOR = (value, index) -> {/*no-op*/};

    protected static Consumer<Block.Builder[]> nullValueCollector(final int index) {
        return blocks -> blocks[index].appendNull();
    }

    protected static BiConsumer<Block.Builder[], String> stringValueCollector(final int index) {
        return (blocks, value) -> {
            if (value == null) {
                blocks[index].appendNull();
            } else {
                ((BytesRefBlock.Builder) blocks[index]).appendBytesRef(toBytesRef(value));
            }
        };
    }

    /**
     * Creates a collector for primitive int values.
     * @param index the index of the corresponding block in the target array
     * @param predicate the predicate to apply on the int value to determine whether to append it or a null
     * @return a primitive int collector
     */
    protected static ObjIntConsumer<Block.Builder[]> intValueCollector(final int index, final IntPredicate predicate) {
        return (blocks, value) -> {
            if (predicate.test(value)) {
                ((IntBlock.Builder) blocks[index]).appendInt(value);
            } else {
                blocks[index].appendNull();
            }
        };
    }
}
