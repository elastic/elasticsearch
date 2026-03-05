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
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.util.Arrays;
import java.util.function.BiConsumer;
import java.util.function.IntPredicate;
import java.util.function.ObjIntConsumer;

import static org.elasticsearch.common.lucene.BytesRefs.toBytesRef;

/**
 * The base evaluator that extracts compound output. Subclasses should implement the actual evaluation logic.
 */
public final class CompoundOutputEvaluator implements ColumnExtractOperator.Evaluator {

    private final OutputFieldsCollector outputFieldsCollector;
    private final DataType inputType;
    private final Warnings warnings;

    CompoundOutputEvaluator(DataType inputType, Warnings warnings, OutputFieldsCollector outputFieldsCollector) {
        this.inputType = inputType;
        this.warnings = warnings;
        this.outputFieldsCollector = outputFieldsCollector;
    }

    public interface OutputFieldsCollectorProvider {
        OutputFieldsCollector createOutputFieldsCollector();

        String collectorSimpleName();
    }

    public record Factory(DataType inputType, Source source, OutputFieldsCollectorProvider outputFieldsCollectorProvider)
        implements
            ColumnExtractOperator.Evaluator.Factory {

        public CompoundOutputEvaluator create(DriverContext driverContext) {
            Warnings warnings = (driverContext == null || source == null)
                ? Warnings.NOOP_WARNINGS
                : Warnings.createWarnings(driverContext.warningsMode(), source);
            OutputFieldsCollector outputFieldsCollector = outputFieldsCollectorProvider.createOutputFieldsCollector();
            return new CompoundOutputEvaluator(inputType, warnings, outputFieldsCollector);
        }

        @Override
        public String describe() {
            return "CompoundOutputEvaluator[collector=" + outputFieldsCollectorProvider.collectorSimpleName() + "]";
        }
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
     */
    @Override
    public void computeRow(BytesRefBlock input, int row, Block.Builder[] target, BytesRef spare) {
        outputFieldsCollector.startRow(target);
        try {
            if (input.isNull(row) == false) {
                try {
                    if (input.getValueCount(row) == 1) {
                        BytesRef bytes = input.getBytesRef(input.getFirstValueIndex(row), spare);
                        String inputAsString = getInputAsString(bytes, inputType);
                        outputFieldsCollector.evaluate(inputAsString);
                    } else {
                        warnings.registerException(new IllegalArgumentException("This command doesn't support multi-value input"));
                    }
                } catch (IllegalArgumentException e) {
                    warnings.registerException(e);
                }
            }
        } finally {
            // this takes care of missing fields, partial evaluation and null input
            outputFieldsCollector.endRow();
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

    @Override
    public String toString() {
        return "CompoundOutputEvaluator[collector=" + outputFieldsCollector + "]";
    }

    /**
     * The base class for output fields collectors.
     * Concrete collectors would implement interfaces that correspond to their corresponding evaluating function, in addition to
     * extending this class.
     */
    public abstract static class OutputFieldsCollector {
        @Override
        public String toString() {
            return getClass().getSimpleName();
        }

        /**
         * A {@link Block.Builder[]} holder that is being set before each row evaluation.
         * In addition, it tracks the status of the output fields for the current row.
         */
        protected final RowOutput rowOutput;

        protected OutputFieldsCollector(int outputFieldCount) {
            this.rowOutput = new RowOutput(outputFieldCount);
        }

        /**
         * initialize the row output state for a new row evaluation.
         * @param target the output column blocks
         */
        final void startRow(Block.Builder[] target) {
            rowOutput.startRow(target);
        }

        final void endRow() {
            rowOutput.fillMissingValues();
            rowOutput.reset();
        }

        /**
         * Subclasses would apply the actual evaluation logic here and fill the target blocks accordingly.
         * @param input the input string to evaluate the function on
         */
        protected abstract void evaluate(String input);
    }

    /**
     * A {@link Block.Builder[]} holder that is being set before each row evaluation.
     * In addition, it tracks the status of the output fields for the current row.
     */
    public static final class RowOutput {
        final boolean[] valuesSet;
        Block.Builder[] blocks;

        RowOutput(int size) {
            valuesSet = new boolean[size];
        }

        void startRow(Block.Builder[] blocks) {
            this.blocks = blocks;
            Arrays.fill(valuesSet, false);
        }

        void appendValue(String value, int index) {
            if (value != null) {
                ((BytesRefBlock.Builder) blocks[index]).appendBytesRef(toBytesRef(value));
                valuesSet[index] = true;
            }
        }

        void appendValue(int value, int index) {
            ((IntBlock.Builder) blocks[index]).appendInt(value);
            valuesSet[index] = true;
        }

        void fillMissingValues() {
            for (int i = 0; i < valuesSet.length; i++) {
                if (valuesSet[i] == false) {
                    blocks[i].appendNull();
                }
            }
        }

        void reset() {
            // avoid leaking blocks
            blocks = null;
        }
    }

    public static final BiConsumer<RowOutput, String> NOOP_STRING_COLLECTOR = (blocks, value) -> {/*no-op*/};
    public static final ObjIntConsumer<RowOutput> NOOP_INT_COLLECTOR = (value, index) -> {/*no-op*/};

    public static BiConsumer<RowOutput, String> stringValueCollector(final int index) {
        return (rowOutput, value) -> rowOutput.appendValue(value, index);
    }

    /**
     * Creates a collector for primitive int values.
     * @param index the index of the corresponding block in the target array
     * @param predicate the predicate to apply on the int value to determine whether to append it or a null
     * @return a primitive int collector
     */
    public static ObjIntConsumer<RowOutput> intValueCollector(final int index, final IntPredicate predicate) {
        return (rowOutput, value) -> {
            if (predicate.test(value)) {
                rowOutput.appendValue(value, index);
            }
        };
    }
}
