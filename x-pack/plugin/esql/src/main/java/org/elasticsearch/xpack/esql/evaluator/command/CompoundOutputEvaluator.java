/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator.command;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
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
import java.util.function.ObjLongConsumer;

/**
 * The base evaluator that extracts compound output. Subclasses should implement the actual evaluation logic.
 */
public final class CompoundOutputEvaluator implements ColumnExtractOperator.Evaluator {

    public enum MultiValueStrategy {
        /** Reject multi-value input with a warning and null output (default for USER_AGENT, URI_PARTS, REGISTERED_DOMAIN). */
        REJECT,
        /** Consume the first value of a multi-value cell (used by IP_LOCATION with first_only=true). */
        TAKE_FIRST
    }

    private final OutputFieldsCollector outputFieldsCollector;
    private final DataType inputType;
    private final MultiValueStrategy multiValueStrategy;
    private final Warnings warnings;

    CompoundOutputEvaluator(
        DataType inputType,
        MultiValueStrategy multiValueStrategy,
        Warnings warnings,
        OutputFieldsCollector outputFieldsCollector
    ) {
        this.inputType = inputType;
        this.multiValueStrategy = multiValueStrategy;
        this.warnings = warnings;
        this.outputFieldsCollector = outputFieldsCollector;
        this.outputFieldsCollector.warnings = warnings;
    }

    public interface OutputFieldsCollectorProvider {
        OutputFieldsCollector createOutputFieldsCollector();

        String collectorSimpleName();
    }

    public record Factory(
        DataType inputType,
        Source source,
        OutputFieldsCollectorProvider outputFieldsCollectorProvider,
        MultiValueStrategy multiValueStrategy
    ) implements ColumnExtractOperator.Evaluator.Factory {

        public CompoundOutputEvaluator create(DriverContext driverContext) {
            Warnings warnings = (driverContext == null || source == null)
                ? Warnings.NOOP_WARNINGS
                : Warnings.createWarnings(driverContext.warningsMode(), source);
            OutputFieldsCollector outputFieldsCollector = outputFieldsCollectorProvider.createOutputFieldsCollector();
            return new CompoundOutputEvaluator(inputType, multiValueStrategy, warnings, outputFieldsCollector);
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
                    if (input.getValueCount(row) == 1 || multiValueStrategy == MultiValueStrategy.TAKE_FIRST) {
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
     * Concrete collectors may implement interfaces that correspond to their corresponding evaluating function, in addition to
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

        /**
         * The warnings sink for this driver. Set exactly once by
         * {@link CompoundOutputEvaluator#CompoundOutputEvaluator} after construction;
         * defaults to {@link Warnings#NOOP_WARNINGS} until then. Package-private so
         * that only collectors in this package (e.g. IP_LOCATION) can access it.
         */
        Warnings warnings = Warnings.NOOP_WARNINGS;

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
     * <p>
     * All {@code append*} methods are allocation-free on the steady-state per-row path:
     * a shared {@link BytesRefBuilder} is reused across rows for all {@link BytesRef}-producing overloads.
     */
    public static final class RowOutput {
        final boolean[] valuesSet;
        private final BytesRefBuilder scratch = new BytesRefBuilder();
        Block.Builder[] blocks;

        RowOutput(int size) {
            valuesSet = new boolean[size];
        }

        void startRow(Block.Builder[] blocks) {
            this.blocks = blocks;
            Arrays.fill(valuesSet, false);
        }

        /**
         * Appends a string value as a KEYWORD (BytesRef) block entry. No-ops on null input.
         */
        void appendValue(String value, int index) {
            if (value != null) {
                scratch.clear();
                scratch.copyChars(value);
                ((BytesRefBlock.Builder) blocks[index]).appendBytesRef(scratch.get());
                valuesSet[index] = true;
            }
        }

        /**
         * Appends a primitive int value. Cannot receive null — callers must guard.
         */
        void appendValue(int value, int index) {
            ((IntBlock.Builder) blocks[index]).appendInt(value);
            valuesSet[index] = true;
        }

        /**
         * Appends a primitive boolean value. Cannot receive null — callers must guard.
         */
        void appendValue(boolean value, int index) {
            ((BooleanBlock.Builder) blocks[index]).appendBoolean(value);
            valuesSet[index] = true;
        }

        /**
         * Appends a primitive long value. Cannot receive null — callers must guard.
         */
        void appendValue(long value, int index) {
            ((LongBlock.Builder) blocks[index]).appendLong(value);
            valuesSet[index] = true;
        }

        /**
         * Encodes a geo-point as WKB directly into the scratch buffer (fixed 21 bytes, no allocation)
         * and appends it to a BytesRef block. The axis swap from (lat, lon) callback order to
         * WKB (x=lon, y=lat) is performed here.
         */
        void appendGeoPoint(double lat, double lon, int index) {
            scratch.clear();
            scratch.grow(21);
            byte[] b = scratch.bytes();
            b[0] = 1;                                          // byte-order: little-endian
            b[1] = 1;
            b[2] = 0;
            b[3] = 0;
            b[4] = 0;          // geometry type: Point (1) as int32 LE
            writeDoubleLE(b, 5, lon);                           // x = longitude
            writeDoubleLE(b, 13, lat);                          // y = latitude
            scratch.setLength(21);
            ((BytesRefBlock.Builder) blocks[index]).appendBytesRef(scratch.get());
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

        private static void writeDoubleLE(byte[] b, int offset, double value) {
            long bits = Double.doubleToLongBits(value);
            b[offset] = (byte) bits;
            b[offset + 1] = (byte) (bits >>> 8);
            b[offset + 2] = (byte) (bits >>> 16);
            b[offset + 3] = (byte) (bits >>> 24);
            b[offset + 4] = (byte) (bits >>> 32);
            b[offset + 5] = (byte) (bits >>> 40);
            b[offset + 6] = (byte) (bits >>> 48);
            b[offset + 7] = (byte) (bits >>> 56);
        }
    }

    public static final BiConsumer<RowOutput, String> NOOP_STRING_COLLECTOR = (blocks, value) -> {/*no-op*/};
    public static final ObjIntConsumer<RowOutput> NOOP_INT_COLLECTOR = (value, index) -> {/*no-op*/};
    public static final ObjLongConsumer<RowOutput> NOOP_LONG_COLLECTOR = (value, index) -> {/*no-op*/};
    public static final ObjBooleanConsumer<RowOutput> NOOP_BOOLEAN_COLLECTOR = (value, b) -> {/*no-op*/};
    public static final GeoPointCollector NOOP_GEO_POINT_COLLECTOR = (out, lat, lon) -> {/*no-op*/};

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

    public static ObjLongConsumer<RowOutput> longValueCollector(final int index) {
        return (rowOutput, value) -> rowOutput.appendValue(value, index);
    }

    public static ObjBooleanConsumer<RowOutput> booleanValueCollector(final int index) {
        return (rowOutput, value) -> rowOutput.appendValue(value, index);
    }

    public static GeoPointCollector geoPointValueCollector(final int index) {
        return (rowOutput, lat, lon) -> rowOutput.appendGeoPoint(lat, lon, index);
    }

    @FunctionalInterface
    public interface ObjBooleanConsumer<T> {
        void accept(T t, boolean value);
    }

    @FunctionalInterface
    public interface GeoPointCollector {
        void accept(RowOutput out, double lat, double lon);
    }
}
