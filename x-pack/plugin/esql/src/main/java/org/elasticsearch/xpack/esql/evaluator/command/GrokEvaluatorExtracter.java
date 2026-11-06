/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator.command;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.operator.ColumnExtractOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.grok.FloatConsumer;
import org.elasticsearch.grok.Grok;
import org.elasticsearch.grok.GrokCaptureConfig;
import org.elasticsearch.grok.GrokCaptureExtracter;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.joni.Region;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

public class GrokEvaluatorExtracter implements ColumnExtractOperator.Evaluator, GrokCaptureExtracter {

    private final Grok parser;
    private final String pattern;
    private final Warnings warnings;

    private final List<GrokCaptureExtracter> fieldExtracters;

    // Values extracted from the current row are buffered here and only written to the block
    // builders once the whole row was extracted successfully. This way a failed type conversion
    // (eg. %{NUMBER:n:int} matching "1.5") can discard the row's values and emit nulls instead,
    // without leaving partially written block builders behind.
    private final Object[] firstValues;
    private final List<Object>[] extraValues;
    private final ElementType[] positionToType;

    private GrokEvaluatorExtracter(
        final Grok parser,
        final String pattern,
        final Map<String, Integer> keyToBlock,
        final Map<String, ElementType> types,
        final Warnings warnings
    ) {
        this.parser = parser;
        this.pattern = pattern;
        this.warnings = warnings;
        this.firstValues = new Object[types.size()];
        this.extraValues = newExtraValues(types.size());
        this.positionToType = new ElementType[types.size()];

        fieldExtracters = new ArrayList<>(parser.captureConfig().size());
        for (GrokCaptureConfig config : parser.captureConfig()) {
            var key = config.name();
            ElementType type = types.get(key);
            Integer blockIdx = keyToBlock.get(key);
            positionToType[blockIdx] = type;

            fieldExtracters.add(config.nativeExtracter(new GrokCaptureConfig.NativeExtracterMap<>() {
                @Override
                public GrokCaptureExtracter forString(Function<Consumer<String>, GrokCaptureExtracter> buildExtracter) {
                    return buildExtracter.apply(value -> addValue(blockIdx, value));
                }

                @Override
                public GrokCaptureExtracter forInt(Function<IntConsumer, GrokCaptureExtracter> buildExtracter) {
                    return buildExtracter.apply(value -> addValue(blockIdx, value));
                }

                @Override
                public GrokCaptureExtracter forLong(Function<LongConsumer, GrokCaptureExtracter> buildExtracter) {
                    return buildExtracter.apply(value -> addValue(blockIdx, value));
                }

                @Override
                public GrokCaptureExtracter forFloat(Function<FloatConsumer, GrokCaptureExtracter> buildExtracter) {
                    return buildExtracter.apply(value -> addValue(blockIdx, value));
                }

                @Override
                public GrokCaptureExtracter forDouble(Function<DoubleConsumer, GrokCaptureExtracter> buildExtracter) {
                    return buildExtracter.apply(value -> addValue(blockIdx, value));
                }

                @Override
                public GrokCaptureExtracter forBoolean(Function<Consumer<Boolean>, GrokCaptureExtracter> buildExtracter) {
                    return buildExtracter.apply(value -> addValue(blockIdx, value));
                }
            }));
        }

    }

    // Arrays of generics cannot be created directly; the array is only ever accessed per element, so this is safe.
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static List<Object>[] newExtraValues(int size) {
        return new List[size];
    }

    private void addValue(int blockIdx, Object value) {
        if (firstValues[blockIdx] == null) {
            firstValues[blockIdx] = value;
        } else {
            if (extraValues[blockIdx] == null) {
                extraValues[blockIdx] = new ArrayList<>();
            }
            extraValues[blockIdx].add(value);
        }
    }

    public record Factory(Source source, Grok parser, String pattern, Map<String, Integer> keyToBlock, Map<String, ElementType> types)
        implements
            ColumnExtractOperator.Evaluator.Factory {

        @Override
        public GrokEvaluatorExtracter create(DriverContext driverContext) {
            // On this branch Source does not implement WarningSourceLocation, so pass the location explicitly.
            Warnings warnings = (driverContext == null || source == null)
                ? Warnings.NOOP_WARNINGS
                : driverContext.createWarnings(source.source().getLineNumber(), source.source().getColumnNumber(), source.text());
            return new GrokEvaluatorExtracter(parser, pattern, keyToBlock, types, warnings);
        }

        @Override
        public String describe() {
            return "GrokEvaluatorExtracter[pattern=" + pattern + "]";
        }
    }

    private static void append(Object value, Block.Builder block, ElementType type) {
        if (value instanceof Float f) {
            // Grok patterns can produce float values (Eg. %{WORD:x:float})
            // Since ESQL does not support floats natively, but promotes them to Double, we are doing promotion here
            // TODO remove when floats are supported
            ((DoubleBlock.Builder) block).appendDouble(f.doubleValue());
        } else {
            BlockUtils.appendValue(block, value, type);
        }
    }

    @Override
    public void computeRow(BytesRefBlock inputBlock, int row, Block.Builder[] blocks, BytesRef spare) {
        int position = inputBlock.getFirstValueIndex(row);
        int valueCount = inputBlock.getValueCount(row);
        Arrays.fill(firstValues, null);
        Arrays.fill(extraValues, null);
        for (int c = 0; c < valueCount; c++) {
            BytesRef input = inputBlock.getBytesRef(position + c, spare);
            try {
                parser.match(input.bytes, input.offset, input.length, this);
            } catch (NumberFormatException e) {
                // A typed capture (eg. %{NUMBER:n:int}) matched a value it could not convert.
                // Treat the row as a failed match instead of failing the whole query.
                warnings.registerException(e);
                Arrays.fill(firstValues, null);
                Arrays.fill(extraValues, null);
                break;
            }
        }
        for (int i = 0; i < firstValues.length; i++) {
            if (firstValues[i] == null) {
                blocks[i].appendNull();
            } else if (extraValues[i] == null) {
                append(firstValues[i], blocks[i], positionToType[i]);
            } else {
                blocks[i].beginPositionEntry();
                append(firstValues[i], blocks[i], positionToType[i]);
                for (Object value : extraValues[i]) {
                    append(value, blocks[i], positionToType[i]);
                }
                blocks[i].endPositionEntry();
            }
        }
    }

    @Override
    public void extract(byte[] utf8Bytes, int offset, Region region) {
        fieldExtracters.forEach(extracter -> extracter.extract(utf8Bytes, offset, region));
    }

    @Override
    public String toString() {
        return "GrokEvaluatorExtracter[pattern=" + pattern + "]";
    }
}
