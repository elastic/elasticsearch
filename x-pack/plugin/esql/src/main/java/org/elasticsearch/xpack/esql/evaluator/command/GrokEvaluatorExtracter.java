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
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.ColumnExtractOperator;
import org.elasticsearch.grok.FloatConsumer;
import org.elasticsearch.grok.Grok;
import org.elasticsearch.grok.GrokCaptureConfig;
import org.elasticsearch.grok.GrokCaptureExtracter;
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

    private final List<GrokCaptureExtracter> fieldExtracters;

    private final boolean[] valuesSet;
    private final Object[] firstValues;
    private final ElementType[] positionToType;
    private Block.Builder[] blocks;

    public GrokEvaluatorExtracter(
        final Grok parser,
        final String pattern,
        final Map<String, Integer> keyToBlock,
        final Map<String, ElementType> types
    ) {
        this.parser = parser;
        this.pattern = pattern;
        this.valuesSet = new boolean[types.size()];
        this.firstValues = new Object[types.size()];
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
                    return buildExtracter.apply(value -> {
                        if (firstValues[blockIdx] == null) {
                            firstValues[blockIdx] = value;
                        } else {
                            BytesRefBlock.Builder block = (BytesRefBlock.Builder) blocks()[blockIdx];
                            if (valuesSet[blockIdx] == false) {
                                block.beginPositionEntry();
                                block.appendBytesRef(new BytesRef((String) firstValues[blockIdx]));
                                valuesSet[blockIdx] = true;
                            }
                            block.appendBytesRef(new BytesRef(value));
                        }
                    });
                }

                @Override
                public GrokCaptureExtracter forInt(Function<IntConsumer, GrokCaptureExtracter> buildExtracter) {
                    return buildExtracter.apply(value -> {
                        if (firstValues[blockIdx] == null) {
                            firstValues[blockIdx] = value;
                        } else {
                            IntBlock.Builder block = (IntBlock.Builder) blocks()[blockIdx];
                            if (valuesSet[blockIdx] == false) {
                                block.beginPositionEntry();
                                block.appendInt((int) firstValues[blockIdx]);
                                valuesSet[blockIdx] = true;
                            }
                            block.appendInt(value);
                        }
                    });
                }

                @Override
                public GrokCaptureExtracter forLong(Function<LongConsumer, GrokCaptureExtracter> buildExtracter) {
                    return buildExtracter.apply(value -> {
                        if (firstValues[blockIdx] == null) {
                            firstValues[blockIdx] = value;
                        } else {
                            LongBlock.Builder block = (LongBlock.Builder) blocks()[blockIdx];
                            if (valuesSet[blockIdx] == false) {
                                block.beginPositionEntry();
                                block.appendLong((long) firstValues[blockIdx]);
                                valuesSet[blockIdx] = true;
                            }
                            block.appendLong(value);
                        }
                    });
                }

                @Override
                public GrokCaptureExtracter forFloat(Function<FloatConsumer, GrokCaptureExtracter> buildExtracter) {
                    return buildExtracter.apply(value -> {
                        if (firstValues[blockIdx] == null) {
                            firstValues[blockIdx] = value;
                        } else {
                            DoubleBlock.Builder block = (DoubleBlock.Builder) blocks()[blockIdx];
                            if (valuesSet[blockIdx] == false) {
                                block.beginPositionEntry();
                                block.appendDouble(((Float) firstValues[blockIdx]).doubleValue());
                                valuesSet[blockIdx] = true;
                            }
                            block.appendDouble(value);
                        }
                    });
                }

                @Override
                public GrokCaptureExtracter forDouble(Function<DoubleConsumer, GrokCaptureExtracter> buildExtracter) {
                    return buildExtracter.apply(value -> {
                        if (firstValues[blockIdx] == null) {
                            firstValues[blockIdx] = value;
                        } else {
                            DoubleBlock.Builder block = (DoubleBlock.Builder) blocks()[blockIdx];
                            if (valuesSet[blockIdx] == false) {
                                block.beginPositionEntry();
                                block.appendDouble((double) firstValues[blockIdx]);
                                valuesSet[blockIdx] = true;
                            }
                            block.appendDouble(value);
                        }
                    });
                }

                @Override
                public GrokCaptureExtracter forBoolean(Function<Consumer<Boolean>, GrokCaptureExtracter> buildExtracter) {
                    return buildExtracter.apply(value -> {
                        if (firstValues[blockIdx] == null) {
                            firstValues[blockIdx] = value;
                        } else {
                            BooleanBlock.Builder block = (BooleanBlock.Builder) blocks()[blockIdx];
                            if (valuesSet[blockIdx] == false) {
                                block.beginPositionEntry();
                                block.appendBoolean((boolean) firstValues[blockIdx]);
                                valuesSet[blockIdx] = true;
                            }
                            block.appendBoolean(value);
                        }
                    });
                }
            }));
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

    public Block.Builder[] blocks() {
        return blocks;
    }

    @Override
    public void computeRow(BytesRefBlock inputBlock, int row, Block.Builder[] blocks, BytesRef spare) {
        this.blocks = blocks;
        int position = inputBlock.getFirstValueIndex(row);
        int valueCount = inputBlock.getValueCount(row);
        Arrays.fill(valuesSet, false);
        Arrays.fill(firstValues, null);
        for (int c = 0; c < valueCount; c++) {
            BytesRef input = inputBlock.getBytesRef(position + c, spare);
            parser.match(input.bytes, input.offset, input.length, this);
        }
        for (int i = 0; i < firstValues.length; i++) {
            if (firstValues[i] == null) {
                this.blocks[i].appendNull();
            } else if (valuesSet[i]) {
                this.blocks[i].endPositionEntry();
            } else {
                append(firstValues[i], blocks[i], positionToType[i]);
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
