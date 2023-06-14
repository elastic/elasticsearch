/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.operator.ColumnExtractOperator;
import org.elasticsearch.grok.Grok;
import org.elasticsearch.grok.GrokCaptureConfig;
import org.elasticsearch.grok.GrokCaptureExtracter;
import org.joni.Region;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class GrokEvaluatorExtracter implements ColumnExtractOperator.Evaluator, GrokCaptureExtracter {

    private final Grok parser;
    private final String pattern;

    private final List<GrokCaptureExtracter> fieldExtracters;

    private final boolean[] valuesSet;
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
        fieldExtracters = new ArrayList<>(parser.captureConfig().size());
        for (GrokCaptureConfig config : parser.captureConfig()) {
            fieldExtracters.add(config.objectExtracter(value -> {
                var key = config.name();
                Integer blockIdx = keyToBlock.get(key);
                if (valuesSet[blockIdx]) {
                    // Grok patterns can return multi-values
                    // eg.
                    // %{WORD:name} (%{WORD:name})?
                    // for now we return the first value
                    // TODO enhance when multi-values are supported
                    return;
                }
                ElementType type = types.get(key);
                if (value instanceof Float f) {
                    // Grok patterns can produce float values (Eg. %{WORD:x:float})
                    // Since ESQL does not support floats natively, but promotes them to Double, we are doing promotion here
                    // TODO remove when floats are supported
                    ((DoubleBlock.Builder) blocks()[blockIdx]).appendDouble(f.doubleValue());
                } else {
                    BlockUtils.appendValue(blocks()[blockIdx], value, type);
                }
                valuesSet[blockIdx] = true;
            }));
        }

    }

    public Block.Builder[] blocks() {
        return blocks;
    }

    @Override
    public void computeRow(BytesRef input, Block.Builder[] blocks) {
        if (input == null) {
            setAllNull(blocks);
            return;
        }
        this.blocks = blocks;
        Arrays.fill(valuesSet, false);
        boolean matched = parser.match(input.bytes, input.offset, input.length, this);
        if (matched) {
            for (int i = 0; i < valuesSet.length; i++) {
                // set null all the optionals not set
                if (valuesSet[i] == false) {
                    this.blocks[i].appendNull();
                }
            }
        } else {
            setAllNull(blocks);
        }
    }

    private static void setAllNull(Block.Builder[] blocks) {
        for (Block.Builder builder : blocks) {
            builder.appendNull();
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
