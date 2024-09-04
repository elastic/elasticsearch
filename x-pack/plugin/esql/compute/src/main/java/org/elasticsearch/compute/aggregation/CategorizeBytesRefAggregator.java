/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.CustomAnalyzer;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.xpack.ml.aggs.categorization.CategorizationBytesRefHash;
import org.elasticsearch.xpack.ml.aggs.categorization.CategorizationPartOfSpeechDictionary;
import org.elasticsearch.xpack.ml.aggs.categorization.InternalCategorizationAggregation;
import org.elasticsearch.xpack.ml.aggs.categorization.SerializableTokenListCategory;
import org.elasticsearch.xpack.ml.aggs.categorization.TokenListCategorizer;
import org.elasticsearch.xpack.ml.job.categorization.CategorizationAnalyzer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Categorizes text strings.
 */
@Aggregator({ @IntermediateState(name = "categorize", type = "BYTES_REF_BLOCK") })
@GroupingAggregator
class CategorizeBytesRefAggregator {
    public static SingleState initSingle(BigArrays bigArrays) {
        return new SingleState(bigArrays, createAnalyzer());
    }

    public static void combine(SingleState state, BytesRef v) {
        state.add(v);
    }

    public static void combineIntermediate(SingleState state, BytesRefBlock values) {
        combineIntermediate(state, values, 0);
    }

    public static void combineIntermediate(SingleState state, BytesRefBlock values, int valuesPosition) {
        BytesRef scratch = new BytesRef();
        int start = values.getFirstValueIndex(valuesPosition);
        int end = start + values.getValueCount(valuesPosition);
        ByteArrayStreamInput in = new ByteArrayStreamInput();
        for (int i = start; i < end; i++) {
            values.getBytesRef(i, scratch);
            in.reset(scratch.bytes, scratch.offset, scratch.length);
            try {
                state.categorizer.mergeWireCategory(new SerializableTokenListCategory(in));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static Block evaluateFinal(SingleState state, DriverContext driverContext) {
        return state.toFinal(driverContext.blockFactory());
    }

    public static GroupingState initGrouping(BigArrays bigArrays) {
        return new GroupingState(bigArrays, createAnalyzer());
    }

    public static void combine(GroupingState state, int groupId, BytesRef v) {
        state.getState(groupId).add(v);
    }

    public static void combineIntermediate(GroupingState state, int groupId, BytesRefBlock values, int valuesPosition) {
        combineIntermediate(state.getState(groupId), values, valuesPosition);
    }

    public static void combineStates(GroupingState current, int currentGroupId, GroupingState state, int statePosition) {
        TokenListCategorizer currentCategorizer = current.getState(currentGroupId).categorizer;
        TokenListCategorizer stateCategorizer = state.getState(statePosition).categorizer;
        for (InternalCategorizationAggregation.Bucket bucket : stateCategorizer.toOrderedBuckets(stateCategorizer.getCategoryCount())) {
            currentCategorizer.mergeWireCategory(bucket.getSerializableCategory());
        }
    }

    public static Block evaluateFinal(GroupingState state, IntVector selected, DriverContext driverContext) {
        return state.toFinal(driverContext.blockFactory(), selected);
    }

    private static CategorizationAnalyzer createAnalyzer() {
        // TODO: add correct analyzer, see also: CategorizationAnalyzerConfig::buildStandardCategorizationAnalyzer
        return new CategorizationAnalyzer(
            new CustomAnalyzer(
                TokenizerFactory.newFactory("whitespace", WhitespaceTokenizer::new),
                new CharFilterFactory[0],
                new TokenFilterFactory[0]
            ),
            true
        );
    }

    public static class SingleState implements Releasable {

        private final CategorizationAnalyzer analyzer;
        private final CategorizationBytesRefHash bytesRefHash;
        private final TokenListCategorizer categorizer;

        private SingleState(BigArrays bigArrays, CategorizationAnalyzer analyzer) {
            bytesRefHash = new CategorizationBytesRefHash(new BytesRefHash(2048, bigArrays));
            categorizer = new TokenListCategorizer(bytesRefHash, CategorizationPartOfSpeechDictionary.getInstance(), 0.70f);
            this.analyzer = analyzer;
        }

        void add(BytesRef v) {
            if (v == null || v.length == 0) {
                return;
            }
            String s = v.utf8ToString();
            try (TokenStream ts = analyzer.tokenStream("text", s)) {
                categorizer.computeCategory(ts, s.length(), 1);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            blocks[offset] = toBlock(driverContext.blockFactory());
        }

        Block toBlock(BlockFactory blockFactory) {
            if (categorizer.getCategoryCount() == 0) {
                return blockFactory.newConstantNullBlock(1);
            }
            try (BytesRefBlock.Builder block = blockFactory.newBytesRefBlockBuilder(categorizer.getCategoryCount())) {
                addToBlockIntermediate(block);
                return block.build();
            }
        }

        void addToBlockIntermediate(BytesRefBlock.Builder block) {
            if (categorizer.getCategoryCount() == 0) {
                block.appendNull();
                return;
            }
            block.beginPositionEntry();
            for (InternalCategorizationAggregation.Bucket bucket : categorizer.toOrderedBuckets(categorizer.getCategoryCount())) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                OutputStreamStreamOutput out = new OutputStreamStreamOutput(baos);
                try {
                    bucket.writeTo(out);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                block.appendBytesRef(new BytesRef(baos.toByteArray()));
            }
            block.endPositionEntry();
        }

        Block toFinal(BlockFactory blockFactory) {
            if (categorizer.getCategoryCount() == 0) {
                return blockFactory.newConstantNullBlock(1);
            }
            try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(categorizer.getCategoryCount())) {
                addToBlockFinal(builder);
                return builder.build();
            }
        }

        void addToBlockFinal(BytesRefBlock.Builder block) {
            if (categorizer.getCategoryCount() == 0) {
                block.appendNull();
                return;
            }
            block.beginPositionEntry();
            for (InternalCategorizationAggregation.Bucket bucket : categorizer.toOrderedBuckets(categorizer.getCategoryCount())) {
                // TODO: find something better for this semi-colon-separated string.
                String result = String.join(
                    ";",
                    bucket.getKeyAsString(),
                    bucket.getSerializableCategory().getRegex(),
                    Long.toString(bucket.getDocCount())
                );
                block.appendBytesRef(new BytesRef(result.getBytes(StandardCharsets.UTF_8)));
            }
            block.endPositionEntry();
        }

        @Override
        public void close() {
            Releasables.close(bytesRefHash);
        }
    }

    public static class GroupingState implements Releasable {

        private final BigArrays bigArrays;
        private final CategorizationAnalyzer analyzer;
        private final Map<Integer, SingleState> states;

        private GroupingState(BigArrays bigArrays, CategorizationAnalyzer analyzer) {
            this.bigArrays = bigArrays;
            this.analyzer = analyzer;
            states = new HashMap<>();
        }

        SingleState getState(int groupId) {
            return states.computeIfAbsent(groupId, key -> new SingleState(bigArrays, analyzer));
        }

        void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            blocks[offset] = toBlock(driverContext.blockFactory(), selected);
        }

        Block toBlock(BlockFactory blockFactory, IntVector selected) {
            if (states.isEmpty()) {
                return blockFactory.newConstantNullBlock(selected.getPositionCount());
            }
            try (BytesRefBlock.Builder block = blockFactory.newBytesRefBlockBuilder(selected.getPositionCount())) {
                for (int s = 0; s < selected.getPositionCount(); s++) {
                    SingleState state = states.get(selected.getInt(s));
                    if (state == null) {
                        block.appendNull();
                    } else {
                        state.addToBlockIntermediate(block);
                    }
                }
                return block.build();
            }
        }

        Block toFinal(BlockFactory blockFactory, IntVector selected) {
            if (states.isEmpty()) {
                return blockFactory.newConstantNullBlock(selected.getPositionCount());
            }
            try (BytesRefBlock.Builder block = blockFactory.newBytesRefBlockBuilder(selected.getPositionCount())) {
                for (int s = 0; s < selected.getPositionCount(); s++) {
                    SingleState state = states.get(selected.getInt(s));
                    if (state == null) {
                        block.appendNull();
                    } else {
                        state.addToBlockFinal(block);
                    }
                }
                return block.build();
            }
        }

        void enableGroupIdTracking(SeenGroupIds seen) {}

        @Override
        public void close() {
            for (SingleState state : states.values()) {
                Releasables.closeExpectNoException(state);
            }
        }
    }
}
