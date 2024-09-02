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
import org.elasticsearch.common.util.LongLongHash;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
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
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.xpack.ml.aggs.categorization.CategorizationBytesRefHash;
import org.elasticsearch.xpack.ml.aggs.categorization.CategorizationPartOfSpeechDictionary;
import org.elasticsearch.xpack.ml.aggs.categorization.InternalCategorizationAggregation;
import org.elasticsearch.xpack.ml.aggs.categorization.SerializableTokenListCategory;
import org.elasticsearch.xpack.ml.aggs.categorization.TokenListCategorizer;
import org.elasticsearch.xpack.ml.aggs.categorization.TokenListCategory;
import org.elasticsearch.xpack.ml.job.categorization.CategorizationAnalyzer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Blah blah
 */
@Aggregator({ @IntermediateState(name = "categorize", type = "BYTES_REF_BLOCK") })
@GroupingAggregator
class CategorizeBytesRefAggregator {
    public static SingleState initSingle(BigArrays bigArrays) {
        return new SingleState(bigArrays);
    }

    public static void combine(SingleState state, BytesRef v) {
        state.add(v);
    }

    public static void combineIntermediate(SingleState state, BytesRefBlock values) {
        BytesRef scratch = new BytesRef();
        int start = values.getFirstValueIndex(0);
        int end = start + values.getValueCount(0);
        for (int i = start; i < end; i++) {
            values.getBytesRef(i, scratch);
            ByteArrayStreamInput in = new ByteArrayStreamInput();
            in.reset(scratch.bytes, scratch.offset, scratch.length);
            try {
                SerializableTokenListCategory category = new SerializableTokenListCategory(in);
                state.categorizer.mergeWireCategory(category);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static Block evaluateFinal(SingleState state, DriverContext driverContext) {
        return state.toFinal(driverContext.blockFactory());
    }

    public static GroupingState initGrouping(BigArrays bigArrays) {
        return new GroupingState(bigArrays);
    }

    public static void combine(GroupingState state, int groupId, BytesRef v) {
        state.values.add(groupId, BlockHash.hashOrdToGroup(state.bytes.add(v)));
    }

    public static void combineIntermediate(GroupingState state, int groupId, BytesRefBlock values, int valuesPosition) {
        BytesRef scratch = new BytesRef();
        int start = values.getFirstValueIndex(valuesPosition);
        int end = start + values.getValueCount(valuesPosition);
        for (int i = start; i < end; i++) {
            combine(state, groupId, values.getBytesRef(i, scratch));
        }
    }

    public static void combineStates(GroupingState current, int currentGroupId, GroupingState state, int statePosition) {
        BytesRef scratch = new BytesRef();
        for (int id = 0; id < state.values.size(); id++) {
            if (state.values.getKey1(id) == statePosition) {
                long value = state.values.getKey2(id);
                combine(current, currentGroupId, state.bytes.get(value, scratch));
            }
        }
    }

    public static Block evaluateFinal(GroupingState state, IntVector selected, DriverContext driverContext) {
        return state.toBlock(driverContext.blockFactory(), selected);
    }

    public static class SingleState implements Releasable {

        private final CategorizationAnalyzer analyzer;
        private final CategorizationBytesRefHash bytesRefHash;
        private final TokenListCategorizer categorizer;

        private SingleState(BigArrays bigArrays) {
            // TODO: add correct analyzer, see also: CategorizationAnalyzerConfig::buildStandardCategorizationAnalyzer
            analyzer = new CategorizationAnalyzer(
                new CustomAnalyzer(
                TokenizerFactory.newFactory("whitespace", WhitespaceTokenizer::new),
                new CharFilterFactory[0],
                new TokenFilterFactory[0]
            ), true);
            bytesRefHash = new CategorizationBytesRefHash(new BytesRefHash(2048, bigArrays));
            categorizer = new TokenListCategorizer(
                bytesRefHash,
                CategorizationPartOfSpeechDictionary.getInstance(),
                0.70f
            );
        }

        void add(BytesRef v) {
            TokenStream ts = analyzer.tokenStream("text", v.utf8ToString());
            try {
                TokenListCategory category = categorizer.computeCategory(ts, 999, 1);
                System.out.println(category + " is category of: " + v.utf8ToString());
                ts.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            blocks[offset] = toBlock(driverContext.blockFactory());
        }

        Block toBlock(BlockFactory blockFactory) {
            InternalCategorizationAggregation.Bucket[] buckets = categorizer.toOrderedBuckets(categorizer.getCategoryCount());
            if (buckets.length == 0) {
                return blockFactory.newConstantNullBlock(1);
            }
//            BytesRef scratch = new BytesRef();
//            if (values.size() == 1) {
//                return blockFactory.newConstantBytesRefBlockWith(BytesRef.deepCopyOf(values.get(0, scratch)), 1);
//            }
            try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(buckets.length)) {
                builder.beginPositionEntry();
                for (int id = 0; id < buckets.length; id++) {
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    OutputStreamStreamOutput out = new OutputStreamStreamOutput(baos);
                    try {
                        System.out.println("***BUCKET["+id+"] = "+buckets[id]);
                        buckets[id].writeTo(out);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    builder.appendBytesRef(new BytesRef(baos.toByteArray()));
                }
                builder.endPositionEntry();
                return builder.build();
            }
        }

        Block toFinal(BlockFactory blockFactory) {
            InternalCategorizationAggregation.Bucket[] buckets = categorizer.toOrderedBuckets(categorizer.getCategoryCount());
            if (buckets.length == 0) {
                return blockFactory.newConstantNullBlock(1);
            }
            try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(buckets.length)) {
                builder.beginPositionEntry();
                for (InternalCategorizationAggregation.Bucket bucket : buckets) {
                    String result = String.join(";",
                        bucket.getKeyAsString(),
                        bucket.getSerializableCategory().getRegex(),
                        Long.toString(bucket.getDocCount())
                    );
                    builder.appendBytesRef(new BytesRef(result.getBytes(StandardCharsets.UTF_8)));
                }
                builder.endPositionEntry();
                return builder.build();
            }
        }

        @Override
        public void close() {
            Releasables.close(bytesRefHash);
        }
    }

    /**
     * State for a grouped {@code VALUES} aggregation. This implementation
     * emphasizes collect-time performance over the performance of rendering
     * results. That's good, but it's a pretty intensive emphasis, requiring
     * an {@code O(n^2)} operation for collection to support a {@code O(1)}
     * collector operation. But at least it's fairly simple.
     */
    public static class GroupingState implements Releasable {

        private final LongLongHash values;
        private final BytesRefHash bytes;

        private GroupingState(BigArrays bigArrays) {
            values = new LongLongHash(1, bigArrays);
            bytes = new BytesRefHash(1, bigArrays);
        }

        void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            blocks[offset] = toBlock(driverContext.blockFactory(), selected);
        }

        Block toBlock(BlockFactory blockFactory, IntVector selected) {
            if (values.size() == 0) {
                return blockFactory.newConstantNullBlock(selected.getPositionCount());
            }
            BytesRef scratch = new BytesRef();
            try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(selected.getPositionCount())) {
                for (int s = 0; s < selected.getPositionCount(); s++) {
                    int selectedGroup = selected.getInt(s);
                    /*
                     * Count can effectively be in three states - 0, 1, many. We use those
                     * states to buffer the first value, so we can avoid calling
                     * beginPositionEntry on single valued fields.
                     */
                    int count = 0;
                    long first = 0;
                    for (int id = 0; id < values.size(); id++) {
                        if (values.getKey1(id) == selectedGroup) {
                            long value = values.getKey2(id);
                            switch (count) {
                                case 0 -> first = value;
                                case 1 -> {
                                    builder.beginPositionEntry();
                                    builder.appendBytesRef(bytes.get(first, scratch));
                                    builder.appendBytesRef(bytes.get(value, scratch));
                                }
                                default -> builder.appendBytesRef(bytes.get(value, scratch));
                            }
                            count++;
                        }
                    }
                    switch (count) {
                        case 0 -> builder.appendNull();
                        case 1 -> builder.appendBytesRef(bytes.get(first, scratch));
                        default -> builder.endPositionEntry();
                    }
                }
                return builder.build();
            }
        }

        void enableGroupIdTracking(SeenGroupIds seen) {
            // we figure out seen values from nulls on the values block
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(values, bytes);
        }
    }
}
