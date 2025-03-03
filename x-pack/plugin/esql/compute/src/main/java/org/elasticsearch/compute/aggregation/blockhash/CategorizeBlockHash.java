/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.xpack.core.ml.job.config.CategorizationAnalyzerConfig;
import org.elasticsearch.xpack.ml.aggs.categorization.CategorizationBytesRefHash;
import org.elasticsearch.xpack.ml.aggs.categorization.CategorizationPartOfSpeechDictionary;
import org.elasticsearch.xpack.ml.aggs.categorization.SerializableTokenListCategory;
import org.elasticsearch.xpack.ml.aggs.categorization.TokenListCategorizer;
import org.elasticsearch.xpack.ml.job.categorization.CategorizationAnalyzer;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * BlockHash implementation for {@code Categorize} grouping function.
 */
public class CategorizeBlockHash extends BlockHash {

    private static final CategorizationAnalyzerConfig ANALYZER_CONFIG = CategorizationAnalyzerConfig.buildStandardCategorizationAnalyzer(
        List.of()
    );
    private static final int NULL_ORD = 0;

    private final int channel;
    private final AggregatorMode aggregatorMode;
    private final TokenListCategorizer.CloseableTokenListCategorizer categorizer;
    private final CategorizeEvaluator evaluator;

    /**
     * Store whether we've seen any {@code null} values.
     * <p>
     *     Null gets the {@link #NULL_ORD} ord.
     * </p>
     */
    private boolean seenNull = false;

    CategorizeBlockHash(BlockFactory blockFactory, int channel, AggregatorMode aggregatorMode, AnalysisRegistry analysisRegistry) {
        super(blockFactory);

        this.channel = channel;
        this.aggregatorMode = aggregatorMode;

        this.categorizer = new TokenListCategorizer.CloseableTokenListCategorizer(
            new CategorizationBytesRefHash(new BytesRefHash(2048, blockFactory.bigArrays())),
            CategorizationPartOfSpeechDictionary.getInstance(),
            0.70f
        );

        if (aggregatorMode.isInputPartial() == false) {
            CategorizationAnalyzer analyzer;
            try {
                Objects.requireNonNull(analysisRegistry);
                analyzer = new CategorizationAnalyzer(analysisRegistry, ANALYZER_CONFIG);
            } catch (Exception e) {
                categorizer.close();
                throw new RuntimeException(e);
            }
            this.evaluator = new CategorizeEvaluator(analyzer);
        } else {
            this.evaluator = null;
        }
    }

    boolean seenNull() {
        return seenNull;
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        try (IntBlock block = add(page)) {
            addInput.add(0, block);
        }
    }

    @Override
    public Block[] getKeys() {
        return new Block[] { aggregatorMode.isOutputPartial() ? buildIntermediateBlock() : buildFinalBlock() };
    }

    @Override
    public IntVector nonEmpty() {
        return IntVector.range(seenNull ? 0 : 1, categorizer.getCategoryCount() + 1, blockFactory);
    }

    @Override
    public BitArray seenGroupIds(BigArrays bigArrays) {
        return new SeenGroupIds.Range(seenNull ? 0 : 1, Math.toIntExact(categorizer.getCategoryCount() + 1)).seenGroupIds(bigArrays);
    }

    @Override
    public final ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        Releasables.close(evaluator, categorizer);
    }

    private IntBlock add(Page page) {
        return aggregatorMode.isInputPartial() == false ? addInitial(page) : addIntermediate(page);
    }

    /**
     * Adds initial (raw) input to the state.
     */
    IntBlock addInitial(Page page) {
        return (IntBlock) evaluator.eval(page.getBlock(channel));
    }

    /**
     * Adds intermediate state to the state.
     */
    private IntBlock addIntermediate(Page page) {
        if (page.getPositionCount() == 0) {
            return null;
        }
        BytesRefBlock categorizerState = page.getBlock(channel);
        if (categorizerState.areAllValuesNull()) {
            seenNull = true;
            return blockFactory.newConstantIntBlockWith(NULL_ORD, 1);
        }
        return recategorize(categorizerState.getBytesRef(0, new BytesRef()), null).asBlock();
    }

    /**
     * Reads the intermediate state from a block and recategorizes the provided IDs.
     * If no IDs are provided, the IDs are the IDs in the categorizer's state in order.
     * (So 0...N-1 or 1...N, depending on whether null is present.)
     */
    IntVector recategorize(BytesRef bytes, IntVector ids) {
        Map<Integer, Integer> idMap = new HashMap<>();
        try (StreamInput in = new BytesArray(bytes).streamInput()) {
            if (in.readBoolean()) {
                seenNull = true;
                idMap.put(NULL_ORD, NULL_ORD);
            }
            int count = in.readVInt();
            for (int oldCategoryId = 0; oldCategoryId < count; oldCategoryId++) {
                int newCategoryId = categorizer.mergeWireCategory(new SerializableTokenListCategory(in)).getId();
                // +1 because the 0 ordinal is reserved for null
                idMap.put(oldCategoryId + 1, newCategoryId + 1);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try (IntVector.Builder newIdsBuilder = blockFactory.newIntVectorBuilder(idMap.size())) {
            if (ids == null) {
                int idOffset = idMap.containsKey(0) ? 0 : 1;
                for (int i = 0; i < idMap.size(); i++) {
                    newIdsBuilder.appendInt(idMap.get(i + idOffset));
                }
            } else {
                for (int i = 0; i < ids.getPositionCount(); i++) {
                    newIdsBuilder.appendInt(idMap.get(ids.getInt(i)));
                }
            }
            return newIdsBuilder.build();
        }
    }

    /**
     * Serializes the intermediate state into a single BytesRef block, or an empty Null block if there are no categories.
     */
    private Block buildIntermediateBlock() {
        if (categorizer.getCategoryCount() == 0) {
            return blockFactory.newConstantNullBlock(seenNull ? 1 : 0);
        }
        int positionCount = categorizer.getCategoryCount() + (seenNull ? 1 : 0);
        // We're returning a block with N positions just because the Page must have all blocks with the same position count!
        return blockFactory.newConstantBytesRefBlockWith(serializeCategorizer(), positionCount);
    }

    BytesRef serializeCategorizer() {
        // TODO: This BytesStreamOutput is not accounted for by the circuit breaker. Fix that!
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeBoolean(seenNull);
            out.writeVInt(categorizer.getCategoryCount());
            for (SerializableTokenListCategory category : categorizer.toCategoriesById()) {
                category.writeTo(out);
            }
            return out.bytes().toBytesRef();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Block buildFinalBlock() {
        BytesRefBuilder scratch = new BytesRefBuilder();

        if (seenNull) {
            try (BytesRefBlock.Builder result = blockFactory.newBytesRefBlockBuilder(categorizer.getCategoryCount())) {
                result.appendNull();
                for (SerializableTokenListCategory category : categorizer.toCategoriesById()) {
                    scratch.copyChars(category.getRegex());
                    result.appendBytesRef(scratch.get());
                    scratch.clear();
                }
                return result.build();
            }
        }

        try (BytesRefVector.Builder result = blockFactory.newBytesRefVectorBuilder(categorizer.getCategoryCount())) {
            for (SerializableTokenListCategory category : categorizer.toCategoriesById()) {
                scratch.copyChars(category.getRegex());
                result.appendBytesRef(scratch.get());
                scratch.clear();
            }
            return result.build().asBlock();
        }
    }

    /**
     * Similar implementation to an Evaluator.
     */
    private final class CategorizeEvaluator implements Releasable {
        private final CategorizationAnalyzer analyzer;

        CategorizeEvaluator(CategorizationAnalyzer analyzer) {
            this.analyzer = analyzer;
        }

        Block eval(BytesRefBlock vBlock) {
            BytesRefVector vVector = vBlock.asVector();
            if (vVector == null) {
                return eval(vBlock.getPositionCount(), vBlock);
            }
            IntVector vector = eval(vBlock.getPositionCount(), vVector);
            return vector.asBlock();
        }

        IntBlock eval(int positionCount, BytesRefBlock vBlock) {
            try (IntBlock.Builder result = blockFactory.newIntBlockBuilder(positionCount)) {
                BytesRef vScratch = new BytesRef();
                for (int p = 0; p < positionCount; p++) {
                    if (vBlock.isNull(p)) {
                        seenNull = true;
                        result.appendInt(NULL_ORD);
                        continue;
                    }
                    int first = vBlock.getFirstValueIndex(p);
                    int count = vBlock.getValueCount(p);
                    if (count == 1) {
                        result.appendInt(process(vBlock.getBytesRef(first, vScratch)));
                        continue;
                    }
                    int end = first + count;
                    result.beginPositionEntry();
                    for (int i = first; i < end; i++) {
                        result.appendInt(process(vBlock.getBytesRef(i, vScratch)));
                    }
                    result.endPositionEntry();
                }
                return result.build();
            }
        }

        IntVector eval(int positionCount, BytesRefVector vVector) {
            try (IntVector.FixedBuilder result = blockFactory.newIntVectorFixedBuilder(positionCount)) {
                BytesRef vScratch = new BytesRef();
                for (int p = 0; p < positionCount; p++) {
                    result.appendInt(p, process(vVector.getBytesRef(p, vScratch)));
                }
                return result.build();
            }
        }

        int process(BytesRef v) {
            var category = categorizer.computeCategory(v.utf8ToString(), analyzer);
            if (category == null) {
                seenNull = true;
                return NULL_ORD;
            }
            return category.getId() + 1;
        }

        @Override
        public void close() {
            analyzer.close();
        }
    }
}
