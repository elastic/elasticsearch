/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.xpack.ml.aggs.categorization.CategorizationBytesRefHash;
import org.elasticsearch.xpack.ml.aggs.categorization.CategorizationPartOfSpeechDictionary;
import org.elasticsearch.xpack.ml.aggs.categorization.SerializableTokenListCategory;
import org.elasticsearch.xpack.ml.aggs.categorization.TokenListCategorizer;

import java.io.IOException;

/**
 * Base BlockHash implementation for {@code Categorize} grouping function.
 */
public abstract class AbstractCategorizeBlockHash extends BlockHash {
    // TODO: this should probably also take an emitBatchSize
    private final int channel;
    private final boolean outputPartial;
    protected final TokenListCategorizer.CloseableTokenListCategorizer categorizer;

    AbstractCategorizeBlockHash(BlockFactory blockFactory, int channel, boolean outputPartial) {
        super(blockFactory);
        this.channel = channel;
        this.outputPartial = outputPartial;
        this.categorizer = new TokenListCategorizer.CloseableTokenListCategorizer(
            new CategorizationBytesRefHash(new BytesRefHash(2048, blockFactory.bigArrays())),
            CategorizationPartOfSpeechDictionary.getInstance(),
            0.70f
        );
    }

    protected int channel() {
        return channel;
    }

    @Override
    public Block[] getKeys() {
        return new Block[] { outputPartial ? buildIntermediateBlock() : buildFinalBlock() };
    }

    @Override
    public IntVector nonEmpty() {
        return IntVector.range(0, categorizer.getCategoryCount(), blockFactory);
    }

    @Override
    public BitArray seenGroupIds(BigArrays bigArrays) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
        throw new UnsupportedOperationException();
    }

    /**
     * Serializes the intermediate state into a single BytesRef block, or an empty Null block if there are no categories.
     */
    private Block buildIntermediateBlock() {
        if (categorizer.getCategoryCount() == 0) {
            return blockFactory.newConstantNullBlock(0);
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            // TODO be more careful here.
            out.writeVInt(categorizer.getCategoryCount());
            for (SerializableTokenListCategory category : categorizer.toCategoriesById()) {
                category.writeTo(out);
            }
            // We're returning a block with N positions just because the Page must have all blocks with the same position count!
            return blockFactory.newConstantBytesRefBlockWith(out.bytes().toBytesRef(), categorizer.getCategoryCount());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Block buildFinalBlock() {
        try (BytesRefVector.Builder result = blockFactory.newBytesRefVectorBuilder(categorizer.getCategoryCount())) {
            BytesRefBuilder scratch = new BytesRefBuilder();
            for (SerializableTokenListCategory category : categorizer.toCategoriesById()) {
                scratch.copyChars(category.getRegex());
                result.appendBytesRef(scratch.get());
                scratch.clear();
            }
            return result.build().asBlock();
        }
    }
}
