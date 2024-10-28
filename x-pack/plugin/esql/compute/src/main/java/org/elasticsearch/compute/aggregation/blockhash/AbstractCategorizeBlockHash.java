/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.xpack.ml.aggs.categorization.SerializableTokenListCategory;
import org.elasticsearch.xpack.ml.aggs.categorization.TokenListCategorizer;

import java.io.IOException;

public abstract class AbstractCategorizeBlockHash extends BlockHash {
    // TODO: this should probably also take an emitBatchSize
    private final int channel;
    private final boolean outputPartial;
    protected final TokenListCategorizer.CloseableTokenListCategorizer categorizer;

    AbstractCategorizeBlockHash(
        BlockFactory blockFactory,
        int channel,
        boolean outputPartial,
        TokenListCategorizer.CloseableTokenListCategorizer categorizer
    ) {
        super(blockFactory);
        this.channel = channel;
        this.outputPartial = outputPartial;
        this.categorizer = categorizer;
    }

    protected int channel() {
        return channel;
    }

    @Override
    public Block[] getKeys() {
        if (outputPartial) {
            return new Block[] { buildIntermediateBlock() };
            // NOCOMMIT load partial
            // Block state = null;
            // Block keys; // NOCOMMIT do we even need to send the keys? it's just going to be 0 to the length of state
            // return new Block[] {new CompositeBlock()};
            // return null;
        }

        // NOCOMMIT load final
        return new Block[0];
    }

    @Override
    public IntVector nonEmpty() {
        // TODO
        return null;
    }

    @Override
    public BitArray seenGroupIds(BigArrays bigArrays) {
        // TODO
        return null;
    }

    @Override
    public final ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
        throw new UnsupportedOperationException();
    }

    // visible for testing
    Block buildIntermediateBlock() {
        if (categorizer.getCategoryCount() == 0) {
            return blockFactory.newConstantNullBlock(1);
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            // TODO be more careful here.
            out.writeVInt(categorizer.getCategoryCount());
            for (SerializableTokenListCategory category : categorizer.toCategories(categorizer.getCategoryCount())) {
                category.writeTo(out);
            }
            return blockFactory.newConstantBytesRefBlockWith(out.bytes().toBytesRef(), 1);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
