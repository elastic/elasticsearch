/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.mapper.BlockLoader;

import java.util.List;

import static org.elasticsearch.compute.data.ElementType.DOC;
import static org.elasticsearch.compute.data.ElementType.LONG;

/**
 * A source operator whose output is the given tuple values. This operator produces pages
 * with two Blocks. The returned pages preserve the order of values as given in the in initial list.
 */
public class TupleDocLongBlockSourceOperator extends TupleAbstractBlockSourceOperator<BlockUtils.Doc, Long> {
    public TupleDocLongBlockSourceOperator(BlockFactory blockFactory, List<Tuple<BlockUtils.Doc, Long>> values) {
        super(blockFactory, values, DOC, LONG);
    }

    public TupleDocLongBlockSourceOperator(BlockFactory blockFactory, List<Tuple<BlockUtils.Doc, Long>> values, int maxPagePositions) {
        super(blockFactory, values, maxPagePositions, DOC, LONG);
    }

    @Override
    protected void consumeFirstElement(BlockUtils.Doc doc, Block.Builder builder) {
        var docBuilder = (DocBlock.Builder) builder;
        docBuilder.appendShard(doc.shard());
        docBuilder.appendSegment(doc.segment());
        docBuilder.appendDoc(doc.doc());
    }

    @Override
    protected void consumeSecondElement(Long l, Block.Builder blockBuilder) {
        ((BlockLoader.LongBuilder) blockBuilder).appendLong(l);
    }
}
