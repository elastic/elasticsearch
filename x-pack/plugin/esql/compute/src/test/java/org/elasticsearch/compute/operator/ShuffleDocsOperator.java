/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.lucene.tests.util.LuceneTestCase.random;

public class ShuffleDocsOperator extends AbstractPageMappingOperator {
    private final BlockFactory blockFactory;

    public ShuffleDocsOperator(BlockFactory blockFactory) {
        this.blockFactory = blockFactory;
    }

    @Override
    protected Page process(Page page) {
        DocVector docVector = (DocVector) page.getBlock(0).asVector();
        int positionCount = docVector.getPositionCount();
        IntVector shards = null;
        IntVector segments = null;
        IntVector docs = null;
        try (
            IntVector.Builder shardsBuilder = blockFactory.newIntVectorBuilder(positionCount);
            IntVector.Builder segmentsBuilder = blockFactory.newIntVectorBuilder(positionCount);
            IntVector.Builder docsBuilder = blockFactory.newIntVectorBuilder(positionCount);
        ) {
            List<Integer> docIds = new ArrayList<>(positionCount);
            for (int i = 0; i < positionCount; i++) {
                shardsBuilder.appendInt(docVector.shards().getInt(i));
                segmentsBuilder.appendInt(docVector.segments().getInt(i));
                docIds.add(docVector.docs().getInt(i));
            }
            shards = shardsBuilder.build();
            segments = segmentsBuilder.build();
            Collections.shuffle(docIds, random());
            for (Integer d : docIds) {
                docsBuilder.appendInt(d);
            }
            docs = docsBuilder.build();
        } finally {
            if (docs == null) {
                Releasables.closeExpectNoException(docVector, shards, segments);
            } else {
                Releasables.closeExpectNoException(docVector);
            }
        }
        Block[] blocks = new Block[page.getBlockCount()];
        blocks[0] = new DocVector(shards, segments, docs, false).asBlock();
        for (int i = 1; i < blocks.length; i++) {
            blocks[i] = page.getBlock(i);
        }
        return new Page(blocks);
    }

    @Override
    public String toString() {
        return "ShuffleDocs";
    }
}
