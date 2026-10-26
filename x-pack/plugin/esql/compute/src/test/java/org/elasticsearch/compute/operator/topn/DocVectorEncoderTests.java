/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocOriginTable;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.lucene.AlwaysReferencedIndexedByShardId;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.RandomBlock;

import static org.hamcrest.Matchers.equalTo;

public class DocVectorEncoderTests extends ComputeTestCase {
    /**
     * A TopN with an origin table only takes documents that left their node, and one without it only takes documents of
     * this node, because their shard numbers mean different things.
     */
    public void testDocumentsMustMatchTheEncoder() {
        BlockFactory blockFactory = blockFactory();
        try (
            DocOriginTable table = new DocOriginTable(blockFactory.breaker());
            DocBlock detached = (DocBlock) RandomBlock.randomDetachedDocBlock(blockFactory, 1, 1).block();
            DocBlock local = new DocVector(
                AlwaysReferencedIndexedByShardId.INSTANCE,
                blockFactory.newConstantIntVector(0, 1),
                blockFactory.newConstantIntVector(0, 1),
                blockFactory.newConstantIntVector(0, 1),
                DocVector.config()
            ).asBlock()
        ) {
            TopNEncoder withTable = new DocVectorEncoder(table);
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> ValueExtractor.extractorFor(ElementType.DOC, withTable, false, local)
            );
            assertThat(e.getMessage(), equalTo("expected a detached DocVector"));

            TopNEncoder withoutTable = new DocVectorEncoder(AlwaysReferencedIndexedByShardId.INSTANCE);
            e = expectThrows(
                IllegalStateException.class,
                () -> ValueExtractor.extractorFor(ElementType.DOC, withoutTable, false, detached)
            );
            assertThat(e.getMessage(), equalTo("expected an attached DocVector"));
        }
    }

    public void testNullBlocksCantStandInForDocuments() {
        try (Block nulls = blockFactory().newConstantNullBlock(1)) {
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> ValueExtractor.extractorFor(ElementType.DOC, TopNEncoder.DEFAULT_UNSORTABLE, false, nulls)
            );
            assertThat(e.getMessage(), equalTo("[DOC] channels can't carry null blocks"));
            e = expectThrows(
                IllegalStateException.class,
                () -> KeyExtractor.extractorFor(ElementType.DOC, TopNEncoder.DEFAULT_SORTABLE, true, (byte) 1, (byte) 2, nulls)
            );
            assertThat(e.getMessage(), equalTo("[DOC] channels can't carry null blocks"));
        }
    }
}
