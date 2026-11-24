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

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

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
            TopNEncoder withTable = DocVectorEncoder.detached().withOrigins(table);
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

    /**
     * The plan marks detached columns without a table. Only an operator can number their origins.
     */
    public void testDetachedColumnsNeedTheTableOfAnOperator() {
        BlockFactory blockFactory = blockFactory();
        try (DocBlock detached = (DocBlock) RandomBlock.randomDetachedDocBlock(blockFactory, 1, 1).block()) {
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> ValueExtractor.extractorFor(ElementType.DOC, DocVectorEncoder.detached(), false, detached)
            );
            assertThat(e.getMessage(), equalTo("detached [_doc] columns need the origin table of their operator"));
            e = expectThrows(
                IllegalStateException.class,
                () -> ResultBuilder.resultBuilderFor(blockFactory, ElementType.DOC, DocVectorEncoder.detached(), false, 1)
            );
            assertThat(e.getMessage(), equalTo("detached [_doc] columns need the origin table of their operator"));
        }
    }

    /**
     * An operator binds the detached columns of the plan to its own table and keeps every other encoder.
     */
    public void testOperatorBindsOnlyDetachedColumns() {
        DocVectorEncoder attached = new DocVectorEncoder(AlwaysReferencedIndexedByShardId.INSTANCE);
        List<TopNEncoder> planned = List.of(TopNEncoder.DEFAULT_SORTABLE, attached, DocVectorEncoder.detached());
        try (DocOriginTable table = DocVectorEncoder.originTableFor(planned, blockFactory().breaker())) {
            assertThat(table, notNullValue());
            List<TopNEncoder> bound = DocVectorEncoder.withOrigins(planned, table);
            assertThat(bound.get(0), sameInstance(TopNEncoder.DEFAULT_SORTABLE));
            assertThat(bound.get(1), sameInstance(attached));
            assertThat(bound.get(2), not(sameInstance(DocVectorEncoder.detached())));
            assertThat(((DocVectorEncoder) bound.get(2)).origins(), sameInstance(table));
        }
    }

    public void testNoTableWithoutDetachedColumns() {
        List<TopNEncoder> planned = List.of(TopNEncoder.DEFAULT_SORTABLE, new DocVectorEncoder(AlwaysReferencedIndexedByShardId.INSTANCE));
        assertThat(DocVectorEncoder.originTableFor(planned, blockFactory().breaker()), nullValue());
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
