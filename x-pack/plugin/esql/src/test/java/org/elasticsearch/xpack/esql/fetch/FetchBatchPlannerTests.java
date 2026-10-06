/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocRefBlock;
import org.elasticsearch.compute.data.DocRefOrigin;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.ShardSearchContextId;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class FetchBatchPlannerTests extends ComputeTestCase {
    private static final DocRefOrigin N1_S0 = origin("n1", 0);
    private static final DocRefOrigin N1_S1 = origin("n1", 1);
    private static final DocRefOrigin N2_S2 = origin("n2", 2);

    /**
     * Nodes and their shards come in the order the cut first names them, and the documents of each shard sorted by
     * segment and doc. Each row reads the response row of its document.
     */
    public void testGroupsByNodeAndShardAndSortsTheDocuments() {
        List<Page> pages = List.of(
            page(new Ref(N2_S2, 0, 5), new Ref(N1_S1, 3, 1), new Ref(N1_S0, 1, 9)),
            page(new Ref(N1_S0, 0, 4), new Ref(N1_S1, 0, 7), new Ref(N1_S0, 1, 2))
        );
        try {
            FetchBatchPlanner.Batch batch = FetchBatchPlanner.plan(pages, 0);

            assertThat(batch.nodes().stream().map(FetchBatchPlanner.NodeBatch::nodeId).toList(), equalTo(List.of("n2", "n1")));
            FetchBatchPlanner.NodeBatch n1 = batch.nodes().get(1);
            assertThat(
                n1.shards().stream().map(FetchRequest.ShardDocs::shardId).toList(),
                equalTo(List.of(N1_S1.shardId(), N1_S0.shardId()))
            );
            assertShard(n1.shards().get(0), new int[] { 0, 3 }, new int[] { 7, 1 });
            assertShard(n1.shards().get(1), new int[] { 0, 1, 1 }, new int[] { 4, 2, 9 });
            assertThat(n1.contextIds(), equalTo(List.of(N1_S1.contextId(), N1_S0.contextId())));
            // n2 answers first with 1 row, then n1 with the rows of shard 1 and then of shard 0
            assertThat(batch.responseRows(), equalTo(new int[] { 0, 2, 5, 3, 1, 4 }));
            assertFalse(batch.deduplicated());
        } finally {
            pages.forEach(Page::releaseBlocks);
        }
    }

    /**
     * A document that several rows reference, for example after a row multiplying command, is asked for once.
     */
    public void testAsksForADocumentOnce() {
        List<Page> pages = List.of(page(new Ref(N1_S0, 1, 3), new Ref(N1_S0, 1, 3), new Ref(N1_S0, 0, 8)));
        try {
            FetchBatchPlanner.Batch batch = FetchBatchPlanner.plan(pages, 0);

            assertShard(batch.nodes().getFirst().shards().getFirst(), new int[] { 0, 1 }, new int[] { 8, 3 });
            assertThat(batch.responseRows(), equalTo(new int[] { 1, 1, 0 }));
            assertTrue(batch.deduplicated());
        } finally {
            pages.forEach(Page::releaseBlocks);
        }
    }

    /**
     * Segment and doc pack into one long to sort. The largest docs of a segment still sort before the next segment.
     */
    public void testSortsLargeSegmentsAndDocs() {
        int maxDoc = Integer.MAX_VALUE - 1;
        List<Page> pages = List.of(page(new Ref(N1_S0, 70_000, 0), new Ref(N1_S0, 1, maxDoc), new Ref(N1_S0, 2, 0), new Ref(N1_S0, 1, 5)));
        try {
            FetchBatchPlanner.Batch batch = FetchBatchPlanner.plan(pages, 0);

            assertShard(batch.nodes().getFirst().shards().getFirst(), new int[] { 1, 1, 2, 70_000 }, new int[] { 5, maxDoc, 0, 0 });
            assertThat(batch.responseRows(), equalTo(new int[] { 3, 1, 2, 0 }));
        } finally {
            pages.forEach(Page::releaseBlocks);
        }
    }

    public void testEmptyCutAsksForNothing() {
        List<Page> pages = List.of(page());
        try {
            FetchBatchPlanner.Batch batch = FetchBatchPlanner.plan(pages, 0);
            assertThat(batch.nodes(), equalTo(List.of()));
            assertThat(batch.responseRows().length, equalTo(0));
        } finally {
            pages.forEach(Page::releaseBlocks);
        }
    }

    /**
     * Every row reads the response row that holds its own document, whatever the order and the duplicates of the cut.
     */
    public void testEveryRowReadsItsOwnDocument() {
        List<DocRefOrigin> origins = randomList(1, 6, FetchBatchPlannerTests::randomOrigin);
        List<Page> pages = new ArrayList<>();
        List<Ref> rows = new ArrayList<>();
        int pageCount = between(1, 4);
        for (int p = 0; p < pageCount; p++) {
            Ref[] refs = new Ref[between(0, 50)];
            for (int r = 0; r < refs.length; r++) {
                refs[r] = randomBoolean() && rows.isEmpty() == false
                    ? randomFrom(rows)
                    : new Ref(randomFrom(origins), randomSegment(), randomDoc());
                rows.add(refs[r]);
            }
            pages.add(page(refs));
        }
        try {
            FetchBatchPlanner.Batch batch = FetchBatchPlanner.plan(pages, 0);

            // the documents in the order the nodes answer: node after node, shard after shard, in request order
            List<Ref> responses = new ArrayList<>();
            for (FetchBatchPlanner.NodeBatch node : batch.nodes()) {
                for (FetchRequest.ShardDocs shard : node.shards()) {
                    DocRefOrigin origin = origins.stream().filter(o -> o.contextId().equals(shard.contextId())).findFirst().orElseThrow();
                    assertThat(origin.nodeId(), equalTo(node.nodeId()));
                    for (int d = 0; d < shard.docCount(); d++) {
                        responses.add(new Ref(origin, shard.segments()[d], shard.docs()[d]));
                    }
                }
            }
            assertThat("each document once", responses.stream().distinct().count(), equalTo((long) responses.size()));
            for (int r = 0; r < rows.size(); r++) {
                assertThat(responses.get(batch.responseRows()[r]), equalTo(rows.get(r)));
            }
        } finally {
            pages.forEach(Page::releaseBlocks);
        }
    }

    public void testRejectsAColumnWithoutDocumentReferences() {
        BlockFactory blockFactory = blockFactory();
        Page page = new Page(blockFactory.newConstantIntBlockWith(1, 2));
        try {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> FetchBatchPlanner.plan(List.of(page), 0));
            assertThat(e.getMessage(), containsString("expected document references in channel [0]"));
        } finally {
            page.releaseBlocks();
        }
    }

    private record Ref(DocRefOrigin origin, int segment, int doc) {}

    private Page page(Ref... refs) {
        BlockFactory blockFactory = blockFactory();
        List<DocRefOrigin> origins = new ArrayList<>();
        try (DocRefBlock.Builder builder = DocRefBlock.newBlockBuilder(blockFactory, refs.length)) {
            for (Ref ref : refs) {
                if (origins.contains(ref.origin()) == false) {
                    origins.add(ref.origin());
                    builder.addOrigin(ref.origin());
                }
            }
            for (Ref ref : refs) {
                builder.append(origins.indexOf(ref.origin()), ref.segment(), ref.doc());
            }
            return new Page(builder.build());
        }
    }

    private static void assertShard(FetchRequest.ShardDocs shard, int[] segments, int[] docs) {
        assertThat(shard.segments(), equalTo(segments));
        assertThat(shard.docs(), equalTo(docs));
    }

    private static DocRefOrigin origin(String nodeId, int shard) {
        return new DocRefOrigin("", nodeId, new ShardId("index", "uuid", shard), new ShardSearchContextId("session", shard));
    }

    private static int randomSegment() {
        return randomBoolean() ? between(0, 5) : between(0, 100_000);
    }

    /**
     * Mostly small docs, so rows share documents, and sometimes the largest ones.
     */
    private static int randomDoc() {
        return randomBoolean() ? between(0, 300) : between(Integer.MAX_VALUE - 300, Integer.MAX_VALUE - 1);
    }

    private static DocRefOrigin randomOrigin() {
        return new DocRefOrigin(
            "",
            randomFrom("n1", "n2", "n3"),
            new ShardId("index", "uuid", between(0, 10)),
            new ShardSearchContextId(randomAlphaOfLength(6), randomNonNegativeLong())
        );
    }
}
