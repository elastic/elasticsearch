/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocOrigin;
import org.elasticsearch.compute.data.DocOrigins;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.ShardSearchContextId;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class FetchBatchPlannerTests extends ComputeTestCase {
    private static final DocOrigin N1_S0 = origin("n1", 0);
    private static final DocOrigin N1_S1 = origin("n1", 1);
    private static final DocOrigin N2_S2 = origin("n2", 2);

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
        List<DocOrigin> origins = randomList(1, 6, FetchBatchPlannerTests::randomOrigin);
        List<Ref> rows = new ArrayList<>();
        List<Page> pages = randomCut(origins, rows);
        try {
            FetchBatchPlanner.Batch batch = FetchBatchPlanner.plan(pages, 0);

            List<Ref> responses = responses(batch, origins, new boolean[shardCount(batch)]);
            assertThat("each document once", responses.stream().distinct().count(), equalTo((long) responses.size()));
            for (int r = 0; r < rows.size(); r++) {
                assertThat(responses.get(batch.responseRows()[r]), equalTo(rows.get(r)));
            }
        } finally {
            pages.forEach(Page::releaseBlocks);
        }
    }

    public void testRejectsAColumnWithoutDocuments() {
        BlockFactory blockFactory = blockFactory();
        Page page = new Page(blockFactory.newConstantIntBlockWith(1, 2));
        try {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> FetchBatchPlanner.plan(List.of(page), 0));
            assertThat(e.getMessage(), containsString("expected documents that left their node in channel [0]"));
        } finally {
            page.releaseBlocks();
        }
    }

    /**
     * The shard slots of documents that never left their node mean nothing on the coordinator.
     */
    public void testRejectsDocumentsOfTheirOwnNode() {
        BlockFactory blockFactory = blockFactory();
        Page page;
        try (DocBlock.Builder builder = DocBlock.newBlockBuilder(blockFactory, 1)) {
            page = new Page(builder.appendShard(0).appendSegment(0).appendDoc(0).build());
        }
        try {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> FetchBatchPlanner.plan(List.of(page), 0));
            assertThat(e.getMessage(), containsString("expected documents that left their node in channel [0]"));
        } finally {
            page.releaseBlocks();
        }
    }

    /**
     * The rows of a shard whose fetch failed get no response row.
     */
    public void testResponseRowsWithoutFailedShardsDropTheirRows() {
        List<Page> pages = List.of(
            page(new Ref(N2_S2, 0, 5), new Ref(N1_S1, 3, 1), new Ref(N1_S0, 1, 9)),
            page(new Ref(N1_S0, 0, 4), new Ref(N1_S1, 0, 7), new Ref(N1_S0, 1, 2))
        );
        try {
            FetchBatchPlanner.Batch batch = FetchBatchPlanner.plan(pages, 0);
            assertThat(batch.responseRows(), equalTo(new int[] { 0, 2, 5, 3, 1, 4 }));

            // the shards in the order of the batch: shard 2 of n2, then shards 1 and 0 of n1. Shard 0 answers last, so
            // its failure moves no other shard.
            assertThat(batch.responseRowsWithout(new boolean[] { false, false, true }), equalTo(new int[] { 0, 2, -1, -1, 1, -1 }));
        } finally {
            pages.forEach(Page::releaseBlocks);
        }
    }

    /**
     * The response rows of the shards after a failed one move up by the documents of the failed shard, also across nodes,
     * so each row still reads the values of its own document.
     */
    public void testResponseRowsWithoutFailedShardsMoveTheOthersUp() {
        List<Page> pages = List.of(
            page(new Ref(N2_S2, 0, 5), new Ref(N1_S1, 3, 1), new Ref(N1_S0, 1, 9)),
            page(new Ref(N1_S0, 0, 4), new Ref(N1_S1, 0, 7), new Ref(N1_S0, 1, 2))
        );
        try {
            FetchBatchPlanner.Batch batch = FetchBatchPlanner.plan(pages, 0);
            assertThat(batch.responseRows(), equalTo(new int[] { 0, 2, 5, 3, 1, 4 }));

            // shard 2 of n2 answers first, with one document, so the rows of both shards of n1 move up by one
            assertThat(batch.responseRowsWithout(new boolean[] { true, false, false }), equalTo(new int[] { -1, 1, 4, 2, 0, 3 }));
        } finally {
            pages.forEach(Page::releaseBlocks);
        }
    }

    /**
     * Whatever shards fail, the rows of the shards that answered read the values of their own documents, and the other
     * rows get no response row.
     */
    public void testEveryRowOfAShardThatAnsweredReadsItsOwnDocument() {
        List<DocOrigin> origins = randomList(1, 6, FetchBatchPlannerTests::randomOrigin);
        List<Ref> rows = new ArrayList<>();
        List<Page> pages = randomCut(origins, rows);
        try {
            FetchBatchPlanner.Batch batch = FetchBatchPlanner.plan(pages, 0);
            boolean[] failed = new boolean[shardCount(batch)];
            for (int s = 0; s < failed.length; s++) {
                failed[s] = randomBoolean();
            }
            List<ShardSearchContextId> shards = batch.nodes().stream().flatMap(n -> n.contextIds().stream()).toList();

            int[] responseRows = batch.responseRowsWithout(failed);
            List<Ref> responses = responses(batch, origins, failed);
            for (int r = 0; r < rows.size(); r++) {
                Ref row = rows.get(r);
                if (failed[shards.indexOf(row.origin().contextId())]) {
                    assertThat(responseRows[r], equalTo(-1));
                } else {
                    assertThat(responses.get(responseRows[r]), equalTo(row));
                }
            }
        } finally {
            pages.forEach(Page::releaseBlocks);
        }
    }

    /**
     * Without a failed shard, every row reads its planned response row.
     */
    public void testResponseRowsWithoutAnyFailedShardAreThePlannedOnes() {
        List<Ref> rows = new ArrayList<>();
        List<Page> pages = randomCut(randomList(1, 6, FetchBatchPlannerTests::randomOrigin), rows);
        try {
            FetchBatchPlanner.Batch batch = FetchBatchPlanner.plan(pages, 0);
            assertThat(batch.responseRowsWithout(new boolean[shardCount(batch)]), equalTo(batch.responseRows()));
        } finally {
            pages.forEach(Page::releaseBlocks);
        }
    }

    /**
     * Failed shards that don't match the shards of the batch are a bug.
     */
    public void testResponseRowsWithoutRejectsAnotherNumberOfShards() {
        List<Page> pages = List.of(page(new Ref(N2_S2, 0, 5), new Ref(N1_S1, 3, 1), new Ref(N1_S0, 1, 9)));
        try {
            FetchBatchPlanner.Batch batch = FetchBatchPlanner.plan(pages, 0);
            int wrongCount = randomValueOtherThan(3, () -> between(0, 6));
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> batch.responseRowsWithout(new boolean[wrongCount])
            );
            assertThat(e.getMessage(), equalTo("[" + wrongCount + "] shards for a batch of [3] shards"));
        } finally {
            pages.forEach(Page::releaseBlocks);
        }
    }

    private record Ref(DocOrigin origin, int segment, int doc) {}

    /**
     * Pages of rows on {@code origins}, with at least one row. A row may repeat an earlier one. Adds the rows to
     * {@code rows}, in order.
     */
    private List<Page> randomCut(List<DocOrigin> origins, List<Ref> rows) {
        List<Page> pages = new ArrayList<>();
        int pageCount = between(1, 4);
        for (int p = 0; p < pageCount; p++) {
            Ref[] refs = new Ref[between(p == 0 ? 1 : 0, 50)];
            for (int r = 0; r < refs.length; r++) {
                refs[r] = randomBoolean() && rows.isEmpty() == false
                    ? randomFrom(rows)
                    : new Ref(randomFrom(origins), randomSegment(), randomDoc());
                rows.add(refs[r]);
            }
            pages.add(page(refs));
        }
        return pages;
    }

    /**
     * The documents in the order the nodes answer: node after node, shard after shard, in request order. A shard whose
     * fetch failed answers nothing.
     *
     * @param failedShards for each shard of the batch, in the order of the batch, whether its fetch failed
     */
    private static List<Ref> responses(FetchBatchPlanner.Batch batch, List<DocOrigin> origins, boolean[] failedShards) {
        List<Ref> responses = new ArrayList<>();
        int s = 0;
        for (FetchBatchPlanner.NodeBatch node : batch.nodes()) {
            for (FetchRequest.ShardDocs shard : node.shards()) {
                DocOrigin origin = origins.stream().filter(o -> o.contextId().equals(shard.contextId())).findFirst().orElseThrow();
                assertThat(origin.nodeId(), equalTo(node.nodeId()));
                if (failedShards[s++] == false) {
                    for (int d = 0; d < shard.docCount(); d++) {
                        responses.add(new Ref(origin, shard.segments()[d], shard.docs()[d]));
                    }
                }
            }
        }
        return responses;
    }

    private static int shardCount(FetchBatchPlanner.Batch batch) {
        return batch.nodes().stream().mapToInt(node -> node.shards().size()).sum();
    }

    private Page page(Ref... refs) {
        BlockFactory blockFactory = blockFactory();
        List<DocOrigin> origins = new ArrayList<>();
        try (DocBlock.Builder builder = DocBlock.newBlockBuilder(blockFactory, refs.length)) {
            for (Ref ref : refs) {
                if (origins.contains(ref.origin()) == false) {
                    origins.add(ref.origin());
                }
                builder.appendShard(origins.indexOf(ref.origin())).appendSegment(ref.segment()).appendDoc(ref.doc());
            }
            // rows may share a document
            return new Page(builder.build(DocVector.config().origins(DocOrigins.of(origins)).mayContainDuplicates()));
        }
    }

    private static void assertShard(FetchRequest.ShardDocs shard, int[] segments, int[] docs) {
        assertThat(shard.segments(), equalTo(segments));
        assertThat(shard.docs(), equalTo(docs));
    }

    private static DocOrigin origin(String nodeId, int shard) {
        return new DocOrigin("", nodeId, new ShardId("index", "uuid", shard), new ShardSearchContextId("session", shard));
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

    private static DocOrigin randomOrigin() {
        return new DocOrigin(
            "",
            randomFrom("n1", "n2", "n3"),
            new ShardId("index", "uuid", between(0, 10)),
            new ShardSearchContextId(randomAlphaOfLength(6), randomNonNegativeLong())
        );
    }
}
