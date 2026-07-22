/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.eql;

import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse.Event;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse.Hits;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse.Sequence;
import org.elasticsearch.xpack.esql.plan.logical.EqlRelation;

import java.util.List;

/**
 * Unit tests for {@link EqlPageConverter}: they build an {@link EqlSearchResponse} by hand and assert the
 * produced {@link Page} matches the fixed schema, with no client or driver involved.
 */
public class EqlPageConverterTests extends ESTestCase {

    public void testEventMode() {
        Event e0 = event("logs-1", "a1", "{\"process\":{\"pid\":1}}");
        Event e1 = event("logs-2", "b2", "{\"process\":{\"pid\":2}}");
        EqlSearchResponse response = eventResponse(List.of(e0, e1));

        Page page = EqlPageConverter.toPage(response, EqlRelation.Mode.EVENT, TestBlockFactory.getNonBreakingInstance());
        response.decRef();
        try {
            assertEquals(3, page.getBlockCount());
            assertEquals(2, page.getPositionCount());
            assertBytesRefColumn(page, 0, "logs-1", "logs-2");   // _index
            assertBytesRefColumn(page, 1, "a1", "b2");            // _id
            assertBytesRefColumn(page, 2, "{\"process\":{\"pid\":1}}", "{\"process\":{\"pid\":2}}"); // _source
        } finally {
            page.releaseBlocks();
        }
    }

    public void testSequenceModeUnnestsToOneRowPerEvent() {
        Sequence s0 = new Sequence(List.of("host-a"), List.of(event("logs", "p0", "{}"), event("logs", "n0", "{}")));
        Sequence s1 = new Sequence(List.of("host-b"), List.of(event("logs", "p1", "{}"), event("logs", "n1", "{}")));
        EqlSearchResponse response = sequenceResponse(List.of(s0, s1));

        Page page = EqlPageConverter.toPage(response, EqlRelation.Mode.SEQUENCE, TestBlockFactory.getNonBreakingInstance());
        response.decRef();
        try {
            assertEquals(6, page.getBlockCount());
            assertEquals(4, page.getPositionCount()); // 2 sequences * 2 events

            LongBlock seq = page.getBlock(0);
            IntBlock position = page.getBlock(1);
            assertEquals(0L, seq.getLong(0));
            assertEquals(0, position.getInt(0));
            assertEquals(0L, seq.getLong(1));
            assertEquals(1, position.getInt(1));
            assertEquals(1L, seq.getLong(2));
            assertEquals(0, position.getInt(2));
            assertEquals(1L, seq.getLong(3));
            assertEquals(1, position.getInt(3));

            assertBytesRefColumn(page, 2, "host-a", "host-a", "host-b", "host-b"); // by
            assertBytesRefColumn(page, 4, "p0", "n0", "p1", "n1");                 // _id
        } finally {
            page.releaseBlocks();
        }
    }

    public void testMissingEventBecomesNulls() {
        Event present = event("logs", "p0", "{}");
        Event missing = new Event("", "", null, null, true);
        Sequence s0 = new Sequence(List.of("k"), List.of(present, missing));
        EqlSearchResponse response = sequenceResponse(List.of(s0));

        Page page = EqlPageConverter.toPage(response, EqlRelation.Mode.SEQUENCE, TestBlockFactory.getNonBreakingInstance());
        response.decRef();
        try {
            BytesRefBlock id = page.getBlock(4);
            assertFalse(id.isNull(0));
            assertTrue("missing event's _id must be null", id.isNull(1));
            BytesRefBlock source = page.getBlock(5);
            assertTrue("missing event's _source must be null", source.isNull(1));
        } finally {
            page.releaseBlocks();
        }
    }

    private static Event event(String index, String id, String source) {
        BytesReference sourceRef = new BytesArray(source);
        return new Event(index, id, sourceRef, null, false);
    }

    private static EqlSearchResponse eventResponse(List<Event> events) {
        Hits hits = new Hits(events, null, new TotalHits(events.size(), TotalHits.Relation.EQUAL_TO));
        return new EqlSearchResponse(hits, 1, false, noFailures());
    }

    private static EqlSearchResponse sequenceResponse(List<Sequence> sequences) {
        Hits hits = new Hits(null, sequences, new TotalHits(sequences.size(), TotalHits.Relation.EQUAL_TO));
        return new EqlSearchResponse(hits, 1, false, noFailures());
    }

    private static org.elasticsearch.action.search.ShardSearchFailure[] noFailures() {
        return new org.elasticsearch.action.search.ShardSearchFailure[0];
    }

    private static void assertBytesRefColumn(Page page, int blockIndex, String... expected) {
        BytesRefBlock block = page.getBlock(blockIndex);
        BytesRef scratch = new BytesRef();
        for (int i = 0; i < expected.length; i++) {
            assertEquals("row " + i + " of block " + blockIndex, new BytesRef(expected[i]), block.getBytesRef(i, scratch));
        }
    }
}
