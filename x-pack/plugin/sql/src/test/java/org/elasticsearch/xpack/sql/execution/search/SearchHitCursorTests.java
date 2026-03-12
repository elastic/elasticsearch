/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.execution.search;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.ql.execution.search.extractor.ConstantExtractorTests;
import org.elasticsearch.xpack.ql.execution.search.extractor.HitExtractor;
import org.elasticsearch.xpack.sql.AbstractSqlWireSerializingTestCase;
import org.elasticsearch.xpack.sql.execution.search.extractor.ComputingExtractorTests;
import org.elasticsearch.xpack.sql.plugin.CursorTests;
import org.elasticsearch.xpack.sql.session.Cursor;
import org.elasticsearch.xpack.sql.session.Cursors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.common.bytes.BytesReferenceTestUtils.equalBytes;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SearchHitCursorTests extends AbstractSqlWireSerializingTestCase<SearchHitCursor> {
    public static SearchHitCursor randomSearchHitCursor() {
        int extractorsSize = between(1, 20);
        List<HitExtractor> extractors = new ArrayList<>(extractorsSize);
        for (int i = 0; i < extractorsSize; i++) {
            extractors.add(randomHitExtractor(0));
        }
        return new SearchHitCursor(
            new SearchSourceBuilder().size(randomInt(1000)),
            extractors,
            CompositeAggregationCursorTests.randomBitSet(extractorsSize),
            randomIntBetween(10, 1024),
            randomBoolean(),
            randomBoolean()
        );
    }

    static HitExtractor randomHitExtractor(int depth) {
        List<Supplier<HitExtractor>> options = new ArrayList<>();
        if (depth < 5) {
            options.add(() -> ComputingExtractorTests.randomComputingExtractor());
        }
        options.add(ConstantExtractorTests::randomConstantExtractor);
        return randomFrom(options).get();
    }

    @Override
    protected SearchHitCursor mutateInstance(SearchHitCursor instance) {
        return new SearchHitCursor(
            instance.next(),
            instance.extractors(),
            randomValueOtherThan(instance.mask(), () -> CompositeAggregationCursorTests.randomBitSet(instance.extractors().size())),
            randomValueOtherThan(instance.limit(), () -> randomIntBetween(1, 1024)),
            instance.includeFrozen() == false,
            instance.allowPartialSearchResults() == false
        );
    }

    @Override
    protected SearchHitCursor createTestInstance() {
        return randomSearchHitCursor();
    }

    @Override
    protected Reader<SearchHitCursor> instanceReader() {
        return SearchHitCursor::new;
    }

    @Override
    protected SearchHitCursor copyInstance(SearchHitCursor instance, TransportVersion version) throws IOException {
        /* Randomly choose between internal protocol round trip and String based
         * round trips used to toXContent. */
        if (randomBoolean()) {
            return super.copyInstance(instance, version);
        }
        return (SearchHitCursor) CursorTests.decodeFromString(Cursors.encodeToString(instance, randomZone()));
    }

    public void testPitIdIsRefreshedInNextCursor() {
        // Initial PIT ID
        BytesReference initialPitId = new BytesArray("initial_pit_id");
        // New PIT ID returned in the search response
        BytesReference newPitId = new BytesArray("new_pit_id");

        // Create a SearchSourceBuilder with the initial PIT ID
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.pointInTimeBuilder(new PointInTimeBuilder(initialPitId));
        source.size(10);

        // Create a SearchHit with sort values for updateSearchAfter
        org.elasticsearch.search.SearchHit lastHit = org.elasticsearch.search.SearchHit.unpooled(1, "id1");
        lastHit.sortValues(
            new Object[] { 1L },
            new org.elasticsearch.search.DocValueFormat[] { org.elasticsearch.search.DocValueFormat.RAW }
        );
        org.elasticsearch.search.SearchHits hits = org.elasticsearch.search.SearchHits.unpooled(
            new org.elasticsearch.search.SearchHit[] { lastHit },
            new org.apache.lucene.search.TotalHits(1, org.apache.lucene.search.TotalHits.Relation.EQUAL_TO),
            1.0f
        );

        // Mock the SearchResponse to return a different PIT ID
        SearchResponse response = mock(SearchResponse.class);
        when(response.pointInTimeId()).thenReturn(newPitId);
        when(response.getHits()).thenReturn(hits);

        // Mock the SearchHitRowSet to indicate there's more data (hasRemaining = true)
        SearchHitRowSet rowSet = mock(SearchHitRowSet.class);
        when(rowSet.hasRemaining()).thenReturn(true);
        when(rowSet.extractors()).thenReturn(Collections.emptyList());
        when(rowSet.mask()).thenReturn(new BitSet());
        when(rowSet.getRemainingLimit()).thenReturn(100);

        // Capture the cursor created by handle()
        final Cursor[] cursorHolder = new Cursor[1];
        ActionListener<Cursor.Page> listener = new ActionListener<>() {
            @Override
            public void onResponse(Cursor.Page page) {
                cursorHolder[0] = page.next();
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should not fail: " + e.getMessage());
            }
        };

        // Call handle()
        SearchHitCursor.handle(mock(Client.class), response, source, () -> rowSet, listener, false, false);

        // Verify that the source now has the new PIT ID
        assertNotNull(source.pointInTimeBuilder());
        assertThat(source.pointInTimeBuilder().getEncodedId(), equalBytes(newPitId));

        // Verify that the cursor was created
        assertNotNull(cursorHolder[0]);
        assertTrue(cursorHolder[0] instanceof SearchHitCursor);
    }
}
