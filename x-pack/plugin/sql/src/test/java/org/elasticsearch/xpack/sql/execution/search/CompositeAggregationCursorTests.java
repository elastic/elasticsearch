/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.execution.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.ql.execution.search.extractor.BucketExtractor;
import org.elasticsearch.xpack.ql.execution.search.extractor.ConstantExtractorTests;
import org.elasticsearch.xpack.sql.AbstractSqlWireSerializingTestCase;
import org.elasticsearch.xpack.sql.execution.search.extractor.CompositeKeyExtractorTests;
import org.elasticsearch.xpack.sql.execution.search.extractor.MetricAggExtractorTests;
import org.elasticsearch.xpack.sql.session.Cursor;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.common.bytes.BytesReferenceTestUtils.equalBytes;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CompositeAggregationCursorTests extends AbstractSqlWireSerializingTestCase<CompositeAggCursor> {
    public static CompositeAggCursor randomCompositeCursor() {
        int extractorsSize = between(1, 20);
        ZoneId id = randomZone();
        List<BucketExtractor> extractors = new ArrayList<>(extractorsSize);
        for (int i = 0; i < extractorsSize; i++) {
            extractors.add(randomBucketExtractor(id));
        }

        return new CompositeAggCursor(
            new SearchSourceBuilder().size(randomInt(1000)),
            extractors,
            randomBitSet(extractorsSize),
            randomIntBetween(10, 1024),
            randomBoolean(),
            randomAlphaOfLength(5)
        );
    }

    static BucketExtractor randomBucketExtractor(ZoneId zoneId) {
        List<Supplier<BucketExtractor>> options = new ArrayList<>();
        options.add(ConstantExtractorTests::randomConstantExtractor);
        options.add(() -> MetricAggExtractorTests.randomMetricAggExtractor(zoneId));
        options.add(() -> CompositeKeyExtractorTests.randomCompositeKeyExtractor(zoneId));
        return randomFrom(options).get();
    }

    @Override
    protected CompositeAggCursor mutateInstance(CompositeAggCursor instance) {
        return new CompositeAggCursor(
            instance.next(),
            instance.extractors(),
            randomValueOtherThan(instance.mask(), () -> randomBitSet(instance.extractors().size())),
            randomValueOtherThan(instance.limit(), () -> randomIntBetween(1, 512)),
            instance.includeFrozen() == false,
            instance.indices()
        );
    }

    @Override
    protected CompositeAggCursor createTestInstance() {
        return randomCompositeCursor();
    }

    @Override
    protected Reader<CompositeAggCursor> instanceReader() {
        return CompositeAggCursor::new;
    }

    @Override
    protected ZoneId instanceZoneId(CompositeAggCursor instance) {
        List<BucketExtractor> extractors = instance.extractors();
        for (BucketExtractor bucketExtractor : extractors) {
            ZoneId zoneId = MetricAggExtractorTests.extractZoneId(bucketExtractor);
            zoneId = zoneId == null ? CompositeKeyExtractorTests.extractZoneId(bucketExtractor) : zoneId;

            if (zoneId != null) {
                return zoneId;
            }
        }
        return randomZone();
    }

    static BitSet randomBitSet(int size) {
        BitSet mask = new BitSet(size);
        for (int i = 0; i < size; i++) {
            mask.set(i, randomBoolean());
        }
        return mask;
    }

    public void testPitIdIsRefreshedInNextCursor() {
        // Initial PIT ID
        BytesReference initialPitId = new BytesArray("initial_pit_id");
        // New PIT ID returned in the search response
        BytesReference newPitId = new BytesArray("new_pit_id");

        // Create a SearchSourceBuilder with the initial PIT ID
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.pointInTimeBuilder(new PointInTimeBuilder(initialPitId));

        // Mock the SearchResponse to return a different PIT ID
        SearchResponse response = mock(SearchResponse.class);
        when(response.pointInTimeId()).thenReturn(newPitId);

        // Mock the CompositeAggRowSet to indicate there's more data (remainingData > 0)
        // Use null afterKey to skip updateSourceAfterKey which requires aggregations setup
        CompositeAggRowSet rowSet = mock(CompositeAggRowSet.class);
        when(rowSet.afterKey()).thenReturn(null);
        when(rowSet.remainingData()).thenReturn(100);
        when(rowSet.extractors()).thenReturn(Collections.emptyList());
        when(rowSet.mask()).thenReturn(new BitSet());

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
        CompositeAggCursor.handle(
            mock(Client.class),
            response,
            source,
            () -> rowSet,
            (q, r) -> new CompositeAggCursor(q, r.extractors(), r.mask(), r.remainingData(), false, "test_index"),
            () -> fail("Retry should not be called"),
            listener,
            false
        );

        // Verify that the source now has the new PIT ID
        assertNotNull(source.pointInTimeBuilder());
        assertThat(source.pointInTimeBuilder().getEncodedId(), equalBytes(newPitId));

        // Verify that the cursor was created
        assertNotNull(cursorHolder[0]);
        assertTrue(cursorHolder[0] instanceof CompositeAggCursor);
    }
}
