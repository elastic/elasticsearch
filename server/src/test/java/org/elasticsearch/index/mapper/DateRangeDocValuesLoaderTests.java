/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.mapper.blockloader.docvalues.DateRangeDocValuesLoader;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

public class DateRangeDocValuesLoaderTests extends ESTestCase {

    private static final String FIELD_NAME = "test_date_range";

    /**
     * Verifies that a document with multiple stored ranges produces a multi-value entry in the loaded
     * block, with each range represented as a half-open [from, to) interval.
     */
    public void testMultipleRangesProducesMultiValueEntry() throws IOException {
        // encodeLongRanges sorts by from then to, so [1000,2000] comes before [3000,4000].
        var twoRanges = BinaryRangeUtil.encodeLongRanges(
            Set.of(
                new RangeFieldMapper.Range(RangeType.DATE, 1000L, 2000L, true, true),
                new RangeFieldMapper.Range(RangeType.DATE, 3000L, 4000L, true, true)
            )
        );

        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
            iw.addDocument(List.of(new BinaryDocValuesField(FIELD_NAME, twoRanges)));
            iw.forceMerge(1);

            try (DirectoryReader dr = iw.getReader()) {
                LeafReaderContext ctx = getOnlyLeafReader(dr).getContext();
                var loader = new DateRangeDocValuesLoader(FIELD_NAME);
                try (var reader = loader.columnAtATimeReader(ctx).apply(newLimitedBreaker(ByteSizeValue.ofMb(1)))) {
                    var block = (TestBlock) reader.read(TestBlock.factory(), TestBlock.docs(0), 0, false);

                    assertThat(block.size(), equalTo(1));
                    // Both ranges stored as inclusive [from, to] are converted to half-open [from, to+1).
                    // The from-values and to-values are each collected into a list for the multi-value position.
                    assertThat(block.get(0), equalTo(List.of(List.of(1000L, 3000L), List.of(2001L, 4001L))));
                }
            }
        }
    }

    /**
     * Verifies that a range with to=Long.MAX_VALUE (the maximum storable value) does not overflow
     * when converted from inclusive to exclusive. The result must be Long.MAX_VALUE (used as a
     * sentinel for "unbounded above"), not Long.MIN_VALUE from integer overflow.
     */
    public void testMaxValueToDoesNotOverflow() throws IOException {
        var maxRange = BinaryRangeUtil.encodeLongRanges(
            Set.of(new RangeFieldMapper.Range(RangeType.DATE, 1000L, Long.MAX_VALUE, true, true))
        );

        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
            iw.addDocument(List.of(new BinaryDocValuesField(FIELD_NAME, maxRange)));
            iw.forceMerge(1);

            try (DirectoryReader dr = iw.getReader()) {
                LeafReaderContext ctx = getOnlyLeafReader(dr).getContext();
                var loader = new DateRangeDocValuesLoader(FIELD_NAME);
                try (var reader = loader.columnAtATimeReader(ctx).apply(newLimitedBreaker(ByteSizeValue.ofMb(1)))) {
                    var block = (TestBlock) reader.read(TestBlock.factory(), TestBlock.docs(0), 0, false);

                    assertThat(block.size(), equalTo(1));
                    // to=Long.MAX_VALUE (inclusive) stays Long.MAX_VALUE in the exclusive representation,
                    // acting as an "unbounded above" sentinel — it must NOT overflow to Long.MIN_VALUE.
                    assertThat(block.get(0), equalTo(List.of(1000L, Long.MAX_VALUE)));
                }
            }
        }
    }

    /**
     * Verifies that a document whose binary doc value encodes zero ranges produces a null entry in the
     * loaded block rather than silently skipping the position, which would cause the block to have
     * fewer positions than expected and trigger a sanity-check failure in ValuesSourceReaderOperator.
     */
    public void testEmptyRangesAppendsNull() throws IOException {
        // Encode a single valid inclusive range [1000, 2000].
        var oneRange = BinaryRangeUtil.encodeLongRanges(Set.of(new RangeFieldMapper.Range(RangeType.DATE, 1000L, 2000L, true, true)));
        // Encode zero ranges — this is the edge case that triggered the bug.
        var emptyRanges = BinaryRangeUtil.encodeLongRanges(Set.of());

        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
            // One document has a valid range, one has a binary doc value that encodes zero ranges (the bug
            // case), and one has no binary doc value at all (the normal null case). RandomIndexWriter's
            // forceMerge does not guarantee that doc IDs in the merged segment preserve insertion order
            // (e.g. MockRandomMergePolicy shuffles segments), so the assertions below don't rely on which
            // doc ID ends up holding which value.
            iw.addDocument(List.of(new BinaryDocValuesField(FIELD_NAME, oneRange)));
            iw.addDocument(List.of(new BinaryDocValuesField(FIELD_NAME, emptyRanges)));
            iw.addDocument(List.of());
            iw.forceMerge(1);

            try (DirectoryReader dr = iw.getReader()) {
                LeafReaderContext ctx = getOnlyLeafReader(dr).getContext();
                var loader = new DateRangeDocValuesLoader(FIELD_NAME);
                try (var reader = loader.columnAtATimeReader(ctx).apply(newLimitedBreaker(ByteSizeValue.ofMb(1)))) {
                    var block = (TestBlock) reader.read(TestBlock.factory(), TestBlock.docs(0, 1, 2), 0, false);

                    // All three positions must be present: neither the empty-ranges doc nor the missing
                    // doc value may cause a position to be skipped.
                    assertThat(block.size(), equalTo(3));
                    List<Object> values = Arrays.asList(block.get(0), block.get(1), block.get(2));
                    // [1000, 2000] inclusive is stored as half-open [1000, 2001) in the block.
                    assertThat(values, hasItem(List.of(1000L, 2001L)));
                    // Zero ranges encoded and no doc value both produce null (not a skipped position).
                    assertThat(values.stream().filter(Objects::isNull).count(), equalTo(2L));
                }
            }
        }
    }
}
