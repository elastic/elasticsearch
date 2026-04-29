/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;

import java.util.List;
import java.util.Map;

public class FileLayoutTests extends ESTestCase {

    private static SourceMetadata sampleMetadata() {
        return new SimpleSourceMetadata(List.<Attribute>of(), "parquet", "s3://b/f.parquet");
    }

    public void testRequiresMetadata() {
        expectThrows(IllegalArgumentException.class, () -> new FileLayout(null, List.of()));
    }

    public void testNullSplitRangesBecomesEmpty() {
        FileLayout layout = new FileLayout(sampleMetadata(), null);
        assertNotNull(layout.splitRanges());
        assertTrue(layout.splitRanges().isEmpty());
    }

    public void testSplitRangesAreDefensivelyCopied() {
        RangeAwareFormatReader.SplitRange r = new RangeAwareFormatReader.SplitRange(0, 10, Map.of("k", 1L));
        java.util.ArrayList<RangeAwareFormatReader.SplitRange> mutable = new java.util.ArrayList<>();
        mutable.add(r);
        FileLayout layout = new FileLayout(sampleMetadata(), mutable);

        mutable.clear();
        assertEquals(1, layout.splitRanges().size());
        assertSame(r, layout.splitRanges().get(0));

        // Returned list should be unmodifiable.
        expectThrows(UnsupportedOperationException.class, () -> layout.splitRanges().add(r));
    }

    public void testOfMetadataReturnsEmptyRanges() {
        FileLayout layout = FileLayout.ofMetadata(sampleMetadata());
        assertNotNull(layout.metadata());
        assertTrue(layout.splitRanges().isEmpty());
    }

    public void testCarriesProvidedRanges() {
        RangeAwareFormatReader.SplitRange r1 = new RangeAwareFormatReader.SplitRange(0, 10, Map.of("a", 1L));
        RangeAwareFormatReader.SplitRange r2 = new RangeAwareFormatReader.SplitRange(10, 20, Map.of("b", 2L));
        FileLayout layout = new FileLayout(sampleMetadata(), List.of(r1, r2));
        assertEquals(2, layout.splitRanges().size());
        assertEquals(r1, layout.splitRanges().get(0));
        assertEquals(r2, layout.splitRanges().get(1));
    }
}
