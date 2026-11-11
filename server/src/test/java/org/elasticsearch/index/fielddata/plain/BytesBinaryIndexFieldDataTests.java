/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.search.SortField;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.index.mapper.BinaryDocValuesFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class BytesBinaryIndexFieldDataTests extends ESTestCase {

    private static BytesBinaryIndexFieldData fieldData(BinaryDocValuesFormat format) {
        return new BytesBinaryIndexFieldData("kw", null, null, IndexVersion.current(), format);
    }

    private static BytesBinaryIndexFieldData fieldData(BinaryDocValuesFormat format, IndexVersion indexVersion) {
        return new BytesBinaryIndexFieldData("kw", null, null, indexVersion, format);
    }

    public void testSortFieldWithoutNestedReturnsBinaryDocValuesSortField() {
        final SortField sf = fieldData(BinaryDocValuesFormat.SEPARATE_COUNT).sortField("_last", MultiValueMode.MIN, null, false);
        assertThat(sf, instanceOf(MultiValuedBinaryDocValuesSortField.class));
    }

    public void testSortFieldEqualsIndexSort() {
        for (final BinaryDocValuesFormat format : BinaryDocValuesFormat.values()) {
            final BytesBinaryIndexFieldData fd = fieldData(format);
            for (final boolean reverse : new boolean[] { false, true }) {
                for (final Object mv : new Object[] { null, "_last", "_first" }) {
                    for (final MultiValueMode mode : new MultiValueMode[] { MultiValueMode.MIN, MultiValueMode.MAX }) {
                        final SortField querySf = fd.sortField(mv, mode, null, reverse);
                        final SortField indexSf = fd.indexSort(IndexVersion.current(), mv, mode, reverse);
                        assertEquals(querySf, indexSf);
                    }
                }
            }
        }
    }

    public void testOldSeparateCountSortFieldDivergesFromIndexSort() {
        final IndexVersion old = IndexVersions.TIME_SERIES_USE_SYNTHETIC_ID_BEST_COMPRESSION;
        final BytesBinaryIndexFieldData fd = fieldData(BinaryDocValuesFormat.SEPARATE_COUNT, old);
        final SortField querySf = fd.sortField("_last", MultiValueMode.MIN, null, false);
        final SortField indexSf = fd.indexSort(old, "_last", MultiValueMode.MIN, false);
        assertThat(querySf, not(instanceOf(MultiValuedBinaryDocValuesSortField.class)));
        assertThat(indexSf, instanceOf(MultiValuedBinaryDocValuesSortField.class));
        assertNotEquals(querySf, indexSf);
    }

    public void testNonSeparateCountFormatsIgnoreIndexVersionForSortField() {
        final IndexVersion old = IndexVersions.TIME_SERIES_USE_SYNTHETIC_ID_BEST_COMPRESSION;
        for (final BinaryDocValuesFormat format : new BinaryDocValuesFormat[] {
            BinaryDocValuesFormat.ARRAY_ORDER_INLINE_NULL,
            BinaryDocValuesFormat.COLUMNAR_PAYLOAD }) {
            final SortField sf = fieldData(format, old).sortField("_last", MultiValueMode.MIN, null, false);
            assertThat(sf, instanceOf(MultiValuedBinaryDocValuesSortField.class));
        }
    }

    public void testFuzzyLiteralMissingValueAlwaysFallsBackToComparatorSource() {
        for (int i = 0; i < 20; i++) {
            final String literal = randomValueOtherThanMany(
                v -> v == null || "_first".equals(v) || "_last".equals(v),
                () -> randomAlphaOfLengthBetween(1, 20)
            );
            final BinaryDocValuesFormat format = randomFrom(BinaryDocValuesFormat.values());
            final SortField sf = fieldData(format).sortField(
                literal,
                randomFrom(MultiValueMode.MIN, MultiValueMode.MAX),
                null,
                randomBoolean()
            );
            assertThat(sf, not(instanceOf(MultiValuedBinaryDocValuesSortField.class)));
            assertThat(sf.getComparatorSource(), instanceOf(BytesRefFieldComparatorSource.class));
        }
    }

    public void testSortFieldMissingValueRespectsDirection() {
        final BytesBinaryIndexFieldData fd = fieldData(BinaryDocValuesFormat.SEPARATE_COUNT);
        assertSame(SortField.STRING_LAST, fd.sortField("_last", MultiValueMode.MIN, null, false).getMissingValue());
        assertSame(SortField.STRING_FIRST, fd.sortField("_last", MultiValueMode.MIN, null, true).getMissingValue());
        assertSame(SortField.STRING_FIRST, fd.sortField("_first", MultiValueMode.MIN, null, false).getMissingValue());
        assertSame(SortField.STRING_LAST, fd.sortField("_first", MultiValueMode.MIN, null, true).getMissingValue());
    }

    public void testSortFieldEqualityDistinguishesMaxMode() {
        final BytesBinaryIndexFieldData fd = fieldData(BinaryDocValuesFormat.SEPARATE_COUNT);
        final SortField min = fd.sortField("_last", MultiValueMode.MIN, null, false);
        final SortField max = fd.sortField("_last", MultiValueMode.MAX, null, false);
        assertNotEquals("MIN and MAX sort fields must not be equal", min, max);
        assertNotEquals("MIN and MAX sort field hashCodes should differ", min.hashCode(), max.hashCode());
    }

    public void testSortFieldEqualityDistinguishesBinaryFormat() {
        final SortField separateCount = fieldData(BinaryDocValuesFormat.SEPARATE_COUNT).sortField("_last", MultiValueMode.MIN, null, false);
        final SortField inlineNull = fieldData(BinaryDocValuesFormat.ARRAY_ORDER_INLINE_NULL).sortField(
            "_last",
            MultiValueMode.MIN,
            null,
            false
        );
        assertNotEquals("Different binary formats must not be equal", separateCount, inlineNull);
    }

    public void testSortFieldWithNestedFallsBackToComparatorSource() {
        final Nested nested = new Nested(null, null, null, null);
        final SortField sf = fieldData(BinaryDocValuesFormat.SEPARATE_COUNT).sortField("_last", MultiValueMode.MIN, nested, false);
        assertThat(sf, not(instanceOf(MultiValuedBinaryDocValuesSortField.class)));
        assertThat(sf.getComparatorSource(), instanceOf(BytesRefFieldComparatorSource.class));
        assertEquals(SortField.Type.CUSTOM, sf.getType());
    }

    public void testSortFieldWithLiteralMissingValueFallsBackToComparatorSource() {
        final SortField sf = fieldData(BinaryDocValuesFormat.SEPARATE_COUNT).sortField("missing-literal", MultiValueMode.MIN, null, false);
        assertThat(sf, not(instanceOf(MultiValuedBinaryDocValuesSortField.class)));
        assertThat(sf.getComparatorSource(), instanceOf(BytesRefFieldComparatorSource.class));
    }

    public void testOldSeparateCountIndexFallsBackToComparatorSource() {
        final IndexVersion old = IndexVersions.TIME_SERIES_USE_SYNTHETIC_ID_BEST_COMPRESSION;
        final BytesBinaryIndexFieldData fd = fieldData(BinaryDocValuesFormat.SEPARATE_COUNT, old);
        final SortField sf = fd.sortField("_last", MultiValueMode.MIN, null, false);
        assertThat(sf, not(instanceOf(MultiValuedBinaryDocValuesSortField.class)));
        assertThat(sf.getComparatorSource(), instanceOf(BytesRefFieldComparatorSource.class));
    }
}
