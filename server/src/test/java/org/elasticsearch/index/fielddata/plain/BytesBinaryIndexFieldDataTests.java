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
import org.elasticsearch.index.mapper.BinaryDocValuesFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.instanceOf;

public class BytesBinaryIndexFieldDataTests extends ESTestCase {

    private static BytesBinaryIndexFieldData fieldData(BinaryDocValuesFormat format) {
        return new BytesBinaryIndexFieldData("kw", null, null, IndexVersion.current(), format);
    }

    public void testSortFieldWithoutNestedReturnsBinaryDocValuesSortField() {
        final SortField sf = fieldData(BinaryDocValuesFormat.SEPARATE_COUNT).sortField("_last", MultiValueMode.MIN, null, false);
        assertThat(sf, instanceOf(MultiValuedBinaryDocValuesSortField.class));
    }

    public void testSortFieldEqualsIndexSort() {
        for (final BinaryDocValuesFormat format : BinaryDocValuesFormat.values()) {
            final BytesBinaryIndexFieldData fd = fieldData(format);
            for (final boolean reverse : new boolean[] { false, true }) {
                for (final String mv : new String[] { "_last", "_first" }) {
                    for (final MultiValueMode mode : new MultiValueMode[] { MultiValueMode.MIN, MultiValueMode.MAX }) {
                        final SortField querySf = fd.sortField(mv, mode, null, reverse);
                        final SortField indexSf = fd.indexSort(IndexVersion.current(), mv, mode, reverse);
                        assertEquals(querySf, indexSf);
                    }
                }
            }
        }
    }

    public void testSortFieldMissingValueRespectsDirection() {
        final BytesBinaryIndexFieldData fd = fieldData(BinaryDocValuesFormat.SEPARATE_COUNT);
        assertSame(SortField.STRING_LAST, fd.sortField("_last", MultiValueMode.MIN, null, false).getMissingValue());
        assertSame(SortField.STRING_FIRST, fd.sortField("_last", MultiValueMode.MIN, null, true).getMissingValue());
        assertSame(SortField.STRING_FIRST, fd.sortField("_first", MultiValueMode.MIN, null, false).getMissingValue());
        assertSame(SortField.STRING_LAST, fd.sortField("_first", MultiValueMode.MIN, null, true).getMissingValue());
    }
}
