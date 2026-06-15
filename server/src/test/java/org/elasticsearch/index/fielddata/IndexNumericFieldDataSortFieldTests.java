/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.elasticsearch.index.fielddata.plain.SortedDoublesIndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/**
 * Unit tests for the SortField produced by IndexNumericFieldData.indexSort(), specifically
 * verifying that single-valued fields (doc_values.multi_value: false) emit a plain SortField
 * rather than SortedNumericSortField, which requires SORTED_NUMERIC doc values at merge time.
 */
public class IndexNumericFieldDataSortFieldTests extends ESTestCase {

    private static final String FIELD = "value";
    private static final IndexType INDEX_TYPE = IndexType.points(true, true);

    // --- integral types ---

    public void testMultiValuedIntegralIndexSortEmitsSortedNumericSortField() {
        SortedNumericIndexFieldData fd = new SortedNumericIndexFieldData(
            FIELD,
            NumericType.LONG,
            NumericType.LONG.getValuesSourceType(),
            (values, name) -> null,
            INDEX_TYPE
        );
        SortField sf = fd.indexSort(IndexVersion.current(), "_last", MultiValueMode.MIN, false);
        assertThat(sf, instanceOf(SortedNumericSortField.class));
    }

    public void testSingleValuedIntegralIndexSortEmitsPlainSortField() {
        IndexNumericFieldData fd = (IndexNumericFieldData) new SortedNumericIndexFieldData.Builder(
            FIELD,
            NumericType.LONG,
            (values, name) -> null,
            INDEX_TYPE
        ).singleValued().build(null, null);
        SortField sf = fd.indexSort(IndexVersion.current(), "_last", MultiValueMode.MIN, false);
        assertThat(
            "single-valued field must use a plain SortField so Lucene's NUMERIC doc-values type is accepted at merge time",
            sf,
            not(instanceOf(SortedNumericSortField.class))
        );
        assertEquals(SortField.Type.LONG, sf.getType());
    }

    public void testSingleValuedIntegralNonIndexSortStillUsesComparatorSource() {
        IndexNumericFieldData fd = (IndexNumericFieldData) new SortedNumericIndexFieldData.Builder(
            FIELD,
            NumericType.LONG,
            (values, name) -> null,
            INDEX_TYPE
        ).singleValued().build(null, null);
        // non-index sort should use an XFieldComparatorSource (Type.CUSTOM), not a numeric sort field
        SortField sf = fd.sortField("_last", MultiValueMode.MIN, null, false);
        assertEquals(SortField.Type.CUSTOM, sf.getType());
    }

    // --- floating-point types ---

    public void testMultiValuedDoubleIndexSortEmitsSortedNumericSortField() {
        SortedDoublesIndexFieldData fd = new SortedDoublesIndexFieldData(
            FIELD,
            NumericType.DOUBLE,
            NumericType.DOUBLE.getValuesSourceType(),
            (values, name) -> null,
            INDEX_TYPE
        );
        SortField sf = fd.indexSort(IndexVersion.current(), "_last", MultiValueMode.MIN, false);
        assertThat(sf, instanceOf(SortedNumericSortField.class));
    }

    public void testSingleValuedDoubleIndexSortEmitsPlainSortField() {
        IndexNumericFieldData fd = (IndexNumericFieldData) new SortedDoublesIndexFieldData.Builder(
            FIELD,
            NumericType.DOUBLE,
            NumericType.DOUBLE.getValuesSourceType(),
            (values, name) -> null,
            INDEX_TYPE
        ).singleValued().build(null, null);
        SortField sf = fd.indexSort(IndexVersion.current(), "_last", MultiValueMode.MIN, false);
        assertThat(
            "single-valued field must use a plain SortField so Lucene's NUMERIC doc-values type is accepted at merge time",
            sf,
            not(instanceOf(SortedNumericSortField.class))
        );
        assertEquals(SortField.Type.DOUBLE, sf.getType());
    }

    public void testSingleValuedDoubleNonIndexSortStillUsesComparatorSource() {
        IndexNumericFieldData fd = (IndexNumericFieldData) new SortedDoublesIndexFieldData.Builder(
            FIELD,
            NumericType.DOUBLE,
            NumericType.DOUBLE.getValuesSourceType(),
            (values, name) -> null,
            INDEX_TYPE
        ).singleValued().build(null, null);
        SortField sf = fd.sortField("_last", MultiValueMode.MIN, null, false);
        assertEquals(SortField.Type.CUSTOM, sf.getType());
    }
}
