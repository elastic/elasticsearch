/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class StatValueComparatorTests extends ESTestCase {

    public void testLongVsInteger() {
        assertThat(StatValueComparator.compare(62L, 62), equalTo(0));
        assertThat(StatValueComparator.compare(100L, 62), greaterThan(0));
        assertThat(StatValueComparator.compare(10L, 62), lessThan(0));
    }

    public void testIntegerVsLong() {
        assertThat(StatValueComparator.compare(62, 62L), equalTo(0));
        assertThat(StatValueComparator.compare(100, 62L), greaterThan(0));
        assertThat(StatValueComparator.compare(10, 62L), lessThan(0));
    }

    public void testIntegralVsFloating() {
        assertThat(StatValueComparator.compare(2, 2.5d), lessThan(0));
        assertThat(StatValueComparator.compare(3L, 2.5d), greaterThan(0));
        assertThat(StatValueComparator.compare(2.0d, 2), equalTo(0));
    }

    public void testBytesRefVsString() {
        assertThat(StatValueComparator.compare(new BytesRef("abc"), "abc"), equalTo(0));
        assertThat(StatValueComparator.compare(new BytesRef("abd"), "abc"), greaterThan(0));
        assertThat(StatValueComparator.compare("abc", new BytesRef("abd")), lessThan(0));
    }

    public void testNullIsIncomparable() {
        assertThat(StatValueComparator.compare(null, 1), equalTo(StatValueComparator.INCOMPARABLE));
        assertThat(StatValueComparator.compare(1, null), equalTo(StatValueComparator.INCOMPARABLE));
        assertThat(StatValueComparator.compare(null, null), equalTo(StatValueComparator.INCOMPARABLE));
    }

    public void testIncompatibleTypesAreIncomparable() {
        assertThat(StatValueComparator.compare("abc", 1), equalTo(StatValueComparator.INCOMPARABLE));
        assertThat(StatValueComparator.compare(new BytesRef("abc"), 1), equalTo(StatValueComparator.INCOMPARABLE));
    }

    public void testKeywordComparedInUtf8ByteOrderNotUtf16() {
        // U+FFFD (BMP) vs U+1F600 (astral): String.compareTo (UTF-16) would order the emoji first, but the runtime
        // keyword comparison uses UTF-8 byte order (BytesRef), where the replacement char sorts first. The classifier
        // must match runtime order, or a filtered COUNT/MIN/MAX MATCH-vs-MISS diverges from a scan.
        String bmp = "�";
        String astral = "😀";
        assertThat(StatValueComparator.compare(bmp, astral), lessThan(0));
        assertThat(StatValueComparator.compare(astral, bmp), greaterThan(0));
        // Both stat representations (parquet String, text BytesRef) and a mix must all use UTF-8 order.
        assertThat(StatValueComparator.compare(new BytesRef(bmp), new BytesRef(astral)), lessThan(0));
        assertThat(StatValueComparator.compare(bmp, new BytesRef(astral)), lessThan(0));
        assertThat(StatValueComparator.compare(new BytesRef(bmp), astral), lessThan(0));
    }
}
