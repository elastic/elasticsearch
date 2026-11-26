/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * What a read description with an {@link ExpandRead} enforces before any reader sees it: the rows of such a read are
 * elements, so it harvests no statistics and carries no row positions, whatever the caller asked for.
 */
public class FormatReadContextExpandReadTests extends ESTestCase {

    private static final ExpandRead EXPAND = new ExpandRead("items", List.of(new ExpandRead.Leaf("sku", DataType.KEYWORD)));

    /** Statistics capture is off for an expanding read even when the caller asked for every column's statistics. */
    public void testStatisticsCaptureOff() {
        StripeStatsSink sink = (location, identity, ordinal, start, end, atStart, atEnd, eof, statistics) -> {};
        FormatReadContext expanding = FormatReadContext.builder()
            .projectedColumns(List.of("id"))
            .batchSize(10)
            .statsColumnScope(StripeColumnScope.ALL)
            .statsStripeSink(sink)
            .expandRead(EXPAND)
            .build();
        assertThat(expanding.statsColumnScope(), equalTo(StripeColumnScope.NONE));
        assertThat(expanding.statsStripeSink(), nullValue());

        // The same description without the expand read keeps what it asked for
        FormatReadContext plain = FormatReadContext.builder()
            .projectedColumns(List.of("id"))
            .batchSize(10)
            .statsColumnScope(StripeColumnScope.ALL)
            .statsStripeSink(sink)
            .build();
        assertThat(plain.statsColumnScope(), equalTo(StripeColumnScope.ALL));
        assertThat(plain.statsStripeSink(), sameInstance(sink));
    }

    /** A row position addresses a file row, so both read descriptions refuse one beside an expand read. */
    public void testRowPositionsRefused() {
        List<String> withPosition = List.of("id", ColumnExtractor.ROW_POSITION_COLUMN);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> FormatReadContext.builder().projectedColumns(withPosition).batchSize(10).expandRead(EXPAND).build()
        );
        assertThat(e.getMessage(), containsString("a read that expands [items] cannot carry [" + ColumnExtractor.ROW_POSITION_COLUMN));

        RangeReadContext range = new RangeReadContext(withPosition, 10, 0, 100, List.of(), ErrorPolicy.STRICT);
        e = expectThrows(IllegalArgumentException.class, () -> range.setExpandRead(EXPAND));
        assertThat(e.getMessage(), containsString("cannot carry [" + ColumnExtractor.ROW_POSITION_COLUMN));
        assertThat(range.expandRead(), nullValue());
    }

    /** A leaf named twice would emit two columns for one field; the description refuses it. */
    public void testLeafRequestedTwiceRefused() {
        ExpandRead.Leaf sku = new ExpandRead.Leaf("sku", DataType.KEYWORD);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new ExpandRead("items", List.of(sku, sku)));
        assertThat(e.getMessage(), containsString("leaf [sku] of [items] is requested twice"));
    }
}
