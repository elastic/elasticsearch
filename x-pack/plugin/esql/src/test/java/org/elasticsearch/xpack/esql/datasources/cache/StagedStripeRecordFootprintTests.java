/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.SplitStats;
import org.elasticsearch.xpack.esql.datasources.spi.StripeStatsSink;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThan;

/**
 * What a staged record stops retaining. A read stages one record per stripe and holds it until the query ends,
 * so the quantity that matters is the flat map per record that is no longer kept.
 * <p>
 * Only the flat side is measured, and deliberately. {@code RamUsageEstimator.sizeOfMap} walks a real
 * {@code HashMap} and reports its entries, keys and values. The compact side has no sound instrument here:
 * {@link SplitStats} is not {@code Accountable}, so {@code sizeOfObject} returns
 * {@code UNKNOWN_DEFAULT_RAM_BYTES_USED} -- a 256-byte constant that reads exactly like a measurement -- and
 * {@link StagedStripeRecord} IS a {@code Map}, so {@code sizeOfObject} on one routes to {@code sizeOfMap},
 * builds the flat form and measures that instead of the compact form. Reconstructing {@code SplitStats}'
 * private array layout in a test would be a figure that drifts silently. So this pins the side that can be
 * measured and claims nothing about the ratio; the end-to-end figure belongs to a heap measurement on a
 * cluster, not to a unit test.
 */
public class StagedStripeRecordFootprintTests extends ESTestCase {

    private static final long MTIME = 1_700_000_000_000L;
    private static final int WIDE_COLUMNS = 105;

    public void testTheFlatFormIsWorthNotRetaining() {
        assumeTrue("footprint figures are pinned for the compressed-reference layout", RamUsageEstimator.COMPRESSED_REFS_ENABLED);

        StagedStripeRecord record = wideRecord();
        Map<String, Object> flat = new HashMap<>(record);
        assertFalse("the copy must be a walk, leaving the record compact", record.keepsFlatForm());

        long flatBytes = RamUsageEstimator.sizeOfMap(flat);
        long perColumn = flatBytes / WIDE_COLUMNS;

        // Printed rather than only asserted: the figure is the point, and a reader of a failure wants it.
        logger.info(
            "a {}-column staged stripe presents {} entries, {} bytes of flat map, {} bytes per column",
            WIDE_COLUMNS,
            flat.size(),
            flatBytes,
            perColumn
        );

        assertEquals("a wide stripe must present four entries per column plus the addressing keys", 4 * WIDE_COLUMNS + 13, flat.size());
        // A floor, not an equality: the exact figure moves with the JDK's object layout and with the column
        // names, and what this guards is that the flat form is still worth not keeping. It fails if the flat
        // map ever collapses to something a record could simply hold.
        assertThat(
            "the flat map must stay far larger than the statistics it presents, or a record could simply hold it",
            flatBytes,
            greaterThan(50_000L)
        );
    }

    private static StagedStripeRecord wideRecord() {
        SplitStats.Builder builder = new SplitStats.Builder().rowCount(10_000L);
        for (int i = 0; i < WIDE_COLUMNS; i++) {
            int column = builder.addColumn("column_" + i);
            builder.nullCount(column, 3L);
            builder.valueCount(column, 9_997L);
            builder.min(column, new BytesRef("aaaaaaaaaaaaaaaa"));
            builder.max(column, new BytesRef("zzzzzzzzzzzzzzzz"));
        }
        return new StagedStripeRecord(
            new StripeStatsSink.ReadIdentity(MTIME, "fingerprint", "read-config", true, 8L * 1024 * 1024),
            0L,
            0L,
            8L * 1024 * 1024,
            true,
            true,
            true,
            builder.build()
        );
    }
}
