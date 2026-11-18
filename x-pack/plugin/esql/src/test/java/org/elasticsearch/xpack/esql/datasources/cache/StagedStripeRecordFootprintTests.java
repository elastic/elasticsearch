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
 * What a staged record stops retaining: the map the flat form has to allocate to present itself. A read stages
 * one record per stripe and holds it until the query ends.
 * <p>
 * Only the map's own skeleton is measured -- its table, its nodes and the key strings {@link StripeStatsHarvester}
 * builds -- and the values are excluded on two separate grounds. They are not saved: {@link SplitStats#min} hands
 * back the stored reference, so an extremum in the flat map is the same object the compact form already holds,
 * and the flags are the canonical {@code Boolean}s. And they cannot be measured here anyway:
 * {@code RamUsageEstimator.sizeOfObject} answers any type it does not know with
 * {@code UNKNOWN_DEFAULT_RAM_BYTES_USED}, a 256-byte constant that reads exactly like a measurement, and neither
 * {@code BytesRef} nor {@code Boolean} is known to it. A measurement over the values is invariant to how wide the
 * text in them is, which is what makes the trap hard to see.
 * <p>
 * So this pins the part that is genuinely owned and genuinely measurable, and claims nothing about the ratio to
 * the compact form -- {@link SplitStats} is not {@code Accountable}, so it hits the same constant, and
 * {@link StagedStripeRecord} IS a {@code Map}, so measuring one routes back through the flat form. The
 * end-to-end figure belongs to a heap measurement on a cluster.
 */
public class StagedStripeRecordFootprintTests extends ESTestCase {

    private static final long MTIME = 1_700_000_000_000L;
    private static final int WIDE_COLUMNS = 105;

    public void testTheFlatFormIsWorthNotRetaining() {
        assumeTrue("object sizes are the compressed-reference layout's", RamUsageEstimator.COMPRESSED_REFS_ENABLED);

        StagedStripeRecord record = wideRecord();
        Map<String, Object> flat = new HashMap<>(record);
        assertFalse("the copy must be a walk, leaving the record compact", record.keepsFlatForm());

        assertEquals("a wide stripe must present four entries per column plus the addressing keys", 4 * WIDE_COLUMNS + 13, flat.size());

        // The same keys with their values nulled out: the table, the nodes and the key strings, and nothing
        // that the compact form also holds. sizeOfObject(null) is 0, so no value can contribute.
        Map<String, Object> skeleton = new HashMap<>();
        for (String key : flat.keySet()) {
            skeleton.put(key, null);
        }
        long skeletonBytes = RamUsageEstimator.sizeOfMap(skeleton);

        // The guard the first version of this test needed and did not have. It pins the reason the values are
        // excluded as a live fact rather than a comment: every extremum and every flag is answered with the
        // unknown-size constant, so a measurement including them would be part placeholder and would not move
        // when the text in them got wider. If Lucene ever learns these types, this fails and the exclusion
        // above has to be re-argued rather than silently producing a different number.
        int placeholders = 0;
        for (Object value : flat.values()) {
            if (RamUsageEstimator.sizeOfObject(value) == RamUsageEstimator.UNKNOWN_DEFAULT_RAM_BYTES_USED) {
                placeholders++;
                assertTrue(
                    "only the extrema and the flags may be unmeasurable here, not a count",
                    value instanceof BytesRef || value instanceof Boolean
                );
            }
        }
        assertEquals("two extrema per column plus the five flags are unmeasurable by this estimator", 2 * WIDE_COLUMNS + 5, placeholders);

        long keyBytes = 0;
        for (String key : flat.keySet()) {
            keyBytes += RamUsageEstimator.sizeOfObject(key);
        }

        // Printed rather than only asserted: the figure is the point, and a reader of a failure wants it.
        logger.info(
            "a {}-column staged stripe presents {} entries costing {} bytes of map skeleton, {} of that in key strings, {} bytes per entry",
            WIDE_COLUMNS,
            flat.size(),
            skeletonBytes,
            keyBytes,
            skeletonBytes / flat.size()
        );

        // A floor, not an equality: the exact figure moves with the JDK's object layout and with the column
        // names. What this guards is that presenting the flat form still costs enough to be worth not keeping.
        assertThat("the flat map's own skeleton must stay far larger than the statistics it presents", skeletonBytes, greaterThan(40_000L));
        assertThat("the key strings must dominate that skeleton", keyBytes, greaterThan(skeletonBytes / 2));
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
