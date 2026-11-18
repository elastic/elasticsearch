/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.datasources.SplitStats;
import org.elasticsearch.xpack.esql.datasources.spi.StripeStatsSink;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThan;

/**
 * What a staged record stops retaining: the map the flat form has to allocate to present itself. A read stages
 * one record per stripe and holds it until the query ends.
 * <p>
 * Counted in entries and key characters, not bytes. A byte figure here would be {@code RamUsageEstimator}'s model
 * of a map rather than the map: the same entries in a capacity-16 and a capacity-8192 {@code HashMap} measure
 * identically, so the table is invisible to it, and it charges two bytes a character for keys the JVM stores as
 * compact Latin-1. The end-to-end figure belongs to a heap measurement on a cluster.
 * <p>
 * What is counted here is exact, and it is what scales: every statistic of every column owns a key that repeats
 * the column's name, where the compact form holds that name once.
 */
public class StagedStripeRecordFootprintTests extends ESTestCase {

    private static final long MTIME = 1_700_000_000_000L;
    private static final int WIDE_COLUMNS = 105;
    private static final int ADDRESSING_KEYS = 13;
    private static final int KEYS_PER_COLUMN = 4;

    public void testTheFlatFormIsWorthNotRetaining() {
        StagedStripeRecord record = wideRecord();
        Map<String, Object> flat = new HashMap<>(record);
        assertFalse("the copy must be a walk, leaving the record compact", record.keepsFlatForm());

        assertEquals(
            "a wide stripe must present four entries per column plus the addressing keys",
            KEYS_PER_COLUMN * WIDE_COLUMNS + ADDRESSING_KEYS,
            flat.size()
        );

        long nameCharsOnce = 0;
        long nameCharsRepeated = 0;
        for (int i = 0; i < WIDE_COLUMNS; i++) {
            String column = "column_" + i;
            String prefix = SourceStatisticsSerializer.STATS_COL_PREFIX + column + ".";
            long keys = flat.keySet().stream().filter(key -> key.startsWith(prefix)).count();
            assertEquals("every column must present its two counts and its two extrema", KEYS_PER_COLUMN, keys);
            nameCharsOnce += column.length();
            nameCharsRepeated += keys * column.length();
        }

        long keyChars = 0;
        long columnKeyChars = 0;
        for (String key : flat.keySet()) {
            keyChars += key.length();
            if (key.startsWith(SourceStatisticsSerializer.STATS_COL_PREFIX)) {
                columnKeyChars += key.length();
            }
        }

        logger.info(
            "a {}-column staged stripe presents {} entries whose keys run to {} characters, {} of them column names "
                + "the compact form holds once each in {}",
            WIDE_COLUMNS,
            flat.size(),
            keyChars,
            nameCharsRepeated,
            nameCharsOnce
        );

        assertEquals("the flat form repeats every column name once per statistic", KEYS_PER_COLUMN * nameCharsOnce, nameCharsRepeated);
        assertThat("the per-column keys must dwarf the addressing keys", columnKeyChars, greaterThan(10 * (keyChars - columnKeyChars)));
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
