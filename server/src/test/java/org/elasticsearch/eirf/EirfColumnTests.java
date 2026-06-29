/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eirf;

import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.sourcebatch.SourceColumn;
import org.elasticsearch.sourcebatch.SourceRow;
import org.elasticsearch.test.ESTestCase;

public class EirfColumnTests extends ESTestCase {

    /**
     * Builds a small batch with known scalar values and verifies that {@link SourceColumn}
     * returns the same values as row-by-row access via {@link SourceRow}.
     */
    public void testColumnViewMatchesRowView() {
        try (EirfBatch batch = buildMixedBatch()) {
            int docCount = batch.docCount();
            int colCount = batch.columnCount();
            assertTrue("batch must have at least one row", docCount > 0);
            assertTrue("batch must have at least one column", colCount > 0);

            for (int c = 0; c < colCount; c++) {
                SourceColumn col = batch.column(c);
                assertEquals(c, col.columnIndex());
                assertEquals(docCount, col.docCount());

                for (int d = 0; d < docCount; d++) {
                    SourceRow row = batch.row(d);
                    assertEquals("type mismatch at col=" + c + " doc=" + d, row.getTypeByte(c), col.getTypeByte(d));
                    assertEquals("isAbsent mismatch at col=" + c + " doc=" + d, row.isAbsent(c), col.isAbsent(d));
                    if (row.isAbsent(c) == false && row.isNull(c) == false) {
                        byte type = row.getTypeByte(c);
                        switch (type) {
                            case EirfType.INT -> assertEquals(
                                "int mismatch at col=" + c + " doc=" + d,
                                row.getIntValue(c),
                                col.getIntValue(d)
                            );
                            case EirfType.LONG -> assertEquals(
                                "long mismatch at col=" + c + " doc=" + d,
                                row.getLongValue(c),
                                col.getLongValue(d)
                            );
                            case EirfType.FLOAT -> assertEquals(
                                "float mismatch at col=" + c + " doc=" + d,
                                row.getFloatValue(c),
                                col.getFloatValue(d),
                                0f
                            );
                            case EirfType.DOUBLE -> assertEquals(
                                "double mismatch at col=" + c + " doc=" + d,
                                row.getDoubleValue(c),
                                col.getDoubleValue(d),
                                0.0
                            );
                            case EirfType.STRING -> assertEquals(
                                "string mismatch at col=" + c + " doc=" + d,
                                row.getStringValue(c).string(),
                                col.getStringValue(d).string()
                            );
                            case EirfType.TRUE, EirfType.FALSE -> assertEquals(
                                "boolean mismatch at col=" + c + " doc=" + d,
                                row.getBooleanValue(c),
                                col.getBooleanValue(d)
                            );
                        }
                    }
                }
            }
        }
    }

    public void testColumnBoundsCheck() {
        try (EirfBatch batch = buildMixedBatch()) {
            expectThrows(IndexOutOfBoundsException.class, () -> batch.column(-1));
            expectThrows(IndexOutOfBoundsException.class, () -> batch.column(batch.columnCount()));
        }
    }

    public void testColumnOnSlice() {
        try (EirfBatch parent = buildMixedBatch()) {
            SourceBatch sliced = parent.slice(1, parent.docCount());
            int colCount = sliced.columnCount();
            for (int c = 0; c < colCount; c++) {
                SourceColumn col = sliced.column(c);
                assertEquals(c, col.columnIndex());
                assertEquals(sliced.docCount(), col.docCount());
                for (int d = 0; d < sliced.docCount(); d++) {
                    assertEquals("type mismatch at col=" + c + " doc=" + d, sliced.row(d).getTypeByte(c), col.getTypeByte(d));
                }
            }
        }
    }

    /**
     * Builds a batch with heterogeneous types: int, long, float, double, boolean, and string columns,
     * across three rows (with one absent value to test the absent path).
     */
    private static EirfBatch buildMixedBatch() {
        EirfRowBuilder builder = new EirfRowBuilder();
        // Row 0: all columns set
        builder.startDocument();
        builder.setInt("count", 42);
        builder.setLong("ts", 1_000_000L);
        builder.setFloat("score", 3.14f);
        builder.setDouble("ratio", 0.999);
        builder.setBoolean("active", true);
        builder.setString("label", "alpha");
        builder.endDocument();

        // Row 1: some columns absent
        builder.startDocument();
        builder.setInt("count", 7);
        builder.setLong("ts", 2_000_000L);
        // score absent in this row
        builder.setDouble("ratio", 1.5);
        builder.setBoolean("active", false);
        builder.setString("label", "beta");
        builder.endDocument();

        // Row 2: another combination
        builder.startDocument();
        builder.setInt("count", 0);
        builder.setLong("ts", 3_000_000L);
        builder.setFloat("score", 0.0f);
        builder.setDouble("ratio", 2.0);
        builder.setBoolean("active", true);
        builder.setString("label", "gamma");
        builder.endDocument();

        return builder.build();
    }
}
