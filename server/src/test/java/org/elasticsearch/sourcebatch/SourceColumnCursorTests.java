/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sourcebatch;

import org.elasticsearch.eirf.EirfBatch;
import org.elasticsearch.eirf.EirfRowBuilder;
import org.elasticsearch.eirf.EirfType;
import org.elasticsearch.test.ESTestCase;

/**
 * Tests {@link SourceColumnCursor} over the row-major {@link org.elasticsearch.eirf.EirfColumn}, which uses
 * the generic {@link RandomAccessSourceColumnCursor}. A cursor must visit every document in order, report
 * {@link EirfType#ABSENT} for absent documents, and expose the value matching {@link SourceColumnCursor#type()}.
 * The specialized column-major cursors are exercised separately once a column-major format exists.
 */
public class SourceColumnCursorTests extends ESTestCase {

    private static SourceColumn columnByName(SourceBatch batch, String name) {
        for (int i = 0; i < batch.columnCount(); i++) {
            if (batch.schema().getFullPath(i).equals(name)) {
                return batch.column(i);
            }
        }
        throw new AssertionError("no column named [" + name + "]");
    }

    public void testLongColumnCursor() {
        // "ts" present in every row; "score" absent in row 1.
        EirfRowBuilder builder = new EirfRowBuilder();
        builder.startDocument();
        builder.setLong("ts", 10L);
        builder.endDocument();
        builder.startDocument();
        builder.setLong("ts", 20L);
        builder.endDocument();
        builder.startDocument();
        builder.setLong("ts", 30L);
        builder.endDocument();

        try (EirfBatch batch = builder.build()) {
            SourceColumnCursor cursor = columnByName(batch, "ts").cursor();
            assertTrue(cursor.advance());
            assertEquals(EirfType.LONG, cursor.type());
            assertEquals(10L, cursor.longValue());
            assertTrue(cursor.advance());
            assertEquals(EirfType.LONG, cursor.type());
            assertEquals(20L, cursor.longValue());
            assertTrue(cursor.advance());
            assertEquals(EirfType.LONG, cursor.type());
            assertEquals(30L, cursor.longValue());
            assertFalse(cursor.advance());
        }
    }

    public void testDoubleColumnCursor() {
        EirfRowBuilder builder = new EirfRowBuilder();
        builder.startDocument();
        builder.setDouble("v", 1.5);
        builder.endDocument();
        builder.startDocument();
        builder.setDouble("v", -3.25);
        builder.endDocument();

        try (EirfBatch batch = builder.build()) {
            SourceColumnCursor cursor = columnByName(batch, "v").cursor();
            assertTrue(cursor.advance());
            assertEquals(EirfType.DOUBLE, cursor.type());
            assertEquals(1.5, cursor.doubleValue(), 0.0);
            assertTrue(cursor.advance());
            assertEquals(EirfType.DOUBLE, cursor.type());
            assertEquals(-3.25, cursor.doubleValue(), 0.0);
            assertFalse(cursor.advance());
        }
    }

    public void testStringColumnCursor() {
        EirfRowBuilder builder = new EirfRowBuilder();
        builder.startDocument();
        builder.setString("v", "alpha");
        builder.endDocument();
        builder.startDocument();
        builder.setString("v", "gamma");
        builder.endDocument();

        try (EirfBatch batch = builder.build()) {
            SourceColumnCursor cursor = columnByName(batch, "v").cursor();
            assertTrue(cursor.advance());
            assertEquals(EirfType.STRING, cursor.type());
            assertEquals("alpha", cursor.stringValue().string());
            assertTrue(cursor.advance());
            assertEquals(EirfType.STRING, cursor.type());
            assertEquals("gamma", cursor.stringValue().string());
            assertFalse(cursor.advance());
        }
    }

    public void testCursorReportsAbsentDocuments() {
        // The "score" float is present in row 0 and absent in row 1; the cursor must report ABSENT for row 1.
        EirfRowBuilder builder = new EirfRowBuilder();
        builder.startDocument();
        builder.setLong("ts", 1_000_000L);
        builder.setFloat("score", 3.14f);
        builder.endDocument();
        builder.startDocument();
        builder.setLong("ts", 2_000_000L);
        // score absent
        builder.endDocument();

        try (EirfBatch batch = builder.build()) {
            SourceColumnCursor ts = columnByName(batch, "ts").cursor();
            assertTrue(ts.advance());
            assertEquals(EirfType.LONG, ts.type());
            assertEquals(1_000_000L, ts.longValue());
            assertTrue(ts.advance());
            assertEquals(EirfType.LONG, ts.type());
            assertEquals(2_000_000L, ts.longValue());
            assertFalse(ts.advance());

            SourceColumnCursor score = columnByName(batch, "score").cursor();
            assertTrue(score.advance());
            assertEquals(EirfType.FLOAT, score.type());
            assertTrue(score.advance());
            assertEquals(EirfType.ABSENT, score.type());
            assertFalse(score.advance());
        }
    }
}
