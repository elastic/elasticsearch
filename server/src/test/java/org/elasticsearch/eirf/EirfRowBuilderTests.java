/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eirf;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

public class EirfRowBuilderTests extends ESTestCase {

    public void testSimpleScalarDocuments() {
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            builder.setString("name", "alice");
            builder.setInt("age", 30);
            builder.endDocument();

            builder.startDocument();
            builder.setString("name", "bob");
            builder.setInt("age", 25);
            builder.endDocument();

            EirfBatch batch = builder.build();
            assertEquals(2, batch.docCount());
            assertEquals(2, batch.columnCount());

            EirfRowReader row0 = batch.getRowReader(0);
            assertEquals("alice", row0.getStringValue(0).string());
            assertEquals(30, row0.getIntValue(1));

            EirfRowReader row1 = batch.getRowReader(1);
            assertEquals("bob", row1.getStringValue(0).string());
            assertEquals(25, row1.getIntValue(1));

            batch.close();
        }
    }

    public void testNestedObjectFields() {
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            builder.setString("user.name", "alice");
            builder.setInt("user.age", 30);
            builder.endDocument();

            EirfBatch batch = builder.build();
            EirfSchema schema = batch.schema();
            assertEquals("user.name", schema.getFullPath(0));
            assertEquals("user.age", schema.getFullPath(1));

            batch.close();
        }
    }

    public void testIntAndLong() {
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            builder.setInt("small", 42);
            builder.setLong("big", Long.MAX_VALUE);
            builder.endDocument();

            EirfBatch batch = builder.build();
            EirfRowReader row0 = batch.getRowReader(0);
            assertEquals(EirfType.INT, row0.getTypeByte(0));
            assertEquals(42, row0.getIntValue(0));
            assertEquals(EirfType.LONG, row0.getTypeByte(1));
            assertEquals(Long.MAX_VALUE, row0.getLongValue(1));

            batch.close();
        }
    }

    public void testFloatAndDouble() {
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            builder.setFloat("f", 1.5f);
            builder.setDouble("d", 1.23456789012345);
            builder.endDocument();

            EirfBatch batch = builder.build();
            EirfRowReader row0 = batch.getRowReader(0);
            assertEquals(EirfType.FLOAT, row0.getTypeByte(0));
            assertEquals(1.5f, row0.getFloatValue(0), 0.0f);
            assertEquals(EirfType.DOUBLE, row0.getTypeByte(1));
            assertEquals(1.23456789012345, row0.getDoubleValue(1), 0.0);

            batch.close();
        }
    }

    public void testBooleanValues() {
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            builder.setBoolean("active", true);
            builder.setBoolean("deleted", false);
            builder.endDocument();

            EirfBatch batch = builder.build();
            EirfRowReader row0 = batch.getRowReader(0);
            assertTrue(row0.getBooleanValue(0));
            assertFalse(row0.getBooleanValue(1));

            batch.close();
        }
    }

    public void testNullValues() {
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            builder.setNull("field");
            builder.endDocument();

            EirfBatch batch = builder.build();
            assertTrue(batch.getRowReader(0).isNull(0));

            batch.close();
        }
    }

    public void testKeyValueColumn() {
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            // KEY_VALUE with one entry: key_length(i32 LE)=1, key="x", type=INT, value=42 (LE)
            byte[] kvBytes = new byte[] {
                1,
                0,
                0,
                0,  // key_length = 1 (i32 LE)
                'x',         // key bytes
                EirfType.INT,
                42,
                0,
                0,
                0  // INT value = 42 (LE)
            };
            builder.startDocument();
            builder.setKeyValue("data", kvBytes);
            builder.endDocument();

            EirfBatch batch = builder.build();
            EirfRowReader row0 = batch.getRowReader(0);
            assertEquals(EirfType.KEY_VALUE, row0.getTypeByte(0));
            EirfKeyValueReader kv = row0.getKeyValue(0);
            assertTrue(kv.next());
            assertEquals("x", kv.key());
            assertEquals(EirfType.INT, kv.type());
            assertEquals(42, kv.intValue());
            assertFalse(kv.next());

            batch.close();
        }
    }

    public void testSchemaEvolution() {
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            builder.setString("name", "alice");
            builder.setInt("age", 30);
            builder.endDocument();

            builder.startDocument();
            builder.setString("name", "bob");
            builder.setString("email", "bob@test.com");
            builder.endDocument();

            EirfBatch batch = builder.build();
            assertEquals(3, batch.columnCount());

            EirfRowReader row0 = batch.getRowReader(0);
            assertTrue(row0.isNull(2));

            EirfRowReader row1 = batch.getRowReader(1);
            assertTrue(row1.isNull(1));
            assertEquals("bob@test.com", row1.getStringValue(2).string());

            batch.close();
        }
    }

    public void testEquivalenceWithEncoder() throws IOException {
        List<BytesReference> sources = List.of(
            new BytesArray("{\"name\":\"alice\",\"age\":30}"),
            new BytesArray("{\"name\":\"bob\",\"age\":25}")
        );
        EirfBatch encoderBatch = EirfEncoder.encode(sources, XContentType.JSON);

        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            builder.setString("name", "alice");
            builder.setInt("age", 30);
            builder.endDocument();

            builder.startDocument();
            builder.setString("name", "bob");
            builder.setInt("age", 25);
            builder.endDocument();

            EirfBatch builderBatch = builder.build();

            assertEquals(encoderBatch.docCount(), builderBatch.docCount());
            assertEquals(encoderBatch.columnCount(), builderBatch.columnCount());

            for (int doc = 0; doc < encoderBatch.docCount(); doc++) {
                EirfRowReader er = encoderBatch.getRowReader(doc);
                EirfRowReader br = builderBatch.getRowReader(doc);
                for (int col = 0; col < er.columnCount(); col++) {
                    assertEquals(er.getTypeByte(col), br.getTypeByte(col));
                }
            }

            builderBatch.close();
        }
        encoderBatch.close();
    }

    public void testStartDocumentWithoutEnd() {
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            expectThrows(IllegalStateException.class, builder::startDocument);
        }
    }

    public void testEndDocumentWithoutStart() {
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            expectThrows(IllegalStateException.class, builder::endDocument);
        }
    }

    public void testBuildWhileInDocument() {
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            expectThrows(IllegalStateException.class, builder::build);
        }
    }

    public void testSetValueOutsideDocument() {
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            expectThrows(IllegalStateException.class, () -> builder.setString("name", "test"));
        }
    }

    public void testDuplicateColumnRejected() {
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            builder.setInt("a", 1);
            expectThrows(IllegalStateException.class, () -> builder.setInt("a", 2));
        }
    }

    public void testDuplicateColumnAtRejected() {
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            builder.setIntAt(0, 1);
            expectThrows(IllegalStateException.class, () -> builder.setStringAt(0, "overwrite"));
        }
    }

    public void testDuplicateNullColumnRejected() {
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            builder.setNull("a");
            expectThrows(IllegalStateException.class, () -> builder.setInt("a", 1));
        }
    }

    public void testSameColumnAcrossDocumentsAllowed() {
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            builder.setInt("a", 1);
            builder.endDocument();

            builder.startDocument();
            builder.setInt("a", 2);
            builder.endDocument();

            EirfBatch batch = builder.build();
            assertEquals(2, batch.docCount());
            assertEquals(1, batch.getRowReader(0).getIntValue(0));
            assertEquals(2, batch.getRowReader(1).getIntValue(0));
            batch.close();
        }
    }

    public void testManyColumns() {
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            for (int i = 0; i < 20; i++) {
                builder.setInt("col" + i, i);
            }
            builder.endDocument();

            EirfBatch batch = builder.build();
            assertEquals(20, batch.columnCount());
            EirfRowReader row0 = batch.getRowReader(0);
            for (int i = 0; i < 20; i++) {
                assertEquals(i, row0.getIntValue(i));
            }

            batch.close();
        }
    }
}
