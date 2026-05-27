/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;
import java.util.Set;

public class ExternalSchemaTests extends ESTestCase {

    public void testNamesCacheMirrorsAttributes() {
        ExternalSchema s = schema("name", "age", "city");
        assertEquals(Set.of("name", "age", "city"), s.names());
        assertEquals(3, s.size());
        assertFalse(s.isEmpty());
        assertEquals("name", s.get(0).name());
        assertEquals("city", s.get(2).name());
    }

    public void testEmptyConstantIsCanonical() {
        assertTrue(ExternalSchema.EMPTY.isEmpty());
        assertEquals(0, ExternalSchema.EMPTY.size());
        assertEquals(Set.of(), ExternalSchema.EMPTY.names());
        assertEquals("two empty schemas are equal", ExternalSchema.EMPTY, new ExternalSchema(List.of()));
    }

    public void testIteratorPreservesOrder() {
        ExternalSchema s = schema("a", "b", "c");
        StringBuilder sb = new StringBuilder();
        for (Attribute attr : s) {
            sb.append(attr.name()).append(',');
        }
        assertEquals("a,b,c,", sb.toString());
    }

    public void testEqualityIsContentBased() {
        // Attribute equality includes the per-instance id, so reuse the same Attribute objects
        // on both sides of the equality — schema equality delegates to the list's equals.
        Attribute name = attr("name");
        Attribute age = attr("age");
        Attribute city = attr("city");
        ExternalSchema s1 = new ExternalSchema(List.of(name, age));
        ExternalSchema s2 = new ExternalSchema(List.of(name, age));
        ExternalSchema s3 = new ExternalSchema(List.of(name, city));
        assertEquals("same content equal", s1, s2);
        assertEquals("same content hash equal", s1.hashCode(), s2.hashCode());
        assertNotEquals("different content not equal", s1, s3);
    }

    public void testConstructorDefensiveCopy() {
        // Mutating the input list after construction must not affect the schema.
        java.util.List<Attribute> mutable = new java.util.ArrayList<>();
        mutable.add(attr("name"));
        mutable.add(attr("age"));
        ExternalSchema s = new ExternalSchema(mutable);
        mutable.add(attr("evil"));
        assertEquals(2, s.size());
        assertFalse(s.names().contains("evil"));
    }

    public void testDataAttributesOfFiltersMetadata() {
        Attribute name = attr("name");
        Attribute age = attr("age");
        Attribute index = new MetadataAttribute(Source.EMPTY, "_index", DataType.KEYWORD, false);
        Attribute id = new MetadataAttribute(Source.EMPTY, "_id", DataType.KEYWORD, false);

        ExternalSchema filtered = ExternalSchema.dataAttributesOf(List.of(name, age, index, id));

        assertEquals("metadata attributes stripped", 2, filtered.size());
        assertEquals("relative order preserved", List.of("name", "age"), filtered.attributes().stream().map(Attribute::name).toList());
        assertFalse(filtered.names().contains("_index"));
        assertFalse(filtered.names().contains("_id"));
    }

    public void testDataAttributesOfEmptyInput() {
        ExternalSchema filtered = ExternalSchema.dataAttributesOf(List.of());
        assertTrue(filtered.isEmpty());
    }

    public void testDataAttributesOfPureData() {
        ExternalSchema filtered = ExternalSchema.dataAttributesOf(List.of(attr("a"), attr("b")));
        assertEquals(2, filtered.size());
        assertEquals(Set.of("a", "b"), filtered.names());
    }

    public void testDataAttributesOfPureMetadata() {
        ExternalSchema filtered = ExternalSchema.dataAttributesOf(
            List.of(
                new MetadataAttribute(Source.EMPTY, "_index", DataType.KEYWORD, false),
                new MetadataAttribute(Source.EMPTY, "_id", DataType.KEYWORD, false)
            )
        );
        assertTrue("all metadata, nothing survives", filtered.isEmpty());
    }

    private static ExternalSchema schema(String... names) {
        return new ExternalSchema(java.util.Arrays.stream(names).<Attribute>map(ExternalSchemaTests::attr).toList());
    }

    private static Attribute attr(String name) {
        return new ReferenceAttribute(Source.EMPTY, null, name, DataType.KEYWORD, Nullability.TRUE, null, false);
    }
}
