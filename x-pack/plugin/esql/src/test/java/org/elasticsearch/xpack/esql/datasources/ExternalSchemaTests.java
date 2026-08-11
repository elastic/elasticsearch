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

    public void testDataAttributesOfExcludesPartitionColumns() {
        // Hive partition columns are appended after the data columns by the resolver. The data-only
        // view must drop them so its width matches the file-backed ColumnMapping.
        Attribute id = attr("id");
        Attribute value = attr("value");
        Attribute year = attr("year");

        ExternalSchema filtered = ExternalSchema.dataAttributesOf(List.of(id, value, year), Set.of("year"));

        assertEquals("partition column dropped", 2, filtered.size());
        assertEquals(List.of("id", "value"), filtered.attributes().stream().map(Attribute::name).toList());
        assertFalse(filtered.names().contains("year"));
    }

    public void testDataAttributesOfExcludesVirtualAndPartitionColumns() {
        // Both classes of non-data column must be stripped together: virtual (metadata) and partition.
        Attribute id = attr("id");
        Attribute index = new MetadataAttribute(Source.EMPTY, "_index", DataType.KEYWORD, false);
        Attribute region = attr("region");
        Attribute value = attr("value");

        ExternalSchema filtered = ExternalSchema.dataAttributesOf(List.of(id, index, region, value), Set.of("region"));

        assertEquals("virtual and partition columns dropped", 2, filtered.size());
        assertEquals("relative order preserved", List.of("id", "value"), filtered.attributes().stream().map(Attribute::name).toList());
        assertFalse(filtered.names().contains("_index"));
        assertFalse(filtered.names().contains("region"));
    }

    public void testDataAttributesOfShadowedCollisionExcludesPartition() {
        // Collision shape: a partition key 'year' shadows a same-named physical column. On the data
        // node the resolver already dropped the physical 'year' and appended the partition 'year' at
        // the tail, so the data-only view must exclude it to agree with the file-backed mapping width.
        Attribute id = attr("id");
        Attribute value = attr("value");
        Attribute yearPartition = attr("year");

        ExternalSchema filtered = ExternalSchema.dataAttributesOf(List.of(id, value, yearPartition), Set.of("year"));

        assertEquals("colliding partition column excluded", 2, filtered.size());
        assertEquals(List.of("id", "value"), filtered.attributes().stream().map(Attribute::name).toList());
    }

    public void testDataAttributesOfSingleArgEqualsEmptyPartitionSet() {
        List<Attribute> attributes = List.of(attr("a"), attr("b"));
        assertEquals(
            "single-arg overload is the empty-partition-set case",
            ExternalSchema.dataAttributesOf(attributes),
            ExternalSchema.dataAttributesOf(attributes, Set.of())
        );
    }

    public void testDataAttributesOfEmptyPartitionSetKeepsAllData() {
        // A partition-column set that does not match any attribute name leaves the data columns intact.
        List<Attribute> attributes = List.of(attr("a"), attr("b"));
        ExternalSchema filtered = ExternalSchema.dataAttributesOf(attributes, Set.of("does_not_exist"));
        assertEquals(2, filtered.size());
        assertEquals(Set.of("a", "b"), filtered.names());
    }

    private static ExternalSchema schema(String... names) {
        return new ExternalSchema(java.util.Arrays.stream(names).<Attribute>map(ExternalSchemaTests::attr).toList());
    }

    private static Attribute attr(String name) {
        return new ReferenceAttribute(Source.EMPTY, null, name, DataType.KEYWORD, Nullability.TRUE, null, false);
    }
}
