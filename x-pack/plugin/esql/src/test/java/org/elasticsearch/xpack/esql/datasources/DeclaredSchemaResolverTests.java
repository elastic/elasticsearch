/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.cluster.metadata.DatasetMapping;
import org.elasticsearch.cluster.metadata.DatasetMapping.Dynamic;
import org.elasticsearch.cluster.metadata.DatasetMapping.Mappings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DeclaredSchemaResolverTests extends ESTestCase {

    private static DatasetMapping schema(Map<String, DatasetFieldMapping> props) {
        return new DatasetMapping(new Mappings(Dynamic.TRUE, props), null, null);
    }

    public void testDeclaredAttributesTypesNamesAndOrder() {
        Map<String, DatasetFieldMapping> props = new LinkedHashMap<>();
        props.put("when", new DatasetFieldMapping("date", "ts"));
        props.put("status", new DatasetFieldMapping("integer", null));
        props.put("name", new DatasetFieldMapping("string", null)); // alias -> KEYWORD

        List<Attribute> attrs = DeclaredSchemaResolver.declaredAttributes(schema(props));

        assertEquals(List.of("when", "status", "name"), attrs.stream().map(Attribute::name).toList());
        assertEquals(DataType.DATETIME, attrs.get(0).dataType());
        assertEquals(DataType.INTEGER, attrs.get(1).dataType());
        assertEquals(DataType.KEYWORD, attrs.get(2).dataType()); // "string" alias
    }

    public void testDeclaredAttributesUseLogicalNamesNotSource() {
        // The physical source name must never appear as an attribute name.
        Map<String, DatasetFieldMapping> props = new LinkedHashMap<>();
        props.put("customer_id", new DatasetFieldMapping("keyword", "custID"));
        List<Attribute> attrs = DeclaredSchemaResolver.declaredAttributes(schema(props));
        assertEquals(List.of("customer_id"), attrs.stream().map(Attribute::name).toList());
    }

    public void testPhysicalAttributesUseSourceNamePairedWithLogical() {
        Map<String, DatasetFieldMapping> props = new LinkedHashMap<>();
        props.put("when", new DatasetFieldMapping("date", "ts"));        // renamed
        props.put("status", new DatasetFieldMapping("integer", null));   // not renamed

        List<Attribute> logical = DeclaredSchemaResolver.declaredAttributes(schema(props));
        List<Attribute> physical = DeclaredSchemaResolver.physicalAttributes(schema(props));

        // same arity, same types, paired position-for-position
        assertEquals(logical.size(), physical.size());
        assertEquals(List.of("when", "status"), logical.stream().map(Attribute::name).toList());
        assertEquals(List.of("ts", "status"), physical.stream().map(Attribute::name).toList());
        assertEquals(logical.get(0).dataType(), physical.get(0).dataType());
        assertEquals(DataType.DATETIME, physical.get(0).dataType());
    }

    public void testRenameMapOnlyRenamedColumns() {
        Map<String, DatasetFieldMapping> props = new LinkedHashMap<>();
        props.put("when", new DatasetFieldMapping("date", "ts"));
        props.put("status", new DatasetFieldMapping("integer", null));
        props.put("customer_id", new DatasetFieldMapping("keyword", "custID"));

        Map<String, String> renames = DeclaredSchemaResolver.renameMap(schema(props));
        assertEquals(Map.of("when", "ts", "customer_id", "custID"), renames);
    }

    public void testNoMappingsYieldsEmpty() {
        DatasetMapping roleOnly = new DatasetMapping(null, "@timestamp", "row_id");
        assertTrue(DeclaredSchemaResolver.declaredAttributes(roleOnly).isEmpty());
        assertTrue(DeclaredSchemaResolver.renameMap(roleOnly).isEmpty());
        assertTrue(DeclaredSchemaResolver.declaredAttributes(null).isEmpty());
        assertTrue(DeclaredSchemaResolver.renameMap(null).isEmpty());
    }

    public void testUnsupportedTypeThrowsDefensively() {
        Map<String, DatasetFieldMapping> props = new LinkedHashMap<>();
        props.put("c", new DatasetFieldMapping("not_a_type", null));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DeclaredSchemaResolver.declaredAttributes(schema(props))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("not_a_type"));
    }
}
