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
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalClientException;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class DeclaredSchemaResolverTests extends ESTestCase {

    private static DatasetMapping mapping(Map<String, DatasetFieldMapping> props) {
        return new DatasetMapping(new Mappings(Dynamic.TRUE, props));
    }

    private static ReferenceAttribute attr(String name, DataType type) {
        return new ReferenceAttribute(Source.EMPTY, null, name, type);
    }

    public void testDeclaredWidthOverCapIsRefused() {
        Map<String, DatasetFieldMapping> props = new LinkedHashMap<>();
        props.put("a", new DatasetFieldMapping("long", null));
        props.put("b", new DatasetFieldMapping("long", null));
        props.put("c", new DatasetFieldMapping("long", null));

        DeclaredSchemaResolver.checkDeclaredWidth(mapping(props), 3);
        DeclaredSchemaResolver.checkDeclaredWidth(null, 1);
        ExternalClientException e = expectThrows(
            ExternalClientException.class,
            () -> DeclaredSchemaResolver.checkDeclaredWidth(mapping(props), 2)
        );
        assertThat(e.getMessage(), containsString("declares [3] columns"));
        assertThat(e.getMessage(), containsString("raise [esql.external.schema_max_fields]"));
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
    }

    /** At the ceiling raising the cap is refused too, so the refusal does not suggest it. */
    public void testDeclaredWidthOverCeilingDoesNotSuggestRaisingTheCap() {
        int ceiling = ExternalSourceSettings.MAX_SCHEMA_MAX_FIELDS;
        Map<String, DatasetFieldMapping> props = new LinkedHashMap<>();
        for (int i = 0; i <= ceiling; i++) {
            props.put("c" + i, new DatasetFieldMapping("long", null));
        }
        ExternalClientException e = expectThrows(
            ExternalClientException.class,
            () -> DeclaredSchemaResolver.checkDeclaredWidth(mapping(props), ceiling)
        );
        assertThat(e.getMessage(), containsString("declare fewer columns"));
        assertThat(e.getMessage(), not(containsString("raise")));
    }

    public void testOverlayNonStrictRenamesAndRetypesDeclaredColumnsOnly() {
        List<Attribute> inferred = List.of(
            attr("emp_no", DataType.INTEGER),
            attr("first_name", DataType.KEYWORD),
            attr("dept", DataType.KEYWORD)
        );
        Map<String, DatasetFieldMapping> props = new LinkedHashMap<>();
        props.put("id", new DatasetFieldMapping("long", "emp_no"));     // rename emp_no -> id, retype to long
        props.put("name", new DatasetFieldMapping("keyword", "first_name")); // rename first_name -> name
        // dept is undeclared and must pass through unchanged

        DeclaredSchemaResolver.Overlaid o = DeclaredSchemaResolver.overlayNonStrict(inferred, mapping(props));

        // user-facing output: logical names, declared types for declared columns, dept untouched
        assertEquals(List.of("id", "name", "dept"), o.output().stream().map(Attribute::name).toList());
        assertEquals(DataType.LONG, o.output().get(0).dataType()); // declared long beats inferred integer
        assertEquals(DataType.KEYWORD, o.output().get(1).dataType());
        assertEquals(DataType.KEYWORD, o.output().get(2).dataType());
        // per-file schema is now LOGICAL too: the operator stays in logical names and the physical column is resolved
        // at the reader (rename map for by-name readers, positional for text). Declared columns retyped; dept untouched.
        assertEquals(List.of("id", "name", "dept"), o.fileSchema().stream().map(Attribute::name).toList());
        assertEquals(DataType.LONG, o.fileSchema().get(0).dataType());
    }

    public void testOverlayRejectsRenameCollidingWithInferredColumn() {
        // File has both x and y; declaring logical `y` with source `x` would emit two `y` columns (renamed-from-x plus
        // the pass-through inferred y). Reject against the unified schema (non-lenient).
        List<Attribute> inferred = List.of(attr("x", DataType.KEYWORD), attr("y", DataType.KEYWORD));
        Map<String, DatasetFieldMapping> props = new LinkedHashMap<>();
        props.put("y", new DatasetFieldMapping("keyword", "x"));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DeclaredSchemaResolver.overlayNonStrict(inferred, mapping(props))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("duplicate column [y]"));
    }

    public void testOverlayNonStrictErrorsOnDeclaredColumnMissingFromSource() {
        List<Attribute> inferred = List.of(attr("a", DataType.KEYWORD));
        Map<String, DatasetFieldMapping> props = new LinkedHashMap<>();
        props.put("b", new DatasetFieldMapping("long", null)); // 'b' is not in the inferred source
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DeclaredSchemaResolver.overlayNonStrict(inferred, mapping(props))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("b"));
    }

    public void testOverlayNonStrictNoMappingsPassesThrough() {
        List<Attribute> inferred = List.of(attr("a", DataType.KEYWORD));
        DeclaredSchemaResolver.Overlaid o = DeclaredSchemaResolver.overlayNonStrict(
            inferred,
            new DatasetMapping(new Mappings(Dynamic.TRUE, Map.of(), "row_id"))
        );
        assertSame(inferred, o.output());
        assertSame(inferred, o.fileSchema());
    }

    public void testDeclaredAttributesTypesNamesAndOrder() {
        Map<String, DatasetFieldMapping> props = new LinkedHashMap<>();
        props.put("when", new DatasetFieldMapping("date", "ts"));
        props.put("status", new DatasetFieldMapping("integer", null));
        props.put("name", new DatasetFieldMapping("string", null)); // alias -> KEYWORD

        List<Attribute> attrs = DeclaredSchemaResolver.declaredAttributes(mapping(props));

        assertEquals(List.of("when", "status", "name"), attrs.stream().map(Attribute::name).toList());
        assertEquals(DataType.DATETIME, attrs.get(0).dataType());
        assertEquals(DataType.INTEGER, attrs.get(1).dataType());
        assertEquals(DataType.KEYWORD, attrs.get(2).dataType()); // "string" alias
    }

    public void testDeclaredAttributesUseLogicalNamesNotSource() {
        // The physical source name must never appear as an attribute name.
        Map<String, DatasetFieldMapping> props = new LinkedHashMap<>();
        props.put("customer_id", new DatasetFieldMapping("keyword", "custID"));
        List<Attribute> attrs = DeclaredSchemaResolver.declaredAttributes(mapping(props));
        assertEquals(List.of("customer_id"), attrs.stream().map(Attribute::name).toList());
    }

    public void testRenameMapOnlyRenamedColumns() {
        Map<String, DatasetFieldMapping> props = new LinkedHashMap<>();
        props.put("when", new DatasetFieldMapping("date", "ts"));
        props.put("status", new DatasetFieldMapping("integer", null));
        props.put("customer_id", new DatasetFieldMapping("keyword", "custID"));

        Map<String, String> renames = DeclaredSchemaResolver.renameMap(mapping(props));
        assertEquals(Map.of("when", "ts", "customer_id", "custID"), renames);
    }

    public void testNoMappingsYieldsEmpty() {
        DatasetMapping roleOnly = new DatasetMapping(new Mappings(Dynamic.TRUE, Map.of(), "row_id"));
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
            () -> DeclaredSchemaResolver.declaredAttributes(mapping(props))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("not_a_type"));
    }

    public void testMoveConsumesPhysicalInPlace() {
        // @timestamp with source ts: ts is consumed and @timestamp takes its position (shipped move order preserved).
        List<Attribute> inferred = List.of(attr("ts", DataType.DATETIME), attr("other", DataType.KEYWORD));
        Map<String, DatasetFieldMapping> props = new LinkedHashMap<>();
        props.put("@timestamp", new DatasetFieldMapping("date", "ts"));
        DatasetMapping m = mapping(props);
        List<String> names = DeclaredSchemaResolver.overlayNonStrict(inferred, m).output().stream().map(Attribute::name).toList();
        assertEquals(List.of("@timestamp", "other"), names);
        assertEquals(Map.of("@timestamp", "ts"), DeclaredSchemaResolver.renameMap(m));
    }
}
