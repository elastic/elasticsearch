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
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.LinkedHashMap;
import java.util.Map;

public class DeclaredSchemaValidatorTests extends ESTestCase {

    private static DatasetMapping mapping(Dynamic dynamic, Map<String, DatasetFieldMapping> props, String id) {
        return new DatasetMapping(new Mappings(dynamic, props), id);
    }

    private static Map<String, DatasetFieldMapping> props(Object... pairs) {
        Map<String, DatasetFieldMapping> m = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            m.put((String) pairs[i], new DatasetFieldMapping((String) pairs[i + 1], null));
        }
        return m;
    }

    public void testNullSchemaIsValid() {
        DeclaredSchemaValidator.validate(null); // no throw
    }

    public void testDuplicatePhysicalSourceRejected() {
        // Two columns resolving to the same physical name (via source, or a source colliding with another logical name)
        // breaks the 1:1 rename the read path assumes — must be rejected at PUT.
        Map<String, DatasetFieldMapping> sameSource = new LinkedHashMap<>();
        sameSource.put("a", new DatasetFieldMapping("keyword", "x"));
        sameSource.put("b", new DatasetFieldMapping("long", "x"));
        IllegalArgumentException e1 = expectThrows(
            IllegalArgumentException.class,
            () -> DeclaredSchemaValidator.validate(new DatasetMapping(new Mappings(Dynamic.FALSE, sameSource), null))
        );
        assertTrue(e1.getMessage(), e1.getMessage().contains("physical column [x]"));

        // source of one column collides with another column's (un-renamed) logical name
        Map<String, DatasetFieldMapping> sourceHitsLogical = new LinkedHashMap<>();
        sourceHitsLogical.put("a", new DatasetFieldMapping("keyword", "b"));
        sourceHitsLogical.put("b", new DatasetFieldMapping("long", null));
        IllegalArgumentException e2 = expectThrows(
            IllegalArgumentException.class,
            () -> DeclaredSchemaValidator.validate(new DatasetMapping(new Mappings(Dynamic.FALSE, sourceHitsLogical), null))
        );
        assertTrue(e2.getMessage(), e2.getMessage().contains("physical column [b]"));

        // distinct sources are fine
        Map<String, DatasetFieldMapping> distinct = new LinkedHashMap<>();
        distinct.put("a", new DatasetFieldMapping("keyword", "x"));
        distinct.put("b", new DatasetFieldMapping("long", "y"));
        DeclaredSchemaValidator.validate(new DatasetMapping(new Mappings(Dynamic.FALSE, distinct), null)); // no throw
    }

    public void testSourceRenameAcceptedAtPut() {
        // `source` rename is shape-valid at PUT — the read path honors it (logical file schema + a rename map for the
        // by-name readers). Only the type vocabulary is checked here.
        Map<String, DatasetFieldMapping> withRename = new LinkedHashMap<>();
        withRename.put("id", new DatasetFieldMapping("long", "emp_no"));
        DeclaredSchemaValidator.validate(new DatasetMapping(new Mappings(Dynamic.TRUE, withRename), null)); // no throw
    }

    /**
     * Pin the declarable-type vocabulary to the ES|QL type registry: every type we allow must round-trip through
     * its canonical ES type name, so our supported types cannot drift from the core type names (a rename or removal
     * of one we depend on breaks this test rather than silently diverging).
     */
    public void testDeclarableTypesStayInSyncWithTypeRegistry() {
        for (DataType t : DeclaredSchemaValidator.DECLARABLE_TYPES) {
            assertEquals("declarable type [" + t + "] must resolve by its canonical ES type name", t, DataType.fromNameOrAlias(t.esType()));
        }
    }

    public void testAllDeclarableTypesPass() {
        DeclaredSchemaValidator.validate(
            mapping(
                Dynamic.TRUE,
                props("a", "keyword", "b", "long", "c", "integer", "d", "double", "e", "boolean", "f", "date", "g", "unsigned_long"),
                null
            )
        );
    }

    public void testTypeAliasesPass() {
        // int/bool/string are accepted aliases (parsed like ::type casts)
        DeclaredSchemaValidator.validate(mapping(Dynamic.TRUE, props("a", "int", "b", "bool", "c", "string"), null));
    }

    public void testUnsupportedTypeRejected() {
        for (String bad : new String[] { "ip", "geo_point", "date_nanos", "binary", "short", "float", "not_a_type" }) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> DeclaredSchemaValidator.validate(mapping(Dynamic.TRUE, props("col", bad), null))
            );
            assertTrue(e.getMessage(), e.getMessage().contains("unsupported declared type [" + bad + "]"));
            assertTrue(e.getMessage(), e.getMessage().contains("col"));
        }
    }

    public void testStrictRequiresRoleColumnDeclared() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DeclaredSchemaValidator.validate(mapping(Dynamic.FALSE, props("a", "keyword"), "missing_id"))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("id_field"));
        assertTrue(e.getMessage(), e.getMessage().contains("not declared"));
    }

    public void testNonStrictDefersUndeclaredRoleColumn() {
        // id_field references a column not in properties — under non-strict it may come from inference, so PUT allows it.
        DeclaredSchemaValidator.validate(mapping(Dynamic.TRUE, props("a", "keyword"), "inferred_id"));
    }

    public void testRoleOnlyNoMappingsIsValid() {
        // id_field with no mappings block at all — orthogonal role designation, deferred to query time.
        DeclaredSchemaValidator.validate(new DatasetMapping(null, "row_id"));
    }
}
