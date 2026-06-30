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

import java.util.LinkedHashMap;
import java.util.Map;

public class DeclaredSchemaValidatorTests extends ESTestCase {

    private static DatasetMapping mapping(Dynamic dynamic, Map<String, DatasetFieldMapping> props, String timestamp, String id) {
        return new DatasetMapping(new Mappings(dynamic, props), timestamp, id);
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

    public void testAllDeclarableTypesPass() {
        DeclaredSchemaValidator.validate(
            mapping(
                Dynamic.TRUE,
                props("a", "keyword", "b", "long", "c", "integer", "d", "double", "e", "boolean", "f", "date", "g", "unsigned_long"),
                null,
                null
            )
        );
    }

    public void testTypeAliasesPass() {
        // int/bool/string are accepted aliases (parsed like ::type casts)
        DeclaredSchemaValidator.validate(mapping(Dynamic.TRUE, props("a", "int", "b", "bool", "c", "string"), null, null));
    }

    public void testUnsupportedTypeRejected() {
        for (String bad : new String[] { "ip", "geo_point", "date_nanos", "binary", "short", "float", "not_a_type" }) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> DeclaredSchemaValidator.validate(mapping(Dynamic.TRUE, props("col", bad), null, null))
            );
            assertTrue(e.getMessage(), e.getMessage().contains("unsupported declared type [" + bad + "]"));
            assertTrue(e.getMessage(), e.getMessage().contains("col"));
        }
    }

    public void testTimestampFieldMustBeDateWhenDeclared() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DeclaredSchemaValidator.validate(mapping(Dynamic.TRUE, props("when", "keyword"), "when", null))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("must be a date type"));
    }

    public void testTimestampFieldDeclaredAsDatePasses() {
        DeclaredSchemaValidator.validate(mapping(Dynamic.TRUE, props("when", "date"), "when", null));
    }

    public void testStrictRequiresRoleColumnDeclared() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DeclaredSchemaValidator.validate(mapping(Dynamic.FALSE, props("a", "keyword"), null, "missing_id"))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("id_field"));
        assertTrue(e.getMessage(), e.getMessage().contains("not declared"));
    }

    public void testNonStrictDefersUndeclaredRoleColumn() {
        // id_field references a column not in properties — under non-strict it may come from inference, so PUT allows it.
        DeclaredSchemaValidator.validate(mapping(Dynamic.TRUE, props("a", "keyword"), null, "inferred_id"));
    }

    public void testRoleOnlyNoMappingsIsValid() {
        // timestamp_field with no mappings block at all — orthogonal role designation, deferred to query time.
        DeclaredSchemaValidator.validate(new DatasetMapping(null, "@timestamp", "row_id"));
    }
}
