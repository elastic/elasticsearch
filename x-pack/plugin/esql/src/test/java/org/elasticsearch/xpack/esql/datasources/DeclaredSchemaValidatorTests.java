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

    public void testSourceRenameAcceptedAtPut() {
        // `source` rename is shape-valid at PUT — the read path honors it (logical file schema + a rename map for the
        // by-name readers). Only the type vocabulary is checked here.
        Map<String, DatasetFieldMapping> withRename = new LinkedHashMap<>();
        withRename.put("id", new DatasetFieldMapping("long", "emp_no"));
        DeclaredSchemaValidator.validate(new DatasetMapping(new Mappings(Dynamic.TRUE, withRename), null, null)); // no throw
    }

    public void testRenameAllowedForWiredFormatsRejectedForColumnar() {
        Map<String, DatasetFieldMapping> withRename = new LinkedHashMap<>();
        withRename.put("id", new DatasetFieldMapping("long", "emp_no"));
        DatasetMapping mapping = new DatasetMapping(new Mappings(Dynamic.TRUE, withRename), null, null);

        for (String wired : new String[] { "csv", "tsv", "ndjson", "NDJSON" }) {
            DeclaredSchemaValidator.validateRenameFormat(mapping, wired); // no throw
        }
        for (String columnar : new String[] { "parquet", "orc", null }) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> DeclaredSchemaValidator.validateRenameFormat(mapping, columnar)
            );
            assertTrue(e.getMessage(), e.getMessage().contains("not yet supported for format"));
        }

        // No rename declared -> the format gate is a no-op even for columnar formats.
        Map<String, DatasetFieldMapping> noRename = new LinkedHashMap<>();
        noRename.put("id", new DatasetFieldMapping("long", null));
        DeclaredSchemaValidator.validateRenameFormat(new DatasetMapping(new Mappings(Dynamic.TRUE, noRename), null, null), "parquet");
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
