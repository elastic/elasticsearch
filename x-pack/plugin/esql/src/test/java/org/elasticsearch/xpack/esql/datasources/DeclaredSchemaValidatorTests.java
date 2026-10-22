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

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class DeclaredSchemaValidatorTests extends ESTestCase {

    private static DatasetMapping mapping(Dynamic dynamic, Map<String, DatasetFieldMapping> props) {
        return new DatasetMapping(new Mappings(dynamic, props));
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

    public void testFormatOnDateColumnAcceptedAtPut() {
        // A date-parse pattern on a `date` column is shape-valid at PUT; the pattern is validated with the same ES
        // DateFormatter the readers use.
        Map<String, DatasetFieldMapping> props = new LinkedHashMap<>();
        props.put("ts", DatasetFieldMapping.withFormat("date", null, "dd/MMM/yyyy:HH:mm:ss Z"));
        DeclaredSchemaValidator.validate(new DatasetMapping(new Mappings(Dynamic.TRUE, props))); // no throw
    }

    public void testFormatOnDateNanosColumnAcceptedAtPut() {
        // `format` is accepted on date_nanos exactly as on date: the declared pattern is the string-parse dialect.
        // (The numeric-epoch read stays format-free — there the declared type itself names the unit.)
        Map<String, DatasetFieldMapping> props = new LinkedHashMap<>();
        props.put("ts", DatasetFieldMapping.withFormat("date_nanos", null, "yyyy-MM-dd HH:mm:ss.SSSSSSSSS"));
        DeclaredSchemaValidator.validate(new DatasetMapping(new Mappings(Dynamic.TRUE, props))); // no throw
    }

    public void testInvalidFormatPatternOnDateNanosRejectedAtPut() {
        Map<String, DatasetFieldMapping> props = new LinkedHashMap<>();
        props.put("ts", DatasetFieldMapping.withFormat("date_nanos", null, "not-a-valid-pattern-{{{"));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DeclaredSchemaValidator.validate(new DatasetMapping(new Mappings(Dynamic.TRUE, props)))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("invalid [format]"));
    }

    public void testFormatOnNonDateColumnRejected() {
        // `format` is a date-parse pattern, so it is only meaningful on a date column.
        Map<String, DatasetFieldMapping> props = new LinkedHashMap<>();
        props.put("name", DatasetFieldMapping.withFormat("keyword", null, "yyyy-MM-dd"));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DeclaredSchemaValidator.validate(new DatasetMapping(new Mappings(Dynamic.TRUE, props)))
        );
        assertTrue(
            e.getMessage(),
            e.getMessage().contains("[format] on column [name] is only supported on [date] and [date_nanos] columns")
        );
    }

    public void testInvalidFormatPatternRejectedAtPut() {
        // A bad pattern must fail the PUT, not the first query.
        Map<String, DatasetFieldMapping> props = new LinkedHashMap<>();
        props.put("ts", DatasetFieldMapping.withFormat("date", null, "not-a-valid-pattern-{{{"));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DeclaredSchemaValidator.validate(new DatasetMapping(new Mappings(Dynamic.TRUE, props)))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("invalid [format]"));
    }

    public void testTwoSourcesOntoOnePhysicalRejected() {
        // Two columns reading one physical break the 1:1 read-path rename, so the shared source is rejected.
        Map<String, DatasetFieldMapping> sameSource = new LinkedHashMap<>();
        sameSource.put("a", new DatasetFieldMapping("keyword", "x"));
        sameSource.put("b", new DatasetFieldMapping("keyword", "x"));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DeclaredSchemaValidator.validate(new DatasetMapping(new Mappings(Dynamic.FALSE, sameSource)))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("physical column [x]"));
    }

    public void testSourceRenameAcceptedAtPut() {
        // `source` rename is shape-valid at PUT — the read path honors it (logical file schema + a rename map for the
        // by-name readers). Only the type vocabulary is checked here.
        Map<String, DatasetFieldMapping> withRename = new LinkedHashMap<>();
        withRename.put("id", new DatasetFieldMapping("long", "emp_no"));
        DeclaredSchemaValidator.validate(new DatasetMapping(new Mappings(Dynamic.TRUE, withRename))); // no throw
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
                props(
                    "a",
                    "keyword",
                    "b",
                    "long",
                    "c",
                    "integer",
                    "d",
                    "double",
                    "e",
                    "boolean",
                    "f",
                    "date",
                    "g",
                    "unsigned_long",
                    "h",
                    "ip",
                    "i",
                    "date_nanos"
                )
            )
        );
    }

    public void testTypeAliasesPass() {
        // int/bool/string are accepted aliases (parsed like ::type casts)
        DeclaredSchemaValidator.validate(mapping(Dynamic.TRUE, props("a", "int", "b", "bool", "c", "string")));
    }

    public void testUnsupportedTypeRejected() {
        for (String bad : new String[] { "geo_point", "binary", "short", "float", "version", "not_a_type", "text" }) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> DeclaredSchemaValidator.validate(mapping(Dynamic.TRUE, props("col", bad)))
            );
            assertTrue(e.getMessage(), e.getMessage().contains("unsupported declared type [" + bad + "]"));
            assertTrue(e.getMessage(), e.getMessage().contains("col"));
        }
    }

    /**
     * {@code text} is rejected like any undeclarable type, and the message additionally names the replacement,
     * which no other rejected type does. Separate from {@link #testUnsupportedTypeRejected} because that method's
     * assertions are shared across its whole array.
     */
    public void testDeclaredTextIsRejectedAndNamesTheReplacement() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DeclaredSchemaValidator.validate(mapping(Dynamic.TRUE, props("msg", "text")))
        );
        assertThat(
            e.getMessage(),
            allOf(
                containsString("unsupported declared type [text] for column [msg]"),
                // The literal, not TEXT_ROUTE: asserting the constant moves both sides together.
                containsString("apply TO_TEXT in the query")
            )
        );
        // The whole list, not the absence of `text` from it: the names are sorted, so a "does not contain"
        // assertion lands mid-list and is one edit away from matching nothing and passing for free.
        assertThat(
            e.getMessage(),
            containsString("supported types are [boolean, date_nanos, datetime, double, integer, ip, keyword, long, unsigned_long]")
        );
    }

    /**
     * Nine declarable types. A count as well as {@link #testAllDeclarableTypesPass}'s enumeration, so widening the
     * PUT-time vocabulary takes a deliberate edit here.
     */
    public void testDeclarableTypeCount() {
        assertThat(DeclaredSchemaValidator.declarableTypes(), hasSize(9));
        assertThat(DeclaredSchemaValidator.declarableTypes(), not(hasItem(DataType.TEXT)));
    }

    public void testStrictWithNoPropertiesRejected() {
        // dynamic:false means "the declaration IS the schema" — no properties, no schema; the zero-column relation
        // downstream is not a queryable thing, so PUT rejects the shape outright.
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DeclaredSchemaValidator.validate(new DatasetMapping(new Mappings(Dynamic.FALSE, Map.of())))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("at least one declared column"));
    }

    public void testBlankNamesRejected() {
        // Index-mapping precedent: field names must be non-empty. A blank property key or path rejects.
        Map<String, DatasetFieldMapping> blankKey = new LinkedHashMap<>();
        blankKey.put(" ", new DatasetFieldMapping("keyword", null));
        expectThrows(
            IllegalArgumentException.class,
            () -> DeclaredSchemaValidator.validate(new DatasetMapping(new Mappings(Dynamic.TRUE, blankKey)))
        );

        Map<String, DatasetFieldMapping> blankPath = new LinkedHashMap<>();
        blankPath.put("a", new DatasetFieldMapping("keyword", ""));
        expectThrows(
            IllegalArgumentException.class,
            () -> DeclaredSchemaValidator.validate(new DatasetMapping(new Mappings(Dynamic.TRUE, blankPath)))
        );
    }
}
