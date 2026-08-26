/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.DeclaredReadSpec;
import org.elasticsearch.xpack.esql.datasources.SchemaProvenance;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The resolved read configuration must split exactly the reads that produce different rows or values, and no others. Splitting too
 * eagerly fragments the cache and costs warmth; splitting too little serves one read's measurement to another.
 */
public class ReadConfigFingerprintTests extends ESTestCase {

    public void testNoOpRedeclarationSharesTheInferredShape() {
        // A dataset that declares a column as exactly what inference already produced reads the file identically, so
        // it must keep sharing the inferred read's cached statistics rather than paying a cold scan for saying so.
        List<Attribute> schema = List.of(attr("user", DataType.KEYWORD), attr("count", DataType.LONG));
        String inferred = ReadConfigFingerprint.of(schema, DeclaredReadSpec.NONE);
        String redeclared = ReadConfigFingerprint.of(schema, spec(Map.of(), Map.of(), SchemaProvenance.INFERRED));
        assertEquals(inferred, redeclared);
    }

    public void testRetypeThatKeepsColumnNamesStillSplits() {
        // The names are identical and only the type moved — but a long read of a column whose values do not all parse
        // drops different rows than a keyword read, so a name-set comparison would miss this entirely.
        List<Attribute> asKeyword = List.of(attr("age", DataType.KEYWORD));
        List<Attribute> asLong = List.of(attr("age", DataType.LONG));
        assertNotEquals(
            ReadConfigFingerprint.of(asKeyword, DeclaredReadSpec.NONE),
            ReadConfigFingerprint.of(asLong, DeclaredReadSpec.NONE)
        );
    }

    public void testUnionByNameWideningSplitsWithNothingDeclared() {
        // The case an options-derived identity cannot see: no declaration anywhere, the reconciled schema simply
        // widens a column, and the file is read at a type it was never read at alone.
        List<Attribute> narrow = List.of(attr("id", DataType.INTEGER));
        List<Attribute> widened = List.of(attr("id", DataType.LONG));
        assertNotEquals(ReadConfigFingerprint.of(narrow, DeclaredReadSpec.NONE), ReadConfigFingerprint.of(widened, DeclaredReadSpec.NONE));
    }

    public void testDeclaredDateFormatSplits() {
        List<Attribute> schema = List.of(attr("ts", DataType.DATETIME));
        String isoDefault = ReadConfigFingerprint.of(schema, DeclaredReadSpec.NONE);
        String withPattern = ReadConfigFingerprint.of(schema, spec(Map.of(), Map.of("ts", "yyyyMMdd"), SchemaProvenance.DECLARED));
        assertNotEquals(isoDefault, withPattern);
    }

    public void testBindingModeSplits() {
        // Same columns, same types: a declared schema binds by name and reports absent columns, an inferred one binds
        // by position. Different reads of the same bytes.
        List<Attribute> schema = List.of(attr("a", DataType.KEYWORD), attr("b", DataType.LONG));
        assertNotEquals(
            ReadConfigFingerprint.of(schema, spec(Map.of(), Map.of(), SchemaProvenance.INFERRED)),
            ReadConfigFingerprint.of(schema, spec(Map.of(), Map.of(), SchemaProvenance.DECLARED))
        );
    }

    public void testColumnOrderSplits() {
        // Under positional binding the order IS the binding: swapping two columns feeds different file columns into
        // different outputs.
        List<Attribute> forward = List.of(attr("a", DataType.KEYWORD), attr("b", DataType.LONG));
        List<Attribute> reversed = List.of(attr("b", DataType.LONG), attr("a", DataType.KEYWORD));
        assertNotEquals(
            ReadConfigFingerprint.of(forward, DeclaredReadSpec.NONE),
            ReadConfigFingerprint.of(reversed, DeclaredReadSpec.NONE)
        );
    }

    public void testRenameIsNormalizedAway() {
        // Harvested statistic keys are physical on every rail, so a pure rename must not split a dataset off from its
        // own harvests: reading file column `user_name` as `user` is the same read of the same bytes.
        String unrenamed = ReadConfigFingerprint.of(List.of(attr("user_name", DataType.KEYWORD)), DeclaredReadSpec.NONE);
        String renamed = ReadConfigFingerprint.of(
            List.of(attr("user", DataType.KEYWORD)),
            spec(Map.of("user", "user_name"), Map.of(), SchemaProvenance.INFERRED)
        );
        assertEquals(unrenamed, renamed);
    }

    public void testOpenVocabularyNamesCannotForgeAFieldBoundary() {
        // Column names reach arbitrary strings through an `_id.path` rename, so they can contain whatever delimiter a
        // naive join would use. Two genuinely different read configurations must not render identically.
        String twoColumns = ReadConfigFingerprint.of(
            List.of(attr("a", DataType.KEYWORD), attr("b", DataType.KEYWORD)),
            DeclaredReadSpec.NONE
        );
        // The name is built to render byte-identically to the two-column encoding under a plain `value:` join —
        // one field per delimiter, including the empty date-format field. Length prefixes are what break the tie.
        String oneColliding = ReadConfigFingerprint.of(List.of(attr("a:keyword::b", DataType.KEYWORD)), DeclaredReadSpec.NONE);
        assertNotEquals(twoColumns, oneColliding);
    }

    public void testUnknownWhenNoSchemaIsAvailable() {
        // A legitimate state (an older node, a source that computes no pin) — but it must never compare equal to a
        // real read configuration, or "we do not know" would silently license sharing.
        assertEquals(ReadConfigFingerprint.UNKNOWN, ReadConfigFingerprint.of(null, DeclaredReadSpec.NONE));
        assertEquals(ReadConfigFingerprint.UNKNOWN, ReadConfigFingerprint.of(List.of(), DeclaredReadSpec.NONE));
        assertNotEquals(
            ReadConfigFingerprint.UNKNOWN,
            ReadConfigFingerprint.of(List.of(attr("a", DataType.KEYWORD)), DeclaredReadSpec.NONE)
        );
    }

    public void testStableAcrossInvocations() {
        // Both sides derive this independently; a value that varied per JVM or per call would match nothing.
        List<Attribute> schema = List.of(attr("a", DataType.KEYWORD), attr("ts", DataType.DATETIME));
        DeclaredReadSpec readSpec = spec(Map.of("a", "a_file"), Map.of("ts", "yyyyMMdd"), SchemaProvenance.DECLARED);
        assertEquals(ReadConfigFingerprint.of(schema, readSpec), ReadConfigFingerprint.of(schema, readSpec));
    }

    private static Attribute attr(String name, DataType type) {
        return new ReferenceAttribute(Source.EMPTY, name, type);
    }

    private static DeclaredReadSpec spec(Map<String, String> renames, Map<String, String> dateFormats, SchemaProvenance provenance) {
        return new DeclaredReadSpec(renames, null, dateFormats, Set.of(), provenance);
    }
}
