/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.FormatNameResolver;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;

import java.util.Map;
import java.util.Set;

/**
 * Verifies that {@link PushFiltersToSource#resolveFormatName} delegates to
 * {@link FormatNameResolver#resolve}. Comprehensive resolution tests live in
 * {@link org.elasticsearch.xpack.esql.datasources.FormatNameResolverTests}.
 */
public class PushFiltersToSourceTests extends ESTestCase {

    public void testResolveFormatNameDelegatesToFormatNameResolver() {
        assertEquals(
            FormatNameResolver.resolve(Map.of("reader", "java"), "file.parquet"),
            PushFiltersToSource.resolveFormatName(Map.of("reader", "java"), "file.parquet")
        );
    }

    public void testResolveFormatNameFromExtension() {
        assertEquals("orc", PushFiltersToSource.resolveFormatName(null, "s3://bucket/data/file.orc"));
    }

    // -- referencesAnyColumn: partition/data conjunct split --

    public void testReferencesAnyColumnReturnsTrueForPartitionColumn() {
        Expression expr = new Equals(SRC, fieldAttr("lang"), intLiteral(3));
        assertTrue(PushFiltersToSource.referencesAnyColumn(expr, Set.of("lang")));
    }

    public void testReferencesAnyColumnReturnsFalseForDataColumn() {
        Expression expr = new Equals(SRC, fieldAttr("salary"), intLiteral(100));
        assertFalse(PushFiltersToSource.referencesAnyColumn(expr, Set.of("lang")));
    }

    public void testReferencesAnyColumnReturnsTrueForCompoundExpressionWithPartitionColumn() {
        // A conjunct spanning both a partition column and a data column must be kept in FilterExec,
        // not pushed to the format reader (which has no partition column in its payload).
        Expression mixed = new And(
            SRC,
            new Equals(SRC, fieldAttr("lang"), intLiteral(3)),
            new Equals(SRC, fieldAttr("salary"), intLiteral(100))
        );
        assertTrue(PushFiltersToSource.referencesAnyColumn(mixed, Set.of("lang")));
    }

    public void testReferencesAnyColumnReturnsFalseForEmptyColumnSet() {
        Expression expr = new Equals(SRC, fieldAttr("lang"), intLiteral(3));
        assertFalse(PushFiltersToSource.referencesAnyColumn(expr, Set.of()));
    }

    public void testReferencesAnyColumnReturnsFalseForLiteralWithNoReferences() {
        Expression lit = new Literal(SRC, 3, DataType.INTEGER);
        assertFalse(PushFiltersToSource.referencesAnyColumn(lit, Set.of("lang")));
    }

    private static final Source SRC = Source.EMPTY;

    private static FieldAttribute fieldAttr(String name) {
        return new FieldAttribute(SRC, name, new EsField(name, DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE));
    }

    private static Literal intLiteral(int value) {
        return new Literal(SRC, value, DataType.INTEGER);
    }
}
