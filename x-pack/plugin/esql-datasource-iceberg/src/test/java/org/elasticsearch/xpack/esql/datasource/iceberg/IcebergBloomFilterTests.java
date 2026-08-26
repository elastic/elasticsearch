/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.iceberg;

import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;

import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.type.EsField.TimeSeriesFieldType;

/**
 * Tests that verify Iceberg's bloom filter support infrastructure is available
 * and that the ESQL → Iceberg filter translation produces bloom-filter-compatible
 * expressions.
 * <p>
 * Iceberg's Parquet reading path includes {@code ParquetBloomRowGroupFilter}
 * which automatically checks bloom filters during row-group selection when a scan
 * has a filter expression. This works through the existing filter pushdown path
 * ({@link IcebergPushdownFilters} → {@code TableScan.filter()}) — no ESQL-side
 * code changes are needed.
 * <p>
 * Requirements for bloom filter usage in Iceberg:
 * <ol>
 *   <li>Parquet data files must be written with bloom filters enabled
 *       (table property: {@code write.parquet.bloom-filter-enabled.column.<col>=true})</li>
 *   <li>Scan must have a filter expression set via {@code scan.filter()}</li>
 *   <li>Filter must be an equality or IN predicate (bloom filters don't help with ranges)</li>
 * </ol>
 */
public class IcebergBloomFilterTests extends ESTestCase {

    /**
     * Verify that ParquetBloomRowGroupFilter is available on the classpath.
     * This class is part of iceberg-parquet and is responsible for checking
     * bloom filters during row-group selection.
     */
    public void testParquetBloomRowGroupFilterAvailable() throws ClassNotFoundException {
        Class<?> bloomFilterClass = Class.forName("org.apache.iceberg.parquet.ParquetBloomRowGroupFilter");
        assertNotNull("ParquetBloomRowGroupFilter should be available", bloomFilterClass);
    }

    /**
     * Verify that equality expressions produce the correct Iceberg expression type
     * that ParquetBloomRowGroupFilter can evaluate.
     */
    public void testEqualityExpressionForBloomFilter() {
        Expression eq = Expressions.equal("user_id", "abc123");
        assertNotNull(eq);
        assertEquals(Expression.Operation.EQ, eq.op());
    }

    /**
     * Verify that IN expressions produce the correct Iceberg expression type
     * that ParquetBloomRowGroupFilter can evaluate.
     */
    public void testInExpressionForBloomFilter() {
        Expression in = Expressions.in("user_id", "abc", "def", "ghi");
        assertNotNull(in);
        assertEquals(Expression.Operation.IN, in.op());
    }

    /**
     * Verify the ESQL → Iceberg filter translation produces bloom-filter-compatible
     * expression (EQ) for equality predicates on keyword columns.
     */
    public void testEsqlEqualsTranslatesToBloomFilterCompatibleExpression() {
        FieldAttribute field = createField("user_id", DataType.KEYWORD);
        Literal value = new Literal(Source.EMPTY, new BytesRef("abc123"), DataType.KEYWORD);
        Equals equals = new Equals(Source.EMPTY, field, value);

        Expression icebergExpr = IcebergPushdownFilters.convert(equals);
        assertNotNull("Equals should translate to Iceberg expression", icebergExpr);
        assertEquals(Expression.Operation.EQ, icebergExpr.op());
    }

    /**
     * Verify the ESQL → Iceberg filter translation produces bloom-filter-compatible
     * expression (IN) for IN predicates.
     */
    public void testEsqlInTranslatesToBloomFilterCompatibleExpression() {
        FieldAttribute field = createField("dept", DataType.KEYWORD);
        In inExpr = new In(
            Source.EMPTY,
            field,
            List.of(
                new Literal(Source.EMPTY, new BytesRef("eng"), DataType.KEYWORD),
                new Literal(Source.EMPTY, new BytesRef("sales"), DataType.KEYWORD)
            )
        );

        Expression icebergExpr = IcebergPushdownFilters.convert(inExpr);
        assertNotNull("IN should translate to Iceberg expression", icebergExpr);
        assertEquals(Expression.Operation.IN, icebergExpr.op());
    }

    /**
     * Verify the ESQL → Iceberg filter translation works for integer equality
     * (common for bloom-filter-optimized columns like IDs).
     */
    public void testEsqlIntegerEqualsTranslatesToBloomFilterCompatible() {
        FieldAttribute field = createField("id", DataType.INTEGER);
        Literal value = new Literal(Source.EMPTY, 42, DataType.INTEGER);
        Equals equals = new Equals(Source.EMPTY, field, value);

        Expression icebergExpr = IcebergPushdownFilters.convert(equals);
        assertNotNull("Integer equals should translate to Iceberg expression", icebergExpr);
        assertEquals(Expression.Operation.EQ, icebergExpr.op());
    }

    private FieldAttribute createField(String name, DataType dataType) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, dataType, Collections.emptyMap(), true, TimeSeriesFieldType.NONE));
    }
}
