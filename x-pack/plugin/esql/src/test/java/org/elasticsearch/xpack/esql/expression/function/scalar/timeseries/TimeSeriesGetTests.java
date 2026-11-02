/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;

/** Exercises label binding through ordinary scalar expressions while preserving named inputs for aggregation analysis. */
public class TimeSeriesGetTests extends ESTestCase {
    private static final List<DataType> LABEL_TYPES = List.of(
        DataType.KEYWORD,
        DataType.INTEGER,
        DataType.LONG,
        DataType.UNSIGNED_LONG,
        DataType.DOUBLE,
        DataType.BOOLEAN,
        DataType.IP
    );

    /** Named labels and missing named fields remain visible to the analyzer without a wrapping expression. */
    public void testNamedLabelsAndTypedNullsAreUnchanged() {
        for (DataType type : LABEL_TYPES) {
            Expression column = new ReferenceAttribute(Source.EMPTY, "label", type);
            assertSame(column, TimeSeriesGet.create(Source.EMPTY, column, "label", type, TEST_CFG));
            Expression missing = new Literal(Source.EMPTY, null, type);
            assertSame(missing, TimeSeriesGet.create(Source.EMPTY, missing, "label", type, TEST_CFG));
        }
        var error = expectThrows(
            IllegalArgumentException.class,
            () -> TimeSeriesGet.create(Source.EMPTY, Literal.keyword(Source.EMPTY, "value"), "label", DataType.LONG, TEST_CFG)
        );
        assertEquals("Conflicting types for label [label]", error.getMessage());
    }

    /** Metadata binding must produce executable scalar expressions without requiring a surrogate rewrite. */
    public void testMetadataReadsUseScalarConversions() {
        var metadata = new ReferenceAttribute(Source.EMPTY, "metadata", DataType.TIMESERIES_DATA_TYPE);
        for (DataType type : LABEL_TYPES) {
            Expression value = TimeSeriesGet.create(Source.EMPTY, metadata, "label", type, TEST_CFG);
            assertEquals(type, value.dataType());
            assertTrue(value.resolved());
            var get = asInstanceOf(TimeSeriesGet.class, type == DataType.KEYWORD ? value : value.children().getFirst());
            assertSame(metadata, get.metadata());
            assertEquals(Literal.keyword(Source.EMPTY, "label"), get.children().get(1));
        }
    }

    /** Native label conversion and missing-label behavior apply equally during constant folding. */
    public void testFoldReadsAndConvertsLabels() {
        var metadata = new Literal(Source.EMPTY, new BytesRef("{\"sequence\":\"42\"}"), DataType.TIMESERIES_DATA_TYPE);
        Expression sequence = TimeSeriesGet.create(Source.EMPTY, metadata, "sequence", DataType.LONG, TEST_CFG);
        assertTrue(sequence.foldable());
        assertEquals(42L, sequence.fold(FoldContext.small()));
        assertNull(TimeSeriesGet.create(Source.EMPTY, metadata, "missing", DataType.KEYWORD, TEST_CFG).fold(FoldContext.small()));
        assertNull(TimeSeriesGet.create(Source.EMPTY, metadata, "missing", DataType.LONG, TEST_CFG).fold(FoldContext.small()));
        var empty = new Literal(Source.EMPTY, null, DataType.TIMESERIES_DATA_TYPE);
        assertNull(TimeSeriesGet.create(Source.EMPTY, empty, "sequence", DataType.LONG, TEST_CFG).fold(FoldContext.small()));
    }

    /** Reading and converting a multivalued dimension must retain all values. */
    public void testFoldPreservesMultipleValues() {
        var metadata = new Literal(Source.EMPTY, new BytesRef("{\"shard\":[\"2\",\"10\"]}"), DataType.TIMESERIES_DATA_TYPE);
        assertEquals(
            List.of(new BytesRef("2"), new BytesRef("10")),
            TimeSeriesGet.create(Source.EMPTY, metadata, "shard", DataType.KEYWORD, TEST_CFG).fold(FoldContext.small())
        );
        assertEquals(
            List.of(2L, 10L),
            TimeSeriesGet.create(Source.EMPTY, metadata, "shard", DataType.LONG, TEST_CFG).fold(FoldContext.small())
        );
    }

    /** Rewriting an input or label must preserve the ordinary conversion around the metadata read. */
    public void testRewritesPreserveLabelAndType() {
        var first = new ReferenceAttribute(Source.EMPTY, "first", DataType.TIMESERIES_DATA_TYPE);
        var second = new ReferenceAttribute(Source.EMPTY, "second", DataType.TIMESERIES_DATA_TYPE);
        Expression original = TimeSeriesGet.create(Source.EMPTY, first, "label", DataType.LONG, TEST_CFG);
        Expression replaced = original.transformUp(ReferenceAttribute.class, attribute -> second);
        Expression expected = TimeSeriesGet.create(Source.EMPTY, second, "label", DataType.LONG, TEST_CFG);
        assertEquals(expected, replaced);
        assertEquals(expected.hashCode(), replaced.hashCode());
        assertNotEquals(original, replaced);
        assertEquals(
            TimeSeriesGet.create(Source.EMPTY, first, "other", DataType.LONG, TEST_CFG),
            original.transformUp(Literal.class, label -> Literal.keyword(label.source(), "other"))
        );
        assertNotEquals(replaced, TimeSeriesGet.create(Source.EMPTY, second, "label", DataType.KEYWORD, TEST_CFG));
        Expression invalid = original.transformUp(
            Expression.class,
            expression -> expression == first ? Literal.keyword(Source.EMPTY, "invalid") : expression
        );
        assertFalse(invalid.resolved());
    }

    /** Invalid label names and unsupported native types fail when binding rather than during query execution. */
    public void testMetadataReadsValidateLabelsAndTypes() {
        var metadata = new ReferenceAttribute(Source.EMPTY, "metadata", DataType.TIMESERIES_DATA_TYPE);
        expectThrows(IllegalArgumentException.class, () -> TimeSeriesGet.create(Source.EMPTY, metadata, "", DataType.KEYWORD, TEST_CFG));
        expectThrows(IllegalArgumentException.class, () -> TimeSeriesGet.create(Source.EMPTY, metadata, null, DataType.KEYWORD, TEST_CFG));
        expectThrows(
            IllegalArgumentException.class,
            () -> TimeSeriesGet.create(Source.EMPTY, metadata, "label", DataType.TIMESERIES_DATA_TYPE, TEST_CFG)
        );
    }
}
