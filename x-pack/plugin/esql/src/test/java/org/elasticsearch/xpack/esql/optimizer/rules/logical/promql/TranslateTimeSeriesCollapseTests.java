/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.promql;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesCollapse;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class TranslateTimeSeriesCollapseTests extends ESTestCase {

    /**
     * {@link TimeSeriesCollapse} is only meaningful over {@link org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand};
     * parse rules enforce that for text queries but hand-built plans must do the same.
     */
    public void testRejectsNonPromqlChild() {
        FieldAttribute cluster = new FieldAttribute(
            Source.EMPTY,
            null,
            null,
            "cluster",
            new EsField("cluster", DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.DIMENSION)
        );
        EsRelation relation = EsqlTestUtils.relation(IndexMode.TIME_SERIES).withAttributes(List.of(cluster));
        FieldAttribute value = new FieldAttribute(
            Source.EMPTY,
            null,
            null,
            "v",
            new EsField("v", DataType.DOUBLE, Map.of(), false, EsField.TimeSeriesFieldType.METRIC)
        );
        FieldAttribute step = new FieldAttribute(
            Source.EMPTY,
            null,
            null,
            "s",
            new EsField("s", DataType.DATETIME, Map.of(), false, EsField.TimeSeriesFieldType.METRIC)
        );
        TimeSeriesCollapse collapse = new TimeSeriesCollapse(Source.EMPTY, relation, value, step);

        QlIllegalArgumentException ex = expectThrows(
            QlIllegalArgumentException.class,
            () -> new TranslateTimeSeriesCollapse().apply(collapse, analyzer().buildContext())
        );
        assertThat(ex.getMessage(), containsString("TimeSeriesCollapse requires a PromqlCommand child"));
        assertThat(ex.getMessage(), containsString(EsRelation.class.getName()));
    }

    /**
     * {@link TimeSeriesCollapse#dimensions()} must return every child output column except
     * {@link TimeSeriesCollapse#value()} and {@link TimeSeriesCollapse#step()}, regardless of what
     * the child's output contains. This is a unit test of the derivation logic itself; the full
     * round-trip through analysis and translation is covered by the {@code TS_COLLAPSE} csv-spec
     * tests.
     */
    public void testDimensionsDerivedFromChildOutput() {
        FieldAttribute cluster = new FieldAttribute(
            Source.EMPTY,
            null,
            null,
            "cluster",
            new EsField("cluster", DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.DIMENSION)
        );
        FieldAttribute pod = new FieldAttribute(
            Source.EMPTY,
            null,
            null,
            "pod",
            new EsField("pod", DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.DIMENSION)
        );
        FieldAttribute value = new FieldAttribute(
            Source.EMPTY,
            null,
            null,
            "result",
            new EsField("result", DataType.DOUBLE, Map.of(), false, EsField.TimeSeriesFieldType.METRIC)
        );
        FieldAttribute step = new FieldAttribute(
            Source.EMPTY,
            null,
            null,
            "step",
            new EsField("step", DataType.DATETIME, Map.of(), false, EsField.TimeSeriesFieldType.METRIC)
        );
        EsRelation relation = EsqlTestUtils.relation(IndexMode.TIME_SERIES).withAttributes(List.of(cluster, pod, value, step));
        TimeSeriesCollapse collapse = new TimeSeriesCollapse(Source.EMPTY, relation, value, step);

        // dimensions() = child output minus the value and step columns
        assertThat(collapse.dimensions(), equalTo(List.of(cluster, pod)));
    }
}
