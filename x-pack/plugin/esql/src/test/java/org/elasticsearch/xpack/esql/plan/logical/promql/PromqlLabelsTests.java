/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

/** How PromQL finds a label among the fields of its source and a node's declared output, and what its tags label leaves out. */
public class PromqlLabelsTests extends ESTestCase {

    private final FieldAttribute pod = field("pod", EsField.TimeSeriesFieldType.DIMENSION);
    private final FieldAttribute event = field("event", EsField.TimeSeriesFieldType.NONE);
    private final EsRelation relation = EsqlTestUtils.relation(IndexMode.TIME_SERIES).withAttributes(List.of(pod, event));

    /** Any field of a series is one of its labels, but only a dimension identifies the series. */
    public void testFieldIsALabelDimensionOrNot() {
        assertThat(PromqlLabels.getField(relation, "event"), sameInstance(event));
        assertThat(PromqlLabels.getLabel(relation, "event"), nullValue());
        assertThat(PromqlLabels.getLabel(relation, "pod"), sameInstance(pod));
        assertThat(PromqlLabels.getField(relation, "missing"), nullValue());
    }

    /** The tags label stands for every label a node does not declare by name. */
    public void testTagsLabelAmongDeclaredLabels() {
        UnresolvedPromqlLabelAttribute tags = UnresolvedPromqlLabelAttribute.newTagsLabel(Source.EMPTY);
        UnresolvedPromqlLabelAttribute cluster = UnresolvedPromqlLabelAttribute.label(Source.EMPTY, "cluster");
        List<Attribute> declared = List.of(cluster, tags);
        assertThat(PromqlLabels.getTagsLabel(declared), sameInstance(tags));
        assertTrue(PromqlLabels.isTagsLabel(tags));
        assertFalse(PromqlLabels.isTagsLabel(cluster));
        assertThat(PromqlLabels.getTagsLabel(List.of(cluster)), nullValue());
    }

    private static FieldAttribute field(String name, EsField.TimeSeriesFieldType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, DataType.KEYWORD, Map.of(), true, type));
    }
}
