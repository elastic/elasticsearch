/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.UnionEsField;
import org.elasticsearch.xpack.esql.core.type.UnsupportedEsField;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * What a column declares as its {@code original_types}, which two kinds of column fill for different reasons: an
 * unsupported one to say what disagreed, a union one to say what its legs are. Only the union list is read by
 * position, so these pin the two apart rather than treating the field as one thing.
 */
public class ColumnInfoImplForAttributeTests extends ESTestCase {

    public void testAUnionColumnDeclaresItsLegTypesInLegOrder() {
        FieldAttribute attribute = new FieldAttribute(
            Source.EMPTY,
            null,
            null,
            "duration",
            new UnionEsField("duration", List.of(DataType.KEYWORD, DataType.LONG))
        );
        ColumnInfoImpl column = ColumnInfoImpl.forAttribute(attribute, null);
        assertThat(column.name(), equalTo("duration"));
        assertThat(column.type(), equalTo(DataType.UNION));
        assertThat(column.originalTypes(), equalTo(List.of("keyword", "long")));
    }

    /**
     * The leg order is the field's, not this method's. The canonical order a type conflict produces is sorted by type
     * name, so only a field built out of that order can tell a pass-through from a sort - which is the whole of what
     * this asserts, since a sort here would silently disagree with the block the data node built from the same list.
     */
    public void testALegOrderIsReportedRatherThanImposed() {
        FieldAttribute attribute = new FieldAttribute(
            Source.EMPTY,
            null,
            null,
            "duration",
            new UnionEsField("duration", List.of(DataType.LONG, DataType.KEYWORD))
        );
        assertThat(ColumnInfoImpl.forAttribute(attribute, null).originalTypes(), equalTo(List.of("long", "keyword")));
    }

    public void testAnUnsupportedColumnDeclaresWhatDisagreedSorted() {
        UnsupportedEsField field = new UnsupportedEsField("duration", List.of("long", "keyword", "float"));
        UnsupportedAttribute attribute = new UnsupportedAttribute(Source.EMPTY, "duration", field);
        assertThat(ColumnInfoImpl.forAttribute(attribute, null).originalTypes(), equalTo(List.of("float", "keyword", "long")));
    }

    public void testAnOrdinaryColumnDeclaresNone() {
        FieldAttribute field = new FieldAttribute(
            Source.EMPTY,
            null,
            null,
            "duration",
            new EsField("duration", DataType.LONG, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        assertThat(ColumnInfoImpl.forAttribute(field, null).originalTypes(), nullValue());
        ReferenceAttribute reference = new ReferenceAttribute(Source.EMPTY, "x", DataType.LONG);
        assertThat(ColumnInfoImpl.forAttribute(reference, null).originalTypes(), nullValue());
    }

    public void testTheColumnCarriesTheMetadataItIsGiven() {
        ReferenceAttribute reference = new ReferenceAttribute(Source.EMPTY, "x", DataType.LONG);
        Map<String, Object> meta = Map.of("unit", "millis");
        assertThat(ColumnInfoImpl.forAttribute(reference, meta).meta(), equalTo(meta));
        assertThat(ColumnInfoImpl.forAttribute(reference, null).meta(), nullValue());
    }
}
