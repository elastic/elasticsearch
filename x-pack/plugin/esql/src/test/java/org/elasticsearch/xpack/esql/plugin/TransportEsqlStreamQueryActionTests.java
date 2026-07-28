/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.UnsupportedEsField;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class TransportEsqlStreamQueryActionTests extends ESTestCase {

    public void testBuildColumnsRegularFieldAttribute() {
        FieldAttribute attr = new FieldAttribute(
            Source.EMPTY,
            "myField",
            new EsField("myField", DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
        );
        List<ColumnInfoImpl> columns = TransportEsqlStreamQueryAction.buildColumns(List.of(attr));
        assertEquals(1, columns.size());
        assertEquals("myField", columns.get(0).name());
        assertEquals(DataType.KEYWORD, columns.get(0).type());
        assertNull(columns.get(0).originalTypes());
    }

    public void testBuildColumnsUnsupportedAttributeHasSortedOriginalTypes() {
        UnsupportedAttribute attr = new UnsupportedAttribute(
            Source.EMPTY,
            "badField",
            new UnsupportedEsField("badField", List.of("geo_shape", "dense_vector"))
        );
        List<ColumnInfoImpl> columns = TransportEsqlStreamQueryAction.buildColumns(List.of(attr));
        assertEquals(1, columns.size());
        List<String> originalTypes = columns.get(0).originalTypes();
        assertNotNull(originalTypes);
        assertEquals(List.of("dense_vector", "geo_shape"), originalTypes);
    }

    public void testBuildColumnsReferenceAttribute() {
        ReferenceAttribute attr = new ReferenceAttribute(Source.EMPTY, "derived", DataType.LONG);
        List<ColumnInfoImpl> columns = TransportEsqlStreamQueryAction.buildColumns(List.of(attr));
        assertEquals(1, columns.size());
        assertEquals("derived", columns.get(0).name());
        assertEquals(DataType.LONG, columns.get(0).type());
        assertNull(columns.get(0).originalTypes());
    }

    public void testBuildColumnsMixedAttributes() {
        FieldAttribute field = new FieldAttribute(
            Source.EMPTY,
            "f",
            new EsField("f", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
        );
        UnsupportedAttribute unsupported = new UnsupportedAttribute(Source.EMPTY, "u", new UnsupportedEsField("u", List.of("object")));
        ReferenceAttribute ref = new ReferenceAttribute(Source.EMPTY, "r", DataType.DOUBLE);
        List<ColumnInfoImpl> columns = TransportEsqlStreamQueryAction.buildColumns(List.of(field, unsupported, ref));
        assertEquals(3, columns.size());
        assertEquals("f", columns.get(0).name());
        assertNull(columns.get(0).originalTypes());
        assertEquals("u", columns.get(1).name());
        assertNotNull(columns.get(1).originalTypes());
        assertEquals("r", columns.get(2).name());
        assertNull(columns.get(2).originalTypes());
    }

    public void testCollectIndexFieldNamesFieldAttributeIncluded() {
        FieldAttribute attr = new FieldAttribute(
            Source.EMPTY,
            "myField",
            new EsField("myField", DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
        );
        Set<String> names = TransportEsqlStreamQueryAction.collectIndexFieldNames(List.of(attr));
        assertEquals(Set.of("myField"), names);
    }

    public void testCollectIndexFieldNamesUnsupportedAttributeExcluded() {
        UnsupportedAttribute attr = new UnsupportedAttribute(
            Source.EMPTY,
            "badField",
            new UnsupportedEsField("badField", List.of("geo_shape"))
        );
        Set<String> names = TransportEsqlStreamQueryAction.collectIndexFieldNames(List.of(attr));
        assertTrue(names.isEmpty());
    }

    public void testCollectIndexFieldNamesReferenceAttributeExcluded() {
        ReferenceAttribute attr = new ReferenceAttribute(Source.EMPTY, "derived", DataType.LONG);
        Set<String> names = TransportEsqlStreamQueryAction.collectIndexFieldNames(List.of(attr));
        assertTrue(names.isEmpty());
    }

    public void testCollectIndexFieldNamesMetadataAttributeExcluded() {
        MetadataAttribute attr = new MetadataAttribute(Source.EMPTY, "_index", DataType.KEYWORD, false);
        Set<String> names = TransportEsqlStreamQueryAction.collectIndexFieldNames(List.of(attr));
        assertTrue(names.isEmpty());
    }

    public void testCollectIndexFieldNamesMixedList() {
        FieldAttribute field = new FieldAttribute(
            Source.EMPTY,
            "kept",
            new EsField("kept", DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
        );
        UnsupportedAttribute unsupported = new UnsupportedAttribute(
            Source.EMPTY,
            "dropped",
            new UnsupportedEsField("dropped", List.of("geo_shape"))
        );
        ReferenceAttribute ref = new ReferenceAttribute(Source.EMPTY, "alsoDropped", DataType.LONG);
        Set<String> names = TransportEsqlStreamQueryAction.collectIndexFieldNames(List.of(field, unsupported, ref));
        assertEquals(Set.of("kept"), names);
    }

    public void testCollectIndexPatternsEsQueryExec() {
        EsQueryExec plan = new EsQueryExec(Source.EMPTY, "logs-*", IndexMode.STANDARD, List.of(), null, List.of(), null, List.of());
        Set<String> patterns = TransportEsqlStreamQueryAction.collectIndexPatterns(plan);
        assertEquals(Set.of("logs-*"), patterns);
    }

    public void testCollectIndexPatternsEsSourceExec() {
        EsSourceExec plan = new EsSourceExec(Source.EMPTY, "metrics-*", IndexMode.STANDARD, List.of(), null);
        Set<String> patterns = TransportEsqlStreamQueryAction.collectIndexPatterns(plan);
        assertEquals(Set.of("metrics-*"), patterns);
    }

    public void testCollectIndexPatternsFragmentExec() {
        EsRelation relation = new EsRelation(Source.EMPTY, "traces-*", IndexMode.STANDARD, Map.of(), Map.of(), Map.of(), List.of());
        FragmentExec plan = new FragmentExec(relation);
        Set<String> patterns = TransportEsqlStreamQueryAction.collectIndexPatterns(plan);
        assertEquals(Set.of("traces-*"), patterns);
    }

    public void testClassifyNullColumnsEmptyFieldIsTrue() {
        FieldAttribute attr = new FieldAttribute(
            Source.EMPTY,
            "sparse",
            new EsField("sparse", DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
        );
        boolean[] mask = TransportEsqlStreamQueryAction.classifyNullColumns(List.of(attr), Set.of("sparse"));
        assertEquals(1, mask.length);
        assertTrue(mask[0]);
    }

    public void testClassifyNullColumnsNonEmptyFieldIsFalse() {
        FieldAttribute attr = new FieldAttribute(
            Source.EMPTY,
            "present",
            new EsField("present", DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
        );
        boolean[] mask = TransportEsqlStreamQueryAction.classifyNullColumns(List.of(attr), Set.of("other"));
        assertEquals(1, mask.length);
        assertFalse(mask[0]);
    }

    public void testClassifyNullColumnsUnsupportedAttributeAlwaysFalse() {
        UnsupportedAttribute attr = new UnsupportedAttribute(Source.EMPTY, "unsup", new UnsupportedEsField("unsup", List.of("geo_shape")));
        boolean[] mask = TransportEsqlStreamQueryAction.classifyNullColumns(List.of(attr), Set.of("unsup"));
        assertEquals(1, mask.length);
        assertFalse(mask[0]);
    }

    public void testClassifyNullColumnsDerivedAndMetadataAlwaysFalse() {
        List<Attribute> output = List.of(
            new ReferenceAttribute(Source.EMPTY, "ref", DataType.LONG),
            new MetadataAttribute(Source.EMPTY, "_index", DataType.KEYWORD, false)
        );
        boolean[] mask = TransportEsqlStreamQueryAction.classifyNullColumns(output, Set.of("ref", "_index"));
        assertEquals(2, mask.length);
        assertFalse(mask[0]);
        assertFalse(mask[1]);
    }

    public void testClassifyNullColumnsEmptyOutput() {
        boolean[] mask = TransportEsqlStreamQueryAction.classifyNullColumns(List.of(), Set.of("anything"));
        assertEquals(0, mask.length);
    }
}
