/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.UnsupportedEsField;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.physical.EsSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.session.Result;

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

    public void testCollectAliasSourcesEmptyForLeafPlan() {
        EsSourceExec plan = new EsSourceExec(Source.EMPTY, "logs-*", IndexMode.STANDARD, List.of(), null);
        AttributeMap<Attribute> map = TransportEsqlStreamQueryAction.collectAliasSources(plan);
        assertTrue("leaf plan with no aliases must produce an empty map", map.isEmpty());
    }

    public void testCollectAliasSourcesEvalExec() {
        FieldAttribute fa = new FieldAttribute(
            Source.EMPTY,
            "f",
            new EsField("f", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        Alias alias = new Alias(Source.EMPTY, "s", fa);
        EvalExec plan = new EvalExec(
            Source.EMPTY,
            new EsSourceExec(Source.EMPTY, "idx", IndexMode.STANDARD, List.of(), null),
            List.of(alias)
        );
        AttributeMap<Attribute> map = TransportEsqlStreamQueryAction.collectAliasSources(plan);
        assertEquals("alias.toAttribute() must map to the source FieldAttribute", fa, map.resolve(alias.toAttribute(), null));
    }

    public void testCollectAliasSourcesProjectExec() {
        FieldAttribute fa = new FieldAttribute(
            Source.EMPTY,
            "f",
            new EsField("f", DataType.LONG, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        Alias alias = new Alias(Source.EMPTY, "renamed", fa);
        ProjectExec plan = new ProjectExec(
            Source.EMPTY,
            new EsSourceExec(Source.EMPTY, "idx", IndexMode.STANDARD, List.of(), null),
            List.of(alias)
        );
        AttributeMap<Attribute> map = TransportEsqlStreamQueryAction.collectAliasSources(plan);
        assertEquals("alias.toAttribute() must map to the source FieldAttribute", fa, map.resolve(alias.toAttribute(), null));
    }

    public void testCollectAliasSourcesFragmentExecWithLogicalEval() {
        FieldAttribute fa = new FieldAttribute(
            Source.EMPTY,
            "f",
            new EsField("f", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        Alias alias = new Alias(Source.EMPTY, "s", fa);
        EsRelation relation = new EsRelation(Source.EMPTY, "traces-*", IndexMode.STANDARD, Map.of(), Map.of(), Map.of(), List.of());
        Eval logicalEval = new Eval(Source.EMPTY, relation, List.of(alias));
        FragmentExec plan = new FragmentExec(logicalEval);
        AttributeMap<Attribute> map = TransportEsqlStreamQueryAction.collectAliasSources(plan);
        assertEquals("alias inside FragmentExec.fragment() must be collected", fa, map.resolve(alias.toAttribute(), null));
    }

    public void testCollectAliasSourcesAliasChain() {
        FieldAttribute fa = new FieldAttribute(
            Source.EMPTY,
            "f",
            new EsField("f", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        Alias aliasA = new Alias(Source.EMPTY, "a", fa);
        EvalExec evalExec = new EvalExec(
            Source.EMPTY,
            new EsSourceExec(Source.EMPTY, "idx", IndexMode.STANDARD, List.of(), null),
            List.of(aliasA)
        );
        Alias aliasB = new Alias(Source.EMPTY, "b", aliasA.toAttribute());
        ProjectExec projectExec = new ProjectExec(Source.EMPTY, evalExec, List.of(aliasB));
        AttributeMap<Attribute> map = TransportEsqlStreamQueryAction.collectAliasSources(projectExec);
        Attribute terminal = map.resolve(aliasB.toAttribute(), aliasB.toAttribute());
        assertEquals("resolving b through the chain a -> f must reach the FieldAttribute", fa, terminal);
    }

    public void testResolveIndexFieldNamesBareFieldAttributeIsDroppable() {
        FieldAttribute fa = new FieldAttribute(
            Source.EMPTY,
            "f",
            new EsField("f", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        String[] names = TransportEsqlStreamQueryAction.resolveIndexFieldNames(List.of(fa), AttributeMap.emptyAttributeMap());
        assertEquals(1, names.length);
        assertEquals("f", names[0]);
    }

    public void testResolveIndexFieldNamesNonAggregatableIsNotDroppable() {
        FieldAttribute fa = new FieldAttribute(
            Source.EMPTY,
            "noidx",
            new EsField("noidx", DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
        );
        String[] names = TransportEsqlStreamQueryAction.resolveIndexFieldNames(List.of(fa), AttributeMap.emptyAttributeMap());
        assertEquals(1, names.length);
        assertNull("non-aggregatable field must not be a drop candidate", names[0]);
    }

    public void testResolveIndexFieldNamesAggregateMetricDoubleIsNotDroppable() {
        FieldAttribute fa = new FieldAttribute(
            Source.EMPTY,
            "metric",
            new EsField("metric", DataType.AGGREGATE_METRIC_DOUBLE, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        String[] names = TransportEsqlStreamQueryAction.resolveIndexFieldNames(List.of(fa), AttributeMap.emptyAttributeMap());
        assertEquals(1, names.length);
        assertNull("AGGREGATE_METRIC_DOUBLE must not be a drop candidate regardless of isAggregatable()", names[0]);
    }

    public void testResolveIndexFieldNamesBareReferenceAttributeIsNull() {
        ReferenceAttribute ref = new ReferenceAttribute(Source.EMPTY, "derived", DataType.KEYWORD);
        String[] names = TransportEsqlStreamQueryAction.resolveIndexFieldNames(List.of(ref), AttributeMap.emptyAttributeMap());
        assertEquals(1, names.length);
        assertNull("bare ReferenceAttribute with no mapping must resolve to null", names[0]);
    }

    public void testResolveIndexFieldNamesAliasedFieldAttribute() {
        FieldAttribute fa = new FieldAttribute(
            Source.EMPTY,
            "f",
            new EsField("f", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        Alias alias = new Alias(Source.EMPTY, "s", fa);
        AttributeMap<Attribute> aliasSources = AttributeMap.<Attribute>of(alias.toAttribute(), fa);
        String[] names = TransportEsqlStreamQueryAction.resolveIndexFieldNames(List.of(alias.toAttribute()), aliasSources);
        assertEquals(1, names.length);
        assertEquals("alias of a FieldAttribute must resolve to the field name", "f", names[0]);
    }

    public void testResolveIndexFieldNamesAliasedUnsupportedAttributeIsNull() {
        UnsupportedAttribute ua = new UnsupportedAttribute(Source.EMPTY, "bad", new UnsupportedEsField("bad", List.of("geo_shape")));
        Alias alias = new Alias(Source.EMPTY, "renamed", ua);
        AttributeMap<Attribute> aliasSources = AttributeMap.<Attribute>of(alias.toAttribute(), ua);
        String[] names = TransportEsqlStreamQueryAction.resolveIndexFieldNames(List.of(alias.toAttribute()), aliasSources);
        assertEquals(1, names.length);
        assertNull("alias of an UnsupportedAttribute must resolve to null at the terminal", names[0]);
    }

    public void testResolveIndexFieldNamesEnrichShapeIsNull() {
        ReferenceAttribute enrichAttr = new ReferenceAttribute(Source.EMPTY, "enrich_field", DataType.KEYWORD);
        Alias alias = new Alias(Source.EMPTY, "output", enrichAttr);
        AttributeMap<Attribute> aliasSources = AttributeMap.<Attribute>of(alias.toAttribute(), enrichAttr);
        String[] names = TransportEsqlStreamQueryAction.resolveIndexFieldNames(List.of(alias.toAttribute()), aliasSources);
        assertEquals(1, names.length);
        assertNull("alias of a ReferenceAttribute (enrich shape) must resolve to null", names[0]);
    }

    public void testResolveIndexFieldNamesMetadataAttributeIsNull() {
        MetadataAttribute attr = new MetadataAttribute(Source.EMPTY, "_index", DataType.KEYWORD, false);
        String[] names = TransportEsqlStreamQueryAction.resolveIndexFieldNames(List.of(attr), AttributeMap.emptyAttributeMap());
        assertEquals(1, names.length);
        assertNull("MetadataAttribute must not be a drop candidate", names[0]);
    }

    public void testResolveIndexFieldNamesUnsupportedAttributeIsNull() {
        UnsupportedAttribute ua = new UnsupportedAttribute(Source.EMPTY, "unsup", new UnsupportedEsField("unsup", List.of("geo_shape")));
        String[] names = TransportEsqlStreamQueryAction.resolveIndexFieldNames(List.of(ua), AttributeMap.emptyAttributeMap());
        assertEquals(1, names.length);
        assertNull("UnsupportedAttribute must not be a drop candidate", names[0]);
    }

    public void testResolveIndexFieldNamesMixedOutput() {
        FieldAttribute droppable = new FieldAttribute(
            Source.EMPTY,
            "kept",
            new EsField("kept", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        FieldAttribute notDroppable = new FieldAttribute(
            Source.EMPTY,
            "noidx",
            new EsField("noidx", DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
        );
        ReferenceAttribute bareRef = new ReferenceAttribute(Source.EMPTY, "ref", DataType.LONG);
        String[] names = TransportEsqlStreamQueryAction.resolveIndexFieldNames(
            List.of(droppable, notDroppable, bareRef),
            AttributeMap.emptyAttributeMap()
        );
        assertEquals(3, names.length);
        assertEquals("kept", names[0]);
        assertNull("non-aggregatable field must be null", names[1]);
        assertNull("bare ReferenceAttribute must be null", names[2]);
    }

    public void testResolveIndexFieldNamesEmptyOutput() {
        String[] names = TransportEsqlStreamQueryAction.resolveIndexFieldNames(List.of(), AttributeMap.emptyAttributeMap());
        assertEquals(0, names.length);
    }

    public void testCollectIndexNamesFragmentExec() {
        EsRelation relation = new EsRelation(
            Source.EMPTY,
            "traces-*",
            IndexMode.STANDARD,
            Map.of(),
            Map.of(),
            Map.of("traces-2024.01.01", new IndexProperties(IndexMode.STANDARD, 0)),
            List.of()
        );
        FragmentExec plan = new FragmentExec(relation);
        Set<String> names = TransportEsqlStreamQueryAction.collectIndexNames(plan);
        assertEquals(Set.of("traces-2024.01.01"), names);
    }

    public void testCollectIndexNamesFragmentExecUsesConcreteIndicesNotPattern() {
        EsRelation relation = new EsRelation(
            Source.EMPTY,
            "index1,index2",
            IndexMode.STANDARD,
            Map.of(),
            Map.of(),
            Map.of("index1", new IndexProperties(IndexMode.STANDARD, 0), "index2", new IndexProperties(IndexMode.STANDARD, 0)),
            List.of()
        );
        FragmentExec plan = new FragmentExec(relation);
        Set<String> names = TransportEsqlStreamQueryAction.collectIndexNames(plan);
        assertEquals(Set.of("index1", "index2"), names);
    }

    public void testCollectIndexNamesFragmentExecKeepsClusterAliasQualification() {
        EsRelation relation = new EsRelation(
            Source.EMPTY,
            "remote:idx,local_idx",
            IndexMode.STANDARD,
            Map.of(),
            Map.of(),
            Map.of("remote:idx", new IndexProperties(IndexMode.STANDARD, 0), "local_idx", new IndexProperties(IndexMode.STANDARD, 0)),
            List.of()
        );
        FragmentExec plan = new FragmentExec(relation);
        Set<String> names = TransportEsqlStreamQueryAction.collectIndexNames(plan);
        assertEquals(Set.of("remote:idx", "local_idx"), names);
    }

    public void testCollectIndexNamesFragmentExecEmptyConcreteIndices() {
        EsRelation relation = new EsRelation(Source.EMPTY, "empty-index", IndexMode.STANDARD, Map.of(), Map.of(), Map.of(), List.of());
        FragmentExec plan = new FragmentExec(relation);
        Set<String> names = TransportEsqlStreamQueryAction.collectIndexNames(plan);
        assertEquals(Set.of(), names);
    }

    public void testMarkPartialFromCompletionInfoFlipsExecutionInfo() {
        EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(alias -> false, EsqlExecutionInfo.IncludeExecutionMetadata.NEVER);
        assertFalse("executionInfo must start as non-partial", executionInfo.isPartial());

        DriverCompletionInfo partialCompletion = new DriverCompletionInfo(0, 0, 0, 0, 0, 0, List.of(), List.of(), Map.of(), true, Set.of());
        Result partialResult = new Result(List.of(), List.of(), Map.of(), EsqlTestUtils.TEST_CFG, partialCompletion, executionInfo);
        TransportEsqlStreamQueryAction.markPartialFromCompletionInfo(partialResult);
        assertTrue("is_partial must be true when completionInfo.partial() is true", executionInfo.isPartial());
    }

    public void testMarkPartialFromCompletionInfoLeavesNonPartialUnchanged() {
        EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(alias -> false, EsqlExecutionInfo.IncludeExecutionMetadata.NEVER);
        Result nonPartialResult = new Result(
            List.of(),
            List.of(),
            Map.of(),
            EsqlTestUtils.TEST_CFG,
            DriverCompletionInfo.EMPTY,
            executionInfo
        );
        TransportEsqlStreamQueryAction.markPartialFromCompletionInfo(nonPartialResult);
        assertFalse("is_partial must remain false when completionInfo.partial() is false", executionInfo.isPartial());
    }
}
