/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

public class MappingAsAttributesTests extends ESTestCase {

    public void testPartialMappingMaterializesOnlyRequestedFields() {
        Map<String, EsField> mapping = wideMapping(50);
        Source source = Source.EMPTY;

        List<Attribute> partial = Analyzer.mappingAsAttributes(source, mapping, Set.of("wanted"));
        List<Attribute> full = Analyzer.mappingAsAttributes(source, mapping);

        assertThat(partial, hasSize(1));
        assertThat(partial.getFirst().name(), equalTo("wanted"));
        assertThat(full.size(), equalTo(51));
    }

    public void testPartialMappingNestedField() {
        Map<String, EsField> parentProps = Map.of("child", new EsField("child", KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
        Map<String, EsField> mapping = Map.of(
            "parent",
            new EsField("parent", DataType.OBJECT, parentProps, false, EsField.TimeSeriesFieldType.NONE)
        );

        List<Attribute> partial = Analyzer.mappingAsAttributes(Source.EMPTY, mapping, Set.of("parent.child"));
        assertThat(partial.stream().map(Attribute::name).collect(Collectors.toList()), contains("parent.child"));
    }

    public void testPartialMappingWildcardPrefix() {
        Map<String, EsField> nested = Map.of(
            "a",
            new EsField("a", KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE),
            "b",
            new EsField("b", KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        Map<String, EsField> mapping = Map.of(
            "group",
            new EsField("group", DataType.OBJECT, nested, false, EsField.TimeSeriesFieldType.NONE),
            "other",
            new EsField("other", KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );

        List<Attribute> partial = Analyzer.mappingAsAttributes(Source.EMPTY, mapping, Set.of("group.*"));
        assertThat(partial.stream().map(Attribute::name).sorted().collect(Collectors.toList()), contains("group.a", "group.b"));
    }

    public void testPartialMappingDeeplyNestedWildcardPrefix() {
        Map<String, EsField> deepProp = Map.of(
            "deepField",
            new EsField("deepField", KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        Map<String, EsField> innerProp = Map.of(
            "inner",
            new EsField("inner", DataType.OBJECT, deepProp, false, EsField.TimeSeriesFieldType.NONE)
        );
        Map<String, EsField> mapping = Map.of(
            "outer",
            new EsField("outer", DataType.OBJECT, innerProp, false, EsField.TimeSeriesFieldType.NONE)
        );

        List<Attribute> partial = Analyzer.mappingAsAttributes(Source.EMPTY, mapping, Set.of("outer.inner.*"));
        assertThat(partial.stream().map(Attribute::name).collect(Collectors.toList()), contains("outer.inner.deepField"));
    }

    public void testPartialMappingDeeplyNestedExactMatch() {
        Map<String, EsField> deepProp = Map.of(
            "deepField",
            new EsField("deepField", KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        Map<String, EsField> innerProp = Map.of(
            "inner",
            new EsField("inner", DataType.OBJECT, deepProp, false, EsField.TimeSeriesFieldType.NONE)
        );
        Map<String, EsField> mapping = Map.of(
            "outer",
            new EsField("outer", DataType.OBJECT, innerProp, false, EsField.TimeSeriesFieldType.NONE)
        );

        List<Attribute> partial = Analyzer.mappingAsAttributes(Source.EMPTY, mapping, Set.of("outer.inner.deepField"));
        assertThat(partial.stream().map(Attribute::name).collect(Collectors.toList()), contains("outer.inner.deepField"));
    }

    /**
     * Wide enrich-style policy mappings can contain thousands of fields while only a handful are used.
     * Partial materialization must stay proportional to requested fields, not mapping width.
     */
    public void testPartialMappingAtLeast300xFewerAttributesThanFull() {
        Map<String, EsField> mapping = wideMapping(600);
        mapping.put("match_key", new EsField("match_key", KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
        mapping.put("enrich_value", new EsField("enrich_value", KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
        Source source = Source.EMPTY;
        Set<String> enrichPolicyFields = Set.of("match_key", "enrich_value");

        int fullCount = Analyzer.mappingAsAttributes(source, mapping).size();
        int partialCount = Analyzer.mappingAsAttributes(source, mapping, enrichPolicyFields).size();

        assertThat(fullCount, greaterThanOrEqualTo(603));
        assertThat(partialCount, equalTo(2));
        assertThat(fullCount / partialCount, greaterThanOrEqualTo(300));
    }

    public void testPartialMappingMatchesFullForRequestedTopLevelFields() {
        Map<String, EsField> mapping = wideMapping(20);
        mapping.put("alpha", new EsField("alpha", KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
        mapping.put("beta", new EsField("beta", KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
        Source source = Source.EMPTY;
        Set<String> requested = Set.of("alpha", "beta");

        List<String> partial = Analyzer.mappingAsAttributes(source, mapping, requested).stream().map(Attribute::name).sorted().toList();
        List<String> fromFull = Analyzer.mappingAsAttributes(source, mapping)
            .stream()
            .map(Attribute::name)
            .filter(requested::contains)
            .sorted()
            .toList();

        assertThat(partial, equalTo(fromFull));
        assertThat(partial, contains("alpha", "beta"));
    }

    public void testPartialMappingFallbackForSuffixWildcard() {
        Map<String, EsField> mapping = wideMapping(5);
        mapping.put("first_name", new EsField("first_name", KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
        mapping.put("last_name", new EsField("last_name", KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
        Source source = Source.EMPTY;

        List<String> result = Analyzer.mappingAsAttributes(source, mapping, Set.of("*_name"))
            .stream()
            .map(Attribute::name)
            .sorted()
            .toList();
        List<String> full = Analyzer.mappingAsAttributes(source, mapping).stream().map(Attribute::name).sorted().toList();

        assertThat(result, equalTo(full));
    }

    public void testPartialMappingFallbackForPrefixWildcard() {
        Map<String, EsField> mapping = wideMapping(5);
        mapping.put("first_name", new EsField("first_name", KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
        mapping.put("first_value", new EsField("first_value", KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
        Source source = Source.EMPTY;

        List<String> result = Analyzer.mappingAsAttributes(source, mapping, Set.of("first*"))
            .stream()
            .map(Attribute::name)
            .sorted()
            .toList();
        List<String> full = Analyzer.mappingAsAttributes(source, mapping).stream().map(Attribute::name).sorted().toList();

        assertThat(result, equalTo(full));
    }

    public void testPartialMappingFallbackForInfixWildcard() {
        Map<String, EsField> mapping = wideMapping(5);
        mapping.put("first_name", new EsField("first_name", KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
        mapping.put("last_name", new EsField("last_name", KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
        Source source = Source.EMPTY;

        List<String> result = Analyzer.mappingAsAttributes(source, mapping, Set.of("*t*name"))
            .stream()
            .map(Attribute::name)
            .sorted()
            .toList();
        List<String> full = Analyzer.mappingAsAttributes(source, mapping).stream().map(Attribute::name).sorted().toList();

        assertThat(result, equalTo(full));
    }

    private static Map<String, EsField> wideMapping(int extraFields) {
        Map<String, EsField> mapping = new HashMap<>();
        mapping.put("wanted", new EsField("wanted", KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
        for (int i = 0; i < extraFields; i++) {
            mapping.put("extra" + i, new EsField("extra" + i, KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
        }
        return mapping;
    }
}
