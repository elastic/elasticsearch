/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.extractor;

import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.NGram;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.OneHotEncoding;
import org.elasticsearch.xpack.ml.test.SearchHitBuilder;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeSet;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExtractedFieldsTests extends ESTestCase {

    public void testAllTypesOfFields() {
        ExtractedField docValue1 = new DocValueField("doc1", Collections.singleton("keyword"));
        ExtractedField docValue2 = new DocValueField("doc2", Collections.singleton("ip"));
        ExtractedField scriptField1 = new ScriptField("scripted1");
        ExtractedField scriptField2 = new ScriptField("scripted2");
        ExtractedField sourceField1 = new SourceField("src1", Collections.singleton("text"));
        ExtractedField sourceField2 = new SourceField("src2", Collections.singleton("text"));
        ExtractedFields extractedFields = new ExtractedFields(
            Arrays.asList(docValue1, docValue2, scriptField1, scriptField2, sourceField1, sourceField2),
            Collections.emptyList(),
            Collections.emptyMap()
        );

        assertThat(extractedFields.getAllFields().size(), equalTo(6));
        assertThat(
            extractedFields.getDocValueFields().stream().map(ExtractedField::getName).toArray(String[]::new),
            equalTo(new String[] { "doc1", "doc2" })
        );
        assertThat(extractedFields.getSourceFields(), equalTo(new String[] { "src1", "src2" }));
    }

    public void testBuildGivenMixtureOfTypes() {
        Map<String, FieldCapabilities> timeCaps = new HashMap<>();
        timeCaps.put("date", createFieldCaps(true));
        Map<String, FieldCapabilities> valueCaps = new HashMap<>();
        valueCaps.put("float", createFieldCaps(true));
        valueCaps.put("keyword", createFieldCaps(true));
        Map<String, FieldCapabilities> airlineCaps = new HashMap<>();
        airlineCaps.put("text", createFieldCaps(false));
        FieldCapabilitiesResponse fieldCapabilitiesResponse = mock(FieldCapabilitiesResponse.class);
        when(fieldCapabilitiesResponse.getField("time")).thenReturn(timeCaps);
        when(fieldCapabilitiesResponse.getField("value")).thenReturn(valueCaps);
        when(fieldCapabilitiesResponse.getField("airline")).thenReturn(airlineCaps);

        ExtractedFields extractedFields = ExtractedFields.build(
            new TreeSet<>(Arrays.asList("time", "value", "airline", "airport")),
            new HashSet<>(Collections.singletonList("airport")),
            fieldCapabilitiesResponse,
            Collections.emptyMap(),
            Collections.emptyList()
        );

        assertThat(extractedFields.getDocValueFields().size(), equalTo(2));
        assertThat(extractedFields.getDocValueFields().get(0).getName(), equalTo("time"));
        assertThat(extractedFields.getDocValueFields().get(0).getDocValueFormat(), equalTo("epoch_millis"));
        assertThat(extractedFields.getDocValueFields().get(1).getName(), equalTo("value"));
        assertThat(extractedFields.getDocValueFields().get(1).getDocValueFormat(), equalTo(null));
        assertThat(extractedFields.getSourceFields(), equalTo(new String[] { "airline" }));
        assertThat(extractedFields.getAllFields().size(), equalTo(4));
    }

    public void testBuildGivenMultiFields() {
        Map<String, FieldCapabilities> text = new HashMap<>();
        text.put("text", createFieldCaps(false));
        Map<String, FieldCapabilities> keyword = new HashMap<>();
        keyword.put("keyword", createFieldCaps(true));
        FieldCapabilitiesResponse fieldCapabilitiesResponse = mock(FieldCapabilitiesResponse.class);
        when(fieldCapabilitiesResponse.getField("airline")).thenReturn(text);
        when(fieldCapabilitiesResponse.getField("airline.text")).thenReturn(text);
        when(fieldCapabilitiesResponse.getField("airport")).thenReturn(text);
        when(fieldCapabilitiesResponse.getField("airport.keyword")).thenReturn(keyword);

        ExtractedFields extractedFields = ExtractedFields.build(
            new TreeSet<>(Arrays.asList("airline.text", "airport.keyword")),
            Collections.emptySet(),
            fieldCapabilitiesResponse,
            Collections.emptyMap(),
            Collections.emptyList()
        );

        assertThat(extractedFields.getDocValueFields().size(), equalTo(1));
        assertThat(extractedFields.getDocValueFields().get(0).getName(), equalTo("airport.keyword"));
        assertThat(extractedFields.getSourceFields().length, equalTo(1));
        assertThat(extractedFields.getSourceFields()[0], equalTo("airline"));
        assertThat(extractedFields.getAllFields().size(), equalTo(2));

        ExtractedField airlineField = extractedFields.getAllFields().get(0);
        assertThat(airlineField.isMultiField(), is(true));
        assertThat(airlineField.getName(), equalTo("airline.text"));
        assertThat(airlineField.getSearchField(), equalTo("airline"));
        assertThat(airlineField.getParentField(), equalTo("airline"));

        ExtractedField airportField = extractedFields.getAllFields().get(1);
        assertThat(airportField.isMultiField(), is(true));
        assertThat(airportField.getName(), equalTo("airport.keyword"));
        assertThat(airportField.getSearchField(), equalTo("airport.keyword"));
        assertThat(airportField.getParentField(), equalTo("airport"));
    }

    public void testApplyBooleanMapping_GivenDocValueField() {
        DocValueField aBool = new DocValueField("a_bool", Collections.singleton("boolean"));

        ExtractedField mapped = ExtractedFields.applyBooleanMapping(aBool);

        SearchHit hitTrue = new SearchHitBuilder(42).addField("a_bool", true).build();
        SearchHit hitFalse = new SearchHitBuilder(42).addField("a_bool", false).build();

        assertThat(mapped.value(hitTrue, new SourceSupplier(hitTrue)), equalTo(new Integer[] { 1 }));
        assertThat(mapped.value(hitFalse, new SourceSupplier(hitFalse)), equalTo(new Integer[] { 0 }));

        assertThat(mapped.getName(), equalTo(aBool.getName()));
        assertThat(mapped.getMethod(), equalTo(aBool.getMethod()));
        assertThat(mapped.supportsFromSource(), is(aBool.supportsFromSource()));
    }

    public void testApplyBooleanMapping_GivenSourceField() {
        SourceField aBool = new SourceField("a_bool", Collections.singleton("boolean"));

        ExtractedField mapped = ExtractedFields.applyBooleanMapping(aBool);

        SearchHit hitTrue = new SearchHitBuilder(42).setSource("{\"a_bool\": true}").build();
        SearchHit hitFalse = new SearchHitBuilder(42).setSource("{\"a_bool\": false}").build();
        SearchHit hitTrueArray = new SearchHitBuilder(42).setSource("{\"a_bool\": [\"true\", true]}").build();
        SearchHit hitFalseArray = new SearchHitBuilder(42).setSource("{\"a_bool\": [\"false\", false]}").build();

        assertThat(mapped.value(hitTrue, new SourceSupplier(hitTrue)), equalTo(new Integer[] { 1 }));
        assertThat(mapped.value(hitFalse, new SourceSupplier(hitFalse)), equalTo(new Integer[] { 0 }));
        assertThat(mapped.value(hitTrueArray, new SourceSupplier(hitTrueArray)), equalTo(new Integer[] { 1, 1 }));
        assertThat(mapped.value(hitFalseArray, new SourceSupplier(hitFalseArray)), equalTo(new Integer[] { 0, 0 }));

        assertThat(mapped.getName(), equalTo(aBool.getName()));
        assertThat(mapped.getMethod(), equalTo(aBool.getMethod()));
        assertThat(mapped.supportsFromSource(), is(aBool.supportsFromSource()));
    }

    public void testApplyBooleanMapping_GivenNonBooleanField() {
        SourceField aBool = new SourceField("not_a_bool", Collections.singleton("integer"));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ExtractedFields.applyBooleanMapping(aBool));

        assertThat(e.getMessage(), equalTo("cannot apply boolean mapping to field [not_a_bool]"));
    }

    public void testBuildGivenFieldWithoutMappings() {
        FieldCapabilitiesResponse fieldCapabilitiesResponse = mock(FieldCapabilitiesResponse.class);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ExtractedFields.build(
                Collections.singleton("value"),
                Collections.emptySet(),
                fieldCapabilitiesResponse,
                Collections.emptyMap(),
                Collections.emptyList()
            )
        );
        assertThat(e.getMessage(), equalTo("cannot retrieve field [value] because it has no mappings"));
    }

    public void testExtractFeatureOrganicAndProcessedNames() {
        ExtractedField docValue1 = new DocValueField("doc1", Collections.singleton("keyword"));
        ExtractedField docValue2 = new DocValueField("doc2", Collections.singleton("ip"));
        ExtractedField scriptField1 = new ScriptField("scripted1");
        ExtractedField scriptField2 = new ScriptField("scripted2");
        ExtractedField sourceField1 = new SourceField("src1", Collections.singleton("text"));
        ExtractedField sourceField2 = new SourceField("src2", Collections.singleton("text"));

        Map<String, String> hotMap = new LinkedHashMap<>();
        hotMap.put("bar", "bar_column");
        hotMap.put("foo", "foo_column");

        ExtractedFields extractedFields = new ExtractedFields(
            Arrays.asList(docValue1, docValue2, scriptField1, scriptField2, sourceField1, sourceField2),
            Arrays.asList(
                new ProcessedField(new NGram("doc1", "f", new int[] { 1, 2 }, 0, 2, true)),
                new ProcessedField(new OneHotEncoding("src1", hotMap, true))
            ),
            Collections.emptyMap()
        );

        String[] organic = extractedFields.extractOrganicFeatureNames();
        assertThat(organic, arrayContaining("doc2", "scripted1", "scripted2", "src2"));

        String[] processed = extractedFields.extractProcessedFeatureNames();
        assertThat(processed, arrayContaining("f.10", "f.11", "f.20", "bar_column", "foo_column"));
    }

    private static FieldCapabilities createFieldCaps(boolean isAggregatable) {
        FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
        when(fieldCaps.isAggregatable()).thenReturn(isAggregatable);
        return fieldCaps;
    }
}
