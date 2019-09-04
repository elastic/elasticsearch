/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.fields;

import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExtractedFieldsTests extends ESTestCase {

    public void testAllTypesOfFields() {
        ExtractedField docValue1 = ExtractedField.newField("doc1", Collections.singleton("keyword"),
            ExtractedField.ExtractionMethod.DOC_VALUE);
        ExtractedField docValue2 = ExtractedField.newField("doc2", Collections.singleton("ip"),
            ExtractedField.ExtractionMethod.DOC_VALUE);
        ExtractedField scriptField1 = ExtractedField.newField("scripted1", Collections.emptySet(),
            ExtractedField.ExtractionMethod.SCRIPT_FIELD);
        ExtractedField scriptField2 = ExtractedField.newField("scripted2", Collections.emptySet(),
            ExtractedField.ExtractionMethod.SCRIPT_FIELD);
        ExtractedField sourceField1 = ExtractedField.newField("src1", Collections.singleton("text"),
            ExtractedField.ExtractionMethod.SOURCE);
        ExtractedField sourceField2 = ExtractedField.newField("src2", Collections.singleton("text"),
            ExtractedField.ExtractionMethod.SOURCE);
        ExtractedFields extractedFields = new ExtractedFields(Arrays.asList(
                docValue1, docValue2, scriptField1, scriptField2, sourceField1, sourceField2));

        assertThat(extractedFields.getAllFields().size(), equalTo(6));
        assertThat(extractedFields.getDocValueFields().stream().map(ExtractedField::getName).toArray(String[]::new),
            equalTo(new String[] {"doc1", "doc2"}));
        assertThat(extractedFields.getSourceFields(), equalTo(new String[] {"src1", "src2"}));
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

        ExtractedFields extractedFields = ExtractedFields.build(Arrays.asList("time", "value", "airline", "airport"),
            new HashSet<>(Collections.singletonList("airport")), fieldCapabilitiesResponse);

        assertThat(extractedFields.getDocValueFields().size(), equalTo(2));
        assertThat(extractedFields.getDocValueFields().get(0).getName(), equalTo("time"));
        assertThat(extractedFields.getDocValueFields().get(0).getDocValueFormat(), equalTo("epoch_millis"));
        assertThat(extractedFields.getDocValueFields().get(1).getName(), equalTo("value"));
        assertThat(extractedFields.getDocValueFields().get(1).getDocValueFormat(), equalTo(null));
        assertThat(extractedFields.getSourceFields(), equalTo(new String[] {"airline"}));
        assertThat(extractedFields.getAllFields().size(), equalTo(4));
    }

    public void testBuildGivenMultiFields() {
        Job.Builder jobBuilder = new Job.Builder("foo");
        jobBuilder.setDataDescription(new DataDescription.Builder());
        Detector.Builder detector = new Detector.Builder("count", null);
        detector.setByFieldName("airline.text");
        detector.setOverFieldName("airport.keyword");
        jobBuilder.setAnalysisConfig(new AnalysisConfig.Builder(Collections.singletonList(detector.build())));

        DatafeedConfig.Builder datafeedBuilder = new DatafeedConfig.Builder("feed", jobBuilder.getId());
        datafeedBuilder.setIndices(Collections.singletonList("foo"));

        Map<String, FieldCapabilities> text = new HashMap<>();
        text.put("text", createFieldCaps(false));
        Map<String, FieldCapabilities> keyword = new HashMap<>();
        keyword.put("keyword", createFieldCaps(true));
        FieldCapabilitiesResponse fieldCapabilitiesResponse = mock(FieldCapabilitiesResponse.class);
        when(fieldCapabilitiesResponse.getField("airline")).thenReturn(text);
        when(fieldCapabilitiesResponse.getField("airline.text")).thenReturn(text);
        when(fieldCapabilitiesResponse.getField("airport")).thenReturn(text);
        when(fieldCapabilitiesResponse.getField("airport.keyword")).thenReturn(keyword);

        ExtractedFields extractedFields = ExtractedFields.build(Arrays.asList("airline.text", "airport.keyword"),
                Collections.emptySet(), fieldCapabilitiesResponse);

        assertThat(extractedFields.getDocValueFields().size(), equalTo(1));
        assertThat(extractedFields.getDocValueFields().get(0).getName(), equalTo("airport.keyword"));
        assertThat(extractedFields.getSourceFields().length, equalTo(1));
        assertThat(extractedFields.getSourceFields()[0], equalTo("airline"));
        assertThat(extractedFields.getAllFields().size(), equalTo(2));

        assertThat(extractedFields.getAllFields().stream().filter(f -> f.getName().equals("airport.keyword")).findFirst().get().getAlias(),
                equalTo("airport.keyword"));
        assertThat(extractedFields.getAllFields().stream().filter(f -> f.getName().equals("airline")).findFirst().get().getAlias(),
                equalTo("airline.text"));
    }

    public void testBuildGivenFieldWithoutMappings() {
        FieldCapabilitiesResponse fieldCapabilitiesResponse = mock(FieldCapabilitiesResponse.class);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ExtractedFields.build(
                Collections.singletonList("value"), Collections.emptySet(), fieldCapabilitiesResponse));
        assertThat(e.getMessage(), equalTo("cannot retrieve field [value] because it has no mappings"));
    }

    private static FieldCapabilities createFieldCaps(boolean isAggregatable) {
        FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
        when(fieldCaps.isAggregatable()).thenReturn(isAggregatable);
        return fieldCaps;
    }
}
