/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.scroll;

import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.extractor.DocValueField;
import org.elasticsearch.xpack.ml.extractor.ExtractedField;
import org.elasticsearch.xpack.ml.extractor.ExtractedFields;
import org.elasticsearch.xpack.ml.extractor.ScriptField;
import org.elasticsearch.xpack.ml.extractor.SourceField;
import org.elasticsearch.xpack.ml.extractor.SourceSupplier;
import org.elasticsearch.xpack.ml.extractor.TimeField;
import org.elasticsearch.xpack.ml.test.SearchHitBuilder;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TimeBasedExtractedFieldsTests extends ESTestCase {

    private ExtractedField timeField = new TimeField("time", ExtractedField.Method.DOC_VALUE);

    public void testInvalidConstruction() {
        expectThrows(IllegalArgumentException.class, () -> new TimeBasedExtractedFields(timeField, Collections.emptyList()));
    }

    public void testTimeFieldOnly() {
        TimeBasedExtractedFields extractedFields = new TimeBasedExtractedFields(timeField, Arrays.asList(timeField));

        assertThat(extractedFields.getAllFields(), equalTo(Arrays.asList(timeField)));
        assertThat(extractedFields.timeField(), equalTo("time"));
        assertThat(
            extractedFields.getDocValueFields().stream().map(ExtractedField::getName).toArray(String[]::new),
            equalTo(new String[] { timeField.getName() })
        );
        assertThat(extractedFields.getSourceFields().length, equalTo(0));
    }

    public void testAllTypesOfFields() {
        ExtractedField docValue1 = new DocValueField("doc1", Collections.singleton("keyword"));
        ExtractedField docValue2 = new DocValueField("doc2", Collections.singleton("float"));
        ExtractedField scriptField1 = new ScriptField("scripted1");
        ExtractedField scriptField2 = new ScriptField("scripted2");
        ExtractedField sourceField1 = new SourceField("src1", Collections.singleton("text"));
        ExtractedField sourceField2 = new SourceField("src2", Collections.singleton("text"));
        TimeBasedExtractedFields extractedFields = new TimeBasedExtractedFields(
            timeField,
            Arrays.asList(timeField, docValue1, docValue2, scriptField1, scriptField2, sourceField1, sourceField2)
        );

        assertThat(extractedFields.getAllFields().size(), equalTo(7));
        assertThat(extractedFields.timeField(), equalTo("time"));
        assertThat(
            extractedFields.getDocValueFields().stream().map(ExtractedField::getName).toArray(String[]::new),
            equalTo(new String[] { "time", "doc1", "doc2" })
        );
        assertThat(extractedFields.getSourceFields(), equalTo(new String[] { "src1", "src2" }));
    }

    public void testStringTimeFieldValue() {
        long millis = randomLong();
        SearchHit hit = new SearchHitBuilder(randomInt()).addField("time", Long.toString(millis)).build();
        TimeBasedExtractedFields extractedFields = new TimeBasedExtractedFields(timeField, Collections.singletonList(timeField));
        assertThat(extractedFields.timeFieldValue(hit, new SourceSupplier(hit)), equalTo(millis));
    }

    public void testPre6xTimeFieldValue() {
        // Prior to 6.x, timestamps were simply `long` milliseconds-past-the-epoch values
        long millis = randomLong();
        SearchHit hit = new SearchHitBuilder(randomInt()).addField("time", millis).build();
        TimeBasedExtractedFields extractedFields = new TimeBasedExtractedFields(timeField, Collections.singletonList(timeField));
        assertThat(extractedFields.timeFieldValue(hit, new SourceSupplier(hit)), equalTo(millis));
    }

    public void testTimeFieldValueGivenEmptyArray() {
        SearchHit hit = new SearchHitBuilder(1).addField("time", Collections.emptyList()).build();

        TimeBasedExtractedFields extractedFields = new TimeBasedExtractedFields(timeField, Arrays.asList(timeField));

        expectThrows(RuntimeException.class, () -> extractedFields.timeFieldValue(hit, new SourceSupplier(hit)));
    }

    public void testTimeFieldValueGivenValueHasTwoElements() {
        SearchHit hit = new SearchHitBuilder(1).addField("time", Arrays.asList(1L, 2L)).build();

        TimeBasedExtractedFields extractedFields = new TimeBasedExtractedFields(timeField, Arrays.asList(timeField));

        expectThrows(RuntimeException.class, () -> extractedFields.timeFieldValue(hit, new SourceSupplier(hit)));
    }

    public void testTimeFieldValueGivenValueIsString() {
        SearchHit hit = new SearchHitBuilder(1).addField("time", "a string").build();

        TimeBasedExtractedFields extractedFields = new TimeBasedExtractedFields(timeField, Arrays.asList(timeField));

        expectThrows(RuntimeException.class, () -> extractedFields.timeFieldValue(hit, new SourceSupplier(hit)));
    }

    public void testBuildGivenMixtureOfTypes() {
        Job.Builder jobBuilder = new Job.Builder("foo");
        jobBuilder.setDataDescription(new DataDescription.Builder());
        Detector.Builder detector = new Detector.Builder("mean", "value");
        detector.setByFieldName("airline");
        detector.setOverFieldName("airport");
        jobBuilder.setAnalysisConfig(new AnalysisConfig.Builder(Collections.singletonList(detector.build())));

        DatafeedConfig.Builder datafeedBuilder = new DatafeedConfig.Builder("feed", jobBuilder.getId());
        datafeedBuilder.setIndices(Collections.singletonList("foo"));
        datafeedBuilder.setScriptFields(Collections.singletonList(new SearchSourceBuilder.ScriptField("airport", null, false)));

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

        TimeBasedExtractedFields extractedFields = TimeBasedExtractedFields.build(
            jobBuilder.build(new Date()),
            datafeedBuilder.build(),
            fieldCapabilitiesResponse
        );

        assertThat(extractedFields.timeField(), equalTo("time"));
        assertThat(extractedFields.getDocValueFields().size(), equalTo(2));
        assertThat(extractedFields.getDocValueFields().get(0).getName(), equalTo("time"));
        assertThat(extractedFields.getDocValueFields().get(0).getDocValueFormat(), equalTo("epoch_millis"));
        assertThat(extractedFields.getDocValueFields().get(1).getName(), equalTo("value"));
        assertThat(extractedFields.getDocValueFields().get(1).getDocValueFormat(), equalTo(null));
        assertThat(extractedFields.getSourceFields().length, equalTo(1));
        assertThat(extractedFields.getSourceFields()[0], equalTo("airline"));
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

        Map<String, FieldCapabilities> timeCaps = new HashMap<>();
        timeCaps.put("date", createFieldCaps(true));
        Map<String, FieldCapabilities> text = new HashMap<>();
        text.put("text", createFieldCaps(false));
        Map<String, FieldCapabilities> keyword = new HashMap<>();
        keyword.put("keyword", createFieldCaps(true));
        FieldCapabilitiesResponse fieldCapabilitiesResponse = mock(FieldCapabilitiesResponse.class);
        when(fieldCapabilitiesResponse.getField("time")).thenReturn(timeCaps);
        when(fieldCapabilitiesResponse.getField("airline")).thenReturn(text);
        when(fieldCapabilitiesResponse.getField("airline.text")).thenReturn(text);
        when(fieldCapabilitiesResponse.getField("airport")).thenReturn(text);
        when(fieldCapabilitiesResponse.getField("airport.keyword")).thenReturn(keyword);

        TimeBasedExtractedFields extractedFields = TimeBasedExtractedFields.build(
            jobBuilder.build(new Date()),
            datafeedBuilder.build(),
            fieldCapabilitiesResponse
        );

        assertThat(extractedFields.timeField(), equalTo("time"));
        assertThat(extractedFields.getDocValueFields().size(), equalTo(2));
        assertThat(extractedFields.getDocValueFields().get(0).getName(), equalTo("time"));
        assertThat(extractedFields.getDocValueFields().get(1).getName(), equalTo("airport.keyword"));
        assertThat(extractedFields.getSourceFields().length, equalTo(1));
        assertThat(extractedFields.getSourceFields()[0], equalTo("airline"));
        assertThat(extractedFields.getAllFields().size(), equalTo(3));

        assertThat(findField("time", extractedFields).getSearchField(), equalTo("time"));
        assertThat(findField("airport.keyword", extractedFields).getSearchField(), equalTo("airport.keyword"));
        assertThat(findField("airline.text", extractedFields).getSearchField(), equalTo("airline"));
    }

    public void testBuildGivenTimeFieldIsNotAggregatable() {
        Job.Builder jobBuilder = new Job.Builder("foo");
        jobBuilder.setDataDescription(new DataDescription.Builder());
        Detector.Builder detector = new Detector.Builder("count", null);
        jobBuilder.setAnalysisConfig(new AnalysisConfig.Builder(Collections.singletonList(detector.build())));

        DatafeedConfig.Builder datafeedBuilder = new DatafeedConfig.Builder("feed", jobBuilder.getId());
        datafeedBuilder.setIndices(Collections.singletonList("foo"));

        Map<String, FieldCapabilities> timeCaps = new HashMap<>();
        timeCaps.put("date", createFieldCaps(false));
        FieldCapabilitiesResponse fieldCapabilitiesResponse = mock(FieldCapabilitiesResponse.class);
        when(fieldCapabilitiesResponse.getField("time")).thenReturn(timeCaps);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TimeBasedExtractedFields.build(jobBuilder.build(new Date()), datafeedBuilder.build(), fieldCapabilitiesResponse)
        );
        assertThat(e.getMessage(), equalTo("cannot retrieve time field [time] because it is not aggregatable"));
    }

    public void testBuildGivenTimeFieldIsNotAggregatableInSomeIndices() {
        Job.Builder jobBuilder = new Job.Builder("foo");
        jobBuilder.setDataDescription(new DataDescription.Builder());
        Detector.Builder detector = new Detector.Builder("count", null);
        jobBuilder.setAnalysisConfig(new AnalysisConfig.Builder(Collections.singletonList(detector.build())));

        DatafeedConfig.Builder datafeedBuilder = new DatafeedConfig.Builder("feed", jobBuilder.getId());
        datafeedBuilder.setIndices(Collections.singletonList("foo"));

        Map<String, FieldCapabilities> timeCaps = new HashMap<>();
        timeCaps.put("date", createFieldCaps(true));
        timeCaps.put("text", createFieldCaps(false));
        FieldCapabilitiesResponse fieldCapabilitiesResponse = mock(FieldCapabilitiesResponse.class);
        when(fieldCapabilitiesResponse.getField("time")).thenReturn(timeCaps);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TimeBasedExtractedFields.build(jobBuilder.build(new Date()), datafeedBuilder.build(), fieldCapabilitiesResponse)
        );
        assertThat(e.getMessage(), equalTo("cannot retrieve time field [time] because it is not aggregatable"));
    }

    public void testBuildGivenFieldWithoutMappings() {
        Job.Builder jobBuilder = new Job.Builder("foo");
        jobBuilder.setDataDescription(new DataDescription.Builder());
        Detector.Builder detector = new Detector.Builder("max", "value");
        jobBuilder.setAnalysisConfig(new AnalysisConfig.Builder(Collections.singletonList(detector.build())));

        DatafeedConfig.Builder datafeedBuilder = new DatafeedConfig.Builder("feed", jobBuilder.getId());
        datafeedBuilder.setIndices(Collections.singletonList("foo"));

        Map<String, FieldCapabilities> timeCaps = new HashMap<>();
        timeCaps.put("date", createFieldCaps(true));
        FieldCapabilitiesResponse fieldCapabilitiesResponse = mock(FieldCapabilitiesResponse.class);
        when(fieldCapabilitiesResponse.getField("time")).thenReturn(timeCaps);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TimeBasedExtractedFields.build(jobBuilder.build(new Date()), datafeedBuilder.build(), fieldCapabilitiesResponse)
        );
        assertThat(e.getMessage(), equalTo("cannot retrieve field [value] because it has no mappings"));
    }

    private static FieldCapabilities createFieldCaps(boolean isAggregatable) {
        FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
        when(fieldCaps.isAggregatable()).thenReturn(isAggregatable);
        return fieldCaps;
    }

    private static ExtractedField findField(String name, ExtractedFields fields) {
        return fields.getAllFields().stream().filter(f -> f.getName().equals(name)).findFirst().get();
    }
}
