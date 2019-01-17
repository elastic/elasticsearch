/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.datafeed.extractor.fields.ExtractedField;
import org.elasticsearch.xpack.ml.datafeed.extractor.fields.ExtractedFields;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DataFrameDataExtractorFactoryTests extends ESTestCase {

    public void testDetectExtractedFields_GivenFloatField() {
        FieldCapabilitiesResponse fieldCapabilities= new MockFieldCapsResponseBuilder()
            .addAggregatableField("some_float", "float").build();

        ExtractedFields extractedFields = DataFrameDataExtractorFactory.detectExtractedFields(fieldCapabilities);

        List<ExtractedField> allFields = extractedFields.getAllFields();
        assertThat(allFields.size(), equalTo(1));
        assertThat(allFields.get(0).getName(), equalTo("some_float"));
    }

    public void testDetectExtractedFields_GivenNumericFieldWithMultipleTypes() {
        FieldCapabilitiesResponse fieldCapabilities= new MockFieldCapsResponseBuilder()
            .addAggregatableField("some_number", "long", "integer", "short", "byte", "double", "float", "half_float", "scaled_float")
            .build();

        ExtractedFields extractedFields = DataFrameDataExtractorFactory.detectExtractedFields(fieldCapabilities);

        List<ExtractedField> allFields = extractedFields.getAllFields();
        assertThat(allFields.size(), equalTo(1));
        assertThat(allFields.get(0).getName(), equalTo("some_number"));
    }

    public void testDetectExtractedFields_GivenNonNumericField() {
        FieldCapabilitiesResponse fieldCapabilities= new MockFieldCapsResponseBuilder()
            .addAggregatableField("some_keyword", "keyword").build();

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> DataFrameDataExtractorFactory.detectExtractedFields(fieldCapabilities));
        assertThat(e.getMessage(), equalTo("No compatible fields could be detected"));
    }

    public void testDetectExtractedFields_GivenFieldWithNumericAndNonNumericTypes() {
        FieldCapabilitiesResponse fieldCapabilities= new MockFieldCapsResponseBuilder()
            .addAggregatableField("indecisive_field", "float", "keyword").build();

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> DataFrameDataExtractorFactory.detectExtractedFields(fieldCapabilities));
        assertThat(e.getMessage(), equalTo("No compatible fields could be detected"));
    }

    public void testDetectExtractedFields_GivenMultipleFields() {
        FieldCapabilitiesResponse fieldCapabilities= new MockFieldCapsResponseBuilder()
            .addAggregatableField("some_float", "float")
            .addAggregatableField("some_long", "long")
            .addAggregatableField("some_keyword", "keyword")
            .build();

        ExtractedFields extractedFields = DataFrameDataExtractorFactory.detectExtractedFields(fieldCapabilities);

        List<ExtractedField> allFields = extractedFields.getAllFields();
        assertThat(allFields.size(), equalTo(2));
        assertThat(allFields.stream().map(ExtractedField::getName).collect(Collectors.toSet()),
            containsInAnyOrder("some_float", "some_long"));
    }

    public void testDetectExtractedFields_GivenIgnoredField() {
        FieldCapabilitiesResponse fieldCapabilities= new MockFieldCapsResponseBuilder()
            .addAggregatableField("_id", "float").build();

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> DataFrameDataExtractorFactory.detectExtractedFields(fieldCapabilities));
        assertThat(e.getMessage(), equalTo("No compatible fields could be detected"));
    }

    public void testDetectExtractedFields_ShouldSortFieldsAlphabetically() {
        int fieldCount = randomIntBetween(10, 20);
        List<String> fields = new ArrayList<>();
        for (int i = 0; i < fieldCount; i++) {
            fields.add(randomAlphaOfLength(20));
        }
        List<String> sortedFields = new ArrayList<>(fields);
        Collections.sort(sortedFields);

        MockFieldCapsResponseBuilder mockFieldCapsResponseBuilder = new MockFieldCapsResponseBuilder();
        for (String field : fields) {
            mockFieldCapsResponseBuilder.addAggregatableField(field, "float");
        }
        FieldCapabilitiesResponse fieldCapabilities = mockFieldCapsResponseBuilder.build();

        ExtractedFields extractedFields = DataFrameDataExtractorFactory.detectExtractedFields(fieldCapabilities);

        List<String> extractedFieldNames = extractedFields.getAllFields().stream().map(ExtractedField::getName)
            .collect(Collectors.toList());
        assertThat(extractedFieldNames, equalTo(sortedFields));
    }

    private static class MockFieldCapsResponseBuilder {

        private final Map<String, Map<String, FieldCapabilities>> fieldCaps = new HashMap<>();

        private MockFieldCapsResponseBuilder addAggregatableField(String field, String... types) {
            Map<String, FieldCapabilities> caps = new HashMap<>();
            for (String type : types) {
                caps.put(type, new FieldCapabilities(field, type, true, true));
            }
            fieldCaps.put(field, caps);
            return this;
        }

        private FieldCapabilitiesResponse build() {
            FieldCapabilitiesResponse response = mock(FieldCapabilitiesResponse.class);
            when(response.get()).thenReturn(fieldCaps);

            for (String field : fieldCaps.keySet()) {
                when(response.getField(field)).thenReturn(fieldCaps.get(field));
            }
            return response;
        }
    }
}
