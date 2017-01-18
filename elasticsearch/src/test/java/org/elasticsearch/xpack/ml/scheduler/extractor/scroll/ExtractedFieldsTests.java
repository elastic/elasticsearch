/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.scheduler.extractor.scroll;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class ExtractedFieldsTests extends ESTestCase {

    private ExtractedField timeField = ExtractedField.newField("time", ExtractedField.ExtractionMethod.DOC_VALUE);

    public void testInvalidConstruction() {
        expectThrows(IllegalArgumentException.class, () -> new ExtractedFields(timeField, Collections.emptyList()));
    }

    public void testTimeFieldOnly() {
        ExtractedFields extractedFields = new ExtractedFields(timeField, Arrays.asList(timeField));

        assertThat(extractedFields.getAllFields(), equalTo(Arrays.asList(timeField)));
        assertThat(extractedFields.timeField(), equalTo("time"));
        assertThat(extractedFields.getDocValueFields(), equalTo(new String[] { timeField.getName() }));
        assertThat(extractedFields.getSourceFields().length, equalTo(0));
    }

    public void testAllTypesOfFields() {
        ExtractedField docValue1 = ExtractedField.newField("doc1", ExtractedField.ExtractionMethod.DOC_VALUE);
        ExtractedField docValue2 = ExtractedField.newField("doc2", ExtractedField.ExtractionMethod.DOC_VALUE);
        ExtractedField scriptField1 = ExtractedField.newField("scripted1", ExtractedField.ExtractionMethod.SCRIPT_FIELD);
        ExtractedField scriptField2 = ExtractedField.newField("scripted2", ExtractedField.ExtractionMethod.SCRIPT_FIELD);
        ExtractedField sourceField1 = ExtractedField.newField("src1", ExtractedField.ExtractionMethod.SOURCE);
        ExtractedField sourceField2 = ExtractedField.newField("src2", ExtractedField.ExtractionMethod.SOURCE);
        ExtractedFields extractedFields = new ExtractedFields(timeField, Arrays.asList(timeField,
                docValue1, docValue2, scriptField1, scriptField2, sourceField1, sourceField2));

        assertThat(extractedFields.getAllFields().size(), equalTo(7));
        assertThat(extractedFields.timeField(), equalTo("time"));
        assertThat(extractedFields.getDocValueFields(), equalTo(new String[] {"time", "doc1", "doc2"}));
        assertThat(extractedFields.getSourceFields(), equalTo(new String[] {"src1", "src2"}));
    }

    public void testTimeFieldValue() {
        SearchHit hit = new ExtractedFieldTests.SearchHitBuilder(1).addField("time", 1000L).build();

        ExtractedFields extractedFields = new ExtractedFields(timeField, Arrays.asList(timeField));

        assertThat(extractedFields.timeFieldValue(hit), equalTo(1000L));
    }

    public void testTimeFieldValueGivenEmptyArray() {
        SearchHit hit = new ExtractedFieldTests.SearchHitBuilder(1).addField("time", Collections.emptyList()).build();

        ExtractedFields extractedFields = new ExtractedFields(timeField, Arrays.asList(timeField));

        expectThrows(RuntimeException.class, () -> extractedFields.timeFieldValue(hit));
    }

    public void testTimeFieldValueGivenValueHasTwoElements() {
        SearchHit hit = new ExtractedFieldTests.SearchHitBuilder(1).addField("time", Arrays.asList(1L, 2L)).build();

        ExtractedFields extractedFields = new ExtractedFields(timeField, Arrays.asList(timeField));

        expectThrows(RuntimeException.class, () -> extractedFields.timeFieldValue(hit));
    }

    public void testTimeFieldValueGivenValueIsString() {
        SearchHit hit = new ExtractedFieldTests.SearchHitBuilder(1).addField("time", "a string").build();

        ExtractedFields extractedFields = new ExtractedFields(timeField, Arrays.asList(timeField));

        expectThrows(RuntimeException.class, () -> extractedFields.timeFieldValue(hit));
    }
}