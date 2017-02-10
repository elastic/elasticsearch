/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.scroll;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;

public class SearchHitToJsonProcessorTests extends ESTestCase {

    public void testProcessGivenSingleHit() throws IOException {
        ExtractedField timeField = ExtractedField.newField("time", ExtractedField.ExtractionMethod.DOC_VALUE);
        ExtractedField missingField = ExtractedField.newField("missing", ExtractedField.ExtractionMethod.DOC_VALUE);
        ExtractedField singleField = ExtractedField.newField("single", ExtractedField.ExtractionMethod.DOC_VALUE);
        ExtractedField arrayField = ExtractedField.newField("array", ExtractedField.ExtractionMethod.DOC_VALUE);
        ExtractedFields extractedFields = new ExtractedFields(timeField, Arrays.asList(timeField, missingField, singleField, arrayField));

        SearchHit hit = new ExtractedFieldTests.SearchHitBuilder(8)
                .addField("time", 1000L)
                .addField("single", "a")
                .addField("array", Arrays.asList("b", "c"))
                .build();

        String json = searchHitToString(extractedFields, hit);

        assertThat(json, equalTo("{\"time\":1000,\"single\":\"a\",\"array\":[\"b\",\"c\"]}"));
    }

    public void testProcessGivenMultipleHits() throws IOException {
        ExtractedField timeField = ExtractedField.newField("time", ExtractedField.ExtractionMethod.DOC_VALUE);
        ExtractedField missingField = ExtractedField.newField("missing", ExtractedField.ExtractionMethod.DOC_VALUE);
        ExtractedField singleField = ExtractedField.newField("single", ExtractedField.ExtractionMethod.DOC_VALUE);
        ExtractedField arrayField = ExtractedField.newField("array", ExtractedField.ExtractionMethod.DOC_VALUE);
        ExtractedFields extractedFields = new ExtractedFields(timeField, Arrays.asList(timeField, missingField, singleField, arrayField));

        SearchHit hit1 = new ExtractedFieldTests.SearchHitBuilder(8)
                .addField("time", 1000L)
                .addField("single", "a1")
                .addField("array", Arrays.asList("b1", "c1"))
                .build();

        SearchHit hit2 = new ExtractedFieldTests.SearchHitBuilder(8)
                .addField("time", 2000L)
                .addField("single", "a2")
                .addField("array", Arrays.asList("b2", "c2"))
                .build();

        String json = searchHitToString(extractedFields, hit1, hit2);

        assertThat(json, equalTo("{\"time\":1000,\"single\":\"a1\",\"array\":[\"b1\",\"c1\"]} " +
                "{\"time\":2000,\"single\":\"a2\",\"array\":[\"b2\",\"c2\"]}"));
    }

    private String searchHitToString(ExtractedFields fields, SearchHit... searchHits) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (SearchHitToJsonProcessor hitProcessor = new SearchHitToJsonProcessor(fields, outputStream)) {
            for (int i = 0; i < searchHits.length; i++) {
                hitProcessor.process(searchHits[i]);
            }
        }
        return outputStream.toString(StandardCharsets.UTF_8.name());
    }
}
