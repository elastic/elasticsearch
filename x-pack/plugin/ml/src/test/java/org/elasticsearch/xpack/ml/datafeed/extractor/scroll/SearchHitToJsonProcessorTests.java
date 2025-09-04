/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.scroll;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.extractor.DocValueField;
import org.elasticsearch.xpack.ml.extractor.ExtractedField;
import org.elasticsearch.xpack.ml.extractor.ExtractedFields;
import org.elasticsearch.xpack.ml.extractor.SourceSupplier;
import org.elasticsearch.xpack.ml.extractor.TimeField;
import org.elasticsearch.xpack.ml.test.SearchHitBuilder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class SearchHitToJsonProcessorTests extends ESTestCase {

    public void testProcessGivenSingleHit() throws IOException {
        ExtractedField timeField = new TimeField("time", ExtractedField.Method.DOC_VALUE);
        ExtractedField missingField = new DocValueField("missing", Collections.singleton("float"));
        ExtractedField singleField = new DocValueField("single", Collections.singleton("keyword"));
        ExtractedField arrayField = new DocValueField("array", Collections.singleton("keyword"));
        TimeBasedExtractedFields extractedFields = new TimeBasedExtractedFields(
            timeField,
            Arrays.asList(timeField, missingField, singleField, arrayField)
        );

        SearchHit hit = new SearchHitBuilder(8).addField("time", 1000L)
            .addField("single", "a")
            .addField("array", Arrays.asList("b", "c"))
            .build();

        String json = searchHitToString(extractedFields, hit);

        assertThat(json, equalTo("{\"time\":1000,\"single\":\"a\",\"array\":[\"b\",\"c\"]}"));
    }

    public void testProcessGivenMultipleHits() throws IOException {
        ExtractedField timeField = new TimeField("time", ExtractedField.Method.DOC_VALUE);
        ExtractedField missingField = new DocValueField("missing", Collections.singleton("float"));
        ExtractedField singleField = new DocValueField("single", Collections.singleton("keyword"));
        ExtractedField arrayField = new DocValueField("array", Collections.singleton("keyword"));
        TimeBasedExtractedFields extractedFields = new TimeBasedExtractedFields(
            timeField,
            Arrays.asList(timeField, missingField, singleField, arrayField)
        );

        SearchHit hit1 = new SearchHitBuilder(8).addField("time", 1000L)
            .addField("single", "a1")
            .addField("array", Arrays.asList("b1", "c1"))
            .build();

        SearchHit hit2 = new SearchHitBuilder(8).addField("time", 2000L)
            .addField("single", "a2")
            .addField("array", Arrays.asList("b2", "c2"))
            .build();

        String json = searchHitToString(extractedFields, hit1, hit2);

        assertThat(json, equalTo("""
            {"time":1000,"single":"a1","array":["b1","c1"]} \
            {"time":2000,"single":"a2","array":["b2","c2"]}"""));
    }

    private String searchHitToString(ExtractedFields fields, SearchHit... searchHits) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (SearchHitToJsonProcessor hitProcessor = new SearchHitToJsonProcessor(fields, outputStream)) {
            for (int i = 0; i < searchHits.length; i++) {
                hitProcessor.process(searchHits[i], new SourceSupplier(searchHits[i]));
            }
        }
        return outputStream.toString(StandardCharsets.UTF_8);
    }
}
