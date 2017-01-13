/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.scheduler.extractor;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHitField;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class SearchHitFieldExtractorTests extends ESTestCase {

    public void testExtractTimeFieldGivenHitContainsNothing() throws IOException {
        InternalSearchHit searchHit = new InternalSearchHit(42);

        expectThrows(RuntimeException.class, () -> SearchHitFieldExtractor.extractTimeField(searchHit, "time"));
    }

    public void testExtractTimeFieldGivenSingleValueInFields() throws IOException {
        InternalSearchHit searchHit = new InternalSearchHit(42);
        Map<String, SearchHitField> fields = new HashMap<>();
        fields.put("time", new InternalSearchHitField("time", Arrays.asList(3L)));
        searchHit.fields(fields);

        assertThat(SearchHitFieldExtractor.extractTimeField(searchHit, "time"), equalTo(3L));
    }

    public void testExtractTimeFieldGivenSingleValueInSource() throws IOException {
        InternalSearchHit searchHit = new InternalSearchHit(42);
        searchHit.sourceRef(new BytesArray("{\"time\":1482418307000}"));

        assertThat(SearchHitFieldExtractor.extractTimeField(searchHit, "time"), equalTo(1482418307000L));
    }

    public void testExtractTimeFieldGivenArrayValue() throws IOException {
        InternalSearchHit searchHit = new InternalSearchHit(42);
        Map<String, SearchHitField> fields = new HashMap<>();
        fields.put("time", new InternalSearchHitField("time", Arrays.asList(3L, 5L)));
        searchHit.fields(fields);

        expectThrows(RuntimeException.class, () -> SearchHitFieldExtractor.extractTimeField(searchHit, "time"));
    }

    public void testExtractTimeFieldGivenSingleNonLongValue() throws IOException {
        InternalSearchHit searchHit = new InternalSearchHit(42);
        Map<String, SearchHitField> fields = new HashMap<>();
        fields.put("time", new InternalSearchHitField("time", Arrays.asList(3)));
        searchHit.fields(fields);

        expectThrows(RuntimeException.class, () -> SearchHitFieldExtractor.extractTimeField(searchHit, "time"));
    }
}
