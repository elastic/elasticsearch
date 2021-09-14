/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@ESIntegTestCase.SuiteScopeTestCase
public abstract class AbstractNumericTestCase extends ESIntegTestCase {
    protected static long minValue, maxValue, minValues, maxValues;

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        createIndex("idx_unmapped");

        List<IndexRequestBuilder> builders = new ArrayList<>();

        final int numDocs = 10;
        for (int i = 0; i < numDocs; i++) { // TODO randomize the size and the params in here?
            builders.add(client().prepareIndex("idx").setId(String.valueOf(i)).setSource(jsonBuilder()
                    .startObject()
                    .field("value", i+1)
                    .startArray("values").value(i+2).value(i+3).endArray()
                    .endObject()));
        }
        minValue = 1;
        minValues = 2;
        maxValue = numDocs;
        maxValues = numDocs + 2;
        indexRandom(true, builders);

        // creating an index to test the empty buckets functionality. The way it works is by indexing
        // two docs {value: 0} and {value : 2}, then building a histogram agg with interval 1 and with empty
        // buckets computed.. the empty bucket is the one associated with key "1". then each test will have
        // to check that this bucket exists with the appropriate sub aggregations.
        prepareCreate("empty_bucket_idx").setMapping("value", "type=integer").execute().actionGet();
        builders = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            builders.add(client().prepareIndex("empty_bucket_idx").setId(String.valueOf(i)).setSource(jsonBuilder()
                    .startObject()
                    .field("value", i*2)
                    .endObject()));
        }
        indexRandom(true, builders);
        ensureSearchable();
    }

    public void testEmptyAggregation() throws Exception {}

    public void testUnmapped() throws Exception {}

    public void testSingleValuedField() throws Exception {}

    public void testSingleValuedFieldGetProperty() throws Exception {}

    public void testSingleValuedFieldPartiallyUnmapped() throws Exception {}

    public void testSingleValuedFieldWithValueScript() throws Exception {}

    public void testSingleValuedFieldWithValueScriptWithParams() throws Exception {}

    public void testMultiValuedField() throws Exception {}

    public void testMultiValuedFieldWithValueScript() throws Exception {}

    public void testMultiValuedFieldWithValueScriptWithParams() throws Exception {}

    public void testScriptSingleValued() throws Exception {}

    public void testScriptSingleValuedWithParams() throws Exception {}

    public void testScriptMultiValued() throws Exception {}

    public void testScriptMultiValuedWithParams() throws Exception {}

    public void testOrderByEmptyAggregation() throws Exception {}
}
