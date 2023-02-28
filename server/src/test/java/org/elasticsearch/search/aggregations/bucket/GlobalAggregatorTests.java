/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.global.InternalGlobal;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;

import java.io.IOException;
import java.util.List;
import java.util.function.BiConsumer;

public class GlobalAggregatorTests extends AggregatorTestCase {
    public void testNoDocs() throws IOException {
        testCase(iw -> {
            // Intentionally not writing any docs
        }, new MatchAllDocsQuery(), (global, min) -> {
            assertEquals(0, global.getDocCount());
            assertEquals(Double.POSITIVE_INFINITY, min.value(), 0);
        });
    }

    public void testSomeDocs() throws IOException {
        testCase(iw -> {
            iw.addDocument(List.of(new SortedNumericDocValuesField("number", 7)));
            iw.addDocument(List.of(new SortedNumericDocValuesField("number", 1)));
        }, new MatchAllDocsQuery(), (global, min) -> {
            assertEquals(2, global.getDocCount());
            assertEquals(1, min.value(), 0);
        });
    }

    public void testIgnoresQuery() throws IOException {
        testCase(iw -> {
            iw.addDocument(List.of(new SortedNumericDocValuesField("number", 7)));
            iw.addDocument(List.of(new SortedNumericDocValuesField("number", 1)));
        }, LongPoint.newRangeQuery("number", 2, Long.MAX_VALUE), (global, min) -> {
            assertEquals(2, global.getDocCount());
            assertEquals(1, min.value(), 0);
        });
    }

    private void testCase(
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Query topLevelQuery,
        BiConsumer<InternalGlobal, Min> verify
    ) throws IOException {
        GlobalAggregationBuilder aggregationBuilder = new GlobalAggregationBuilder("_name");
        aggregationBuilder.subAggregation(new MinAggregationBuilder("in_global").field("number"));
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.LONG);

        testCase(buildIndex, (InternalGlobal result) -> {
            Min min = result.getAggregations().get("in_global");
            verify.accept(result, min);
        }, new AggTestConfig(aggregationBuilder, fieldType).withQuery(topLevelQuery));
    }
}
