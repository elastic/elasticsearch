/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket;

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.mapper.CustomTermFreqField;
import org.elasticsearch.index.mapper.DocCountFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Collections.singleton;

public class DocCountProviderTests extends AggregatorTestCase {

    private static final String DOC_COUNT_FIELD = DocCountFieldMapper.NAME;
    private static final String NUMBER_FIELD = "number";

    public void testDocsWithDocCount() throws IOException {
        testAggregation(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(
                List.of(new CustomTermFreqField(DOC_COUNT_FIELD, DOC_COUNT_FIELD, 4), new SortedNumericDocValuesField(NUMBER_FIELD, 1))
            );
            iw.addDocument(
                List.of(new CustomTermFreqField(DOC_COUNT_FIELD, DOC_COUNT_FIELD, 5), new SortedNumericDocValuesField(NUMBER_FIELD, 7))
            );
            iw.addDocument(
                List.of(
                    // Intentionally omit doc_count field
                    new SortedNumericDocValuesField(NUMBER_FIELD, 1)
                )
            );
        }, global -> { assertEquals(10, global.getDocCount()); });
    }

    public void testDocsWithoutDocCount() throws IOException {
        testAggregation(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField(NUMBER_FIELD, 1)));
            iw.addDocument(singleton(new SortedNumericDocValuesField(NUMBER_FIELD, 7)));
            iw.addDocument(singleton(new SortedNumericDocValuesField(NUMBER_FIELD, 1)));
        }, global -> { assertEquals(3, global.getDocCount()); });
    }

    public void testQueryFiltering() throws IOException {
        testAggregation(IntPoint.newRangeQuery(NUMBER_FIELD, 4, 5), iw -> {
            iw.addDocument(List.of(new CustomTermFreqField(DOC_COUNT_FIELD, DOC_COUNT_FIELD, 4), new IntPoint(NUMBER_FIELD, 6)));
            iw.addDocument(List.of(new CustomTermFreqField(DOC_COUNT_FIELD, DOC_COUNT_FIELD, 2), new IntPoint(NUMBER_FIELD, 5)));
            iw.addDocument(
                List.of(
                    // Intentionally omit doc_count field
                    new IntPoint(NUMBER_FIELD, 1)
                )
            );
            iw.addDocument(
                List.of(
                    // Intentionally omit doc_count field
                    new IntPoint(NUMBER_FIELD, 5)
                )
            );
        }, global -> { assertEquals(3, global.getDocCount()); });
    }

    private void testAggregation(Query query, CheckedConsumer<RandomIndexWriter, IOException> indexer, Consumer<InternalFilter> verify)
        throws IOException {
        AggregationBuilder builder = new FilterAggregationBuilder("f", new MatchAllQueryBuilder());
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NUMBER_FIELD, NumberFieldMapper.NumberType.LONG);
        MappedFieldType docCountFieldType = new DocCountFieldMapper.DocCountFieldType();
        testCase(indexer, verify, new AggTestConfig(builder, fieldType, docCountFieldType).withQuery(query));
    }
}
