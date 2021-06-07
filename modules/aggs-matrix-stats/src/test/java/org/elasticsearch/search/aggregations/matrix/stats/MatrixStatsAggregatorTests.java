/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.matrix.stats;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.matrix.MatrixAggregationPlugin;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MatrixStatsAggregatorTests extends AggregatorTestCase {

    public void testNoData() throws Exception {
        MappedFieldType ft =
            new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.DOUBLE);

        try (Directory directory = newDirectory();
            RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
            if (randomBoolean()) {
                indexWriter.addDocument(Collections.singleton(new StringField("another_field", "value", Field.Store.NO)));
            }
            try (IndexReader reader = indexWriter.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                MatrixStatsAggregationBuilder aggBuilder = new MatrixStatsAggregationBuilder("my_agg")
                    .fields(Collections.singletonList("field"));
                InternalMatrixStats stats = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, ft);
                assertNull(stats.getStats());
                assertEquals(0L, stats.getDocCount());
            }
        }
    }

    public void testUnmapped() throws Exception {
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.DOUBLE);

        try (Directory directory = newDirectory();
             RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
            if (randomBoolean()) {
                indexWriter.addDocument(Collections.singleton(new StringField("another_field", "value", Field.Store.NO)));
            }
            try (IndexReader reader = indexWriter.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                MatrixStatsAggregationBuilder aggBuilder = new MatrixStatsAggregationBuilder("my_agg")
                    .fields(Collections.singletonList("bogus"));
                InternalMatrixStats stats = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, ft);
                assertNull(stats.getStats());
                assertEquals(0L, stats.getDocCount());
            }
        }
    }

    public void testTwoFields() throws Exception {
        String fieldA = "a";
        MappedFieldType ftA = new NumberFieldMapper.NumberFieldType(fieldA, NumberFieldMapper.NumberType.DOUBLE);
        String fieldB = "b";
        MappedFieldType ftB = new NumberFieldMapper.NumberFieldType(fieldB, NumberFieldMapper.NumberType.DOUBLE);

        try (Directory directory = newDirectory();
             RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {

            int numDocs = scaledRandomIntBetween(8192, 16384);
            Double[] fieldAValues = new Double[numDocs];
            Double[] fieldBValues = new Double[numDocs];
            for (int docId = 0; docId < numDocs; docId++) {
                Document document = new Document();
                fieldAValues[docId] = randomDouble();
                document.add(new SortedNumericDocValuesField(fieldA, NumericUtils.doubleToSortableLong(fieldAValues[docId])));

                fieldBValues[docId] = randomDouble();
                document.add(new SortedNumericDocValuesField(fieldB, NumericUtils.doubleToSortableLong(fieldBValues[docId])));
                indexWriter.addDocument(document);
            }

            MultiPassStats multiPassStats = new MultiPassStats(fieldA, fieldB);
            multiPassStats.computeStats(Arrays.asList(fieldAValues), Arrays.asList(fieldBValues));
            try (IndexReader reader = indexWriter.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                MatrixStatsAggregationBuilder aggBuilder = new MatrixStatsAggregationBuilder("my_agg")
                    .fields(Arrays.asList(fieldA, fieldB));
                InternalMatrixStats stats = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, ftA, ftB);
                multiPassStats.assertNearlyEqual(stats);
                assertTrue(MatrixAggregationInspectionHelper.hasValue(stats));
            }
        }
    }

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return Collections.singletonList(new MatrixAggregationPlugin());
    }
}
