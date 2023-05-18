/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.sampler;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.TextFieldMapper.TextFieldType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilters;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;

import java.io.IOException;

public class SamplerAggregatorTests extends AggregatorTestCase {
    /**
     * Uses the sampler aggregation to find the minimum value of a field out of the top 3 scoring documents in a search.
     */
    public void testSampler() throws IOException {
        TextFieldType textFieldType = new TextFieldType("text", randomBoolean());
        MappedFieldType numericFieldType = new NumberFieldMapper.NumberFieldType("int", NumberFieldMapper.NumberType.LONG);

        IndexWriterConfig indexWriterConfig = newIndexWriterConfig();
        indexWriterConfig.setMaxBufferedDocs(100);
        indexWriterConfig.setRAMBufferSizeMB(100); // flush on open to have a single segment with predictable docIds
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, indexWriterConfig)) {
            for (long value : new long[] { 7, 3, -10, -6, 5, 50 }) {
                Document doc = new Document();
                StringBuilder text = new StringBuilder();
                for (int i = 0; i < value; i++) {
                    text.append("good ");
                }
                doc.add(new Field("text", text.toString(), TextFieldMapper.Defaults.FIELD_TYPE));
                doc.add(new SortedNumericDocValuesField("int", value));
                w.addDocument(doc);
            }

            SamplerAggregationBuilder aggBuilder = new SamplerAggregationBuilder("sampler").shardSize(3)
                .subAggregation(new MinAggregationBuilder("min").field("int"));
            try (DirectoryReader reader = DirectoryReader.open(w)) {
                assertEquals("test expects a single segment", 1, reader.leaves().size());
                IndexSearcher searcher = newIndexSearcher(reader);
                InternalSampler sampler = searchAndReduce(
                    searcher,
                    new AggTestConfig(aggBuilder, textFieldType, numericFieldType).withQuery(new TermQuery(new Term("text", "good")))
                );
                Min min = sampler.getAggregations().get("min");
                assertEquals(5.0, min.value(), 0);
                assertTrue(AggregationInspectionHelper.hasValue(sampler));
            }
        }
    }

    public void testRidiculousSize() throws IOException {
        TextFieldType textFieldType = new TextFieldType("text", randomBoolean());
        MappedFieldType numericFieldType = new NumberFieldMapper.NumberFieldType("int", NumberFieldMapper.NumberType.LONG);

        IndexWriterConfig indexWriterConfig = newIndexWriterConfig();
        indexWriterConfig.setMaxBufferedDocs(100);
        indexWriterConfig.setRAMBufferSizeMB(100); // flush on open to have a single segment with predictable docIds
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, indexWriterConfig)) {
            for (long value : new long[] { 7, 3, -10, -6, 5, 50 }) {
                Document doc = new Document();
                StringBuilder text = new StringBuilder();
                for (int i = 0; i < value; i++) {
                    text.append("good ");
                }
                doc.add(new Field("text", text.toString(), TextFieldMapper.Defaults.FIELD_TYPE));
                doc.add(new SortedNumericDocValuesField("int", value));
                w.addDocument(doc);
            }

            // Test with an outrageously large size to ensure that the maxDoc protection works
            SamplerAggregationBuilder aggBuilder = new SamplerAggregationBuilder("sampler").shardSize(Integer.MAX_VALUE)
                .subAggregation(new MinAggregationBuilder("min").field("int"));
            try (DirectoryReader reader = DirectoryReader.open(w)) {
                assertEquals("test expects a single segment", 1, reader.leaves().size());
                IndexSearcher searcher = newIndexSearcher(reader);
                InternalSampler sampler = searchAndReduce(
                    searcher,
                    new AggTestConfig(aggBuilder, textFieldType, numericFieldType).withQuery(new TermQuery(new Term("text", "good")))
                );
                Min min = sampler.getAggregations().get("min");
                assertEquals(3.0, min.value(), 0);
                assertTrue(AggregationInspectionHelper.hasValue(sampler));
            }
        }
    }

    /**
     * Tests that the sampler aggregation works correctly if the parent bucket does not contain any hit.
     */
    public void testEmptyParentBucket() throws Exception {
        IndexWriterConfig indexWriterConfig = newIndexWriterConfig();
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, indexWriterConfig)) {

            writer.addDocument(new Document());

            try (IndexReader reader = DirectoryReader.open(writer)) {
                IndexSearcher searcher = new IndexSearcher(reader);

                QueryBuilder[] filters = new QueryBuilder[] { new MatchAllQueryBuilder(), new MatchNoneQueryBuilder() };
                FiltersAggregationBuilder samplerParent = new FiltersAggregationBuilder("filters", filters);
                TermsAggregationBuilder samplerChild = new TermsAggregationBuilder("child").field("field");
                SamplerAggregationBuilder sampler = new SamplerAggregationBuilder("sampler").subAggregation(samplerChild);
                samplerParent.subAggregation(sampler);

                InternalFilters response = searchAndReduce(searcher, new AggTestConfig(samplerParent));
                assertEquals(response.getBuckets().size(), 2);
                assertEquals(response.getBuckets().get(0).getDocCount(), 1);
                assertEquals(response.getBuckets().get(1).getDocCount(), 0);
            }
        }
    }

}
