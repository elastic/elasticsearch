/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics.stringstats;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.search.aggregations.support.ValueType;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Collections.singleton;

public class StringStatsAggregatorTests extends AggregatorTestCase {

    private void testCase(Query query,
                          CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
                          Consumer<InternalStringStats> verify) throws IOException {
        TextFieldMapper.TextFieldType fieldType = new TextFieldMapper.TextFieldType();
        fieldType.setName("text");
        fieldType.setFielddata(true);

        AggregationBuilder aggregationBuilder = new StringStatsAggregationBuilder("_name").field("text");
        testCase(aggregationBuilder, query, buildIndex, verify, fieldType);
    }

    private void testCase(AggregationBuilder aggregationBuilder, Query query,
                          CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
                          Consumer<InternalStringStats> verify, MappedFieldType fieldType) throws IOException {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        buildIndex.accept(indexWriter);
        indexWriter.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

        StringStatsAggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType);
        aggregator.preCollection();
        indexSearcher.search(query, aggregator);
        aggregator.postCollection();
        verify.accept((InternalStringStats) aggregator.buildAggregation(0L));

        indexReader.close();
        directory.close();
    }

    public void testNoDocs() throws IOException {
        this.<InternalStringStats>testCase(new MatchAllDocsQuery(), iw -> {
            // Intentionally not writing any docs
        }, stats -> {
            assertEquals(0, stats.getCount());
            assertEquals(Integer.MIN_VALUE, stats.getMaxLength());
            assertEquals(Integer.MAX_VALUE, stats.getMinLength());
            assertEquals(Double.NaN, stats.getAvgLength(), 0);
            assertTrue(stats.getDistribution().isEmpty());
            assertEquals(0.0, stats.getEntropy(), 0);
        });
    }

    public void testUnmappedField() throws IOException {
        StringStatsAggregationBuilder aggregationBuilder = new StringStatsAggregationBuilder("_name").field("text");
        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            for(int i = 0; i < 10; i++) {
                iw.addDocument(singleton(new TextField("text", "test" + i, Field.Store.NO)));
            }
        }, stats -> {
            assertEquals(0, stats.getCount());
            assertEquals(Integer.MIN_VALUE, stats.getMaxLength());
            assertEquals(Integer.MAX_VALUE, stats.getMinLength());
            assertEquals(Double.NaN, stats.getAvgLength(), 0);
            assertTrue(stats.getDistribution().isEmpty());
            assertEquals(0.0, stats.getEntropy(), 0);

        }, null);
    }

    public void testUnmappedWithMissingField() throws IOException {
        StringStatsAggregationBuilder aggregationBuilder = new StringStatsAggregationBuilder("_name")
            .field("text")
            .missing("abca");
        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            for(int i=0; i < 10; i++) {
                iw.addDocument(singleton(new TextField("text", "test" + i, Field.Store.NO)));
            }
        }, stats -> {
            assertEquals(10, stats.getCount());
            assertEquals(4, stats.getMaxLength());
            assertEquals(4, stats.getMinLength());
            assertEquals(4.0, stats.getAvgLength(), 0);
            assertEquals(3, stats.getDistribution().size());
            assertEquals(0.50, stats.getDistribution().get("a"), 0);
            assertEquals(0.25, stats.getDistribution().get("b"), 0);
            assertEquals(0.25, stats.getDistribution().get("c"), 0);
            assertEquals(1.5, stats.getEntropy(), 0);
        }, null);
    }

    public void testSingleValuedField() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            for(int i=0; i < 10; i++) {
                iw.addDocument(singleton(new TextField("text", "test" + i, Field.Store.NO)));
            }
        }, stats -> {
            assertEquals(10, stats.getCount());
            assertEquals(5, stats.getMaxLength());
            assertEquals(5, stats.getMinLength());
            assertEquals(5.0, stats.getAvgLength(), 0);
            assertEquals(13, stats.getDistribution().size());
            assertEquals(0.4, stats.getDistribution().get("t"), 0);
            assertEquals(0.2, stats.getDistribution().get("e"), 0);
            assertEquals(0.02, stats.getDistribution().get("0"), 0);
            assertEquals(2.58631, stats.getEntropy(), 0.00001);
        });
    }

    public void testNoMatchingField() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            for(int i=0; i < 10; i++) {
                iw.addDocument(singleton(new TextField("wrong_field", "test" + i, Field.Store.NO)));
            }
        }, stats -> {
            assertEquals(0, stats.getCount());
            assertEquals(Integer.MIN_VALUE, stats.getMaxLength());
            assertEquals(Integer.MAX_VALUE, stats.getMinLength());
            assertEquals(Double.NaN, stats.getAvgLength(), 0);
            assertTrue(stats.getDistribution().isEmpty());
            assertEquals(0.0, stats.getEntropy(), 0);
        });
    }

    public void testQueryFiltering() throws IOException {
        testCase(new TermInSetQuery("text", new BytesRef("test0"), new BytesRef("test1")), iw -> {
            for(int i=0; i < 10; i++) {
                iw.addDocument(singleton(new TextField("text", "test" + i, Field.Store.NO)));
            }
        }, stats -> {
            assertEquals(2, stats.getCount());
            assertEquals(5, stats.getMaxLength());
            assertEquals(5, stats.getMinLength());
            assertEquals(5.0, stats.getAvgLength(), 0);
            assertEquals(5, stats.getDistribution().size());
            assertEquals(0.4, stats.getDistribution().get("t"), 0);
            assertEquals(0.2, stats.getDistribution().get("e"), 0);
            assertEquals(0.1, stats.getDistribution().get("0"), 0);
            assertEquals(2.12193, stats.getEntropy(), 0.00001);
        });
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/47469")
    public void testSingleValuedFieldWithFormatter() throws IOException {
        TextFieldMapper.TextFieldType fieldType = new TextFieldMapper.TextFieldType();
        fieldType.setName("text");
        fieldType.setFielddata(true);

        StringStatsAggregationBuilder aggregationBuilder = new StringStatsAggregationBuilder("_name")
            .field("text")
            .format("0000.00")
            .showDistribution(true);

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            for(int i=0; i < 10; i++) {
                iw.addDocument(singleton(new TextField("text", "test" + i, Field.Store.NO)));
            }
        }, stats -> {
            assertEquals("0010.00", stats.getCountAsString());
            assertEquals("0005.00", stats.getMaxLengthAsString());
            assertEquals("0005.00", stats.getMinLengthAsString());
            assertEquals("0005.00", stats.getAvgLengthAsString());
            assertEquals("0002.58", stats.getEntropyAsString());
        }, fieldType);
    }

    /**
     * Test a string_stats aggregation as a subaggregation of a terms aggregation
     */
    public void testNestedAggregation() throws IOException {
        MappedFieldType numericFieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.INTEGER);
        numericFieldType.setName("value");
        numericFieldType.setHasDocValues(true);

        TextFieldMapper.TextFieldType textFieldType = new TextFieldMapper.TextFieldType();
        textFieldType.setName("text");
        textFieldType.setFielddata(true);

        TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("terms", ValueType.NUMERIC)
            .field("value")
            .subAggregation(new StringStatsAggregationBuilder("text_stats").field("text").valueType(ValueType.STRING));

        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        final int numDocs = 10;
        for (int i = 0; i < numDocs; i++) {
            for (int j = 0; j < 4; j++)
            indexWriter.addDocument(List.of(
                new NumericDocValuesField("value", i + 1),
                new TextField("text", "test" + j, Field.Store.NO))
            );
        }
        indexWriter.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

        TermsAggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, numericFieldType, textFieldType);
        aggregator.preCollection();
        indexSearcher.search(new MatchAllDocsQuery(), aggregator);
        aggregator.postCollection();

        Terms terms = (Terms) aggregator.buildAggregation(0L);
        assertNotNull(terms);
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertNotNull(buckets);
        assertEquals(10, buckets.size());

        for (int i = 0; i < 10; i++) {
            Terms.Bucket bucket = buckets.get(i);
            assertNotNull(bucket);
            assertEquals((long) i + 1, bucket.getKeyAsNumber());
            assertEquals(4L, bucket.getDocCount());

            InternalStringStats stats = bucket.getAggregations().get("text_stats");
            assertNotNull(stats);
            assertEquals(4L, stats.getCount());
            assertEquals(5, stats.getMaxLength());
            assertEquals(5, stats.getMinLength());
            assertEquals(5.0, stats.getAvgLength(), 0);
            assertEquals(7, stats.getDistribution().size());
            assertEquals(0.4, stats.getDistribution().get("t"), 0);
            assertEquals(0.2, stats.getDistribution().get("e"), 0);
            assertEquals(2.32193, stats.getEntropy(), 0.00001);
        }

        indexReader.close();
        directory.close();
    }

}
