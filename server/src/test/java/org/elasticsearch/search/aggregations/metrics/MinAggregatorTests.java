/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.metrics.min.InternalMin;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregator;

public class MinAggregatorTests extends AggregatorTestCase {

    public void testMinAggregator_numericDv() throws Exception {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        Document document = new Document();
        document.add(new NumericDocValuesField("number", 9));
        indexWriter.addDocument(document);
        document = new Document();
        document.add(new NumericDocValuesField("number", 7));
        indexWriter.addDocument(document);
        document = new Document();
        document.add(new NumericDocValuesField("number", 5));
        indexWriter.addDocument(document);
        document = new Document();
        document.add(new NumericDocValuesField("number", 3));
        indexWriter.addDocument(document);
        document = new Document();
        document.add(new NumericDocValuesField("number", 1));
        indexWriter.addDocument(document);
        document = new Document();
        document.add(new NumericDocValuesField("number", -1));
        indexWriter.addDocument(document);
        indexWriter.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("_name").field("number");
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
        fieldType.setName("number");

        MinAggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType);
        aggregator.preCollection();
        indexSearcher.search(new MatchAllDocsQuery(), aggregator);
        aggregator.postCollection();
        InternalMin result = (InternalMin) aggregator.buildAggregation(0L);
        assertEquals(-1.0, result.getValue(), 0);

        indexReader.close();
        directory.close();
    }

    public void testMinAggregator_sortedNumericDv() throws Exception {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        Document document = new Document();
        document.add(new SortedNumericDocValuesField("number", 9));
        document.add(new SortedNumericDocValuesField("number", 7));
        indexWriter.addDocument(document);
        document = new Document();
        document.add(new SortedNumericDocValuesField("number", 5));
        document.add(new SortedNumericDocValuesField("number", 3));
        indexWriter.addDocument(document);
        document = new Document();
        document.add(new SortedNumericDocValuesField("number", 1));
        document.add(new SortedNumericDocValuesField("number", -1));
        indexWriter.addDocument(document);
        indexWriter.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("_name").field("number");
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
        fieldType.setName("number");

        MinAggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType);
        aggregator.preCollection();
        indexSearcher.search(new MatchAllDocsQuery(), aggregator);
        aggregator.postCollection();
        InternalMin result = (InternalMin) aggregator.buildAggregation(0L);
        assertEquals(-1.0, result.getValue(), 0);

        indexReader.close();
        directory.close();
    }

    public void testMinAggregator_noValue() throws Exception {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        Document document = new Document();
        document.add(new SortedNumericDocValuesField("number1", 7));
        indexWriter.addDocument(document);
        document = new Document();
        document.add(new SortedNumericDocValuesField("number1", 3));
        indexWriter.addDocument(document);
        document = new Document();
        document.add(new SortedNumericDocValuesField("number1", 1));
        indexWriter.addDocument(document);
        indexWriter.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("_name").field("number2");
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
        fieldType.setName("number2");

        MinAggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType);
        aggregator.preCollection();
        indexSearcher.search(new MatchAllDocsQuery(), aggregator);
        aggregator.postCollection();
        InternalMin result = (InternalMin) aggregator.buildAggregation(0L);
        assertEquals(Double.POSITIVE_INFINITY, result.getValue(), 0);

        indexReader.close();
        directory.close();
    }

    public void testMinAggregator_noDocs() throws Exception {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        indexWriter.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("_name").field("number");
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
        fieldType.setName("number");

        MinAggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType);
        aggregator.preCollection();
        indexSearcher.search(new MatchAllDocsQuery(), aggregator);
        aggregator.postCollection();
        InternalMin result = (InternalMin) aggregator.buildAggregation(0L);
        assertEquals(Double.POSITIVE_INFINITY, result.getValue(), 0);

        indexReader.close();
        directory.close();
    }

}
