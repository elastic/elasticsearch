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

package org.elasticsearch.search.aggregations.bucket.sampler;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
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
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.TextFieldMapper.TextFieldType;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;

import java.io.IOException;

public class SamplerAggregatorTests extends AggregatorTestCase {
    /**
     * Uses the sampler aggregation to find the minimum value of a field out of the top 3 scoring documents in a search.
     */
    public void testSampler() throws IOException {
        TextFieldType textFieldType = new TextFieldType();
        textFieldType.setIndexAnalyzer(new NamedAnalyzer("foo", AnalyzerScope.GLOBAL, new StandardAnalyzer()));
        MappedFieldType numericFieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
        numericFieldType.setName("int");

        IndexWriterConfig indexWriterConfig = newIndexWriterConfig();
        indexWriterConfig.setMaxBufferedDocs(100);
        indexWriterConfig.setRAMBufferSizeMB(100); // flush on open to have a single segment with predictable docIds
        try (Directory dir = newDirectory();
                IndexWriter w = new IndexWriter(dir, indexWriterConfig)) {
            for (long value : new long[] {7, 3, -10, -6, 5, 50}) {
                Document doc = new Document();
                StringBuilder text = new StringBuilder();
                for (int i = 0; i < value; i++) {
                    text.append("good ");
                }
                doc.add(new Field("text", text.toString(), textFieldType));
                doc.add(new SortedNumericDocValuesField("int", value));
                w.addDocument(doc);
            }

            SamplerAggregationBuilder aggBuilder = new SamplerAggregationBuilder("sampler")
                    .shardSize(3)
                    .subAggregation(new MinAggregationBuilder("min")
                            .field("int"));
            try (IndexReader reader = DirectoryReader.open(w)) {
                assertEquals("test expects a single segment", 1, reader.leaves().size());
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalSampler sampler = searchAndReduce(searcher, new TermQuery(new Term("text", "good")), aggBuilder, textFieldType,
                        numericFieldType);
                Min min = sampler.getAggregations().get("min");
                assertEquals(5.0, min.getValue(), 0);
                assertTrue(AggregationInspectionHelper.hasValue(sampler));
            }
        }
    }

    public void testRidiculousSize() throws IOException {
        TextFieldType textFieldType = new TextFieldType();
        textFieldType.setIndexAnalyzer(new NamedAnalyzer("foo", AnalyzerScope.GLOBAL, new StandardAnalyzer()));
        MappedFieldType numericFieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
        numericFieldType.setName("int");

        IndexWriterConfig indexWriterConfig = newIndexWriterConfig();
        indexWriterConfig.setMaxBufferedDocs(100);
        indexWriterConfig.setRAMBufferSizeMB(100); // flush on open to have a single segment with predictable docIds
        try (Directory dir = newDirectory();
             IndexWriter w = new IndexWriter(dir, indexWriterConfig)) {
            for (long value : new long[] {7, 3, -10, -6, 5, 50}) {
                Document doc = new Document();
                StringBuilder text = new StringBuilder();
                for (int i = 0; i < value; i++) {
                    text.append("good ");
                }
                doc.add(new Field("text", text.toString(), textFieldType));
                doc.add(new SortedNumericDocValuesField("int", value));
                w.addDocument(doc);
            }

            // Test with an outrageously large size to ensure that the maxDoc protection works
            SamplerAggregationBuilder aggBuilder = new SamplerAggregationBuilder("sampler")
                .shardSize(Integer.MAX_VALUE)
                .subAggregation(new MinAggregationBuilder("min")
                    .field("int"));
            try (IndexReader reader = DirectoryReader.open(w)) {
                assertEquals("test expects a single segment", 1, reader.leaves().size());
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalSampler sampler = searchAndReduce(searcher, new TermQuery(new Term("text", "good")), aggBuilder, textFieldType,
                    numericFieldType);
                Min min = sampler.getAggregations().get("min");
                assertEquals(3.0, min.getValue(), 0);
                assertTrue(AggregationInspectionHelper.hasValue(sampler));
            }
        }
    }
}
