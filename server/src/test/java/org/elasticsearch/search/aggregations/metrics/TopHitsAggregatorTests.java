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

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;

import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.aggregations.AggregationBuilders.topHits;

public class TopHitsAggregatorTests extends AggregatorTestCase {
    public void testTopLevel() throws Exception {
        Aggregation result;
        if (randomBoolean()) {
            result = testCase(new MatchAllDocsQuery(), topHits("_name").sort("string", SortOrder.DESC));
        } else {
            Query query = new QueryParser("string", new KeywordAnalyzer()).parse("d^1000 c^100 b^10 a^1");
            result = testCase(query, topHits("_name"));
        }
        SearchHits searchHits = ((TopHits) result).getHits();
        assertEquals(3L, searchHits.getTotalHits().value);
        assertEquals("3", searchHits.getAt(0).getId());
        assertEquals("type", searchHits.getAt(0).getType());
        assertEquals("2", searchHits.getAt(1).getId());
        assertEquals("type", searchHits.getAt(1).getType());
        assertEquals("1", searchHits.getAt(2).getId());
        assertEquals("type", searchHits.getAt(2).getType());
        assertTrue(AggregationInspectionHelper.hasValue(((InternalTopHits)result)));
    }

    public void testNoResults() throws Exception {
        TopHits result = (TopHits) testCase(new MatchNoDocsQuery(), topHits("_name").sort("string", SortOrder.DESC));
        SearchHits searchHits = ((TopHits) result).getHits();
        assertEquals(0L, searchHits.getTotalHits().value);
        assertFalse(AggregationInspectionHelper.hasValue(((InternalTopHits)result)));
    }

    /**
     * Tests {@code top_hits} inside of {@code terms}. While not strictly a unit test this is a fairly common way to run {@code top_hits}
     * and serves as a good example of running {@code top_hits} inside of another aggregation.
     */
    public void testInsideTerms() throws Exception {
        Aggregation result;
        if (randomBoolean()) {
            result = testCase(new MatchAllDocsQuery(),
                    terms("term").field("string")
                        .subAggregation(topHits("top").sort("string", SortOrder.DESC)));
        } else {
            Query query = new QueryParser("string", new KeywordAnalyzer()).parse("d^1000 c^100 b^10 a^1");
            result = testCase(query,
                    terms("term").field("string")
                        .subAggregation(topHits("top")));
        }
        Terms terms = (Terms) result;

        // The "a" bucket
        TopHits hits = (TopHits) terms.getBucketByKey("a").getAggregations().get("top");
        SearchHits searchHits = (hits).getHits();
        assertEquals(2L, searchHits.getTotalHits().value);
        assertEquals("2", searchHits.getAt(0).getId());
        assertEquals("1", searchHits.getAt(1).getId());
        assertTrue(AggregationInspectionHelper.hasValue(((InternalTopHits) terms.getBucketByKey("a").getAggregations().get("top"))));

        // The "b" bucket
        searchHits = ((TopHits) terms.getBucketByKey("b").getAggregations().get("top")).getHits();
        assertEquals(2L, searchHits.getTotalHits().value);
        assertEquals("3", searchHits.getAt(0).getId());
        assertEquals("1", searchHits.getAt(1).getId());
        assertTrue(AggregationInspectionHelper.hasValue(((InternalTopHits) terms.getBucketByKey("b").getAggregations().get("top"))));

        // The "c" bucket
        searchHits = ((TopHits) terms.getBucketByKey("c").getAggregations().get("top")).getHits();
        assertEquals(1L, searchHits.getTotalHits().value);
        assertEquals("2", searchHits.getAt(0).getId());
        assertTrue(AggregationInspectionHelper.hasValue(((InternalTopHits) terms.getBucketByKey("c").getAggregations().get("top"))));

        // The "d" bucket
        searchHits = ((TopHits) terms.getBucketByKey("d").getAggregations().get("top")).getHits();
        assertEquals(1L, searchHits.getTotalHits().value);
        assertEquals("3", searchHits.getAt(0).getId());
        assertTrue(AggregationInspectionHelper.hasValue(((InternalTopHits) terms.getBucketByKey("d").getAggregations().get("top"))));
    }

    private static final MappedFieldType STRING_FIELD_TYPE = new KeywordFieldMapper.KeywordFieldType();
    static {
        STRING_FIELD_TYPE.setName("string");
        STRING_FIELD_TYPE.setHasDocValues(true);
    }

    private Aggregation testCase(Query query, AggregationBuilder builder) throws IOException {
        Directory directory = newDirectory();
        RandomIndexWriter iw = new RandomIndexWriter(random(), directory);
        iw.addDocument(document("1", "a", "b"));
        iw.addDocument(document("2", "c", "a"));
        iw.addDocument(document("3", "b", "d"));
        iw.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        // We do not use LuceneTestCase.newSearcher because we need a DirectoryReader for "testInsideTerms"
        IndexSearcher indexSearcher = new IndexSearcher(indexReader);

        Aggregation result = searchAndReduce(indexSearcher, query, builder, STRING_FIELD_TYPE);
        indexReader.close();
        directory.close();
        return result;
    }

    private Document document(String id, String... stringValues) {
        Document document = new Document();
        document.add(new Field(IdFieldMapper.NAME, Uid.encodeId(id), IdFieldMapper.Defaults.FIELD_TYPE));
        for (String stringValue : stringValues) {
            document.add(new Field("string", stringValue, STRING_FIELD_TYPE));
            document.add(new SortedSetDocValuesField("string", new BytesRef(stringValue)));
        }
        return document;
    }

    public void testSetScorer() throws Exception {
        Directory directory = newDirectory();
        IndexWriter w = new IndexWriter(directory, newIndexWriterConfig()
                // only merge adjacent segments
                .setMergePolicy(newLogMergePolicy()));
        // first window (see BooleanScorer) has matches on one clause only
        for (int i = 0; i < 2048; ++i) {
            Document doc = new Document();
            doc.add(new StringField("_id", Uid.encodeId(Integer.toString(i)), Store.YES));
            if (i == 1000) { // any doc in 0..2048
                doc.add(new StringField("string", "bar", Store.NO));
            }
            w.addDocument(doc);
        }
        // second window has matches in two clauses
        for (int i = 0; i < 2048; ++i) {
            Document doc = new Document();
            doc.add(new StringField("_id", Uid.encodeId(Integer.toString(2048 + i)), Store.YES));
            if (i == 500) { // any doc in 0..2048
                doc.add(new StringField("string", "baz", Store.NO));
            } else if (i == 1500) {
                doc.add(new StringField("string", "bar", Store.NO));
            }
            w.addDocument(doc);
        }

        w.forceMerge(1); // we need all docs to be in the same segment

        IndexReader reader = DirectoryReader.open(w);
        w.close();

        IndexSearcher searcher = new IndexSearcher(reader);
        Query query = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("string", "bar")), Occur.SHOULD)
                .add(new TermQuery(new Term("string", "baz")), Occur.SHOULD)
                .build();
        AggregationBuilder agg = AggregationBuilders.topHits("top_hits");
        TopHits result = searchAndReduce(searcher, query, agg, STRING_FIELD_TYPE);
        assertEquals(3, result.getHits().getTotalHits().value);
        reader.close();
        directory.close();
    }

    public void testSortByScore() throws Exception {
        // just check that it does not fail with exceptions
        testCase(new MatchAllDocsQuery(), topHits("_name").sort("_score", SortOrder.DESC));
        testCase(new MatchAllDocsQuery(), topHits("_name").sort("_score"));
    }
}
