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
package org.elasticsearch.index.percolator;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class QueryMetadataServiceTests extends ESTestCase {

    private QueryMetadataService queryMetadataService = new QueryMetadataService();

    public void testExtractQueryMetadata_termQuery() {
        TermQuery termQuery = new TermQuery(new Term("_field", "_term"));
        List<Field> fields = new ArrayList<>();
        queryMetadataService.extractQueryMetadata(termQuery, fields);
        assertThat(fields.size(), equalTo(1));
        assertThat(fields.get(0).name(), equalTo(QueryMetadataService.QUERY_METADATA_FIELD_PREFIX + termQuery.getTerm().field()));
        assertThat(fields.get(0).binaryValue(), equalTo(termQuery.getTerm().bytes()));
    }

    public void testExtractQueryMetadata_phraseQuery() {
        PhraseQuery phraseQuery = new PhraseQuery("_field", "_term1", "term2");
        List<Field> fields = new ArrayList<>();
        queryMetadataService.extractQueryMetadata(phraseQuery, fields);
        assertThat(fields.size(), equalTo(2));
        assertThat(fields.get(0).name(), equalTo(QueryMetadataService.QUERY_METADATA_FIELD_PREFIX + phraseQuery.getTerms()[0].field()));
        assertThat(fields.get(0).binaryValue(), equalTo(phraseQuery.getTerms()[0].bytes()));
        assertThat(fields.get(1).name(), equalTo(QueryMetadataService.QUERY_METADATA_FIELD_PREFIX + phraseQuery.getTerms()[1].field()));
        assertThat(fields.get(1).binaryValue(), equalTo(phraseQuery.getTerms()[1].bytes()));
    }

    public void testExtractQueryMetadata_booleanQuery() {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        TermQuery termQuery1 = new TermQuery(new Term("_field", "_term"));
        builder.add(termQuery1, BooleanClause.Occur.SHOULD);
        PhraseQuery phraseQuery = new PhraseQuery("_field", "_term1", "term2");
        builder.add(phraseQuery, BooleanClause.Occur.SHOULD);

        BooleanQuery.Builder subBuilder = new BooleanQuery.Builder();
        TermQuery termQuery2 = new TermQuery(new Term("_field1", "_term"));
        subBuilder.add(termQuery2, BooleanClause.Occur.MUST);
        TermQuery termQuery3 = new TermQuery(new Term("_field3", "_term"));
        subBuilder.add(termQuery3, BooleanClause.Occur.MUST);
        builder.add(subBuilder.build(), BooleanClause.Occur.SHOULD);

        BooleanQuery booleanQuery = builder.build();
        List<Field> fields = new ArrayList<>();
        queryMetadataService.extractQueryMetadata(booleanQuery, fields);
        assertThat(fields.size(), equalTo(5));
        assertThat(fields.get(0).name(), equalTo(QueryMetadataService.QUERY_METADATA_FIELD_PREFIX + termQuery1.getTerm().field()));
        assertThat(fields.get(0).binaryValue(), equalTo(termQuery1.getTerm().bytes()));
        assertThat(fields.get(1).name(), equalTo(QueryMetadataService.QUERY_METADATA_FIELD_PREFIX + phraseQuery.getTerms()[0].field()));
        assertThat(fields.get(1).binaryValue(), equalTo(phraseQuery.getTerms()[0].bytes()));
        assertThat(fields.get(2).name(), equalTo(QueryMetadataService.QUERY_METADATA_FIELD_PREFIX + phraseQuery.getTerms()[1].field()));
        assertThat(fields.get(2).binaryValue(), equalTo(phraseQuery.getTerms()[1].bytes()));
        assertThat(fields.get(3).name(), equalTo(QueryMetadataService.QUERY_METADATA_FIELD_PREFIX + termQuery2.getTerm().field()));
        assertThat(fields.get(3).binaryValue(), equalTo(termQuery2.getTerm().bytes()));
        assertThat(fields.get(4).name(), equalTo(QueryMetadataService.QUERY_METADATA_FIELD_PREFIX + termQuery3.getTerm().field()));
        assertThat(fields.get(4).binaryValue(), equalTo(termQuery3.getTerm().bytes()));
    }

    public void testExtractQueryMetadata_booleanQueryWithMustNot() {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        TermQuery termQuery1 = new TermQuery(new Term("_field", "_term"));
        builder.add(termQuery1, BooleanClause.Occur.MUST_NOT);
        PhraseQuery phraseQuery = new PhraseQuery("_field", "_term1", "term2");
        builder.add(phraseQuery, BooleanClause.Occur.SHOULD);

        BooleanQuery booleanQuery = builder.build();
        List<Field> fields = new ArrayList<>();
        queryMetadataService.extractQueryMetadata(booleanQuery, fields);
        assertThat(fields.size(), equalTo(2));
        assertThat(fields.get(0).name(), equalTo(QueryMetadataService.QUERY_METADATA_FIELD_PREFIX + phraseQuery.getTerms()[0].field()));
        assertThat(fields.get(0).binaryValue(), equalTo(phraseQuery.getTerms()[0].bytes()));
        assertThat(fields.get(1).name(), equalTo(QueryMetadataService.QUERY_METADATA_FIELD_PREFIX + phraseQuery.getTerms()[1].field()));
        assertThat(fields.get(1).binaryValue(), equalTo(phraseQuery.getTerms()[1].bytes()));
    }

    public void testExtractQueryMetadata_constantScoreQuery() {
        TermQuery termQuery1 = new TermQuery(new Term("_field", "_term"));
        ConstantScoreQuery constantScoreQuery = new ConstantScoreQuery(termQuery1);
        List<Field> fields = new ArrayList<>();
        queryMetadataService.extractQueryMetadata(constantScoreQuery, fields);
        assertThat(fields.size(), equalTo(1));
        assertThat(fields.get(0).name(), equalTo(QueryMetadataService.QUERY_METADATA_FIELD_PREFIX + termQuery1.getTerm().field()));
        assertThat(fields.get(0).binaryValue(), equalTo(termQuery1.getTerm().bytes()));
    }

    public void testExtractQueryMetadata_unsupportedQuery() {
        TermRangeQuery termRangeQuery = new TermRangeQuery("_field", null, null, true, false);

        List<Field> fields = new ArrayList<>();
        queryMetadataService.extractQueryMetadata(termRangeQuery, fields);
        assertThat(fields.size(), equalTo(1));
        assertThat(fields.get(0).name(), equalTo(QueryMetadataService.QUERY_METADATA_FIELD_UNKNOWN));
        assertThat(fields.get(0).binaryValue(), equalTo(new BytesRef()));

        TermQuery termQuery1 = new TermQuery(new Term("_field", "_term"));
        BooleanQuery.Builder builder = new BooleanQuery.Builder();;
        builder.add(termQuery1, BooleanClause.Occur.SHOULD);
        builder.add(termRangeQuery, BooleanClause.Occur.SHOULD);

        fields = new ArrayList<>();
        queryMetadataService.extractQueryMetadata(builder.build(), fields);
        assertThat(fields.size(), equalTo(1));
        assertThat(fields.get(0).name(), equalTo(QueryMetadataService.QUERY_METADATA_FIELD_UNKNOWN));
        assertThat(fields.get(0).binaryValue(), equalTo(new BytesRef()));
    }

    public void testCreateQueryMetadataQuery() throws Exception {
        Document document = new Document();
        document.add(new TextField("field1", "the quick brown fox jumps over the lazy dog", Field.Store.NO));
        document.add(new TextField("field2", "some more text", Field.Store.NO));
        document.add(new TextField("_field3", "hide me", Field.Store.NO));
        document.add(new LongField("field4", 123, Field.Store.NO));

        Query query = queryMetadataService.createQueryMetadataQuery(document, new WhitespaceAnalyzer());
        assertThat(query, instanceOf(BooleanQuery.class));

        BooleanQuery booleanQuery = (BooleanQuery) query;
        assertThat(booleanQuery.clauses().size(), equalTo(13));
        assertClause(booleanQuery, 0, QueryMetadataService.QUERY_METADATA_FIELD_UNKNOWN, "");
        assertClause(booleanQuery, 1, QueryMetadataService.QUERY_METADATA_FIELD_PREFIX + "field1", "the");
        assertClause(booleanQuery, 2, QueryMetadataService.QUERY_METADATA_FIELD_PREFIX + "field1", "quick");
        assertClause(booleanQuery, 3, QueryMetadataService.QUERY_METADATA_FIELD_PREFIX + "field1", "brown");
        assertClause(booleanQuery, 4, QueryMetadataService.QUERY_METADATA_FIELD_PREFIX + "field1", "fox");
        assertClause(booleanQuery, 5, QueryMetadataService.QUERY_METADATA_FIELD_PREFIX + "field1", "jumps");
        assertClause(booleanQuery, 6, QueryMetadataService.QUERY_METADATA_FIELD_PREFIX + "field1", "over");
        assertClause(booleanQuery, 7, QueryMetadataService.QUERY_METADATA_FIELD_PREFIX + "field1", "the");
        assertClause(booleanQuery, 8, QueryMetadataService.QUERY_METADATA_FIELD_PREFIX + "field1", "lazy");
        assertClause(booleanQuery, 9, QueryMetadataService.QUERY_METADATA_FIELD_PREFIX + "field1", "dog");
        assertClause(booleanQuery, 10, QueryMetadataService.QUERY_METADATA_FIELD_PREFIX + "field2", "some");
        assertClause(booleanQuery, 11, QueryMetadataService.QUERY_METADATA_FIELD_PREFIX + "field2", "more");
        assertClause(booleanQuery, 12, QueryMetadataService.QUERY_METADATA_FIELD_PREFIX + "field2", "text");
    }

    private void assertClause(BooleanQuery booleanQuery, int i, String expectedField, String expectedValue) {
        assertThat(booleanQuery.clauses().get(i).getOccur(), equalTo(BooleanClause.Occur.SHOULD));
        assertThat(((TermQuery) booleanQuery.clauses().get(i).getQuery()).getTerm().field(), equalTo(expectedField));
        assertThat(((TermQuery) booleanQuery.clauses().get(i).getQuery()).getTerm().bytes().utf8ToString(), equalTo(expectedValue));
    }

}
