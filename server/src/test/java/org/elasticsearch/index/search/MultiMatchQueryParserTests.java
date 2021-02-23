/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.search;

import org.apache.lucene.analysis.MockSynonymAnalyzer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.XCombinedFieldQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.equalTo;

public class MultiMatchQueryParserTests extends ESSingleNodeTestCase {
    private IndexService indexService;

    @Before
    public void setUpIndex() throws IOException {
        indexService = createIndex("test");
        MapperService mapperService = indexService.mapperService();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("properties")
                    .startObject("field1").field("type", "text").endObject()
                    .startObject("field2").field("type", "text").endObject()
                .endObject()
            .endObject());
        mapperService.merge("type", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);
    }

    public void testCombinedFieldWithInvalidDefaultSimilarity() throws IOException {
        IndexService indexService = createIndex("similarity-test", Settings.builder()
            .put("index.similarity.default.type", "boolean")
            .build());
        MapperService mapperService = indexService.mapperService();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", "text")
                    .endObject()
                .endObject()
            .endObject().endObject());
        mapperService.merge("type", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);
        SearchExecutionContext searchExecutionContext = indexService.newSearchExecutionContext(
            randomInt(20), 0, null, () -> 0L, null, emptyMap());

        MultiMatchQueryParser parser = new MultiMatchQueryParser(searchExecutionContext);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> parser.parse(
            MultiMatchQueryBuilder.Type.COMBINED_FIELDS, Map.of("field", 1.0f), "value", null));
        assertThat(e.getMessage(), equalTo(
            "[multi_match] queries in [combined_fields] mode can only be used with the [BM25] similarity"));
    }

    public void testCombinedFieldWithPerFieldSimilarity() throws IOException {
        IndexService indexService = createIndex("similarity-test", Settings.builder()
            .put("index.similarity.tuned_bm25.type", "BM25")
            .put("index.similarity.tuned_bm25.k1", "1.4")
            .put("index.similarity.tuned_bm25.b", "0.8")
            .build());
        MapperService mapperService = indexService.mapperService();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", "text")
                        .field("similarity", "tuned_bm25")
                    .endObject()
                .endObject()
            .endObject().endObject());
        mapperService.merge("type", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);
        SearchExecutionContext searchExecutionContext = indexService.newSearchExecutionContext(
            randomInt(20), 0, null, () -> 0L, null, emptyMap());

        MultiMatchQueryParser parser = new MultiMatchQueryParser(searchExecutionContext);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> parser.parse(
            MultiMatchQueryBuilder.Type.COMBINED_FIELDS, Map.of("field", 1.0f), "value", null));
        assertThat(e.getMessage(), equalTo(
            "[multi_match] queries in [combined_fields] mode cannot be used with per-field similarities"));
    }

    public void testCombinedFieldsMinimumShouldMatch() throws IOException {
        SearchExecutionContext searchExecutionContext = indexService.newSearchExecutionContext(
            randomInt(20), 0, null, () -> 0L, null, emptyMap());
        MultiMatchQueryParser parser = new MultiMatchQueryParser(searchExecutionContext);

        Query actual = parser.parse(MultiMatchQueryBuilder.Type.COMBINED_FIELDS,
            Map.of("field1", 1.0f, "field2", 1.0f), "one two three", "2");
        Query expected = new BooleanQuery.Builder()
            .setMinimumNumberShouldMatch(2)
            .add(new XCombinedFieldQuery.Builder()
                .addField("field1")
                .addField("field2")
                .addTerm(new BytesRef("one"))
                .build(), BooleanClause.Occur.SHOULD)
            .add(new XCombinedFieldQuery.Builder()
                .addField("field1")
                .addField("field2")
                .addTerm(new BytesRef("two"))
                .build(), BooleanClause.Occur.SHOULD)
            .add(new XCombinedFieldQuery.Builder()
                .addField("field1")
                .addField("field2")
                .addTerm(new BytesRef("three"))
                .build(), BooleanClause.Occur.SHOULD)
            .build();
        assertThat(actual, equalTo(expected));
    }

    public void testCombinedFieldsZeroTermsQuery() throws IOException {
        SearchExecutionContext searchExecutionContext = indexService.newSearchExecutionContext(
            randomInt(20), 0, null, () -> 0L, null, emptyMap());
        MultiMatchQueryParser parser = new MultiMatchQueryParser(searchExecutionContext);
        parser.setZeroTermsQuery(MatchQueryParser.ZeroTermsQuery.ALL);
        parser.setAnalyzer(new StopAnalyzer(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET));

        Query actual = parser.parse(MultiMatchQueryBuilder.Type.COMBINED_FIELDS,
            Map.of("field1", 1.0f, "field2", 1.0f), "the and", null);
        assertThat(actual, equalTo(new MatchAllDocsQuery()));
    }

    public void testCombinedFieldsWithSynonyms() throws IOException {
        SearchExecutionContext searchExecutionContext = indexService.newSearchExecutionContext(
            randomInt(20), 0, null, () -> 0L, null, emptyMap());
        MultiMatchQueryParser parser = new MultiMatchQueryParser(searchExecutionContext);
        parser.setOccur(BooleanClause.Occur.MUST);
        parser.setAnalyzer(new MockSynonymAnalyzer());

        Query actual = parser.parse(MultiMatchQueryBuilder.Type.COMBINED_FIELDS,
            Map.of("field1", 1.0f, "field2", 1.0f), "dogs cats", null);
        Query expected = new BooleanQuery.Builder()
            .add(new XCombinedFieldQuery.Builder()
                .addField("field1")
                .addField("field2")
                .addTerm(new BytesRef("dog"))
                .addTerm(new BytesRef("dogs"))
                .build(), BooleanClause.Occur.MUST)
            .add(new XCombinedFieldQuery.Builder()
                .addField("field1")
                .addField("field2")
                .addTerm(new BytesRef("cats"))
                .build(), BooleanClause.Occur.MUST)
            .build();
        assertThat(actual, equalTo(expected));
    }

    public void testCombinedFieldsWithSynonymsPhrase() throws IOException {
        SearchExecutionContext searchExecutionContext = indexService.newSearchExecutionContext(
            randomInt(20), 0, null, () -> 0L, null, emptyMap());
        MultiMatchQueryParser parser = new MultiMatchQueryParser(searchExecutionContext);
        parser.setOccur(BooleanClause.Occur.MUST);
        parser.setAnalyzer(new MockSynonymAnalyzer());

        Query actual = parser.parse(MultiMatchQueryBuilder.Type.COMBINED_FIELDS,
            Map.of("field1", 1.0f, "field2", 1.0f), "guinea pig cats", null);
        Query expected = new BooleanQuery.Builder()
            .add(new BooleanQuery.Builder()
                .add(new BooleanQuery.Builder()
                    .add(new PhraseQuery.Builder()
                        .add(new Term("field1", "guinea"))
                        .add(new Term("field1", "pig"))
                        .build(), BooleanClause.Occur.SHOULD)
                    .add(new PhraseQuery.Builder()
                        .add(new Term("field2", "guinea"))
                        .add(new Term("field2", "pig"))
                        .build(), BooleanClause.Occur.SHOULD)
                    .build(), BooleanClause.Occur.SHOULD)
                .add(new XCombinedFieldQuery.Builder()
                    .addField("field1")
                    .addField("field2")
                    .addTerm(new BytesRef("cavy"))
                    .build(), BooleanClause.Occur.SHOULD)
                .build(), BooleanClause.Occur.MUST)
            .add(new XCombinedFieldQuery.Builder()
                .addField("field1")
                .addField("field2")
                .addTerm(new BytesRef("cats"))
                .build(), BooleanClause.Occur.MUST)
            .build();

        assertEquals(expected, actual);
    }

    public void testKeywordSplitQueriesOnWhitespace() throws IOException {
        IndexService indexService = createIndex("test_keyword", Settings.builder()
            .put("index.analysis.normalizer.my_lowercase.type", "custom")
            .putList("index.analysis.normalizer.my_lowercase.filter", "lowercase").build());
        MapperService mapperService = indexService.mapperService();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", "keyword")
                    .endObject()
                    .startObject("field_normalizer")
                        .field("type", "keyword")
                        .field("normalizer", "my_lowercase")
                    .endObject()
                    .startObject("field_split")
                        .field("type", "keyword")
                        .field("split_queries_on_whitespace", true)
                    .endObject()
                    .startObject("field_split_normalizer")
                        .field("type", "keyword")
                        .field("normalizer", "my_lowercase")
                        .field("split_queries_on_whitespace", true)
                    .endObject()
                .endObject()
            .endObject().endObject());
        mapperService.merge("type", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);
        SearchExecutionContext searchExecutionContext = indexService.newSearchExecutionContext(
            randomInt(20),
            0,
            null, () -> { throw new UnsupportedOperationException(); },
            null,
            emptyMap()
        );
        MultiMatchQueryParser parser = new MultiMatchQueryParser(searchExecutionContext);
        Map<String, Float> fieldNames = new HashMap<>();
        fieldNames.put("field", 1.0f);
        fieldNames.put("field_split", 1.0f);
        fieldNames.put("field_normalizer", 1.0f);
        fieldNames.put("field_split_normalizer", 1.0f);
        Query query = parser.parse(MultiMatchQueryBuilder.Type.BEST_FIELDS, fieldNames, "Foo Bar", null);
        DisjunctionMaxQuery expected = new DisjunctionMaxQuery(
            Arrays.asList(
                new TermQuery(new Term("field_normalizer", "foo bar")),
                new TermQuery(new Term("field", "Foo Bar")),
                new BooleanQuery.Builder()
                    .add(new TermQuery(new Term("field_split", "Foo")), BooleanClause.Occur.SHOULD)
                    .add(new TermQuery(new Term("field_split", "Bar")), BooleanClause.Occur.SHOULD)
                    .build(),
                new BooleanQuery.Builder()
                    .add(new TermQuery(new Term("field_split_normalizer", "foo")), BooleanClause.Occur.SHOULD)
                    .add(new TermQuery(new Term("field_split_normalizer", "bar")), BooleanClause.Occur.SHOULD)
                    .build()
        ), 0.0f);
        assertThat(query, equalTo(expected));
    }
}
