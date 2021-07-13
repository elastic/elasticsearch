/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.analysis.MockSynonymAnalyzer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.sandbox.search.CombinedFieldQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.XCombinedFieldQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.hamcrest.CoreMatchers;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.combinedFieldsQuery;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class CombinedFieldsQueryParsingTests extends MapperServiceTestCase {
    private SearchExecutionContext context;

    @Before
    public void createSearchExecutionContext() throws IOException {
        MapperService mapperService = createMapperService(
            XContentFactory.jsonBuilder().startObject().startObject(MapperService.SINGLE_MAPPING_NAME)
                .startObject("properties")
                    .startObject("field1").field("type", "text").endObject()
                    .startObject("field2").field("type", "text").endObject()
                    .startObject("synonym1").field("type", "text").field("analyzer", "mock_synonym").endObject()
                    .startObject("synonym2").field("type", "text").field("analyzer", "mock_synonym").endObject()
                    .startObject("stopwords1").field("type", "text").field("analyzer", "stop").endObject()
                    .startObject("stopwords2").field("type", "text").field("analyzer", "stop").endObject()
                .endObject()
            .endObject().endObject());
        context = createSearchExecutionContext(mapperService);
    }

    @Override
    protected IndexAnalyzers createIndexAnalyzers(IndexSettings indexSettings) {
        return new IndexAnalyzers(
            Map.of("default", new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer()),
                "mock_synonym", new NamedAnalyzer("mock_synonym", AnalyzerScope.INDEX, new MockSynonymAnalyzer()),
                "stop", new NamedAnalyzer("stop", AnalyzerScope.INDEX, new StopAnalyzer(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET))),
            Map.of(),
            Map.of());
    }

    public void testEmptyArguments() {
        expectThrows(IllegalArgumentException.class, () -> combinedFieldsQuery(null, "field"));
        expectThrows(IllegalArgumentException.class, () -> combinedFieldsQuery("value", (String[]) null));
        expectThrows(IllegalArgumentException.class, () -> combinedFieldsQuery("value", new String[]{""}));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> combinedFieldsQuery("value").toQuery(context));
        assertThat(e.getMessage(), equalTo("In [combined_fields] query, at least one field must be provided"));
    }

    public void testInvalidFieldBoosts() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> combinedFieldsQuery("the quick fox")
                .field("field1", -1.0f)
                .field("field2")
                .toQuery(context));
        assertThat(e.getMessage(), containsString("[combined_fields] requires field boosts to be >= 1.0"));

        e = expectThrows(IllegalArgumentException.class,
            () -> combinedFieldsQuery("the quick fox")
                .field("field1", 0.42f)
                .field("field2")
                .toQuery(context));
        assertThat(e.getMessage(), containsString("[combined_fields] requires field boosts to be >= 1.0"));

        e = expectThrows(IllegalArgumentException.class,
            () -> combinedFieldsQuery("the quick fox")
                .fields(Map.of("field1", 2.0f, "field2", 0.3f))
                .toQuery(context));
        assertThat(e.getMessage(), containsString("[combined_fields] requires field boosts to be >= 1.0"));
    }

    public void testMissingFields() throws Exception {
        assertThat(combinedFieldsQuery("test").field("missing").toQuery(context),
            instanceOf(MatchNoDocsQuery.class));
        assertThat(combinedFieldsQuery("test").field("missing*").toQuery(context),
            instanceOf(MatchNoDocsQuery.class));
    }

    public void testWildcardFieldPattern() throws Exception {
        Query query = combinedFieldsQuery("quick fox")
            .field("field*")
            .toQuery(context);
        assertThat(query, instanceOf(BooleanQuery.class));

        BooleanQuery booleanQuery = (BooleanQuery) query;
        assertThat(booleanQuery.clauses().size(), equalTo(2));
        assertThat(booleanQuery.clauses().get(0).getQuery(), instanceOf(XCombinedFieldQuery.class));
        assertThat(booleanQuery.clauses().get(1).getQuery(), instanceOf(XCombinedFieldQuery.class));
    }

    public void testOperator() throws Exception {
        Operator operator = randomFrom(Operator.values());
        BooleanClause.Occur occur = operator.toBooleanClauseOccur();
        int minimumShouldMatch = randomIntBetween(0, 2);

        Query query = combinedFieldsQuery("quick fox")
            .field("field1")
            .field("field2")
            .operator(operator)
            .minimumShouldMatch(String.valueOf(minimumShouldMatch))
            .toQuery(context);
        assertThat(query, instanceOf(BooleanQuery.class));

        BooleanQuery booleanQuery = (BooleanQuery) query;
        assertThat(booleanQuery.getMinimumNumberShouldMatch(), equalTo(minimumShouldMatch));

        assertThat(booleanQuery.clauses().size(), equalTo(2));
        assertThat(booleanQuery.clauses().get(0).getOccur(), equalTo(occur));
        assertThat(booleanQuery.clauses().get(1).getOccur(), equalTo(occur));
    }

    public void testQueryBoost() throws IOException {
        CombinedFieldsQueryBuilder builder = combinedFieldsQuery("test")
            .field("field1", 5.0f)
            .boost(2.0f);
        Query query = builder.toQuery(context);
        assertThat(query, instanceOf(BoostQuery.class));

        BoostQuery boostQuery = (BoostQuery) query;
        assertThat(boostQuery.getBoost(), equalTo(2.0f));
        assertThat(boostQuery.getQuery(), instanceOf(XCombinedFieldQuery.class));
    }

    public void testInconsistentAnalyzers() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> combinedFieldsQuery("the quick fox")
                .field("field1", 1.2f)
                .field("stopwords1")
                .toQuery(context));
        assertThat(e.getMessage(), CoreMatchers.equalTo("All fields in [combined_fields] query must have the same search analyzer"));
    }

    public void testInvalidDefaultSimilarity() throws IOException {
        Settings settings = Settings.builder()
            .put("index.similarity.default.type", "boolean")
            .build();

        MapperService mapperService = createMapperService(settings,
            XContentFactory.jsonBuilder().startObject().startObject(MapperService.SINGLE_MAPPING_NAME)
                .startObject("properties")
                    .startObject("field").field("type", "text").endObject()
                .endObject()
            .endObject().endObject());
        SearchExecutionContext context = createSearchExecutionContext(mapperService);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            combinedFieldsQuery("value", "field")
                .toQuery(context));
        assertThat(e.getMessage(), equalTo(
            "[combined_fields] queries can only be used with the [BM25] similarity"));
    }

    public void testPerFieldSimilarity() throws IOException {
        Settings settings = Settings.builder()
            .put("index.similarity.tuned_bm25.type", "BM25")
            .put("index.similarity.tuned_bm25.k1", "1.4")
            .put("index.similarity.tuned_bm25.b", "0.8")
            .build();

        MapperService mapperService = createMapperService(settings,
            XContentFactory.jsonBuilder().startObject().startObject(MapperService.SINGLE_MAPPING_NAME)
                .startObject("properties")
                    .startObject("field")
                        .field("type", "text")
                        .field("similarity", "tuned_bm25")
                    .endObject()
                .endObject()
            .endObject().endObject());
        SearchExecutionContext context = createSearchExecutionContext(mapperService);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            combinedFieldsQuery("value", "field")
                .operator(Operator.AND)
                .toQuery(context));
        assertThat(e.getMessage(), equalTo(
            "[combined_fields] queries cannot be used with per-field similarities"));
    }

    public void testCombinedFieldsWithSynonyms() throws IOException {
        Query actual = combinedFieldsQuery("dogs cats", "synonym1", "synonym2")
            .operator(Operator.AND)
            .toQuery(context);

        Query expected = new BooleanQuery.Builder()
            .add(new XCombinedFieldQuery.Builder()
                .addField("synonym1")
                .addField("synonym2")
                .addTerm(new BytesRef("dog"))
                .addTerm(new BytesRef("dogs"))
                .build(), BooleanClause.Occur.MUST)
            .add(new XCombinedFieldQuery.Builder()
                .addField("synonym1")
                .addField("synonym2")
                .addTerm(new BytesRef("cats"))
                .build(), BooleanClause.Occur.MUST)
            .build();

        assertThat(actual, equalTo(expected));
    }

    public void testSynonymsPhrase() throws IOException {
        Query actual = combinedFieldsQuery("guinea pig cats", "synonym1", "synonym2")
            .operator(Operator.AND)
            .toQuery(context);

        Query expected = new BooleanQuery.Builder()
            .add(new BooleanQuery.Builder()
                .add(new BooleanQuery.Builder()
                    .add(new PhraseQuery.Builder()
                        .add(new Term("synonym1", "guinea"))
                        .add(new Term("synonym1", "pig"))
                        .build(), BooleanClause.Occur.SHOULD)
                    .add(new PhraseQuery.Builder()
                        .add(new Term("synonym2", "guinea"))
                        .add(new Term("synonym2", "pig"))
                        .build(), BooleanClause.Occur.SHOULD)
                    .build(), BooleanClause.Occur.SHOULD)
                .add(new XCombinedFieldQuery.Builder()
                    .addField("synonym1")
                    .addField("synonym2")
                    .addTerm(new BytesRef("cavy"))
                    .build(), BooleanClause.Occur.SHOULD)
                .build(), BooleanClause.Occur.MUST)
            .add(new XCombinedFieldQuery.Builder()
                .addField("synonym1")
                .addField("synonym2")
                .addTerm(new BytesRef("cats"))
                .build(), BooleanClause.Occur.MUST)
            .build();

        assertEquals(expected, actual);
    }

    public void testDisabledSynonymsPhrase() throws IOException {
        Query actual = combinedFieldsQuery("guinea pig cats", "synonym1", "synonym2")
            .operator(Operator.AND)
            .autoGenerateSynonymsPhraseQuery(false)
            .toQuery(context);

        Query expected = new BooleanQuery.Builder()
            .add(new BooleanQuery.Builder()
                .add(new BooleanQuery.Builder()
                    .add(new XCombinedFieldQuery.Builder()
                        .addField("synonym1")
                        .addField("synonym2")
                        .addTerm(new BytesRef("guinea"))
                        .build(), BooleanClause.Occur.MUST)
                    .add(new XCombinedFieldQuery.Builder()
                        .addField("synonym1")
                        .addField("synonym2")
                        .addTerm(new BytesRef("pig"))
                        .build(), BooleanClause.Occur.MUST)
                    .build(), BooleanClause.Occur.SHOULD)
                .add(new XCombinedFieldQuery.Builder()
                    .addField("synonym1")
                    .addField("synonym2")
                    .addTerm(new BytesRef("cavy"))
                    .build(), BooleanClause.Occur.SHOULD)
                .build(), BooleanClause.Occur.MUST)
            .add(new XCombinedFieldQuery.Builder()
                .addField("synonym1")
                .addField("synonym2")
                .addTerm(new BytesRef("cats"))
                .build(), BooleanClause.Occur.MUST)
            .build();

        assertEquals(expected, actual);
    }

    public void testStopwords() throws Exception {
        ZeroTermsQueryOption zeroTermsQuery = randomFrom(ZeroTermsQueryOption.ALL,
            ZeroTermsQueryOption.NONE);
        Query expectedEmptyQuery = zeroTermsQuery.asQuery();

        BytesRef quickTerm = new BytesRef("quick");
        BytesRef foxTerm = new BytesRef("fox");

        Query query = combinedFieldsQuery("the quick fox")
            .field("stopwords1")
            .zeroTermsQuery(zeroTermsQuery)
            .toQuery(context);
        Query expected = new BooleanQuery.Builder()
            .add(new XCombinedFieldQuery.Builder().addField("stopwords1").addTerm(quickTerm).build(), BooleanClause.Occur.SHOULD)
            .add(new XCombinedFieldQuery.Builder().addField("stopwords1").addTerm(foxTerm).build(), BooleanClause.Occur.SHOULD)
            .build();
        assertEquals(expected, query);

        query = combinedFieldsQuery("the quick fox")
            .field("stopwords1")
            .field("stopwords2")
            .zeroTermsQuery(zeroTermsQuery)
            .toQuery(context);
        expected = new BooleanQuery.Builder()
            .add(new XCombinedFieldQuery.Builder()
                .addField("stopwords1")
                .addField("stopwords2")
                .addTerm(quickTerm)
                .build(), BooleanClause.Occur.SHOULD)
            .add(new XCombinedFieldQuery.Builder()
                .addField("stopwords1")
                .addField("stopwords2")
                .addTerm(foxTerm)
                .build(), BooleanClause.Occur.SHOULD)
            .build();
        assertEquals(expected, query);

        query = combinedFieldsQuery("the")
            .field("stopwords1")
            .field("stopwords2")
            .zeroTermsQuery(zeroTermsQuery)
            .toQuery(context);
        assertEquals(expectedEmptyQuery, query);

        query = new BoolQueryBuilder()
            .should(combinedFieldsQuery("the")
                .field("stopwords1")
                .zeroTermsQuery(zeroTermsQuery))
            .toQuery(context);
        expected = new BooleanQuery.Builder()
            .add(expectedEmptyQuery, BooleanClause.Occur.SHOULD)
            .build();
        assertEquals(expected, query);

        query = new BoolQueryBuilder()
            .should(combinedFieldsQuery("the")
                .field("stopwords1")
                .field("stopwords2")
                .zeroTermsQuery(zeroTermsQuery))
            .toQuery(context);
        expected = new BooleanQuery.Builder()
            .add(expectedEmptyQuery, BooleanClause.Occur.SHOULD)
            .build();
        assertEquals(expected, query);
    }

}
