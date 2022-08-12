/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.search;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.tests.analysis.MockSynonymAnalyzer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MockFieldMapper.FakeFieldType;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.search.MultiMatchQueryParser.FieldAndBoost;
import org.elasticsearch.lucene.queries.BlendedTermQuery;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.MockKeywordPlugin;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.index.query.QueryBuilders.multiMatchQuery;
import static org.hamcrest.Matchers.equalTo;

public class MultiMatchQueryParserTests extends ESSingleNodeTestCase {

    private IndexService indexService;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(MockKeywordPlugin.class);
    }

    @Before
    public void setup() throws IOException {
        Settings settings = Settings.builder().build();
        IndexService indexService = createIndex("test", settings);
        MapperService mapperService = indexService.mapperService();
        String mapping = """
            {
                "person": {
                    "properties": {
                        "name": {
                            "properties": {
                                "first": {
                                    "type": "text",
                                    "analyzer": "standard"
                                },
                                "last": {
                                    "type": "text",
                                    "analyzer": "standard"
                                },
                                "nickname": {
                                    "type": "text",
                                    "analyzer": "whitespace"
                                }
                            }
                        }
                    }
                }
            }
            """;
        mapperService.merge("person", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);
        this.indexService = indexService;
    }

    public void testCrossFieldMultiMatchQuery() throws IOException {
        SearchExecutionContext searchExecutionContext = indexService.newSearchExecutionContext(
            randomInt(20),
            0,
            null,
            () -> { throw new UnsupportedOperationException(); },
            null,
            emptyMap()
        );
        searchExecutionContext.setAllowUnmappedFields(true);
        for (float tieBreaker : new float[] { 0.0f, 0.5f }) {
            Query parsedQuery = multiMatchQuery("banon").field("name.first", 2)
                .field("name.last", 3)
                .field("foobar")
                .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
                .tieBreaker(tieBreaker)
                .toQuery(searchExecutionContext);
            try (Engine.Searcher searcher = indexService.getShard(0).acquireSearcher("test")) {
                Query rewrittenQuery = searcher.rewrite(parsedQuery);
                Query tq1 = new BoostQuery(new TermQuery(new Term("name.last", "banon")), 3);
                Query tq2 = new BoostQuery(new TermQuery(new Term("name.first", "banon")), 2);
                Query expected = new DisjunctionMaxQuery(Arrays.asList(tq2, tq1), tieBreaker);
                assertEquals(expected, rewrittenQuery);
            }
        }
    }

    public void testBlendTerms() {
        FakeFieldType ft1 = new FakeFieldType("foo");
        FakeFieldType ft2 = new FakeFieldType("bar");
        Term[] terms = new Term[] { new Term("foo", "baz"), new Term("bar", "baz") };
        float[] boosts = new float[] { 2, 3 };
        Query expected = BlendedTermQuery.dismaxBlendedQuery(terms, boosts, 1.0f);
        Query actual = MultiMatchQueryParser.blendTerm(
            indexService.newSearchExecutionContext(
                randomInt(20),
                0,
                null,
                () -> { throw new UnsupportedOperationException(); },
                null,
                emptyMap()
            ),
            new BytesRef("baz"),
            1f,
            false,
            Arrays.asList(new FieldAndBoost(ft1, 2), new FieldAndBoost(ft2, 3))
        );
        assertEquals(expected, actual);
    }

    public void testBlendTermsUnsupportedValueWithLenient() {
        FakeFieldType ft1 = new FakeFieldType("foo");
        FakeFieldType ft2 = new FakeFieldType("bar") {
            @Override
            public Query termQuery(Object value, SearchExecutionContext context) {
                throw new IllegalArgumentException();
            }
        };
        Term[] terms = new Term[] { new Term("foo", "baz") };
        float[] boosts = new float[] { 2 };
        Query expected = new DisjunctionMaxQuery(
            Arrays.asList(
                Queries.newMatchNoDocsQuery("failed [" + ft2.name() + "] query, caused by illegal_argument_exception:[null]"),
                BlendedTermQuery.dismaxBlendedQuery(terms, boosts, 1.0f)
            ),
            1f
        );
        Query actual = MultiMatchQueryParser.blendTerm(
            indexService.newSearchExecutionContext(
                randomInt(20),
                0,
                null,
                () -> { throw new UnsupportedOperationException(); },
                null,
                emptyMap()
            ),
            new BytesRef("baz"),
            1f,
            true,
            Arrays.asList(new FieldAndBoost(ft1, 2), new FieldAndBoost(ft2, 3))
        );
        assertEquals(expected, actual);
    }

    public void testBlendTermsUnsupportedValueWithoutLenient() {
        FakeFieldType ft = new FakeFieldType("bar") {
            @Override
            public Query termQuery(Object value, SearchExecutionContext context) {
                throw new IllegalArgumentException();
            }
        };
        expectThrows(
            IllegalArgumentException.class,
            () -> MultiMatchQueryParser.blendTerm(
                indexService.newSearchExecutionContext(
                    randomInt(20),
                    0,
                    null,
                    () -> { throw new UnsupportedOperationException(); },
                    null,
                    emptyMap()
                ),
                new BytesRef("baz"),
                1f,
                false,
                Arrays.asList(new FieldAndBoost(ft, 1))
            )
        );
    }

    public void testBlendNoTermQuery() {
        FakeFieldType ft1 = new FakeFieldType("foo");
        FakeFieldType ft2 = new FakeFieldType("bar") {
            @Override
            public Query termQuery(Object value, SearchExecutionContext context) {
                return new MatchAllDocsQuery();
            }
        };
        Term[] terms = new Term[] { new Term("foo", "baz") };
        float[] boosts = new float[] { 2 };
        Query expectedDisjunct1 = BlendedTermQuery.dismaxBlendedQuery(terms, boosts, 1.0f);
        Query expectedDisjunct2 = new BoostQuery(new MatchAllDocsQuery(), 3);
        Query expected = new DisjunctionMaxQuery(Arrays.asList(expectedDisjunct2, expectedDisjunct1), 1.0f);
        Query actual = MultiMatchQueryParser.blendTerm(
            indexService.newSearchExecutionContext(
                randomInt(20),
                0,
                null,
                () -> { throw new UnsupportedOperationException(); },
                null,
                emptyMap()
            ),
            new BytesRef("baz"),
            1f,
            false,
            Arrays.asList(new FieldAndBoost(ft1, 2), new FieldAndBoost(ft2, 3))
        );
        assertEquals(expected, actual);
    }

    public void testMultiMatchCrossFieldsWithSynonyms() throws IOException {
        SearchExecutionContext searchExecutionContext = indexService.newSearchExecutionContext(
            randomInt(20),
            0,
            null,
            () -> { throw new UnsupportedOperationException(); },
            null,
            emptyMap()
        );

        MultiMatchQueryParser parser = new MultiMatchQueryParser(searchExecutionContext);
        parser.setAnalyzer(new MockSynonymAnalyzer());
        Map<String, Float> fieldNames = new HashMap<>();
        fieldNames.put("name.first", 1.0f);

        // check that synonym query is used for a single field
        Query parsedQuery = parser.parse(MultiMatchQueryBuilder.Type.CROSS_FIELDS, fieldNames, "dogs", null);
        Query expectedQuery = new SynonymQuery.Builder("name.first").addTerm(new Term("name.first", "dog"))
            .addTerm(new Term("name.first", "dogs"))
            .build();
        assertThat(parsedQuery, equalTo(expectedQuery));

        // check that blended term query is used for multiple fields
        fieldNames.put("name.last", 1.0f);
        parsedQuery = parser.parse(MultiMatchQueryBuilder.Type.CROSS_FIELDS, fieldNames, "dogs", null);
        Term[] terms = new Term[4];
        terms[0] = new Term("name.first", "dog");
        terms[1] = new Term("name.first", "dogs");
        terms[2] = new Term("name.last", "dog");
        terms[3] = new Term("name.last", "dogs");
        float[] boosts = new float[4];
        Arrays.fill(boosts, 1.0f);
        expectedQuery = BlendedTermQuery.dismaxBlendedQuery(terms, boosts, 1.0f);
        assertThat(parsedQuery, equalTo(expectedQuery));

    }

    public void testCrossFieldsWithSynonymsPhrase() throws IOException {
        SearchExecutionContext searchExecutionContext = indexService.newSearchExecutionContext(
            randomInt(20),
            0,
            null,
            () -> { throw new UnsupportedOperationException(); },
            null,
            emptyMap()
        );
        MultiMatchQueryParser parser = new MultiMatchQueryParser(searchExecutionContext);
        parser.setAnalyzer(new MockSynonymAnalyzer());
        Map<String, Float> fieldNames = new HashMap<>();
        fieldNames.put("name.first", 1.0f);
        fieldNames.put("name.last", 1.0f);
        Query query = parser.parse(MultiMatchQueryBuilder.Type.CROSS_FIELDS, fieldNames, "guinea pig", null);

        Term[] terms = new Term[2];
        terms[0] = new Term("name.first", "cavy");
        terms[1] = new Term("name.last", "cavy");
        float[] boosts = new float[2];
        Arrays.fill(boosts, 1.0f);

        List<Query> phraseDisjuncts = new ArrayList<>();
        phraseDisjuncts.add(new PhraseQuery.Builder().add(new Term("name.first", "guinea")).add(new Term("name.first", "pig")).build());
        phraseDisjuncts.add(new PhraseQuery.Builder().add(new Term("name.last", "guinea")).add(new Term("name.last", "pig")).build());
        BooleanQuery expected = new BooleanQuery.Builder().add(
            new BooleanQuery.Builder().add(new DisjunctionMaxQuery(phraseDisjuncts, 0.0f), BooleanClause.Occur.SHOULD)
                .add(BlendedTermQuery.dismaxBlendedQuery(terms, boosts, 1.0f), BooleanClause.Occur.SHOULD)
                .build(),
            BooleanClause.Occur.SHOULD
        ).build();
        assertEquals(expected, query);
    }

    public void testCrossFieldsWithAnalyzerGroups() throws IOException {
        SearchExecutionContext searchExecutionContext = indexService.newSearchExecutionContext(
            randomInt(20),
            0,
            null,
            () -> 0L,
            null,
            emptyMap()
        );

        Map<String, Float> fieldNames = new HashMap<>();
        fieldNames.put("name.first", 1.0f);
        fieldNames.put("name.last", 1.0f);
        fieldNames.put("name.nickname", 1.0f);

        MultiMatchQueryParser parser = new MultiMatchQueryParser(searchExecutionContext);
        parser.setTieBreaker(0.3f);
        Query query = parser.parse(MultiMatchQueryBuilder.Type.CROSS_FIELDS, fieldNames, "Robert", null);

        Term[] terms = new Term[] { new Term("name.first", "robert"), new Term("name.last", "robert") };
        float[] boosts = new float[] { 1.0f, 1.0f };

        DisjunctionMaxQuery expected = new DisjunctionMaxQuery(
            Arrays.asList(BlendedTermQuery.dismaxBlendedQuery(terms, boosts, 0.3f), new TermQuery(new Term("name.nickname", "Robert"))),
            0.3f
        );
        assertEquals(expected, query);
    }

    public void testKeywordSplitQueriesOnWhitespace() throws IOException {
        IndexService indexService = createIndex(
            "test_keyword",
            Settings.builder()
                .put("index.analysis.normalizer.my_lowercase.type", "custom")
                .putList("index.analysis.normalizer.my_lowercase.filter", "lowercase")
                .build()
        );
        MapperService mapperService = indexService.mapperService();
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
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
                .endObject()
                .endObject()
        );
        mapperService.merge("type", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);
        SearchExecutionContext searchExecutionContext = indexService.newSearchExecutionContext(
            randomInt(20),
            0,
            null,
            () -> { throw new UnsupportedOperationException(); },
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
                new BooleanQuery.Builder().add(new TermQuery(new Term("field_split", "Foo")), BooleanClause.Occur.SHOULD)
                    .add(new TermQuery(new Term("field_split", "Bar")), BooleanClause.Occur.SHOULD)
                    .build(),
                new BooleanQuery.Builder().add(new TermQuery(new Term("field_split_normalizer", "foo")), BooleanClause.Occur.SHOULD)
                    .add(new TermQuery(new Term("field_split_normalizer", "bar")), BooleanClause.Occur.SHOULD)
                    .build()
            ),
            0.0f
        );
        assertThat(query, equalTo(expected));
    }
}
