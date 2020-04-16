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

package org.elasticsearch.index.search;

import org.apache.lucene.analysis.MockSynonymAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.BlendedTermQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MockFieldMapper.FakeFieldType;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.search.MultiMatchQuery.FieldAndBoost;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.MockKeywordPlugin;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.multiMatchQuery;
import static org.hamcrest.Matchers.equalTo;

public class MultiMatchQueryTests extends ESSingleNodeTestCase {

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
        String mapping = "{\n" +
                "    \"person\":{\n" +
                "        \"properties\":{\n" +
                "            \"name\":{\n" +
                "                  \"properties\":{\n" +
                "                        \"first\": {\n" +
                "                            \"type\":\"text\",\n" +
                "                            \"analyzer\":\"standard\"\n" +
                "                        }," +
                "                        \"last\": {\n" +
                "                            \"type\":\"text\",\n" +
                "                            \"analyzer\":\"standard\"\n" +
                "                        }" +
                "                   }" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "}";
        mapperService.merge("person", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);
        this.indexService = indexService;
    }

    public void testCrossFieldMultiMatchQuery() throws IOException {
        QueryShardContext queryShardContext = indexService.newQueryShardContext(
                randomInt(20), null, () -> { throw new UnsupportedOperationException(); }, null);
        queryShardContext.setAllowUnmappedFields(true);
        for (float tieBreaker : new float[] {0.0f, 0.5f}) {
            Query parsedQuery = multiMatchQuery("banon")
                .field("name.first", 2)
                .field("name.last", 3)
                .field("foobar")
                .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
                .tieBreaker(tieBreaker)
                .toQuery(queryShardContext);
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
        FakeFieldType ft1 = new FakeFieldType();
        ft1.setName("foo");
        FakeFieldType ft2 = new FakeFieldType();
        ft2.setName("bar");
        Term[] terms = new Term[] { new Term("foo", "baz"), new Term("bar", "baz") };
        float[] boosts = new float[] {2, 3};
        Query expected = BlendedTermQuery.dismaxBlendedQuery(terms, boosts, 1.0f);
        Query actual = MultiMatchQuery.blendTerm(
                indexService.newQueryShardContext(randomInt(20), null, () -> { throw new UnsupportedOperationException(); }, null),
                new BytesRef("baz"), 1f, false, Arrays.asList(new FieldAndBoost(ft1, 2), new FieldAndBoost(ft2, 3)));
        assertEquals(expected, actual);
    }

    public void testBlendTermsWithFieldBoosts() {
        FakeFieldType ft1 = new FakeFieldType();
        ft1.setName("foo");
        ft1.setBoost(100);
        FakeFieldType ft2 = new FakeFieldType();
        ft2.setName("bar");
        ft2.setBoost(10);
        Term[] terms = new Term[] { new Term("foo", "baz"), new Term("bar", "baz") };
        float[] boosts = new float[] {200, 30};
        Query expected = BlendedTermQuery.dismaxBlendedQuery(terms, boosts, 1.0f);
        Query actual = MultiMatchQuery.blendTerm(
                indexService.newQueryShardContext(randomInt(20), null, () -> { throw new UnsupportedOperationException(); }, null),
                new BytesRef("baz"), 1f, false, Arrays.asList(new FieldAndBoost(ft1, 2), new FieldAndBoost(ft2, 3)));
        assertEquals(expected, actual);
    }

    public void testBlendTermsUnsupportedValueWithLenient() {
        FakeFieldType ft1 = new FakeFieldType();
        ft1.setName("foo");
        FakeFieldType ft2 = new FakeFieldType() {
            @Override
            public Query termQuery(Object value, QueryShardContext context) {
                throw new IllegalArgumentException();
            }
        };
        ft2.setName("bar");
        Term[] terms = new Term[] { new Term("foo", "baz") };
        float[] boosts = new float[] {2};
        Query expected = new DisjunctionMaxQuery(Arrays.asList(
            Queries.newMatchNoDocsQuery("failed [" + ft2.name() + "] query, caused by illegal_argument_exception:[null]"),
            BlendedTermQuery.dismaxBlendedQuery(terms, boosts, 1.0f)
        ), 1f);
        Query actual = MultiMatchQuery.blendTerm(
                indexService.newQueryShardContext(randomInt(20), null, () -> { throw new UnsupportedOperationException(); }, null),
                new BytesRef("baz"), 1f, true, Arrays.asList(new FieldAndBoost(ft1, 2), new FieldAndBoost(ft2, 3)));
        assertEquals(expected, actual);
    }

    public void testBlendTermsUnsupportedValueWithoutLenient() {
        FakeFieldType ft = new FakeFieldType() {
            @Override
            public Query termQuery(Object value, QueryShardContext context) {
                throw new IllegalArgumentException();
            }
        };
        ft.setName("bar");
        expectThrows(IllegalArgumentException.class, () -> MultiMatchQuery.blendTerm(
            indexService.newQueryShardContext(randomInt(20), null, () -> { throw new UnsupportedOperationException(); }, null),
            new BytesRef("baz"), 1f, false, Arrays.asList(new FieldAndBoost(ft, 1))));
    }

    public void testBlendNoTermQuery() {
        FakeFieldType ft1 = new FakeFieldType();
        ft1.setName("foo");
        FakeFieldType ft2 = new FakeFieldType() {
            @Override
            public Query termQuery(Object value, QueryShardContext context) {
                return new MatchAllDocsQuery();
            }
        };
        ft2.setName("bar");
        Term[] terms = new Term[] { new Term("foo", "baz") };
        float[] boosts = new float[] {2};
        Query expectedDisjunct1 = BlendedTermQuery.dismaxBlendedQuery(terms, boosts, 1.0f);
        Query expectedDisjunct2 = new BoostQuery(new MatchAllDocsQuery(), 3);
        Query expected = new DisjunctionMaxQuery(
            Arrays.asList(
                expectedDisjunct2,
                expectedDisjunct1
            ), 1.0f);
        Query actual = MultiMatchQuery.blendTerm(
                indexService.newQueryShardContext(randomInt(20), null, () -> { throw new UnsupportedOperationException(); }, null),
                new BytesRef("baz"), 1f, false, Arrays.asList(new FieldAndBoost(ft1, 2), new FieldAndBoost(ft2, 3)));
        assertEquals(expected, actual);
    }

    public void testMultiMatchCrossFieldsWithSynonyms() throws IOException {
        QueryShardContext queryShardContext = indexService.newQueryShardContext(
            randomInt(20), null, () -> { throw new UnsupportedOperationException(); }, null);

        MultiMatchQuery parser = new MultiMatchQuery(queryShardContext);
        parser.setAnalyzer(new MockSynonymAnalyzer());
        Map<String, Float> fieldNames = new HashMap<>();
        fieldNames.put("name.first", 1.0f);

        // check that synonym query is used for a single field
        Query parsedQuery = parser.parse(MultiMatchQueryBuilder.Type.CROSS_FIELDS, fieldNames, "dogs", null);
        Term[] terms = new Term[2];
        terms[0] = new Term("name.first", "dog");
        terms[1] = new Term("name.first", "dogs");
        Query expectedQuery = new SynonymQuery(terms);
        assertThat(parsedQuery, equalTo(expectedQuery));

        // check that blended term query is used for multiple fields
        fieldNames.put("name.last", 1.0f);
        parsedQuery = parser.parse(MultiMatchQueryBuilder.Type.CROSS_FIELDS, fieldNames, "dogs", null);
        terms = new Term[4];
        terms[0] = new Term("name.first", "dog");
        terms[1] = new Term("name.first", "dogs");
        terms[2] = new Term("name.last", "dog");
        terms[3] = new Term("name.last", "dogs");
        float[] boosts = new float[4];
        Arrays.fill(boosts, 1.0f);
        expectedQuery = BlendedTermQuery.dismaxBlendedQuery(terms, boosts, 1.0f);
        assertThat(parsedQuery, equalTo(expectedQuery));

    }

    public void testMultiMatchCrossFieldsWithSynonymsPhrase() throws IOException {
        QueryShardContext queryShardContext = indexService.newQueryShardContext(
            randomInt(20), null, () -> { throw new UnsupportedOperationException(); }, null);
        MultiMatchQuery parser = new MultiMatchQuery(queryShardContext);
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
        phraseDisjuncts.add(
            new PhraseQuery.Builder()
                .add(new Term("name.first", "guinea"))
                .add(new Term("name.first", "pig"))
                .build()
        );
        phraseDisjuncts.add(
            new PhraseQuery.Builder()
                .add(new Term("name.last", "guinea"))
                .add(new Term("name.last", "pig"))
                .build()
        );
        BooleanQuery expected = new BooleanQuery.Builder()
            .add(
                new BooleanQuery.Builder()
                    .add(new DisjunctionMaxQuery(phraseDisjuncts, 0.0f), BooleanClause.Occur.SHOULD)
                    .add(BlendedTermQuery.dismaxBlendedQuery(terms, boosts, 1.0f), BooleanClause.Occur.SHOULD)
                    .build(),
                BooleanClause.Occur.SHOULD
            )
            .build();
        assertEquals(expected, query);
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
        QueryShardContext queryShardContext = indexService.newQueryShardContext(
            randomInt(20), null, () -> {
                throw new UnsupportedOperationException();
            }, null);
        MultiMatchQuery parser = new MultiMatchQuery(queryShardContext);
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
