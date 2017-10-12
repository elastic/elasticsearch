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

import org.apache.lucene.index.Term;
import org.apache.lucene.queries.BlendedTermQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.lucene.search.MultiPhrasePrefixQuery;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MockFieldMapper.FakeFieldType;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.search.MultiMatchQuery.FieldAndFieldType;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.index.query.QueryBuilders.multiMatchQuery;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class MultiMatchQueryTests extends ESSingleNodeTestCase {

    private IndexService indexService;

    @Before
    public void setup() throws IOException {
        Settings settings = Settings.builder()
            .put("index.analysis.filter.syns.type","synonym")
            .putList("index.analysis.filter.syns.synonyms","quick,fast")
            .put("index.analysis.analyzer.syns.tokenizer","standard")
            .put("index.analysis.analyzer.syns.filter","syns").build();
        IndexService indexService = createIndex("test", settings);
        MapperService mapperService = indexService.mapperService();
        String mapping = "{\n" +
                "    \"person\":{\n" +
                "        \"properties\":{\n" +
                "            \"name\":{\n" +
                "                  \"properties\":{\n" +
                "                        \"first\": {\n" +
                "                            \"type\":\"text\",\n" +
                "                            \"analyzer\":\"syns\"\n" +
                "                        }," +
                "                        \"last\": {\n" +
                "                            \"type\":\"text\",\n" +
                "                            \"analyzer\":\"syns\"\n" +
                "                        }" +
                "                   }" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "}";
        mapperService.merge("person", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE, false);
        this.indexService = indexService;
    }

    public void testCrossFieldMultiMatchQuery() throws IOException {
        QueryShardContext queryShardContext = indexService.newQueryShardContext(
                randomInt(20), null, () -> { throw new UnsupportedOperationException(); }, null);
        queryShardContext.setAllowUnmappedFields(true);
        Query parsedQuery = multiMatchQuery("banon").field("name.first", 2).field("name.last", 3).field("foobar").type(MultiMatchQueryBuilder.Type.CROSS_FIELDS).toQuery(queryShardContext);
        try (Engine.Searcher searcher = indexService.getShard(0).acquireSearcher("test")) {
            Query rewrittenQuery = searcher.searcher().rewrite(parsedQuery);
            Query tq1 = new BoostQuery(new TermQuery(new Term("name.first", "banon")), 2);
            Query tq2 = new BoostQuery(new TermQuery(new Term("name.last", "banon")), 3);
            Query expected = new DisjunctionMaxQuery(
                Arrays.asList(
                    new MatchNoDocsQuery("unknown field foobar"),
                    new DisjunctionMaxQuery(Arrays.asList(tq2, tq1), 0f)
                ), 0f);
            assertEquals(expected, rewrittenQuery);
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
                new BytesRef("baz"), null, 1f, new FieldAndFieldType(ft1, 2), new FieldAndFieldType(ft2, 3));
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
                new BytesRef("baz"), null, 1f, new FieldAndFieldType(ft1, 2), new FieldAndFieldType(ft2, 3));
        assertEquals(expected, actual);
    }

    public void testBlendTermsUnsupportedValue() {
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
        Query expected = BlendedTermQuery.dismaxBlendedQuery(terms, boosts, 1.0f);
        Query actual = MultiMatchQuery.blendTerm(
                indexService.newQueryShardContext(randomInt(20), null, () -> { throw new UnsupportedOperationException(); }, null),
                new BytesRef("baz"), null, 1f, new FieldAndFieldType(ft1, 2), new FieldAndFieldType(ft2, 3));
        assertEquals(expected, actual);
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
                new BytesRef("baz"), null, 1f, new FieldAndFieldType(ft1, 2), new FieldAndFieldType(ft2, 3));
        assertEquals(expected, actual);
    }

    public void testMultiMatchPrefixWithAllField() throws IOException {
        QueryShardContext queryShardContext = indexService.newQueryShardContext(
                randomInt(20), null, () -> { throw new UnsupportedOperationException(); }, null);
        queryShardContext.setAllowUnmappedFields(true);
        Query parsedQuery =
            multiMatchQuery("foo").field("_all").type(MultiMatchQueryBuilder.Type.PHRASE_PREFIX).toQuery(queryShardContext);
        assertThat(parsedQuery, instanceOf(MultiPhrasePrefixQuery.class));
        assertThat(parsedQuery.toString(), equalTo("_all:\"foo*\""));
    }

    public void testMultiMatchCrossFieldsWithSynonyms() throws IOException {
        QueryShardContext queryShardContext = indexService.newQueryShardContext(
            randomInt(20), null, () -> { throw new UnsupportedOperationException(); }, null);

        // check that synonym query is used for a single field
        Query parsedQuery =
            multiMatchQuery("quick").field("name.first")
                .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS).toQuery(queryShardContext);
        Term[] terms = new Term[2];
        terms[0] = new Term("name.first", "quick");
        terms[1] = new Term("name.first", "fast");
        Query expectedQuery = new SynonymQuery(terms);
        assertThat(parsedQuery, equalTo(expectedQuery));

        // check that blended term query is used for multiple fields
        parsedQuery =
            multiMatchQuery("quick").field("name.first").field("name.last")
                .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS).toQuery(queryShardContext);
        terms = new Term[4];
        terms[0] = new Term("name.first", "quick");
        terms[1] = new Term("name.first", "fast");
        terms[2] = new Term("name.last", "quick");
        terms[3] = new Term("name.last", "fast");
        float[] boosts = new float[4];
        Arrays.fill(boosts, 1.0f);
        expectedQuery = BlendedTermQuery.dismaxBlendedQuery(terms, boosts, 1.0f);
        assertThat(parsedQuery, equalTo(expectedQuery));

    }
}
