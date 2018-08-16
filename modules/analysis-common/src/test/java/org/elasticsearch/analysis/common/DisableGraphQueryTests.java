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

package org.elasticsearch.analysis.common;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.SimpleQueryStringBuilder;
import org.elasticsearch.index.query.SimpleQueryStringFlag;
import org.elasticsearch.index.search.MatchQuery;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

/**
 * Makes sure that graph analysis is disabled with shingle filters of different size
 */
public class DisableGraphQueryTests extends ESSingleNodeTestCase {
    private static IndexService indexService;
    private static QueryShardContext shardContext;
    private static Query expectedQuery;
    private static Query expectedPhraseQuery;
    private static Query expectedQueryWithUnigram;
    private static Query expectedPhraseQueryWithUnigram;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(CommonAnalysisPlugin.class);
    }

    @Before
    public void setup() {
        Settings settings = Settings.builder()
            .put("index.analysis.filter.shingle.type", "shingle")
            .put("index.analysis.filter.shingle.output_unigrams", false)
            .put("index.analysis.filter.shingle.min_size", 2)
            .put("index.analysis.filter.shingle.max_size", 2)
            .put("index.analysis.filter.shingle_unigram.type", "shingle")
            .put("index.analysis.filter.shingle_unigram.output_unigrams", true)
            .put("index.analysis.filter.shingle_unigram.min_size", 2)
            .put("index.analysis.filter.shingle_unigram.max_size", 2)
            .put("index.analysis.analyzer.text_shingle.tokenizer", "whitespace")
            .put("index.analysis.analyzer.text_shingle.filter", "lowercase, shingle")
            .put("index.analysis.analyzer.text_shingle_unigram.tokenizer", "whitespace")
            .put("index.analysis.analyzer.text_shingle_unigram.filter",
                "lowercase, shingle_unigram")
            .build();
        indexService = createIndex("test", settings, "t",
            "text_shingle", "type=text,analyzer=text_shingle",
            "text_shingle_unigram", "type=text,analyzer=text_shingle_unigram");
        shardContext = indexService.newQueryShardContext(0, null, () -> 0L, null);

        // parsed queries for "text_shingle_unigram:(foo bar baz)" with query parsers
        // that ignores position length attribute
         expectedQueryWithUnigram= new BooleanQuery.Builder()
            .add(
                new SynonymQuery(
                    new Term("text_shingle_unigram", "foo"),
                    new Term("text_shingle_unigram", "foo bar")
                ), BooleanClause.Occur.SHOULD)
            .add(
                new SynonymQuery(
                    new Term("text_shingle_unigram", "bar"),
                    new Term("text_shingle_unigram", "bar baz")
            ), BooleanClause.Occur.SHOULD)
            .add(
                new TermQuery(
                    new Term("text_shingle_unigram", "baz")
                ), BooleanClause.Occur.SHOULD)
            .build();

        // parsed query for "text_shingle_unigram:\"foo bar baz\" with query parsers
        // that ignores position length attribute
        expectedPhraseQueryWithUnigram = new MultiPhraseQuery.Builder()
            .add(
                new Term[] {
                    new Term("text_shingle_unigram", "foo"),
                    new Term("text_shingle_unigram", "foo bar")
                }, 0)
            .add(
                new Term[] {
                    new Term("text_shingle_unigram", "bar"),
                    new Term("text_shingle_unigram", "bar baz")
                }, 1)
            .add(
                new Term[] {
                    new Term("text_shingle_unigram", "baz"),
                }, 2)
            .build();

        // parsed query for "text_shingle:(foo bar baz)
        expectedQuery = new BooleanQuery.Builder()
            .add(
                new TermQuery(new Term("text_shingle", "foo bar")),
                BooleanClause.Occur.SHOULD
            )
            .add(
                new TermQuery(new Term("text_shingle","bar baz")),
                BooleanClause.Occur.SHOULD
            )
            .add(
                new TermQuery(new Term("text_shingle","baz biz")),
                BooleanClause.Occur.SHOULD
            )
            .build();

        // parsed query for "text_shingle:"foo bar baz"
        expectedPhraseQuery = new PhraseQuery.Builder()
            .add(
                new Term("text_shingle", "foo bar")
            )
            .add(
                new Term("text_shingle","bar baz")
            )
            .add(
                new Term("text_shingle","baz biz")
            )
            .build();
    }

    @After
    public void cleanup() {
        indexService = null;
        shardContext = null;
        expectedQuery = null;
        expectedPhraseQuery = null;
    }

    public void testMatchPhraseQuery() throws IOException {
        MatchPhraseQueryBuilder builder =
            new MatchPhraseQueryBuilder("text_shingle_unigram", "foo bar baz");
        Query query = builder.toQuery(shardContext);
        assertThat(expectedPhraseQueryWithUnigram, equalTo(query));

        builder =
            new MatchPhraseQueryBuilder("text_shingle", "foo bar baz biz");
        query = builder.toQuery(shardContext);
        assertThat(expectedPhraseQuery, equalTo(query));
    }

    public void testMatchQuery() throws IOException {
        MatchQueryBuilder builder =
            new MatchQueryBuilder("text_shingle_unigram", "foo bar baz");
        Query query = builder.toQuery(shardContext);
        assertThat(expectedQueryWithUnigram, equalTo(query));

        builder = new MatchQueryBuilder("text_shingle", "foo bar baz biz");
        query = builder.toQuery(shardContext);
        assertThat(expectedQuery, equalTo(query));
    }

    public void testMultiMatchQuery() throws IOException {
        MultiMatchQueryBuilder builder = new MultiMatchQueryBuilder("foo bar baz",
            "text_shingle_unigram");
        Query query = builder.toQuery(shardContext);
        assertThat(expectedQueryWithUnigram, equalTo(query));

        builder.type(MatchQuery.Type.PHRASE);
        query = builder.toQuery(shardContext);
        assertThat(expectedPhraseQueryWithUnigram, equalTo(query));

        builder = new MultiMatchQueryBuilder("foo bar baz biz", "text_shingle");
        query = builder.toQuery(shardContext);
        assertThat(expectedQuery, equalTo(query));

        builder.type(MatchQuery.Type.PHRASE);
        query = builder.toQuery(shardContext);
        assertThat(expectedPhraseQuery, equalTo(query));
    }

    public void testSimpleQueryString() throws IOException {
        SimpleQueryStringBuilder builder = new SimpleQueryStringBuilder("foo bar baz");
        builder.field("text_shingle_unigram");
        builder.flags(SimpleQueryStringFlag.NONE);
        Query query = builder.toQuery(shardContext);
        assertThat(expectedQueryWithUnigram, equalTo(query));

        builder = new SimpleQueryStringBuilder("\"foo bar baz\"");
        builder.field("text_shingle_unigram");
        builder.flags(SimpleQueryStringFlag.PHRASE);
        query = builder.toQuery(shardContext);
        assertThat(expectedPhraseQueryWithUnigram, equalTo(query));

        builder = new SimpleQueryStringBuilder("foo bar baz biz");
        builder.field("text_shingle");
        builder.flags(SimpleQueryStringFlag.NONE);
        query = builder.toQuery(shardContext);
        assertThat(expectedQuery, equalTo(query));

        builder = new SimpleQueryStringBuilder("\"foo bar baz biz\"");
        builder.field("text_shingle");
        builder.flags(SimpleQueryStringFlag.PHRASE);
        query = builder.toQuery(shardContext);
        assertThat(expectedPhraseQuery, equalTo(query));
    }

    public void testQueryString() throws IOException {
        QueryStringQueryBuilder builder = new QueryStringQueryBuilder("foo bar baz");
        builder.field("text_shingle_unigram");
        Query query = builder.toQuery(shardContext);
        assertThat(expectedQueryWithUnigram, equalTo(query));

        builder = new QueryStringQueryBuilder("\"foo bar baz\"");
        builder.field("text_shingle_unigram");
        query = builder.toQuery(shardContext);
        assertThat(expectedPhraseQueryWithUnigram, equalTo(query));

        builder = new QueryStringQueryBuilder("foo bar baz biz");
        builder.field("text_shingle");
        query = builder.toQuery(shardContext);
        assertThat(expectedQuery, equalTo(query));

        builder = new QueryStringQueryBuilder("\"foo bar baz biz\"");
        builder.field("text_shingle");
        query = builder.toQuery(shardContext);
        assertThat(expectedPhraseQuery, equalTo(query));
    }
}
