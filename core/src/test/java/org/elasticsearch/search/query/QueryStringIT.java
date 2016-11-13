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

package org.elasticsearch.search.query;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFirstHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSecondHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasId;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class QueryStringIT extends ESIntegTestCase {

    @Before
    public void setup() throws Exception {
        String indexBody = copyToStringFromClasspath("/org/elasticsearch/search/query/all-query-index.json");
        prepareCreate("test").setSource(indexBody).get();
        ensureGreen("test");
    }

    private QueryStringQueryBuilder lenientQuery(String queryText) {
        return queryStringQuery(queryText).lenient(true);
    }

    public void testBasicAllQuery() throws Exception {
        List<IndexRequestBuilder> reqs = new ArrayList<>();
        reqs.add(client().prepareIndex("test", "doc", "1").setSource("f1", "foo bar baz"));
        reqs.add(client().prepareIndex("test", "doc", "2").setSource("f2", "Bar"));
        reqs.add(client().prepareIndex("test", "doc", "3").setSource("f3", "foo bar baz"));
        indexRandom(true, false, reqs);

        SearchResponse resp = client().prepareSearch("test").setQuery(queryStringQuery("foo")).get();
        assertHitCount(resp, 2L);
        assertHits(resp.getHits(), "1", "3");

        resp = client().prepareSearch("test").setQuery(queryStringQuery("bar")).get();
        assertHitCount(resp, 2L);
        assertHits(resp.getHits(), "1", "3");

        resp = client().prepareSearch("test").setQuery(queryStringQuery("Bar")).get();
        assertHitCount(resp, 3L);
        assertHits(resp.getHits(), "1", "2", "3");

        resp = client().prepareSearch("test").setQuery(queryStringQuery("foa")).get();
        assertHitCount(resp, 1L);
        assertHits(resp.getHits(), "3");
    }

    public void testWithDate() throws Exception {
        List<IndexRequestBuilder> reqs = new ArrayList<>();
        reqs.add(client().prepareIndex("test", "doc", "1").setSource("f1", "foo", "f_date", "2015/09/02"));
        reqs.add(client().prepareIndex("test", "doc", "2").setSource("f1", "bar", "f_date", "2015/09/01"));
        indexRandom(true, false, reqs);

        SearchResponse resp = client().prepareSearch("test").setQuery(queryStringQuery("foo bar")).get();
        assertHits(resp.getHits(), "1", "2");
        assertHitCount(resp, 2L);

        resp = client().prepareSearch("test").setQuery(queryStringQuery("\"2015/09/02\"")).get();
        assertHits(resp.getHits(), "1");
        assertHitCount(resp, 1L);

        resp = client().prepareSearch("test").setQuery(queryStringQuery("bar \"2015/09/02\"")).get();
        assertHits(resp.getHits(), "1", "2");
        assertHitCount(resp, 2L);

        resp = client().prepareSearch("test").setQuery(queryStringQuery("\"2015/09/02\" \"2015/09/01\"")).get();
        assertHits(resp.getHits(), "1", "2");
        assertHitCount(resp, 2L);
    }

    public void testWithLotsOfTypes() throws Exception {
        List<IndexRequestBuilder> reqs = new ArrayList<>();
        reqs.add(client().prepareIndex("test", "doc", "1").setSource("f1", "foo",
                        "f_date", "2015/09/02",
                        "f_float", "1.7",
                        "f_ip", "127.0.0.1"));
        reqs.add(client().prepareIndex("test", "doc", "2").setSource("f1", "bar",
                        "f_date", "2015/09/01",
                        "f_float", "1.8",
                        "f_ip", "127.0.0.2"));
        indexRandom(true, false, reqs);

        SearchResponse resp = client().prepareSearch("test").setQuery(queryStringQuery("foo bar")).get();
        assertHits(resp.getHits(), "1", "2");
        assertHitCount(resp, 2L);

        resp = client().prepareSearch("test").setQuery(queryStringQuery("\"2015/09/02\"")).get();
        assertHits(resp.getHits(), "1");
        assertHitCount(resp, 1L);

        resp = client().prepareSearch("test").setQuery(queryStringQuery("127.0.0.2 \"2015/09/02\"")).get();
        assertHits(resp.getHits(), "1", "2");
        assertHitCount(resp, 2L);

        resp = client().prepareSearch("test").setQuery(queryStringQuery("127.0.0.1 1.8")).get();
        assertHits(resp.getHits(), "1", "2");
        assertHitCount(resp, 2L);
    }

    public void testDocWithAllTypes() throws Exception {
        List<IndexRequestBuilder> reqs = new ArrayList<>();
        String docBody = copyToStringFromClasspath("/org/elasticsearch/search/query/all-example-document.json");
        reqs.add(client().prepareIndex("test", "doc", "1").setSource(docBody));
        indexRandom(true, false, reqs);

        SearchResponse resp = client().prepareSearch("test").setQuery(queryStringQuery("foo")).get();
        assertHits(resp.getHits(), "1");
        resp = client().prepareSearch("test").setQuery(queryStringQuery("Bar")).get();
        assertHits(resp.getHits(), "1");
        resp = client().prepareSearch("test").setQuery(queryStringQuery("Baz")).get();
        assertHits(resp.getHits(), "1");
        resp = client().prepareSearch("test").setQuery(queryStringQuery("sbaz")).get();
        assertHits(resp.getHits(), "1");
        resp = client().prepareSearch("test").setQuery(queryStringQuery("19")).get();
        assertHits(resp.getHits(), "1");
        // nested doesn't match because it's hidden
        resp = client().prepareSearch("test").setQuery(queryStringQuery("1476383971")).get();
        assertHits(resp.getHits(), "1");
        // bool doesn't match
        resp = client().prepareSearch("test").setQuery(queryStringQuery("7")).get();
        assertHits(resp.getHits(), "1");
        resp = client().prepareSearch("test").setQuery(queryStringQuery("23")).get();
        assertHits(resp.getHits(), "1");
        resp = client().prepareSearch("test").setQuery(queryStringQuery("1293")).get();
        assertHits(resp.getHits(), "1");
        resp = client().prepareSearch("test").setQuery(queryStringQuery("42")).get();
        assertHits(resp.getHits(), "1");
        resp = client().prepareSearch("test").setQuery(queryStringQuery("1.7")).get();
        assertHits(resp.getHits(), "1");
        resp = client().prepareSearch("test").setQuery(queryStringQuery("1.5")).get();
        assertHits(resp.getHits(), "1");
        resp = client().prepareSearch("test").setQuery(queryStringQuery("12.23")).get();
        assertHits(resp.getHits(), "1");
        resp = client().prepareSearch("test").setQuery(queryStringQuery("127.0.0.1")).get();
        assertHits(resp.getHits(), "1");
        // binary doesn't match
        // suggest doesn't match
        // geo_point doesn't match
        // geo_shape doesn't match
    }

    public void testKeywordWithWhitespace() throws Exception {
        List<IndexRequestBuilder> reqs = new ArrayList<>();
        reqs.add(client().prepareIndex("test", "doc", "1").setSource("f2", "Foo Bar"));
        reqs.add(client().prepareIndex("test", "doc", "2").setSource("f1", "bar"));
        reqs.add(client().prepareIndex("test", "doc", "3").setSource("f1", "foo bar"));
        indexRandom(true, false, reqs);

        SearchResponse resp = client().prepareSearch("test").setQuery(queryStringQuery("foo")).get();
        assertHits(resp.getHits(), "3");
        assertHitCount(resp, 1L);

        resp = client().prepareSearch("test").setQuery(queryStringQuery("bar")).get();
        assertHits(resp.getHits(), "2", "3");
        assertHitCount(resp, 2L);

        resp = client().prepareSearch("test")
                .setQuery(queryStringQuery("Foo Bar").splitOnWhitespace(false))
                .get();
        assertHits(resp.getHits(), "1", "2", "3");
        assertHitCount(resp, 3L);
    }

    public void testExplicitAllFieldsRequested() throws Exception {
        String indexBody = copyToStringFromClasspath("/org/elasticsearch/search/query/all-query-index-with-all.json");
        prepareCreate("test2").setSource(indexBody).get();
        ensureGreen("test2");

        List<IndexRequestBuilder> reqs = new ArrayList<>();
        reqs.add(client().prepareIndex("test2", "doc", "1").setSource("f1", "foo", "f2", "eggplant"));
        indexRandom(true, false, reqs);

        SearchResponse resp = client().prepareSearch("test2").setQuery(
                queryStringQuery("foo eggplent").defaultOperator(Operator.AND)).get();
        assertHitCount(resp, 0L);

        resp = client().prepareSearch("test2").setQuery(
                queryStringQuery("foo eggplent").defaultOperator(Operator.AND).useAllFields(true)).get();
        assertHits(resp.getHits(), "1");
        assertHitCount(resp, 1L);

        Exception e = expectThrows(Exception.class, () ->
                client().prepareSearch("test2").setQuery(
                        queryStringQuery("blah").field("f1").useAllFields(true)).get());
        assertThat(ExceptionsHelper.detailedMessage(e),
                containsString("cannot use [all_fields] parameter in conjunction with [default_field] or [fields]"));
    }

    @LuceneTestCase.AwaitsFix(bugUrl="currently can't perform phrase queries on fields that don't support positions")
    public void testPhraseQueryOnFieldWithNoPositions() throws Exception {
        List<IndexRequestBuilder> reqs = new ArrayList<>();
        reqs.add(client().prepareIndex("test", "doc", "1").setSource("f1", "foo bar", "f4", "eggplant parmesan"));
        reqs.add(client().prepareIndex("test", "doc", "2").setSource("f1", "foo bar", "f4", "chicken parmesan"));
        indexRandom(true, false, reqs);

        SearchResponse resp = client().prepareSearch("test").setQuery(queryStringQuery("\"eggplant parmesan\"")).get();
        assertHits(resp.getHits(), "1");
        assertHitCount(resp, 1L);
    }

    private void assertHits(SearchHits hits, String... ids) {
        assertThat(hits.totalHits(), equalTo((long) ids.length));
        Set<String> hitIds = new HashSet<>();
        for (SearchHit hit : hits.getHits()) {
            hitIds.add(hit.id());
        }
        assertThat(hitIds, containsInAnyOrder(ids));
    }

}
