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
package org.elasticsearch.search.source;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchException;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collections;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class MetadataFetchingIT extends ESIntegTestCase {
    public void testSimple() {
        assertAcked(prepareCreate("test"));
        ensureGreen();

        client().prepareIndex("test").setId("1").setSource("field", "value").get();
        refresh();

        SearchResponse response = client()
            .prepareSearch("test")
            .storedFields("_none_")
            .setFetchSource(false)
            .get();
        assertThat(response.getHits().getAt(0).getId(), nullValue());
        assertThat(response.getHits().getAt(0).getSourceAsString(), nullValue());

        response = client()
            .prepareSearch("test")
            .storedFields("_none_")
            .get();
        assertThat(response.getHits().getAt(0).getId(), nullValue());
        assertThat(response.getHits().getAt(0).getSourceAsString(), nullValue());
    }

    public void testInnerHits() {
        assertAcked(prepareCreate("test").setMapping("nested", "type=nested"));
        ensureGreen();
        client().prepareIndex("test").setId("1")
            .setSource("field", "value", "nested", Collections.singletonMap("title", "foo")).get();
        refresh();

        SearchResponse response = client()
            .prepareSearch("test")
            .storedFields("_none_")
            .setFetchSource(false)
            .setQuery(
                new NestedQueryBuilder("nested", new TermQueryBuilder("nested.title", "foo"), ScoreMode.Total)
                    .innerHit(new InnerHitBuilder()
                        .setStoredFieldNames(Collections.singletonList("_none_"))
                        .setFetchSourceContext(new FetchSourceContext(false)))
            )
            .get();
        assertThat(response.getHits().getTotalHits().value, equalTo(1L));
        assertThat(response.getHits().getAt(0).getId(), nullValue());
        assertThat(response.getHits().getAt(0).getSourceAsString(), nullValue());
        assertThat(response.getHits().getAt(0).getInnerHits().size(), equalTo(1));
        SearchHits hits = response.getHits().getAt(0).getInnerHits().get("nested");
        assertThat(hits.getTotalHits().value, equalTo(1L));
        assertThat(hits.getAt(0).getId(), nullValue());
        assertThat(hits.getAt(0).getSourceAsString(), nullValue());
    }

    public void testWithRouting() {
        assertAcked(prepareCreate("test"));
        ensureGreen();

        client().prepareIndex("test").setId("1").setSource("field", "value").setRouting("toto").get();
        refresh();

        SearchResponse response = client()
            .prepareSearch("test")
            .storedFields("_none_")
            .setFetchSource(false)
            .get();
        assertThat(response.getHits().getAt(0).getId(), nullValue());
        assertThat(response.getHits().getAt(0).field("_routing"), nullValue());
        assertThat(response.getHits().getAt(0).getSourceAsString(), nullValue());

        response = client()
            .prepareSearch("test")
            .storedFields("_none_")
            .get();
        assertThat(response.getHits().getAt(0).getId(), nullValue());
        assertThat(response.getHits().getAt(0).getSourceAsString(), nullValue());
    }

    public void testInvalid() {
        assertAcked(prepareCreate("test"));
        ensureGreen();

        indexDoc("test", "1", "field", "value");
        refresh();

        {
            SearchPhaseExecutionException exc = expectThrows(SearchPhaseExecutionException.class,
                () -> client().prepareSearch("test").setFetchSource(true).storedFields("_none_").get());
            Throwable rootCause = ExceptionsHelper.unwrap(exc, SearchException.class);
            assertNotNull(rootCause);
            assertThat(rootCause.getClass(), equalTo(SearchException.class));
            assertThat(rootCause.getMessage(),
                equalTo("[stored_fields] cannot be disabled if [_source] is requested"));
        }
        {
            SearchPhaseExecutionException exc = expectThrows(SearchPhaseExecutionException.class,
                () -> client().prepareSearch("test").storedFields("_none_").setVersion(true).get());
            Throwable rootCause = ExceptionsHelper.unwrap(exc, SearchException.class);
            assertNotNull(rootCause);
            assertThat(rootCause.getClass(), equalTo(SearchException.class));
            assertThat(rootCause.getMessage(),
                equalTo("[stored_fields] cannot be disabled if [version] is requested"));
        }
        {
            SearchPhaseExecutionException exc = expectThrows(SearchPhaseExecutionException.class,
                () -> client().prepareSearch("test").storedFields("_none_").addFetchField("field").get());
            Throwable rootCause = ExceptionsHelper.unwrap(exc, SearchException.class);
            assertNotNull(rootCause);
            assertThat(rootCause.getClass(), equalTo(SearchException.class));
            assertThat(rootCause.getMessage(),
                equalTo("[stored_fields] cannot be disabled when using the [fields] option"));
        }
        {
            IllegalArgumentException exc = expectThrows(IllegalArgumentException.class,
                () -> client().prepareSearch("test").storedFields("_none_", "field1").setVersion(true).get());
            assertThat(exc.getMessage(),
                equalTo("cannot combine _none_ with other fields"));
        }
        {
            IllegalArgumentException exc = expectThrows(IllegalArgumentException.class,
                () -> client().prepareSearch("test").storedFields("_none_").storedFields("field1").setVersion(true).get());
            assertThat(exc.getMessage(),
                equalTo("cannot combine _none_ with other fields"));
        }
    }
}

