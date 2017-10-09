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

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchContextException;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class MetadataFetchingIT extends ESIntegTestCase {
    public void testSimple() {
        assertAcked(prepareCreate("test"));
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource("field", "value").execute().actionGet();
        refresh();

        SearchResponse response = client()
            .prepareSearch("test")
            .storedFields("_none_")
            .setFetchSource(false)
            .get();
        assertThat(response.getHits().getAt(0).getId(), nullValue());
        assertThat(response.getHits().getAt(0).getType(), nullValue());
        assertThat(response.getHits().getAt(0).getSourceAsString(), nullValue());

        response = client()
            .prepareSearch("test")
            .storedFields("_none_")
            .get();
        assertThat(response.getHits().getAt(0).getId(), nullValue());
        assertThat(response.getHits().getAt(0).getType(), nullValue());
        assertThat(response.getHits().getAt(0).getSourceAsString(), nullValue());
    }

    public void testWithRouting() {
        assertAcked(prepareCreate("test"));
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource("field", "value").setRouting("toto").execute().actionGet();
        refresh();

        SearchResponse response = client()
            .prepareSearch("test")
            .storedFields("_none_")
            .setFetchSource(false)
            .get();
        assertThat(response.getHits().getAt(0).getId(), nullValue());
        assertThat(response.getHits().getAt(0).getType(), nullValue());
        assertThat(response.getHits().getAt(0).field("_routing"), nullValue());
        assertThat(response.getHits().getAt(0).getSourceAsString(), nullValue());

        response = client()
            .prepareSearch("test")
            .storedFields("_none_")
            .get();
        assertThat(response.getHits().getAt(0).getId(), nullValue());
        assertThat(response.getHits().getAt(0).getType(), nullValue());
        assertThat(response.getHits().getAt(0).getSourceAsString(), nullValue());
    }

    public void testInvalid() {
        assertAcked(prepareCreate("test"));
        ensureGreen();

        index("test", "type1", "1", "field", "value");
        refresh();

        {
            SearchPhaseExecutionException exc = expectThrows(SearchPhaseExecutionException.class,
                () -> client().prepareSearch("test").setFetchSource(true).storedFields("_none_").get());
            Throwable rootCause = ExceptionsHelper.unwrap(exc, SearchContextException.class);
            assertNotNull(rootCause);
            assertThat(rootCause.getClass(), equalTo(SearchContextException.class));
            assertThat(rootCause.getMessage(),
                equalTo("`stored_fields` cannot be disabled if _source is requested"));
        }
        {
            SearchPhaseExecutionException exc = expectThrows(SearchPhaseExecutionException.class,
                () -> client().prepareSearch("test").storedFields("_none_").setVersion(true).get());
            Throwable rootCause = ExceptionsHelper.unwrap(exc, SearchContextException.class);
            assertNotNull(rootCause);
            assertThat(rootCause.getClass(), equalTo(SearchContextException.class));
            assertThat(rootCause.getMessage(),
                equalTo("`stored_fields` cannot be disabled if version is requested"));
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

