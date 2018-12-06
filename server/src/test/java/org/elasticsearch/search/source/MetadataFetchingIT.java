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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchContextException;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collections;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class MetadataFetchingIT extends ESIntegTestCase {
    @Override
    protected boolean forbidPrivateIndexSettings() {
        // needed to create an index with multiple types
        return false;
    }

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

    public void testInnerHits() {
        assertAcked(prepareCreate("test_with_types")
            .addMapping("type1", "nested", "type=nested")
            .addMapping("type2", "nested", "type=nested")
            .setSettings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT.minimumCompatibilityVersion())));
        assertAcked(prepareCreate("test").addMapping("_doc", "nested", "type=nested"));
        ensureGreen();
        client().prepareIndex("test", "_doc", "1")
            .setSource("field", "value", "nested", Collections.singletonMap("title", "foo")).execute().actionGet();
        client().prepareIndex("test_with_types", "type1", "1")
            .setSource("field", "value", "nested", Collections.singletonMap("title", "foo")).execute().actionGet();
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
        assertThat(response.getHits().totalHits, equalTo(1L));
        assertThat(response.getHits().getAt(0).getId(), nullValue());
        assertThat(response.getHits().getAt(0).getType(), equalTo(null));
        assertThat(response.getHits().getAt(0).getSourceAsString(), nullValue());
        assertThat(response.getHits().getAt(0).getInnerHits().size(), equalTo(1));
        SearchHits hits = response.getHits().getAt(0).getInnerHits().get("nested");
        assertThat(hits.totalHits, equalTo(1L));
        assertThat(hits.getAt(0).getId(), nullValue());
        assertThat(hits.getAt(0).getType(), equalTo("_doc"));
        assertThat(hits.getAt(0).getSourceAsString(), nullValue());

        ElasticsearchException exc = expectThrows(ElasticsearchException.class, () -> client()
            .prepareSearch("test_with_types")
            .storedFields("_none_")
            .setFetchSource(false)
            .setAllowPartialSearchResults(false)
            .setQuery(
                new NestedQueryBuilder("nested", new TermQueryBuilder("nested.title", "foo"), ScoreMode.Total)
                    .innerHit(new InnerHitBuilder()
                        .setStoredFieldNames(Collections.singletonList("_none_"))
                        .setFetchSourceContext(new FetchSourceContext(false)))
            )
            .get());
        assertThat(exc.getDetailedMessage(), containsString("It is not allowed to disable stored fields"));
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

