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

package org.elasticsearch.codecs;

import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class CodecTests extends ElasticsearchIntegrationTest {

    @Override
    protected int numberOfShards() {
        return 1;
    }

    @Override
    protected int numberOfReplicas() {
        return 0;
    }

    @Test
    public void testFieldsWithCustomPostingsFormat() throws Exception {
        try {
            client().admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }

        assertAcked(prepareCreate("test")
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("field1")
                        .field("postings_format", "test1").field("index_options", "docs").field("type", "string").endObject().endObject().endObject().endObject())
                .setSettings(ImmutableSettings.settingsBuilder()
                        .put(indexSettings())
                        .put("index.codec.postings_format.test1.type", "default")));

        client().prepareIndex("test", "type1", "1").setSource("field1", "quick brown fox", "field2", "quick brown fox").execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource("field1", "quick lazy huge brown fox", "field2", "quick lazy huge brown fox").setRefresh(true).execute().actionGet();

        SearchResponse searchResponse = client().prepareSearch().setQuery(QueryBuilders.matchQuery("field2", "quick brown").type(MatchQueryBuilder.Type.PHRASE).slop(0)).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        try {
            client().prepareSearch().setQuery(QueryBuilders.matchQuery("field1", "quick brown").type(MatchQueryBuilder.Type.PHRASE).slop(0)).execute().actionGet();
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.getMessage(), endsWith("IllegalStateException[field \"field1\" was indexed without position data; cannot run PhraseQuery (term=quick)]; }"));
        }
    }

    @Test
    public void testCustomDocValuesFormat() throws IOException {
        try {
            client().admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }

        assertAcked(prepareCreate("test")
            .addMapping("test", jsonBuilder().startObject().startObject("test")
                    .startObject("_version").field("doc_values_format", "Lucene410").endObject()
                    .startObject("properties").startObject("field").field("type", "long").field("doc_values_format", "dvf").endObject().endObject()
                    .endObject().endObject())
            .setSettings(ImmutableSettings.settingsBuilder()
                    .put(indexSettings())
                    .put("index.codec.doc_values_format.dvf.type", "default")));

        for (int i = 10; i >= 0; --i) {
            client().prepareIndex("test", "test", Integer.toString(i)).setSource("field", randomLong()).setRefresh(i == 0 || rarely()).execute().actionGet();
        }

        SearchResponse searchResponse = client().prepareSearch().setQuery(QueryBuilders.matchAllQuery()).addSort(new FieldSortBuilder("field")).execute().actionGet();
        assertThat(searchResponse.getFailedShards(), equalTo(0));
    }

}
