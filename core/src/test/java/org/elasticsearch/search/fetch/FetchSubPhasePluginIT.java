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

package org.elasticsearch.search.fetch;


import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.action.termvectors.TermVectorsResponse;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.termvectors.TermVectorsService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.InternalSearchHitField;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.CoreMatchers.equalTo;

@ClusterScope(scope = Scope.SUITE, supportsDedicatedMasters = false, numDataNodes = 1)
public class FetchSubPhasePluginIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(FetchTermVectorsPlugin.class);
    }

    public void testPlugin() throws Exception {
        client().admin()
                .indices()
                .prepareCreate("test")
                .addMapping(
                        "type1",
                        jsonBuilder()
                                .startObject().startObject("type1")
                                .startObject("properties")
                                .startObject("test")
                                .field("type", "text").field("term_vector", "yes")
                                .endObject()
                                .endObject()
                                .endObject().endObject()).execute().actionGet();

        client().index(
                indexRequest("test").type("type1").id("1")
                        .source(jsonBuilder().startObject().field("test", "I am sam i am").endObject())).actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        XContentBuilder extSource = jsonBuilder().startObject()
                .field("term_vectors_fetch", "test")
                .endObject();
         SearchResponse response = client().prepareSearch().setSource(new SearchSourceBuilder().ext(extSource)).get();
        assertSearchResponse(response);
        assertThat(((Map<String, Integer>) response.getHits().getAt(0).field("term_vectors_fetch").getValues().get(0)).get("i"),
                equalTo(2));
        assertThat(((Map<String, Integer>) response.getHits().getAt(0).field("term_vectors_fetch").getValues().get(0)).get("am"),
                equalTo(2));
        assertThat(((Map<String, Integer>) response.getHits().getAt(0).field("term_vectors_fetch").getValues().get(0)).get("sam"),
                equalTo(1));
    }

    public static class FetchTermVectorsPlugin extends Plugin implements SearchPlugin {
        @Override
        public List<FetchSubPhase> getFetchSubPhases(FetchPhaseConstructionContext context) {
            return singletonList(new TermVectorsFetchSubPhase());
        }
    }

    public static final class TermVectorsFetchSubPhase implements FetchSubPhase {

        public static final String NAME = "term_vectors_fetch";

        @Override
        public Map<String, ? extends FetchSubPhaseParser> parsers() {
            return singletonMap("term_vectors_fetch", new TermVectorsFetchParser());
        }

        @Override
        public void hitExecute(SearchContext context, HitContext hitContext) {
            TermVectorsFetchContext fetchSubPhaseContext = context.getFetchSubPhaseContext(NAME);
            if (fetchSubPhaseContext.hitExecutionNeeded() == false) {
                return;
            }
            String field = fetchSubPhaseContext.getField();

            if (hitContext.hit().fieldsOrNull() == null) {
                hitContext.hit().fields(new HashMap<>());
            }
            SearchHitField hitField = hitContext.hit().fields().get(NAME);
            if (hitField == null) {
                hitField = new InternalSearchHitField(NAME, new ArrayList<>(1));
                hitContext.hit().fields().put(NAME, hitField);
            }
            TermVectorsRequest termVectorsRequest = new TermVectorsRequest(context.indexShard().shardId().getIndex().getName(),
                    hitContext.hit().type(), hitContext.hit().id());
            TermVectorsResponse termVector = TermVectorsService.getTermVectors(context.indexShard(), termVectorsRequest);
            try {
                Map<String, Integer> tv = new HashMap<>();
                TermsEnum terms = termVector.getFields().terms(field).iterator();
                BytesRef term;
                while ((term = terms.next()) != null) {
                    tv.put(term.utf8ToString(), terms.postings(null, PostingsEnum.ALL).freq());
                }
                hitField.values().add(tv);
            } catch (IOException e) {
                ESLoggerFactory.getLogger(FetchSubPhasePluginIT.class.getName()).info("Swallowed exception", e);
            }
        }
    }

    public static class TermVectorsFetchParser implements FetchSubPhaseParser<TermVectorsFetchContext> {

        @Override
        public TermVectorsFetchContext parse(XContentParser parser) throws Exception {
            // this is to make sure that the SubFetchPhase knows it should execute
            TermVectorsFetchContext termVectorsFetchContext = new TermVectorsFetchContext();
            termVectorsFetchContext.setHitExecutionNeeded(true);
            XContentParser.Token token = parser.currentToken();
            if (token == XContentParser.Token.VALUE_STRING) {
                String fieldName = parser.text();
                termVectorsFetchContext.setField(fieldName);
            } else {
                throw new IllegalStateException("Expected a VALUE_STRING but got " + token);
            }
            return termVectorsFetchContext;
        }
    }

    public static class TermVectorsFetchContext extends FetchSubPhaseContext {

        private String field = null;

        public TermVectorsFetchContext() {
        }

        public void setField(String field) {
            this.field = field;
        }

        public String getField() {
            return field;
        }
    }
}
