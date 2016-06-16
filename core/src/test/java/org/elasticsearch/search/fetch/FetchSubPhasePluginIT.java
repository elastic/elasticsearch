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
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.termvectors.TermVectorsService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHitField;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.CoreMatchers.equalTo;

/**
 *
 */
@ClusterScope(scope = Scope.SUITE, supportsDedicatedMasters = false, numDataNodes = 1)
public class FetchSubPhasePluginIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(FetchTermVectorsPlugin.class);
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
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForYellowStatus().execute().actionGet();

        client().index(
                indexRequest("test").type("type1").id("1")
                        .source(jsonBuilder().startObject().field("test", "I am sam i am").endObject())).actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        XContentBuilder extSource = jsonBuilder().startObject()
                .field("term_vectors_fetch", "test")
                .endObject();
         SearchResponse response = client().prepareSearch().setSource(new SearchSourceBuilder().ext(extSource)).get();
        assertSearchResponse(response);
        assertThat(((Map<String, Integer>) response.getHits().getAt(0).field("term_vectors_fetch").getValues().get(0)).get("i"), equalTo(2));
        assertThat(((Map<String, Integer>) response.getHits().getAt(0).field("term_vectors_fetch").getValues().get(0)).get("am"),
                equalTo(2));
        assertThat(((Map<String, Integer>) response.getHits().getAt(0).field("term_vectors_fetch").getValues().get(0)).get("sam"),
                equalTo(1));
    }

    public static class FetchTermVectorsPlugin extends Plugin {
        public void onModule(SearchModule searchModule) {
            searchModule.registerFetchSubPhase(new TermVectorsFetchSubPhase());
        }
    }

    public final static class TermVectorsFetchSubPhase implements FetchSubPhase {

        public static final ContextFactory<TermVectorsFetchContext> CONTEXT_FACTORY = new ContextFactory<TermVectorsFetchContext>() {

            @Override
            public String getName() {
                return NAMES[0];
            }

            @Override
            public TermVectorsFetchContext newContextInstance() {
                return new TermVectorsFetchContext();
            }
        };

        public static final String[] NAMES = {"term_vectors_fetch"};

        @Override
        public Map<String, ? extends SearchParseElement> parseElements() {
            return singletonMap("term_vectors_fetch", new TermVectorsFetchParseElement());
        }

        @Override
        public void hitExecute(SearchContext context, HitContext hitContext) {
            if (context.getFetchSubPhaseContext(CONTEXT_FACTORY).hitExecutionNeeded() == false) {
                return;
            }
            String field = context.getFetchSubPhaseContext(CONTEXT_FACTORY).getField();

            if (hitContext.hit().fieldsOrNull() == null) {
                hitContext.hit().fields(new HashMap<>());
            }
            SearchHitField hitField = hitContext.hit().fields().get(NAMES[0]);
            if (hitField == null) {
                hitField = new InternalSearchHitField(NAMES[0], new ArrayList<>(1));
                hitContext.hit().fields().put(NAMES[0], hitField);
            }
            TermVectorsResponse termVector = TermVectorsService.getTermVectors(context.indexShard(), new TermVectorsRequest(context.indexShard().shardId().getIndex().getName(), hitContext.hit().type(), hitContext.hit().id()));
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

    public static class TermVectorsFetchParseElement extends FetchSubPhaseParseElement<TermVectorsFetchContext> {

        @Override
        protected void innerParse(XContentParser parser, TermVectorsFetchContext termVectorsFetchContext, SearchContext searchContext)
                throws Exception {
            XContentParser.Token token = parser.currentToken();
            if (token == XContentParser.Token.VALUE_STRING) {
                String fieldName = parser.text();
                termVectorsFetchContext.setField(fieldName);
            } else {
                throw new IllegalStateException("Expected a VALUE_STRING but got " + token);
            }
        }

        @Override
        protected FetchSubPhase.ContextFactory getContextFactory() {
            return TermVectorsFetchSubPhase.CONTEXT_FACTORY;
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
