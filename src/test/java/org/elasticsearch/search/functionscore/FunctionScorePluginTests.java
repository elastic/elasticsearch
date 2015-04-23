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

package org.elasticsearch.search.functionscore;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.functionscore.DecayFunction;
import org.elasticsearch.index.query.functionscore.DecayFunctionBuilder;
import org.elasticsearch.index.query.functionscore.DecayFunctionParser;
import org.elasticsearch.index.query.functionscore.FunctionScoreModule;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.junit.Test;

import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.client.Requests.searchRequest;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
@ClusterScope(scope = Scope.SUITE, numDataNodes = 1)
public class FunctionScorePluginTests extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("plugin.types", CustomDistanceScorePlugin.class.getName())
                .build();
    }

    @Test
    public void testPlugin() throws Exception {
        client().admin()
                .indices()
                .prepareCreate("test")
                .addMapping(
                        "type1",
                        jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("test")
                                .field("type", "string").endObject().startObject("num1").field("type", "date").endObject().endObject()
                                .endObject().endObject()).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForYellowStatus().execute().actionGet();

        client().index(
                indexRequest("test").type("type1").id("1")
                        .source(jsonBuilder().startObject().field("test", "value").field("num1", "2013-05-26").endObject())).actionGet();
        client().index(
                indexRequest("test").type("type1").id("2")
                        .source(jsonBuilder().startObject().field("test", "value").field("num1", "2013-05-27").endObject())).actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();
        DecayFunctionBuilder gfb = new CustomDistanceScoreBuilder("num1", "2013-05-28", "+1d");

        ActionFuture<SearchResponse> response = client().search(searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                searchSource().explain(false).query(functionScoreQuery(termQuery("test", "value")).add(gfb))));

        SearchResponse sr = response.actionGet();
        ElasticsearchAssertions.assertNoFailures(sr);
        SearchHits sh = sr.getHits();

        assertThat(sh.hits().length, equalTo(2));
        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat(sh.getAt(1).getId(), equalTo("2"));

    }

    public static class CustomDistanceScorePlugin extends AbstractPlugin {

        @Override
        public String name() {
            return "test-plugin-distance-score";
        }

        @Override
        public String description() {
            return "Distance score plugin to test pluggable implementation";
        }

        public void onModule(FunctionScoreModule scoreModule) {
            scoreModule.registerParser(FunctionScorePluginTests.CustomDistanceScoreParser.class);
        }

    }

    public static class CustomDistanceScoreParser extends DecayFunctionParser {

        public static final String[] NAMES = { "linear_mult", "linearMult" };

        @Override
        public String[] getNames() {
            return NAMES;
        }

        static final DecayFunction decayFunction = new LinearMultScoreFunction();

        @Override
        public DecayFunction getDecayFunction() {
            return decayFunction;
        }

        static class LinearMultScoreFunction implements DecayFunction {
            LinearMultScoreFunction() {
            }

            @Override
            public double evaluate(double value, double scale) {
                
                return value;
            }

            @Override
            public Explanation explainFunction(String distanceString, double distanceVal, double scale) {
                return Explanation.match((float) distanceVal, "" + distanceVal);
            }

            @Override
            public double processScale(double userGivenScale, double userGivenValue) {
                return userGivenScale;
            }
        }
    }

    public class CustomDistanceScoreBuilder extends DecayFunctionBuilder {

        public CustomDistanceScoreBuilder(String fieldName, Object origin, Object scale) {
            super(fieldName, origin, scale);
        }

        @Override
        public String getName() {
            return CustomDistanceScoreParser.NAMES[0];
        }

    }
}
