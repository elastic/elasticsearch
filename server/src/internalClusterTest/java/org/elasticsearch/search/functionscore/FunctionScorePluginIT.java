/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.functionscore;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.query.functionscore.DecayFunction;
import org.elasticsearch.index.query.functionscore.DecayFunctionBuilder;
import org.elasticsearch.index.query.functionscore.DecayFunctionParser;
import org.elasticsearch.index.query.functionscore.ScoreFunctionParser;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.elasticsearch.client.internal.Requests.indexRequest;
import static org.elasticsearch.client.internal.Requests.searchRequest;
import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = Scope.SUITE, supportsDedicatedMasters = false, numDataNodes = 1)
public class FunctionScorePluginIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(CustomDistanceScorePlugin.class);
    }

    public void testPlugin() throws Exception {
        client().admin()
            .indices()
            .prepareCreate("test")
            .setMapping(
                jsonBuilder().startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("test")
                    .field("type", "text")
                    .endObject()
                    .startObject("num1")
                    .field("type", "date")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .get();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForYellowStatus().get();

        client().index(
            indexRequest("test").id("1").source(jsonBuilder().startObject().field("test", "value").field("num1", "2013-05-26").endObject())
        ).actionGet();
        client().index(
            indexRequest("test").id("2").source(jsonBuilder().startObject().field("test", "value").field("num1", "2013-05-27").endObject())
        ).actionGet();

        client().admin().indices().prepareRefresh().get();
        DecayFunctionBuilder<?> gfb = new CustomDistanceScoreBuilder("num1", "2013-05-28", "+1d");

        ActionFuture<SearchResponse> response = client().search(
            searchRequest().searchType(SearchType.QUERY_THEN_FETCH)
                .source(searchSource().explain(false).query(functionScoreQuery(termQuery("test", "value"), gfb)))
        );

        SearchResponse sr = response.actionGet();
        ElasticsearchAssertions.assertNoFailures(sr);
        SearchHits sh = sr.getHits();

        assertThat(sh.getHits().length, equalTo(2));
        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat(sh.getAt(1).getId(), equalTo("2"));

    }

    public static class CustomDistanceScorePlugin extends Plugin implements SearchPlugin {
        @Override
        public List<ScoreFunctionSpec<?>> getScoreFunctions() {
            return singletonList(
                new ScoreFunctionSpec<>(CustomDistanceScoreBuilder.NAME, CustomDistanceScoreBuilder::new, CustomDistanceScoreBuilder.PARSER)
            );
        }
    }

    public static class CustomDistanceScoreBuilder extends DecayFunctionBuilder<CustomDistanceScoreBuilder> {
        public static final String NAME = "linear_mult";
        public static final ScoreFunctionParser<CustomDistanceScoreBuilder> PARSER = new DecayFunctionParser<>(
            CustomDistanceScoreBuilder::new
        );

        public CustomDistanceScoreBuilder(String fieldName, Object origin, Object scale) {
            super(fieldName, origin, scale, null);
        }

        CustomDistanceScoreBuilder(String fieldName, BytesReference functionBytes) {
            super(fieldName, functionBytes);
        }

        /**
         * Read from a stream.
         */
        CustomDistanceScoreBuilder(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public DecayFunction getDecayFunction() {
            return decayFunction;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.ZERO;
        }

        private static final DecayFunction decayFunction = new LinearMultScoreFunction();

        private static class LinearMultScoreFunction implements DecayFunction {
            LinearMultScoreFunction() {}

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
}
