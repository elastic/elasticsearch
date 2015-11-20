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

package org.elasticsearch.messy.tests;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.script.groovy.GroovyPlugin;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantStringTerms;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTerms;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTermsAggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTermsBuilder;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.ScriptHeuristic;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 *
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE)
public class SignificantTermsSignificanceScoreTests extends ESIntegTestCase {
    static final String INDEX_NAME = "testidx";
    static final String DOC_TYPE = "doc";
    static final String TEXT_FIELD = "text";
    static final String CLASS_FIELD = "class";

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        return settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("path.conf", this.getDataPath("conf"))
                .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(GroovyPlugin.class);
    }

    public String randomExecutionHint() {
        return randomBoolean() ? null : randomFrom(SignificantTermsAggregatorFactory.ExecutionMode.values()).toString();
    }

    public void testScriptScore() throws ExecutionException, InterruptedException, IOException {
        indexRandomFrequencies01(randomBoolean() ? "string" : "long");
        ScriptHeuristic.ScriptHeuristicBuilder scriptHeuristicBuilder = getScriptSignificanceHeuristicBuilder();
        ensureYellow();
        SearchResponse response = client().prepareSearch(INDEX_NAME)
                .addAggregation(new TermsBuilder("class").field(CLASS_FIELD).subAggregation(new SignificantTermsBuilder("mySignificantTerms")
                        .field(TEXT_FIELD)
                        .executionHint(randomExecutionHint())
                        .significanceHeuristic(scriptHeuristicBuilder)
                        .minDocCount(1).shardSize(2).size(2)))
                .execute()
                .actionGet();
        assertSearchResponse(response);
        for (Terms.Bucket classBucket : ((Terms) response.getAggregations().get("class")).getBuckets()) {
            for (SignificantTerms.Bucket bucket : ((SignificantTerms) classBucket.getAggregations().get("mySignificantTerms")).getBuckets()) {
                assertThat(bucket.getSignificanceScore(), is((double) bucket.getSubsetDf() + bucket.getSubsetSize() + bucket.getSupersetDf() + bucket.getSupersetSize()));
            }
        }
    }

    public void testNoNumberFormatExceptionWithDefaultScriptingEngine() throws ExecutionException, InterruptedException, IOException {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", 1)));
        index("test", "doc", "1", "{\"field\":\"a\"}");
        index("test", "doc", "11", "{\"field\":\"a\"}");
        index("test", "doc", "2", "{\"field\":\"b\"}");
        index("test", "doc", "22", "{\"field\":\"b\"}");
        index("test", "doc", "3", "{\"field\":\"a b\"}");
        index("test", "doc", "33", "{\"field\":\"a b\"}");
        ScriptHeuristic.ScriptHeuristicBuilder scriptHeuristicBuilder = new ScriptHeuristic.ScriptHeuristicBuilder();
        scriptHeuristicBuilder.setScript(new Script("_subset_freq/(_superset_freq - _subset_freq + 1)"));
        ensureYellow();
        refresh();
        SearchResponse response = client()
                .prepareSearch("test")
                .addAggregation(
                        new TermsBuilder("letters").field("field").subAggregation(
                                new SignificantTermsBuilder("mySignificantTerms").field("field").executionHint(randomExecutionHint())
                                        .significanceHeuristic(scriptHeuristicBuilder).minDocCount(1).shardSize(2).size(2))).execute()
                .actionGet();
        assertSearchResponse(response);
        assertThat(((Terms) response.getAggregations().get("letters")).getBuckets().size(), equalTo(2));
        for (Terms.Bucket classBucket : ((Terms) response.getAggregations().get("letters")).getBuckets()) {
            assertThat(((SignificantStringTerms) classBucket.getAggregations().get("mySignificantTerms")).getBuckets().size(), equalTo(2));
            for (SignificantTerms.Bucket bucket : ((SignificantTerms) classBucket.getAggregations().get("mySignificantTerms")).getBuckets()) {
                assertThat(bucket.getSignificanceScore(),
                        closeTo((double) bucket.getSubsetDf() / (bucket.getSupersetDf() - bucket.getSubsetDf() + 1), 1.e-6));
            }
        }
    }

    private ScriptHeuristic.ScriptHeuristicBuilder getScriptSignificanceHeuristicBuilder() throws IOException {
        Map<String, Object> params = null;
        Script script = null;
        String lang = null;
        if (randomBoolean()) {
            params = new HashMap<>();
            params.put("param", randomIntBetween(1, 100));
        }
        int randomScriptKind = randomIntBetween(0, 2);
        if (randomBoolean()) {
            lang = "groovy";
        }
        switch (randomScriptKind) {
        case 0: {
            if (params == null) {
                script = new Script("return _subset_freq + _subset_size + _superset_freq + _superset_size");
            } else {
                script = new Script("return param*(_subset_freq + _subset_size + _superset_freq + _superset_size)/param",
                        ScriptType.INLINE, lang, params);
            }
            break;
        }
        case 1: {
            String scriptString;
            if (params == null) {
                scriptString = "return _subset_freq + _subset_size + _superset_freq + _superset_size";
            } else {
                scriptString = "return param*(_subset_freq + _subset_size + _superset_freq + _superset_size)/param";
            }
            client().prepareIndex().setIndex(ScriptService.SCRIPT_INDEX).setType(ScriptService.DEFAULT_LANG).setId("my_script")
                    .setSource(XContentFactory.jsonBuilder().startObject().field("script", scriptString).endObject()).get();
            refresh();
            script = new Script("my_script", ScriptType.INDEXED, lang, params);
            break;
        }
        case 2: {
            if (params == null) {
                script = new Script("significance_script_no_params", ScriptType.FILE, lang, null);
            } else {
                script = new Script("significance_script_with_params", ScriptType.FILE, lang, params);
            }
            break;
        }

        }
        ScriptHeuristic.ScriptHeuristicBuilder builder = new ScriptHeuristic.ScriptHeuristicBuilder().setScript(script);

        return builder;
    }

    private void indexRandomFrequencies01(String type) throws ExecutionException, InterruptedException {
        String mappings = "{\"" + DOC_TYPE + "\": {\"properties\":{\"" + TEXT_FIELD + "\": {\"type\":\"" + type + "\"}}}}";
        assertAcked(prepareCreate(INDEX_NAME).addMapping(DOC_TYPE, mappings));
        String[] gb = {"0", "1"};
        List<IndexRequestBuilder> indexRequestBuilderList = new ArrayList<>();
        for (int i = 0; i < randomInt(20); i++) {
            int randNum = randomInt(2);
            String[] text = new String[1];
            if (randNum == 2) {
                text = gb;
            } else {
                text[0] = gb[randNum];
            }
            indexRequestBuilderList.add(client().prepareIndex(INDEX_NAME, DOC_TYPE)
                    .setSource(TEXT_FIELD, text, CLASS_FIELD, randomBoolean() ? "one" : "zero"));
        }
        indexRandom(true, indexRequestBuilderList);
    }
}
