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

package org.elasticsearch.index.rankeval;

import org.elasticsearch.index.rankeval.RankEvalSpec.ScriptWithId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class SmokeMultipleTemplatesIT  extends ESIntegTestCase {

    private static final String MATCH_TEMPLATE = "match_template";

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Arrays.asList(RankEvalPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(RankEvalPlugin.class);
    }

    @Before
    public void setup() {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "testtype").setId("1")
                .setSource("text", "berlin", "title", "Berlin, Germany").get();
        client().prepareIndex("test", "testtype").setId("2")
                .setSource("text", "amsterdam").get();
        client().prepareIndex("test", "testtype").setId("3")
                .setSource("text", "amsterdam").get();
        client().prepareIndex("test", "testtype").setId("4")
                .setSource("text", "amsterdam").get();
        client().prepareIndex("test", "testtype").setId("5")
                .setSource("text", "amsterdam").get();
        client().prepareIndex("test", "testtype").setId("6")
                .setSource("text", "amsterdam").get();
        refresh();
    }

    public void testPrecisionAtRequest() throws IOException {
        List<String> indices = Arrays.asList(new String[] { "test" });

        List<RatedRequest> specifications = new ArrayList<>();
        Map<String, Object> ams_params = new HashMap<>();
        ams_params.put("querystring", "amsterdam");
        RatedRequest amsterdamRequest = new RatedRequest(
                "amsterdam_query", createRelevant("2", "3", "4", "5"), ams_params, MATCH_TEMPLATE);

        specifications.add(amsterdamRequest);

        Map<String, Object> berlin_params = new HashMap<>();
        berlin_params.put("querystring", "berlin");
        RatedRequest berlinRequest = new RatedRequest(
                "berlin_query", createRelevant("1"), berlin_params, MATCH_TEMPLATE);
        specifications.add(berlinRequest);

        PrecisionAtK metric = new PrecisionAtK();

        ScriptWithId template =
                new ScriptWithId(
                        MATCH_TEMPLATE,
                        new Script(
                                ScriptType.INLINE,
                                "mustache", "{\"query\": {\"match\": {\"text\": \"{{querystring}}\"}}}",
                                new HashMap<>()));
        Set<ScriptWithId> templates = new HashSet<>();
        templates.add(template);
        RankEvalSpec task = new RankEvalSpec(specifications, metric, templates);
        task.addIndices(indices);
        RankEvalRequestBuilder builder = new RankEvalRequestBuilder(client(), RankEvalAction.INSTANCE, new RankEvalRequest());
        builder.setRankEvalSpec(task);

        RankEvalResponse response = client().execute(RankEvalAction.INSTANCE, builder.request()).actionGet();
        assertEquals(0.9, response.getEvaluationResult(), Double.MIN_VALUE);
    }

    private static List<RatedDocument> createRelevant(String... docs) {
        List<RatedDocument> relevant = new ArrayList<>();
        for (String doc : docs) {
            relevant.add(new RatedDocument("test", doc, Rating.RELEVANT.ordinal()));
        }
        return relevant;
    }

    public enum Rating {
        IRRELEVANT, RELEVANT;
    }

 }
