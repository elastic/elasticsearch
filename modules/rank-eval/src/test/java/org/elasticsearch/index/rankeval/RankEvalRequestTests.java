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

import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.rankeval.PrecisionAtN.Rating;
import org.elasticsearch.index.rankeval.QuerySpec;
import org.elasticsearch.index.rankeval.RankEvalAction;
import org.elasticsearch.index.rankeval.RankEvalPlugin;
import org.elasticsearch.index.rankeval.RankEvalRequest;
import org.elasticsearch.index.rankeval.RankEvalRequestBuilder;
import org.elasticsearch.index.rankeval.RankEvalResponse;
import org.elasticsearch.index.rankeval.RankEvalSpec;
import org.elasticsearch.index.rankeval.RatedDocument;
import org.elasticsearch.index.rankeval.RatedDocumentKey;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, transportClientRatio = 0.0)
// NORELEASE need to fix transport client use case
public class RankEvalRequestTests  extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return pluginList(RankEvalPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(RankEvalPlugin.class);
    }

    @Before
    public void setup() {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "testtype").setId("1")
                .setSource("text", "berlin").get();
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

    public void testPrecisionAtRequest() {
        ArrayList<String> indices = new ArrayList<>();
        indices.add("test");
        ArrayList<String> types = new ArrayList<>();
        types.add("testtype");

        String specId = randomAsciiOfLength(10);
        List<QuerySpec> specifications = new ArrayList<>();
        SearchSourceBuilder testQuery = new SearchSourceBuilder();
        testQuery.query(new MatchAllQueryBuilder());
        specifications.add(new QuerySpec("amsterdam_query",  testQuery, indices, types, createRelevant("2", "3", "4", "5")));
        specifications.add(new QuerySpec("berlin_query",  testQuery, indices, types, createRelevant("1")));

        RankEvalSpec task = new RankEvalSpec(specId, specifications, new PrecisionAtN(10));

        RankEvalRequestBuilder builder = new RankEvalRequestBuilder(
                client(),
                RankEvalAction.INSTANCE,
                new RankEvalRequest());
        builder.setRankEvalSpec(task);

        RankEvalResponse response = client().execute(RankEvalAction.INSTANCE, builder.request()).actionGet();
        assertEquals(specId, response.getSpecId());
        assertEquals(1.0, response.getQualityLevel(), Double.MIN_VALUE);
        Set<Entry<String, Collection<RatedDocumentKey>>> entrySet = response.getUnknownDocs().entrySet();
        assertEquals(2, entrySet.size());
        for (Entry<String, Collection<RatedDocumentKey>> entry : entrySet) {
            if (entry.getKey() == "amsterdam_query") {
                assertEquals(2, entry.getValue().size());
            }
            if (entry.getKey() == "berlin_query") {
                assertEquals(5, entry.getValue().size());
            }
        }
    }

    private static List<RatedDocument> createRelevant(String... docs) {
        List<RatedDocument> relevant = new ArrayList<>();
        for (String doc : docs) {
            relevant.add(new RatedDocument(new RatedDocumentKey("test", "testtype", doc), Rating.RELEVANT.ordinal()));
        }
        return relevant;
    }
 }
