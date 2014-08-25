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

package org.elasticsearch.benchmark.search.function;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.benchmark.scripts.expression.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.sort.ScriptSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.joda.time.PeriodType;

import java.util.Random;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

public class RandomScoreBenchmark {

    static final String clusterName = RandomScoreBenchmark.class.getSimpleName();
    static final String indexName = "test";

    public static void main(String[] args) throws Exception {
        int numDocs = 1000000;
        int numQueries = 1000;
        Client client = setupIndex();
        indexDocs(client, numDocs);

        int seed = (int)System.currentTimeMillis();
        SearchRequestBuilder req = new SearchRequestBuilder(client).setIndices(indexName);
        ScoreFunctionBuilder score = ScoreFunctionBuilders.randomFunction(seed);
        req.setQuery(QueryBuilders.functionScoreQuery(QueryBuilders.matchAllQuery(), score));

        /*timeQueries(req, numQueries / 10); // warmup
        TimeValue time = timeQueries(req, numQueries);
        printResults("Current", time, numQueries);
*/
        score = ScoreFunctionBuilders.randomFunction(seed);
        req.setQuery(QueryBuilders.functionScoreQuery(QueryBuilders.matchAllQuery(), score));
        timeQueries(req, numQueries / 10); // warmup
        TimeValue time = timeQueries(req, numQueries);
        printResults("Consistent", time, numQueries);
    }

    static Client setupIndex() throws Exception {
        // create cluster
        Settings settings = settingsBuilder().put("plugin.types", NativeScriptPlugin.class.getName())
                                             .put("name", "node1")
                                             .build();
        Node node1 = nodeBuilder().clusterName(clusterName).settings(settings).node();
        Client client = node1.client();
        client.admin().cluster().prepareHealth(indexName).setWaitForGreenStatus().setTimeout("10s").execute().actionGet();

        // delete the index, if it exists
        try {
            client.admin().indices().prepareDelete(indexName).execute().actionGet();
        } catch (ElasticsearchException e) {
            // ok if the index didn't exist
        }

        // create mappings
        IndicesAdminClient admin = client.admin().indices();
        admin.prepareCreate(indexName).addMapping("doc", "x", "type=long", "y", "type=double");

        client.admin().cluster().prepareHealth(indexName).setWaitForGreenStatus().setTimeout("10s").execute().actionGet();
        return client;
    }

    static void indexDocs(Client client, int numDocs) {
        System.out.print("Indexing " + numDocs + " random docs...");
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        Random r = new Random(1);
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(client.prepareIndex("test", "doc", Integer.toString(i))
                            .setSource("x", r.nextInt(), "y", r.nextDouble(), "z", r.nextDouble()));

            if (i % 1000 == 0) {
                bulkRequest.execute().actionGet();
                bulkRequest = client.prepareBulk();
            }
        }
        bulkRequest.execute().actionGet();
        client.admin().indices().prepareRefresh("test").execute().actionGet();
        client.admin().indices().prepareFlush("test").setFull(true).execute().actionGet();
        System.out.println("done");
    }

    static TimeValue timeQueries(SearchRequestBuilder req, int numQueries) {

        StopWatch timer = new StopWatch();
        timer.start();
        for (int i = 0; i < numQueries; ++i) {
            req.get();
        }
        timer.stop();
        return timer.totalTime();
    }

    static void printResults(String desc, TimeValue time, int numQueries) {
        long avgReq = time.millis() / numQueries;
        System.out.println(desc + ": " + time.format(PeriodType.seconds()) + " (" + avgReq + " msec per req)");
    }

}
