/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.benchmark.scripts.score;

import org.elasticsearch.benchmark.scripts.score.plugin.NativeScriptExamplesPlugin;
import org.elasticsearch.benchmark.scripts.score.script.NativeNaiveTFIDFScoreScript;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 *
 */
public class ScriptsScoreTFIDFProfile extends BasicScriptBenchmark {


    public static void main(String[] args) throws Exception {
        
        int minTerms = 79;
        int maxTerms = 80;
        int maxIter = 10000;
        int warmerIter = 10;

        init(maxTerms);
        List<Results> allResults = new ArrayList<BasicScriptBenchmark.Results>();
        Settings settings = settingsBuilder().put("plugin.types", NativeScriptExamplesPlugin.class.getName()).build();

        String clusterName = ScriptsScoreBenchmark.class.getSimpleName();
        Node node1 = nodeBuilder().clusterName(clusterName).settings(settingsBuilder().put(settings).put("name", "node1")).node();
        Client client = node1.client();
        client.admin().cluster().prepareHealth("test").setWaitForGreenStatus().setTimeout("10s").execute().actionGet();

        indexData(1000, client, true);
        client.admin().cluster().prepareHealth("test").setWaitForGreenStatus().setTimeout("10s").execute().actionGet();

        Results results = new Results();

        // init script searches
        results.init(maxTerms - minTerms, "native tfidf script score", "Results for native script score:","black", ":");
        List<Entry<String, RequestInfo>>  searchRequests =  initNativeSearchRequests(minTerms, maxTerms, NativeNaiveTFIDFScoreScript.NATIVE_NAIVE_TFIDF_SCRIPT_SCORE, true);
        // run actual benchmark
        runBenchmark(client, maxIter, results, searchRequests, minTerms, warmerIter);
        allResults.add(results);
        
        printOctaveScript(allResults, args);

        client.close();
        node1.close();
    }

}
