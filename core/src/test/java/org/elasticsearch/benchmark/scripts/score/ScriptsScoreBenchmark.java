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
package org.elasticsearch.benchmark.scripts.score;

import org.elasticsearch.Version;
import org.elasticsearch.benchmark.scripts.score.plugin.NativeScriptExamplesPlugin;
import org.elasticsearch.benchmark.scripts.score.script.NativeNaiveTFIDFScoreScript;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 *
 */
public class ScriptsScoreBenchmark extends BasicScriptBenchmark {

    public static void main(String[] args) throws Exception {

        int minTerms = 1;
        int maxTerms = 50;
        int maxIter = 100;
        int warmerIter = 10;

        boolean runMVEL = false;
        init(maxTerms);
        List<Results> allResults = new ArrayList<>();
        String clusterName = ScriptsScoreBenchmark.class.getSimpleName();
        Settings settings = settingsBuilder().put("name", "node1")
            .put("cluster.name", clusterName).build();
        Collection<Class<? extends Plugin>> plugins = Collections.<Class<? extends Plugin>>singletonList(NativeScriptExamplesPlugin.class);
        Node node1 = new MockNode(settings, Version.CURRENT, plugins);
        node1.start();
        Client client = node1.client();
        client.admin().cluster().prepareHealth("test").setWaitForGreenStatus().setTimeout("10s").execute().actionGet();

        indexData(10000, client, false);
        client.admin().cluster().prepareHealth("test").setWaitForGreenStatus().setTimeout("10s").execute().actionGet();

        Results results = new Results();
        results.init(maxTerms - minTerms, "native tfidf script score dense posting list",
                "Results for native script score with dense posting list:", "black", "--");
        // init native script searches
        List<Entry<String, RequestInfo>> searchRequests = initNativeSearchRequests(minTerms, maxTerms,
                NativeNaiveTFIDFScoreScript.NATIVE_NAIVE_TFIDF_SCRIPT_SCORE, true);
        // run actual benchmark
        runBenchmark(client, maxIter, results, searchRequests, minTerms, warmerIter);
        allResults.add(results);

        results = new Results();

        results.init(maxTerms - minTerms, "term query dense posting list", "Results for term query with dense posting lists:", "green",
                "--");
        // init term queries
        searchRequests = initTermQueries(minTerms, maxTerms);
        // run actual benchmark
        runBenchmark(client, maxIter, results, searchRequests, minTerms, warmerIter);
        allResults.add(results);

        if (runMVEL) {

            results = new Results();
            results.init(maxTerms - minTerms, "mvel tfidf dense posting list", "Results for mvel score with dense posting list:", "red",
                    "--");
            // init native script searches
            searchRequests = initNativeSearchRequests(
                    minTerms,
                    maxTerms,
                    "score = 0.0; fi= _terminfo[\"text\"]; for(i=0; i<text.size(); i++){terminfo = fi[text.get(i)]; score = score + terminfo.tf()*fi.getDocCount()/terminfo.df();} return score;",
                    false);
            // run actual benchmark
            runBenchmark(client, maxIter, results, searchRequests, minTerms, warmerIter);
            allResults.add(results);
        }

        indexData(10000, client, true);
        results = new Results();
        results.init(maxTerms - minTerms, "native tfidf script score sparse posting list",
                "Results for native script scorewith sparse posting list:", "black", "-.");
        // init native script searches
        searchRequests = initNativeSearchRequests(minTerms, maxTerms, NativeNaiveTFIDFScoreScript.NATIVE_NAIVE_TFIDF_SCRIPT_SCORE, true);
        // run actual benchmark
        runBenchmark(client, maxIter, results, searchRequests, minTerms, warmerIter);
        allResults.add(results);

        results = new Results();

        results.init(maxTerms - minTerms, "term query sparse posting list", "Results for term query with sparse posting lists:", "green",
                "-.");
        // init term queries
        searchRequests = initTermQueries(minTerms, maxTerms);
        // run actual benchmark
        runBenchmark(client, maxIter, results, searchRequests, minTerms, warmerIter);
        allResults.add(results);

        if (runMVEL) {

            results = new Results();
            results.init(maxTerms - minTerms, "mvel tfidf sparse posting list", "Results for mvel score with sparse posting list:", "red",
                    "-.");
            // init native script searches
            searchRequests = initNativeSearchRequests(
                    minTerms,
                    maxTerms,
                    "score = 0.0; fi= _terminfo[\"text\"]; for(i=0; i<text.size(); i++){terminfo = fi[text.get(i)]; score = score + terminfo.tf()*fi.getDocCount()/terminfo.df();} return score;",
                    false);
            // run actual benchmark
            runBenchmark(client, maxIter, results, searchRequests, minTerms, warmerIter);
            allResults.add(results);
        }
        printOctaveScript(allResults, args);

        client.close();
        node1.close();
    }

}
