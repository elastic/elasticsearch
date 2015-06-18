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

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.script.ScriptScoreFunctionBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.SecureRandom;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import static org.elasticsearch.client.Requests.searchRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.scriptFunction;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;

public class BasicScriptBenchmark {

    public static class RequestInfo {
        public RequestInfo(SearchRequest source, int i) {
            request = source;
            numTerms = i;
        }

        SearchRequest request;
        int numTerms;
    }

    public static class Results {
        public static final String TIME_PER_DOCIN_MILLIS = "timePerDocinMillis";
        public static final String NUM_TERMS = "numTerms";
        public static final String NUM_DOCS = "numDocs";
        public static final String TIME_PER_QUERY_IN_SEC = "timePerQueryInSec";
        public static final String TOTAL_TIME_IN_SEC = "totalTimeInSec";
        Double[] resultSeconds;
        Double[] resultMSPerQuery;
        Long[] numDocs;
        Integer[] numTerms;
        Double[] timePerDoc;
        String label;
        String description;
        public String lineStyle;
        public String color;

        void init(int numVariations, String label, String description, String color, String lineStyle) {
            resultSeconds = new Double[numVariations];
            resultMSPerQuery = new Double[numVariations];
            numDocs = new Long[numVariations];
            numTerms = new Integer[numVariations];
            timePerDoc = new Double[numVariations];
            this.label = label;
            this.description = description;
            this.color = color;
            this.lineStyle = lineStyle;
        }

        void set(SearchResponse searchResponse, StopWatch stopWatch, String message, int maxIter, int which, int numTerms) {
            resultSeconds[which] = (double) ((double) stopWatch.lastTaskTime().getMillis() / (double) 1000);
            resultMSPerQuery[which] = (double) ((double) stopWatch.lastTaskTime().secondsFrac() / (double) maxIter);
            numDocs[which] = searchResponse.getHits().totalHits();
            this.numTerms[which] = numTerms;
            timePerDoc[which] = resultMSPerQuery[which] / numDocs[which];
        }

        public void printResults(BufferedWriter writer) throws IOException {
            String comma = (writer == null) ? "" : ";";
            String results = description + "\n" + Results.TOTAL_TIME_IN_SEC + " = " + getResultArray(resultSeconds) + comma + "\n"
                    + Results.TIME_PER_QUERY_IN_SEC + " = " + getResultArray(resultMSPerQuery) + comma + "\n" + Results.NUM_DOCS + " = "
                    + getResultArray(numDocs) + comma + "\n" + Results.NUM_TERMS + " = " + getResultArray(numTerms) + comma + "\n"
                    + Results.TIME_PER_DOCIN_MILLIS + " = " + getResultArray(timePerDoc) + comma + "\n";
            if (writer != null) {
                writer.write(results);
            } else {
                System.out.println(results);
            }

        }

        private String getResultArray(Object[] resultArray) {
            String result = "[";
            for (int i = 0; i < resultArray.length; i++) {
                result += resultArray[i].toString();
                if (i != resultArray.length - 1) {
                    result += ",";
                }
            }
            result += "]";
            return result;
        }
    }

    public BasicScriptBenchmark() {
    }

    static List<String> termsList = new ArrayList<>();

    static void init(int numTerms) {
        SecureRandom random = new SecureRandom();
        random.setSeed(1);
        termsList.clear();
        for (int i = 0; i < numTerms; i++) {
            String term = new BigInteger(512, random).toString(32);
            termsList.add(term);
        }

    }

    static String[] getTerms(int numTerms) {
        String[] terms = new String[numTerms];
        for (int i = 0; i < numTerms; i++) {
            terms[i] = termsList.get(i);
        }
        return terms;
    }

    public static void writeHelperFunction() throws IOException {
        try (BufferedWriter out = Files.newBufferedWriter(PathUtils.get("addToPlot.m"), StandardCharsets.UTF_8)) {
            out.write("function handle = addToPlot(numTerms, perDoc, color, linestyle, linewidth)\n" + "handle = line(numTerms, perDoc);\n"
                + "set(handle, 'color', color);\n" + "set(handle, 'linestyle',linestyle);\n" + "set(handle, 'LineWidth',linewidth);\n"
                + "end\n");
        }
    }

    public static void printOctaveScript(List<Results> allResults, String[] args) throws IOException {
        if (args.length == 0) {
            return;
        }
        try (BufferedWriter out = Files.newBufferedWriter(PathUtils.get(args[0]), StandardCharsets.UTF_8)) {
            out.write("#! /usr/local/bin/octave -qf");
            out.write("\n\n\n\n");
            out.write("######################################\n");
            out.write("# Octave script for plotting results\n");
            String filename = "scriptScoreBenchmark" + new DateTime(DateTimeZone.UTC).toString();
            out.write("#Call '" + args[0] + "' from the command line. The plot is then in " + filename + "\n\n");

            out.write("handleArray = [];\n tagArray = [];\n plot([]);\n hold on;\n");
            for (Results result : allResults) {
                out.write("\n");
                out.write("# " + result.description);
                result.printResults(out);
                out.write("handleArray = [handleArray, addToPlot(" + Results.NUM_TERMS + ", " + Results.TIME_PER_DOCIN_MILLIS + ", '"
                        + result.color + "','" + result.lineStyle + "',5)];\n");
                out.write("tagArray = [tagArray; '" + result.label + "'];\n");
                out.write("\n");
            }

            out.write("xlabel(\'number of query terms');");
            out.write("ylabel(\'query time per document');");

            out.write("legend(handleArray,tagArray);\n");

            out.write("saveas(gcf,'" + filename + ".png','png')\n");
            out.write("hold off;\n\n");
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        }
        writeHelperFunction();
    }

    static void printResult(SearchResponse searchResponse, StopWatch stopWatch, String queryInfo) {
        System.out.println("--> Searching with " + queryInfo + " took " + stopWatch.lastTaskTime() + ", per query "
                + (stopWatch.lastTaskTime().secondsFrac() / 100) + " for " + searchResponse.getHits().totalHits() + " docs");
    }

    static void indexData(long numDocs, Client client, boolean randomizeTerms) throws IOException {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Throwable t) {
            // index might exist already, in this case we do nothing TODO: make
            // saver in general
        }

        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                .startObject("text").field("type", "string").field("index_options", "offsets").field("analyzer", "payload_float")
                .endObject().endObject().endObject().endObject();
        client.admin()
                .indices()
                .prepareCreate("test")
                .addMapping("type1", mapping)
                .setSettings(
                        Settings.settingsBuilder().put("index.analysis.analyzer.payload_float.tokenizer", "whitespace")
                                .putArray("index.analysis.analyzer.payload_float.filter", "delimited_float")
                                .put("index.analysis.filter.delimited_float.delimiter", "|")
                                .put("index.analysis.filter.delimited_float.encoding", "float")
                                .put("index.analysis.filter.delimited_float.type", "delimited_payload_filter")
                                .put("index.number_of_replicas", 0).put("index.number_of_shards", 1)).execute().actionGet();
        client.admin().cluster().prepareHealth("test").setWaitForGreenStatus().setTimeout("10s").execute().actionGet();
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        Random random = new Random(1);
        for (int i = 0; i < numDocs; i++) {

            bulkRequest.add(client.prepareIndex().setType("type1").setIndex("test")
                    .setSource(jsonBuilder().startObject().field("text", randomText(random, randomizeTerms)).endObject()));
            if (i % 1000 == 0) {
                bulkRequest.execute().actionGet();
                bulkRequest = client.prepareBulk();
            }
        }
        bulkRequest.execute().actionGet();
        client.admin().indices().prepareRefresh("test").execute().actionGet();
        client.admin().indices().prepareFlush("test").execute().actionGet();
        System.out.println("Done indexing " + numDocs + " documents");

    }

    private static String randomText(Random random, boolean randomizeTerms) {
        String text = "";
        for (int i = 0; i < termsList.size(); i++) {
            if (random.nextInt(5) == 3 || !randomizeTerms) {
                text = text + " " + termsList.get(i) + "|1";
            }
        }
        return text;
    }

    static void printTimings(SearchResponse searchResponse, StopWatch stopWatch, String message, int maxIter) {
        System.out.println(message);
        System.out.println(stopWatch.lastTaskTime() + ", " + (stopWatch.lastTaskTime().secondsFrac() / maxIter) + ", "
                + searchResponse.getHits().totalHits() + ", "
                + (stopWatch.lastTaskTime().secondsFrac() / (maxIter + searchResponse.getHits().totalHits())));
    }

    static List<Entry<String, RequestInfo>> initTermQueries(int minTerms, int maxTerms) {
        List<Entry<String, RequestInfo>> termSearchRequests = new ArrayList<>();
        for (int nTerms = minTerms; nTerms < maxTerms; nTerms++) {
            Map<String, Object> params = new HashMap<>();
            String[] terms = getTerms(nTerms + 1);
            params.put("text", terms);
            SearchRequest request = searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                    searchSource().explain(false).size(0).query(QueryBuilders.termsQuery("text", terms)));
            String infoString = "Results for term query with " + (nTerms + 1) + " terms:";
            termSearchRequests.add(new AbstractMap.SimpleEntry<>(infoString, new RequestInfo(request, nTerms + 1)));
        }
        return termSearchRequests;
    }

    static List<Entry<String, RequestInfo>> initNativeSearchRequests(int minTerms, int maxTerms, String script, boolean langNative) {
        List<Entry<String, RequestInfo>> nativeSearchRequests = new ArrayList<>();
        for (int nTerms = minTerms; nTerms < maxTerms; nTerms++) {
            Map<String, Object> params = new HashMap<>();
            String[] terms = getTerms(nTerms + 1);
            params.put("text", terms);
            String infoString = "Results for native script with " + (nTerms + 1) + " terms:";
            ScriptScoreFunctionBuilder scriptFunction = (langNative == true) ? scriptFunction(new Script(script, ScriptType.INLINE,
                    "native", params)) : scriptFunction(new Script(script, ScriptType.INLINE, null, params));
            SearchRequest request = searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                    searchSource()
                            .explain(false)
                            .size(0)
                            .query(functionScoreQuery(QueryBuilders.termsQuery("text", terms), scriptFunction).boostMode(
                                    CombineFunction.REPLACE)));
            nativeSearchRequests.add(new AbstractMap.SimpleEntry<>(infoString, new RequestInfo(request, nTerms + 1)));
        }
        return nativeSearchRequests;
    }

    static List<Entry<String, RequestInfo>> initScriptMatchAllSearchRequests(String script, boolean langNative) {
        List<Entry<String, RequestInfo>> nativeSearchRequests = new ArrayList<>();
        String infoString = "Results for constant score script:";
        ScriptScoreFunctionBuilder scriptFunction = (langNative == true) ? scriptFunction(new Script(script, ScriptType.INLINE, "native",
                null)) : scriptFunction(new Script(script));
        SearchRequest request = searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                searchSource().explain(false).size(0)
                        .query(functionScoreQuery(QueryBuilders.matchAllQuery(), scriptFunction).boostMode(CombineFunction.REPLACE)));
        nativeSearchRequests.add(new AbstractMap.SimpleEntry<>(infoString, new RequestInfo(request, 0)));

        return nativeSearchRequests;
    }

    static void runBenchmark(Client client, int maxIter, Results results, List<Entry<String, RequestInfo>> nativeSearchRequests,
            int minTerms, int warmerIter) throws IOException {
        int counter = 0;
        for (Entry<String, RequestInfo> entry : nativeSearchRequests) {
            SearchResponse searchResponse = null;
            // warm up
            for (int i = 0; i < warmerIter; i++) {
                searchResponse = client.search(entry.getValue().request).actionGet();
            }
            System.gc();
            // run benchmark
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            for (int i = 0; i < maxIter; i++) {
                searchResponse = client.search(entry.getValue().request).actionGet();
            }
            stopWatch.stop();
            results.set(searchResponse, stopWatch, entry.getKey(), maxIter, counter, entry.getValue().numTerms);
            counter++;
        }
        results.printResults(null);
    }
}