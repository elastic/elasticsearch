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

package org.elasticsearch.benchmark.search.lookup;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermsLookupFilterBuilder;
import org.elasticsearch.node.Node;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.*;
import static org.elasticsearch.index.query.QueryBuilders.filteredQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.elasticsearch.search.facet.FacetBuilders.termsFacet;

/**
 * Perform the parent/child search tests using terms query lookup.
 * This should work across multiple shards and not need a special mapping
 */
public class QueryTermsLookupBenchmark {

    // index settings
    public static final int NUM_SHARDS = 3;
    public static final int NUM_REPLICAS = 0;
    public static final String PARENT_INDEX = "joinparent";
    public static final String PARENT_TYPE = "p";
    public static final String CHILD_INDEX = "joinchild";
    public static final String CHILD_TYPE = "c";
    // test settings
    public static final int NUM_PARENTS = 1000000;
    public static final int NUM_CHILDREN_PER_PARENT = 5;
    public static final int BATCH_SIZE = 100;
    public static final int NUM_QUERIES = 50;
    private final Node node;
    private final Client client;
    private final Random random;

    QueryTermsLookupBenchmark() {
        Settings settings = settingsBuilder()
                .put("index.engine.robin.refreshInterval", "-1")
                .put("gateway.type", "local")
                .put(SETTING_NUMBER_OF_SHARDS, NUM_SHARDS)
                .put(SETTING_NUMBER_OF_REPLICAS, NUM_REPLICAS)
                .build();

        this.node = nodeBuilder().settings(settingsBuilder().put(settings).put("name", "node1")).node();
        this.client = node.client();
        this.random = new Random(System.currentTimeMillis());
    }

    public static void main(String[] args) throws Exception {

        QueryTermsLookupBenchmark bench = new QueryTermsLookupBenchmark();
        bench.waitForGreen();
        bench.setupIndex();
        bench.memStatus();

        // don't cache lookup
        bench.benchHasChildSingleTerm(false);
        bench.benchHasParentSingleTerm(false);
        bench.benchHasParentMatchAll(false);
        bench.benchHasChildMatchAll(false);
        bench.benchHasParentRandomTerms(false);

        System.gc();
        bench.memStatus();
        bench.shutdown();
    }

    public void waitForGreen() {
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().setTimeout("10s").execute().actionGet();
    }

    public void shutdown() {
        client.close();
        node.close();
    }

    public void log(String msg) {
        System.out.println("--> " + msg);
    }

    public void memStatus() {
        NodeStats nodeStats = client.admin().cluster().prepareNodesStats().setJvm(true).setIndices(true).execute().actionGet().getNodes()[0];
        log("==== MEMORY ====");
        log("Committed heap size: " + nodeStats.getJvm().getMem().getHeapCommitted());
        log("Used heap size: " + nodeStats.getJvm().getMem().getHeapUsed());
        log("FieldData cache size: " + nodeStats.getIndices().getFieldData().getMemorySize());
        log("");
    }

    public XContentBuilder parentSource(int id, String nameValue) throws IOException {
        return jsonBuilder().startObject().field("id", Integer.toString(id)).field("num", id).field("name", nameValue).endObject();
    }

    public XContentBuilder childSource(String id, int parent, String tag) throws IOException {
        return jsonBuilder().startObject().field("id", id).field("pid", Integer.toString(parent)).field("num", parent).field("tag", tag)
                .endObject();
    }

    public void setupIndex() {
        log("==== INDEX SETUP ====");
        try {
            client.admin().indices().create(createIndexRequest(PARENT_INDEX)).actionGet();
            client.admin().indices().create(createIndexRequest(CHILD_INDEX)).actionGet();
            Thread.sleep(5000);

            StopWatch stopWatch = new StopWatch().start();

            log("Indexing [" + NUM_PARENTS + "] parent documents into [" + PARENT_INDEX + "]");
            log("Indexing [" + (NUM_PARENTS * NUM_CHILDREN_PER_PARENT) + "] child documents into [" + CHILD_INDEX + "]");
            int ITERS = NUM_PARENTS / BATCH_SIZE;
            int i = 1;
            int counter = 0;
            for (; i <= ITERS; i++) {
                BulkRequestBuilder request = client.prepareBulk();
                for (int j = 0; j < BATCH_SIZE; j++) {
                    String parentId = Integer.toString(counter);
                    counter++;
                    request.add(Requests.indexRequest(PARENT_INDEX)
                            .type(PARENT_TYPE)
                            .id(parentId)
                            .source(parentSource(counter, "test" + counter)));

                    for (int k = 0; k < NUM_CHILDREN_PER_PARENT; k++) {
                        String childId = parentId + "_" + k;
                        request.add(Requests.indexRequest(CHILD_INDEX)
                                .type(CHILD_TYPE)
                                .id(childId)
                                .source(childSource(childId, counter, "tag" + k)));
                    }
                }

                BulkResponse response = request.execute().actionGet();
                if (response.hasFailures()) {
                    log("Index Failures...");
                }

                if (((i * BATCH_SIZE) % 10000) == 0) {
                    log("Indexed [" + (i * BATCH_SIZE) * (1 + NUM_CHILDREN_PER_PARENT) + "] took [" + stopWatch.stop().lastTaskTime() + "]");
                    stopWatch.start();
                }
            }

            log("Indexing took [" + stopWatch.totalTime() + "]");
            log("TPS [" + (((double) (NUM_PARENTS * (1 + NUM_CHILDREN_PER_PARENT))) / stopWatch.totalTime().secondsFrac()) + "]");
        } catch (Exception e) {
            log("Indices exist, wait for green");
            waitForGreen();
        }

        client.admin().indices().prepareRefresh().execute().actionGet();
        log("Number of docs in index: " + client.prepareCount(PARENT_INDEX, CHILD_INDEX).setQuery(matchAllQuery()).execute().actionGet().getCount());
        log("");
    }

    public void warmFieldData(String parentField, String childField) {
        ListenableActionFuture<SearchResponse> parentSearch = null;
        ListenableActionFuture<SearchResponse> childSearch = null;

        if (parentField != null) {
            parentSearch = client
                    .prepareSearch(PARENT_INDEX)
                    .setQuery(matchAllQuery()).addFacet(termsFacet("parentfield").field(parentField)).execute();
        }

        if (childField != null) {
            childSearch = client
                    .prepareSearch(CHILD_INDEX)
                    .setQuery(matchAllQuery()).addFacet(termsFacet("childfield").field(childField)).execute();
        }

        if (parentSearch != null) parentSearch.actionGet();
        if (childSearch != null) childSearch.actionGet();
    }

    public long runQuery(String name, int testNum, String index, long expectedHits, QueryBuilder query) {
        SearchResponse searchResponse = client
                .prepareSearch(index)
                .setQuery(query)
                .execute().actionGet();

        if (searchResponse.getFailedShards() > 0) {
            log("Search Failures " + Arrays.toString(searchResponse.getShardFailures()));
        }

        long hits = searchResponse.getHits().totalHits();
        if (hits != expectedHits) {
            log("[" + name + "][#" + testNum + "] Hits Mismatch:  expected [" + expectedHits + "], got [" + hits + "]");
        }

        return searchResponse.getTookInMillis();
    }

    /**
     * Search for parent documents that have children containing a specified tag.
     * Expect all parents returned since one child from each parent will match the lookup.
     * <p/>
     * Parent string field = "id"
     * Parent numeric field = "num"
     * Child string field = "pid"
     * Child numeric field = "num"
     */
    public void benchHasChildSingleTerm(boolean cacheLookup) {
        FilterBuilder lookupFilter;
        QueryBuilder mainQuery = matchAllQuery();

        TermsLookupFilterBuilder stringFilter = termsLookupFilter("id")
                .index(CHILD_INDEX)
                .type(CHILD_TYPE)
                .path("pid")
                .lookupCache(cacheLookup);

        TermsLookupFilterBuilder bloomFilter = termsLookupFilter("id")
                .index(CHILD_INDEX)
                .type(CHILD_TYPE)
                .path("pid")
                .bloomExpectedInsertions(NUM_PARENTS)
                .lookupCache(cacheLookup);

        TermsLookupFilterBuilder numericFilter = termsLookupFilter("num")
                .index(CHILD_INDEX)
                .type(CHILD_TYPE)
                .path("num")
                .lookupCache(cacheLookup);

        long tookString = 0;
        long tookNumeric = 0;
        long tookBloom = 0;
        long expected = NUM_PARENTS;
        warmFieldData("id", "pid");     // for string fields
        warmFieldData("num", "num");    // for numeric fields

        log("==== HAS CHILD SINGLE TERM (cache: " + cacheLookup + ") ====");
        for (int i = 0; i < NUM_QUERIES; i++) {
            lookupFilter = termFilter("tag", "tag" + random.nextInt(NUM_CHILDREN_PER_PARENT));

            stringFilter.lookupFilter(lookupFilter);
            numericFilter.lookupFilter(lookupFilter);
            bloomFilter.lookupFilter(lookupFilter);

            tookString += runQuery("string", i, PARENT_INDEX, expected, filteredQuery(mainQuery, stringFilter));
            tookNumeric += runQuery("numeric", i, PARENT_INDEX, expected, filteredQuery(mainQuery, numericFilter));
            tookBloom += runQuery("bloom", i, PARENT_INDEX, expected, filteredQuery(mainQuery, bloomFilter));
        }

        log("string: " + (tookString / NUM_QUERIES) + "ms avg");
        log("numeric: " + (tookNumeric / NUM_QUERIES) + "ms avg");
        log("bloom: " + (tookBloom / NUM_QUERIES) + "ms avg");
        log("");
    }

    /**
     * Search for parent documents that have any child.
     * Expect all parent documents returned.
     * <p/>
     * Parent string field = "id"
     * Parent numeric field = "num"
     * Child string field = "pid"
     * Child numeric field = "num"
     */
    public void benchHasChildMatchAll(boolean cacheLookup) {
        FilterBuilder lookupFilter = matchAllFilter();
        QueryBuilder mainQuery = matchAllQuery();

        TermsLookupFilterBuilder stringFilter = termsLookupFilter("id")
                .index(CHILD_INDEX)
                .type(CHILD_TYPE)
                .path("pid")
                .lookupCache(cacheLookup)
                .lookupFilter(lookupFilter);

        TermsLookupFilterBuilder bloomFilter = termsLookupFilter("id")
                .index(CHILD_INDEX)
                .type(CHILD_TYPE)
                .path("pid")
                .bloomExpectedInsertions(NUM_PARENTS)
                .lookupCache(cacheLookup)
                .lookupFilter(lookupFilter);

        TermsLookupFilterBuilder numericFilter = termsLookupFilter("num")
                .index(CHILD_INDEX)
                .type(CHILD_TYPE)
                .path("num")
                .lookupCache(cacheLookup)
                .lookupFilter(lookupFilter);

        long tookString = 0;
        long tookNumeric = 0;
        long tookBloom = 0;
        long expected = NUM_PARENTS;
        warmFieldData("id", "pid");     // for string fields
        warmFieldData("num", "num");    // for numeric fields

        log("==== HAS CHILD MATCH-ALL (cache: " + cacheLookup + ") ====");
        for (int i = 0; i < NUM_QUERIES; i++) {
            tookString += runQuery("string", i, PARENT_INDEX, expected, filteredQuery(mainQuery, stringFilter));
            tookNumeric += runQuery("numeric", i, PARENT_INDEX, expected, filteredQuery(mainQuery, numericFilter));
            tookBloom += runQuery("bloom", i, PARENT_INDEX, expected, filteredQuery(mainQuery, bloomFilter));
        }

        log("string: " + (tookString / NUM_QUERIES) + "ms avg");
        log("numeric: " + (tookNumeric / NUM_QUERIES) + "ms avg");
        log("bloom: " + (tookBloom / NUM_QUERIES) + "ms avg");
        log("");
    }

    /**
     * Search for children that have a parent with the specified name.
     * Expect NUM_CHILDREN_PER_PARENT since only one parent matching lookup.
     * <p/>
     * Parent string field = "id"
     * Parent numeric field = "num"
     * Child string field = "pid"
     * Child numeric field = "num"
     */
    public void benchHasParentSingleTerm(boolean cacheLookup) {
        FilterBuilder lookupFilter;
        QueryBuilder mainQuery = matchAllQuery();

        TermsLookupFilterBuilder stringFilter = termsLookupFilter("pid")
                .index(PARENT_INDEX)
                .type(PARENT_TYPE)
                .path("id")
                .lookupCache(cacheLookup);

        TermsLookupFilterBuilder bloomFilter = termsLookupFilter("pid")
                .index(PARENT_INDEX)
                .type(PARENT_TYPE)
                .path("id")
                .bloomExpectedInsertions(NUM_PARENTS)
                .lookupCache(cacheLookup);

        TermsLookupFilterBuilder numericFilter = termsLookupFilter("num")
                .index(PARENT_INDEX)
                .type(PARENT_TYPE)
                .path("num")
                .lookupCache(cacheLookup);

        long tookString = 0;
        long tookNumeric = 0;
        long tookBloom = 0;
        long expected = NUM_CHILDREN_PER_PARENT;
        warmFieldData("id", "pid");     // for string fields
        warmFieldData("num", "num");    // for numeric fields

        log("==== HAS PARENT SINGLE TERM (cache: " + cacheLookup + ") ====");
        for (int i = 0; i < NUM_QUERIES; i++) {
            lookupFilter = termFilter("name", "test" + (random.nextInt(NUM_PARENTS) + 1));

            stringFilter.lookupFilter(lookupFilter);
            numericFilter.lookupFilter(lookupFilter);
            bloomFilter.lookupFilter(lookupFilter);

            tookString += runQuery("string", i, CHILD_INDEX, expected, filteredQuery(mainQuery, stringFilter));
            tookNumeric += runQuery("numeric", i, CHILD_INDEX, expected, filteredQuery(mainQuery, numericFilter));
            tookBloom += runQuery("bloom", i, CHILD_INDEX, expected, filteredQuery(mainQuery, bloomFilter));
        }

        log("string: " + (tookString / NUM_QUERIES) + "ms avg");
        log("numeric: " + (tookNumeric / NUM_QUERIES) + "ms avg");
        log("bloom: " + (tookBloom / NUM_QUERIES) + "ms avg");
        log("");
    }

    /**
     * Search for children that have a parent.
     * Expect all children to be returned.
     * <p/>
     * Parent string field = "id"
     * Parent numeric field = "num"
     * Child string field = "pid"
     * Child numeric field = "num"
     */
    public void benchHasParentMatchAll(boolean cacheLookup) {
        FilterBuilder lookupFilter = matchAllFilter();
        QueryBuilder mainQuery = matchAllQuery();

        TermsLookupFilterBuilder stringFilter = termsLookupFilter("pid")
                .index(PARENT_INDEX)
                .type(PARENT_TYPE)
                .path("id")
                .lookupCache(cacheLookup)
                .lookupFilter(lookupFilter);

        TermsLookupFilterBuilder bloomFilter = termsLookupFilter("pid")
                .index(PARENT_INDEX)
                .type(PARENT_TYPE)
                .path("id")
                .bloomExpectedInsertions(NUM_PARENTS * NUM_CHILDREN_PER_PARENT)
                .lookupCache(cacheLookup)
                .lookupFilter(lookupFilter);

        TermsLookupFilterBuilder numericFilter = termsLookupFilter("num")
                .index(PARENT_INDEX)
                .type(PARENT_TYPE)
                .path("num")
                .lookupCache(cacheLookup)
                .lookupFilter(lookupFilter);

        long tookString = 0;
        long tookNumeric = 0;
        long tookBloom = 0;
        long expected = NUM_CHILDREN_PER_PARENT * NUM_PARENTS;
        warmFieldData("id", "pid");     // for string fields
        warmFieldData("num", "num");    // for numeric fields

        log("==== HAS PARENT MATCH-ALL (cache: " + cacheLookup + ") ====");
        for (int i = 0; i < NUM_QUERIES; i++) {
            tookString += runQuery("string", i, CHILD_INDEX, expected, filteredQuery(mainQuery, stringFilter));
            tookNumeric += runQuery("numeric", i, CHILD_INDEX, expected, filteredQuery(mainQuery, numericFilter));
            tookBloom += runQuery("bloom", i, CHILD_INDEX, expected, filteredQuery(mainQuery, bloomFilter));
        }

        log("string: " + (tookString / NUM_QUERIES) + "ms avg");
        log("numeric: " + (tookNumeric / NUM_QUERIES) + "ms avg");
        log("bloom: " + (tookBloom / NUM_QUERIES) + "ms avg");
        log("");
    }

    /**
     * Search for children that have a parent with any of the specified names.
     * Expect NUM_CHILDREN_PER_PARENT * # of names.
     * <p/>
     * Parent string field = "id"
     * Parent numeric field = "num"
     * Child string field = "pid"
     * Child numeric field = "num"
     */
    public void benchHasParentRandomTerms(boolean cacheLookup) {
        FilterBuilder lookupFilter;
        QueryBuilder mainQuery = matchAllQuery();
        Set<String> names = new HashSet<String>(NUM_PARENTS);

        TermsLookupFilterBuilder stringFilter = termsLookupFilter("pid")
                .index(PARENT_INDEX)
                .type(PARENT_TYPE)
                .path("id")
                .lookupCache(cacheLookup);

        TermsLookupFilterBuilder bloomFilter = termsLookupFilter("pid")
                .index(PARENT_INDEX)
                .type(PARENT_TYPE)
                .path("id")

                /*
                    Tune the bloom filter to give acceptable false positives for this test.
                 */
                .bloomHashFunctions(10)
                .bloomFpp(0.01)
                .lookupCache(cacheLookup);

        TermsLookupFilterBuilder numericFilter = termsLookupFilter("num")
                .index(PARENT_INDEX)
                .type(PARENT_TYPE)
                .path("num")
                .lookupCache(cacheLookup);

        long tookString = 0;
        long tookNumeric = 0;
        long tookBloom = 0;
        int expected = 0;
        warmFieldData("id", "pid");     // for string fields
        warmFieldData("num", "num");    // for numeric fields
        warmFieldData("name", null);    // for field data terms filter

        log("==== HAS PARENT RANDOM TERMS (cache: " + cacheLookup + ") ====");
        for (int i = 0; i < NUM_QUERIES; i++) {

            // add a random number of terms to the set on each iteration
            int randNum = random.nextInt(NUM_PARENTS / NUM_QUERIES) + 1;
            for (int j = 0; j < randNum; j++) {
                names.add("test" + (random.nextInt(NUM_PARENTS) + 1));
            }

            lookupFilter = termsFilter("name", names).execution("fielddata");
            expected = NUM_CHILDREN_PER_PARENT * names.size();
            stringFilter.lookupFilter(lookupFilter);
            numericFilter.lookupFilter(lookupFilter);
            bloomFilter.lookupFilter(lookupFilter).bloomExpectedInsertions(expected); // part of bloom filter tuning

            tookString += runQuery("string", i, CHILD_INDEX, expected, filteredQuery(mainQuery, stringFilter));
            tookNumeric += runQuery("numeric", i, CHILD_INDEX, expected, filteredQuery(mainQuery, numericFilter));
            tookBloom += runQuery("bloom", i, CHILD_INDEX, expected, filteredQuery(mainQuery, bloomFilter));
        }

        log("string: " + (tookString / NUM_QUERIES) + "ms avg");
        log("numeric: " + (tookNumeric / NUM_QUERIES) + "ms avg");
        log("bloom: " + (tookBloom / NUM_QUERIES) + "ms avg");
        log("");
    }
}
