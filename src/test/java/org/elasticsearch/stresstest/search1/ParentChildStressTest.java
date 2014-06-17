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

package org.elasticsearch.stresstest.search1;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.RemoteTransportException;

import java.io.IOException;
import java.util.*;


public class ParentChildStressTest {

    private Node elasticNode;
    private Client client;

    private static final String PARENT_TYPE_NAME = "content";
    private static final String CHILD_TYPE_NAME = "contentFiles";
    private static final String INDEX_NAME = "acme";

    /**
     * Constructor.  Initialize elastic and create the index/mapping
     */
    public ParentChildStressTest() {
        NodeBuilder nodeBuilder = NodeBuilder.nodeBuilder();
        Settings settings = nodeBuilder.settings()
                .build();
        this.elasticNode = nodeBuilder.settings(settings).client(true).node();
        this.client = this.elasticNode.client();

        String mapping =
                "{\"contentFiles\": {" +
                        "\"_parent\": {" +
                        "\"type\" : \"content\"" +
                        "}}}";

        try {
            client.admin().indices().create(new CreateIndexRequest(INDEX_NAME).mapping(CHILD_TYPE_NAME, mapping)).actionGet();
        } catch (RemoteTransportException e) {
            // usually means the index is already created.
        }
    }

    public void shutdown() throws IOException {
        client.close();
        elasticNode.close();
    }

    /**
     * Deletes the item from both the parent and child type locations.
     */
    public void deleteById(String id) {
        client.prepareDelete(INDEX_NAME, PARENT_TYPE_NAME, id).execute().actionGet();
        client.prepareDelete(INDEX_NAME, CHILD_TYPE_NAME, id).execute().actionGet();
    }

    /**
     * Index a parent doc
     */
    public void indexParent(String id, Map<String, Object> objectMap) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();

        // index content
        client.prepareIndex(INDEX_NAME, PARENT_TYPE_NAME, id).setSource(builder.map(objectMap)).execute().actionGet();
    }

    /**
     * Index the file as a child doc
     */
    public void indexChild(String id, Map<String, Object> objectMap) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();

        IndexRequestBuilder indexRequestbuilder = client.prepareIndex(INDEX_NAME, CHILD_TYPE_NAME, id);
        indexRequestbuilder = indexRequestbuilder.setParent(id);
        indexRequestbuilder = indexRequestbuilder.setSource(builder.map(objectMap));
        indexRequestbuilder.execute().actionGet();
    }

    /**
     * Execute a search based on a JSON String in QueryDSL format.
     * <p/>
     * Throws a RuntimeException if there are any shard failures to
     * elevate the visibility of the problem.
     */
    public List<String> executeSearch(String source) {
        SearchRequest request = Requests.searchRequest(INDEX_NAME).source(source);

        List<ShardSearchFailure> failures;
        SearchResponse response;

        response = client.search(request).actionGet();
        failures = Arrays.asList(response.getShardFailures());

        // throw an exception so that we see the shard failures
        if (failures.size() != 0) {
            String failuresStr = failures.toString();
            if (!failuresStr.contains("reason [No active shards]")) {
                throw new RuntimeException(failures.toString());
            }
        }

        ArrayList<String> results = new ArrayList<>();
        if (response != null) {
            for (SearchHit hit : response.getHits()) {
                String sourceStr = hit.sourceAsString();
                results.add(sourceStr);
            }
        }
        return results;
    }

    /**
     * Create a document as a parent and index it.
     * Load a file and index it as a child.
     */
    public String indexDoc() throws IOException {
        String id = UUID.randomUUID().toString();

        Map<String, Object> objectMap = new HashMap<>();
        objectMap.put("title", "this is a document");

        Map<String, Object> objectMap2 = new HashMap<>();
        objectMap2.put("description", "child test");

        this.indexParent(id, objectMap);
        this.indexChild(id, objectMap2);
        return id;
    }

    /**
     * Perform the has_child query for the doc.
     * <p/>
     * Since it might take time to get indexed, it
     * loops until it finds the doc.
     */
    public void searchDocByChild() throws InterruptedException {
        String dslString =
                "{\"query\":{" +
                        "\"has_child\":{" +
                        "\"query\":{" +
                        "\"field\":{" +
                        "\"description\":\"child test\"}}," +
                        "\"type\":\"contentFiles\"}}}";

        int numTries = 0;
        List<String> items = new ArrayList<>();

        while (items.size() != 1 && numTries < 20) {
            items = executeSearch(dslString);

            numTries++;
            if (items.size() != 1) {
                Thread.sleep(250);
            }
        }
        if (items.size() != 1) {
            System.out.println("Exceeded number of retries");
            System.exit(1);
        }
    }

    /**
     * Program to loop on:
     * create parent/child doc
     * search for the doc
     * delete the doc
     * repeat the above until shard failure.
     * <p/>
     * Eventually fails with:
     * <p/>
     * [shard [[74wz0lrXRSmSOsJOqgPvlw][acme][1]], reason [RemoteTransportException
     * [[Kismet][inet[/10.10.30.52:9300]][search/phase/query]]; nested:
     * QueryPhaseExecutionException[[acme][1]:
     * query[ConstantScore(child_filter[contentFiles
     * /content](filtered(file:mission
     * file:statement)->FilterCacheFilterWrapper(
     * _type:contentFiles)))],from[0],size[10]: Query Failed [Failed to execute
     * child query [filtered(file:mission
     * file:statement)->FilterCacheFilterWrapper(_type:contentFiles)]]]; nested:
     * ]]
     *
     * @param args
     */
    public static void main(String[] args) throws IOException {
        ParentChildStressTest elasticTest = new ParentChildStressTest();
        try {
            // loop a bunch of times - usually fails before the count is done.
            int NUM_LOOPS = 1000;
            System.out.println();
            System.out.println("Looping [" + NUM_LOOPS + "] times:");
            System.out.println();
            for (int i = 0; i < NUM_LOOPS; i++) {
                String id = elasticTest.indexDoc();

                elasticTest.searchDocByChild();

                elasticTest.deleteById(id);

                System.out.println("    Success: " + i);
            }
            elasticTest.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            elasticTest.shutdown();
        }
    }
}
