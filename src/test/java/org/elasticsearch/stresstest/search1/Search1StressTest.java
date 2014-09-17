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

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Ignore;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;

/**
 *
 */
@Ignore("Stress Test")
public class Search1StressTest {

    private final ESLogger logger = Loggers.getLogger(getClass());


    private int numberOfNodes = 4;

    private int indexers = 0;
    private SizeValue preIndexDocs = new SizeValue(0);
    private TimeValue indexerThrottle = TimeValue.timeValueMillis(100);
    private int searchers = 0;
    private TimeValue searcherThrottle = TimeValue.timeValueMillis(20);
    private int numberOfIndices = 10;
    private int numberOfTypes = 4;
    private int numberOfValues = 20;
    private int numberOfHits = 300;
    private TimeValue flusherThrottle = TimeValue.timeValueMillis(1000);
    private TimeValue deleteByQueryThrottle = TimeValue.timeValueMillis(5000);

    private Settings settings = ImmutableSettings.Builder.EMPTY_SETTINGS;

    private TimeValue period = TimeValue.timeValueMinutes(20);

    private AtomicLong indexCounter = new AtomicLong();
    private AtomicLong searchCounter = new AtomicLong();


    private Node client;

    public Search1StressTest setNumberOfNodes(int numberOfNodes) {
        this.numberOfNodes = numberOfNodes;
        return this;
    }

    public Search1StressTest setPreIndexDocs(SizeValue preIndexDocs) {
        this.preIndexDocs = preIndexDocs;
        return this;
    }

    public Search1StressTest setIndexers(int indexers) {
        this.indexers = indexers;
        return this;
    }

    public Search1StressTest setIndexerThrottle(TimeValue indexerThrottle) {
        this.indexerThrottle = indexerThrottle;
        return this;
    }

    public Search1StressTest setSearchers(int searchers) {
        this.searchers = searchers;
        return this;
    }

    public Search1StressTest setSearcherThrottle(TimeValue searcherThrottle) {
        this.searcherThrottle = searcherThrottle;
        return this;
    }

    public Search1StressTest setNumberOfIndices(int numberOfIndices) {
        this.numberOfIndices = numberOfIndices;
        return this;
    }

    public Search1StressTest setNumberOfTypes(int numberOfTypes) {
        this.numberOfTypes = numberOfTypes;
        return this;
    }

    public Search1StressTest setNumberOfValues(int numberOfValues) {
        this.numberOfValues = numberOfValues;
        return this;
    }

    public Search1StressTest setNumberOfHits(int numberOfHits) {
        this.numberOfHits = numberOfHits;
        return this;
    }

    public Search1StressTest setFlusherThrottle(TimeValue flusherThrottle) {
        this.flusherThrottle = flusherThrottle;
        return this;
    }

    public Search1StressTest setDeleteByQueryThrottle(TimeValue deleteByQueryThrottle) {
        this.deleteByQueryThrottle = deleteByQueryThrottle;
        return this;
    }

    public Search1StressTest setSettings(Settings settings) {
        this.settings = settings;
        return this;
    }

    public Search1StressTest setPeriod(TimeValue period) {
        this.period = period;
        return this;
    }

    private String nextIndex() {
        return "test" + Math.abs(ThreadLocalRandom.current().nextInt()) % numberOfIndices;
    }

    private String nextType() {
        return "type" + Math.abs(ThreadLocalRandom.current().nextInt()) % numberOfTypes;
    }

    private int nextNumValue() {
        return Math.abs(ThreadLocalRandom.current().nextInt()) % numberOfValues;
    }

    private String nextFieldValue() {
        return "value" + Math.abs(ThreadLocalRandom.current().nextInt()) % numberOfValues;
    }

    private class Searcher extends Thread {

        volatile boolean close = false;

        volatile boolean closed = false;

        @Override
        public void run() {
            while (true) {
                if (close) {
                    closed = true;
                    return;
                }
                try {
                    String indexName = nextIndex();
                    SearchRequestBuilder builder = client.client().prepareSearch(indexName);
                    if (ThreadLocalRandom.current().nextBoolean()) {
                        builder.addSort("num", SortOrder.DESC);
                    } else if (ThreadLocalRandom.current().nextBoolean()) {
                        // add a _score based sorting, won't do any sorting, just to test...
                        builder.addSort("_score", SortOrder.DESC);
                    }
                    if (ThreadLocalRandom.current().nextBoolean()) {
                        builder.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
                    }
                    int size = Math.abs(ThreadLocalRandom.current().nextInt()) % numberOfHits;
                    builder.setSize(size);
                    if (ThreadLocalRandom.current().nextBoolean()) {
                        // update from
                        builder.setFrom(size / 2);
                    }
                    String value = nextFieldValue();
                    builder.setQuery(termQuery("field", value));
                    searchCounter.incrementAndGet();
                    SearchResponse searchResponse = builder.execute().actionGet();
                    if (searchResponse.getFailedShards() > 0) {
                        logger.warn("failed search " + Arrays.toString(searchResponse.getShardFailures()));
                    }
                    // verify that all come from the requested index
                    for (SearchHit hit : searchResponse.getHits()) {
                        if (!hit.shard().index().equals(indexName)) {
                            logger.warn("got wrong index, asked for [{}], got [{}]", indexName, hit.shard().index());
                        }
                    }
                    // verify that all has the relevant value
                    for (SearchHit hit : searchResponse.getHits()) {
                        if (!value.equals(hit.sourceAsMap().get("field"))) {
                            logger.warn("got wrong field, asked for [{}], got [{}]", value, hit.sourceAsMap().get("field"));
                        }
                    }
                    Thread.sleep(searcherThrottle.millis());
                } catch (Exception e) {
                    logger.warn("failed to search", e);
                }
            }
        }
    }

    private class Indexer extends Thread {

        volatile boolean close = false;

        volatile boolean closed = false;

        @Override
        public void run() {
            while (true) {
                if (close) {
                    closed = true;
                    return;
                }
                try {
                    indexDoc();
                    Thread.sleep(indexerThrottle.millis());
                } catch (Exception e) {
                    logger.warn("failed to index / sleep", e);
                }
            }
        }
    }

    private class Flusher extends Thread {
        volatile boolean close = false;

        volatile boolean closed = false;

        @Override
        public void run() {
            while (true) {
                if (close) {
                    closed = true;
                    return;
                }
                try {
                    client.client().admin().indices().prepareFlush().execute().actionGet();
                    Thread.sleep(indexerThrottle.millis());
                } catch (Exception e) {
                    logger.warn("failed to flush / sleep", e);
                }
            }
        }
    }

    private class DeleteByQuery extends Thread {
        volatile boolean close = false;

        volatile boolean closed = false;

        @Override
        public void run() {
            while (true) {
                if (close) {
                    closed = true;
                    return;
                }
                try {
                    client.client().prepareDeleteByQuery().setQuery(termQuery("num", nextNumValue())).execute().actionGet();
                    Thread.sleep(deleteByQueryThrottle.millis());
                } catch (Exception e) {
                    logger.warn("failed to delete_by_query", e);
                }
            }
        }
    }

    private void indexDoc() throws Exception {
        XContentBuilder json = XContentFactory.jsonBuilder().startObject()
                .field("num", nextNumValue())
                .field("field", nextFieldValue());

        json.endObject();

        client.client().prepareIndex(nextIndex(), nextType())
                .setSource(json)
                .execute().actionGet();
        indexCounter.incrementAndGet();
    }

    public void run() throws Exception {
        Node[] nodes = new Node[numberOfNodes];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = NodeBuilder.nodeBuilder().settings(settings).node();
        }
        client = NodeBuilder.nodeBuilder().settings(settings).client(true).node();

        for (int i = 0; i < numberOfIndices; i++) {
            client.client().admin().indices().prepareCreate("test" + i).execute().actionGet();
        }

        logger.info("Pre indexing docs [{}]...", preIndexDocs);
        for (long i = 0; i < preIndexDocs.singles(); i++) {
            indexDoc();
        }
        logger.info("Done pre indexing docs [{}]", preIndexDocs);

        Indexer[] indexerThreads = new Indexer[indexers];
        for (int i = 0; i < indexerThreads.length; i++) {
            indexerThreads[i] = new Indexer();
        }
        for (Indexer indexerThread : indexerThreads) {
            indexerThread.start();
        }

        Thread.sleep(10000);

        Searcher[] searcherThreads = new Searcher[searchers];
        for (int i = 0; i < searcherThreads.length; i++) {
            searcherThreads[i] = new Searcher();
        }
        for (Searcher searcherThread : searcherThreads) {
            searcherThread.start();
        }

        Flusher flusher = null;
        if (flusherThrottle.millis() > 0) {
            flusher = new Flusher();
            flusher.start();
        }

        DeleteByQuery deleteByQuery = null;
        if (deleteByQueryThrottle.millis() > 0) {
            deleteByQuery = new DeleteByQuery();
            deleteByQuery.start();
        }


        long testStart = System.currentTimeMillis();

        while (true) {
            Thread.sleep(5000);
            if ((System.currentTimeMillis() - testStart) > period.millis()) {
                break;
            }
        }

        System.out.println("DONE, closing .....");

        if (flusher != null) {
            flusher.close = true;
        }

        if (deleteByQuery != null) {
            deleteByQuery.close = true;
        }

        for (Searcher searcherThread : searcherThreads) {
            searcherThread.close = true;
        }

        for (Indexer indexerThread : indexerThreads) {
            indexerThread.close = true;
        }

        Thread.sleep(indexerThrottle.millis() + 10000);

        if (flusher != null && !flusher.closed) {
            logger.warn("flusher not closed!");
        }
        if (deleteByQuery != null && !deleteByQuery.closed) {
            logger.warn("deleteByQuery not closed!");
        }
        for (Searcher searcherThread : searcherThreads) {
            if (!searcherThread.closed) {
                logger.warn("search thread not closed!");
            }
        }
        for (Indexer indexerThread : indexerThreads) {
            if (!indexerThread.closed) {
                logger.warn("index thread not closed!");
            }
        }

        client.close();
        for (Node node : nodes) {
            node.close();
        }

        System.out.println("********** DONE, indexed [" + indexCounter.get() + "], searched [" + searchCounter.get() + "]");
    }

    public static void main(String[] args) throws Exception {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("gateway.type", "none")
                .build();

        Search1StressTest test = new Search1StressTest()
                .setPeriod(TimeValue.timeValueMinutes(10))
                .setSettings(settings)
                .setNumberOfNodes(2)
                .setPreIndexDocs(SizeValue.parseSizeValue("100"))
                .setIndexers(2)
                .setIndexerThrottle(TimeValue.timeValueMillis(100))
                .setSearchers(10)
                .setSearcherThrottle(TimeValue.timeValueMillis(10))
                .setDeleteByQueryThrottle(TimeValue.timeValueMillis(-1))
                .setFlusherThrottle(TimeValue.timeValueMillis(1000))
                .setNumberOfIndices(10)
                .setNumberOfTypes(5)
                .setNumberOfValues(50)
                .setNumberOfHits(300);

        test.run();
    }
}
