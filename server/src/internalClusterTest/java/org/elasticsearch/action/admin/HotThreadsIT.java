/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin;

import org.apache.logging.log4j.Level;
import org.apache.lucene.util.Constants;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodeHotThreads;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsRequest;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsResponse;
import org.elasticsearch.action.admin.cluster.node.hotthreads.TransportNodesHotThreadsAction;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.logging.ChunkedLoggingStreamTests;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.monitor.jvm.HotThreads;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.hamcrest.Matcher;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.collection.IsEmptyCollection.empty;

public class HotThreadsIT extends ESIntegTestCase {

    public void testHotThreadsDontFail() throws InterruptedException {
        // This test just checks if nothing crashes or gets stuck etc.
        createIndex("test");
        final int iters = scaledRandomIntBetween(2, 20);
        final AtomicBoolean hasErrors = new AtomicBoolean(false);
        for (int i = 0; i < iters; i++) {
            final NodesHotThreadsRequest request = new NodesHotThreadsRequest();
            if (randomBoolean()) {
                TimeValue timeValue = new TimeValue(rarely() ? randomIntBetween(500, 5000) : randomIntBetween(20, 500));
                request.interval(timeValue);
            }
            if (randomBoolean()) {
                request.threads(rarely() ? randomIntBetween(500, 5000) : randomIntBetween(1, 500));
            }
            request.ignoreIdleThreads(randomBoolean());
            if (randomBoolean()) {
                request.type(HotThreads.ReportType.of(randomFrom("block", "mem", "cpu", "wait")));
            }
            final CountDownLatch latch = new CountDownLatch(1);
            client().execute(TransportNodesHotThreadsAction.TYPE, request, new ActionListener<>() {
                @Override
                public void onResponse(NodesHotThreadsResponse nodeHotThreads) {
                    boolean success = false;
                    try {
                        assertThat(nodeHotThreads, notNullValue());
                        Map<String, NodeHotThreads> nodesMap = nodeHotThreads.getNodesMap();
                        assertThat(nodeHotThreads.failures(), empty());
                        assertThat(nodesMap.size(), equalTo(cluster().size()));
                        for (NodeHotThreads ht : nodeHotThreads.getNodes()) {
                            assertNotNull(ht.getHotThreads());
                        }
                        success = true;
                    } finally {
                        if (success == false) {
                            hasErrors.set(true);
                        }
                        latch.countDown();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("FAILED", e);
                    hasErrors.set(true);
                    latch.countDown();
                    fail();
                }
            });

            indexRandom(
                true,
                prepareIndex("test").setId("1").setSource("field1", "value1"),
                prepareIndex("test").setId("2").setSource("field1", "value2"),
                prepareIndex("test").setId("3").setSource("field1", "value3")
            );
            ensureSearchable();
            while (latch.getCount() > 0) {
                assertHitCount(
                    prepareSearch().setQuery(matchAllQuery())
                        .setPostFilter(
                            boolQuery().must(matchAllQuery())
                                .mustNot(boolQuery().must(termQuery("field1", "value1")).must(termQuery("field1", "value2")))
                        ),
                    3L
                );
            }
            safeAwait(latch);
            assertThat(hasErrors.get(), is(false));
        }
    }

    public void testIgnoreIdleThreads() {
        assumeTrue("no support for hot_threads on FreeBSD", Constants.FREE_BSD == false);

        // First time, don't ignore idle threads:
        final NodesHotThreadsResponse firstResponse = client().execute(
            TransportNodesHotThreadsAction.TYPE,
            new NodesHotThreadsRequest().ignoreIdleThreads(false).threads(Integer.MAX_VALUE)
        ).actionGet(10, TimeUnit.SECONDS);

        final Matcher<String> containsCachedTimeThreadRunMethod = containsString(
            "org.elasticsearch.threadpool.ThreadPool$CachedTimeThread.run"
        );

        int totSizeAll = 0;
        for (NodeHotThreads node : firstResponse.getNodesMap().values()) {
            totSizeAll += node.getHotThreads().length();
            assertThat(node.getHotThreads(), containsCachedTimeThreadRunMethod);
        }

        // Second time, do ignore idle threads:
        final var request = new NodesHotThreadsRequest().threads(Integer.MAX_VALUE);
        // Make sure default is true:
        assertTrue(request.ignoreIdleThreads());
        final NodesHotThreadsResponse secondResponse = client().execute(TransportNodesHotThreadsAction.TYPE, request)
            .actionGet(10, TimeUnit.SECONDS);

        int totSizeIgnoreIdle = 0;
        for (NodeHotThreads node : secondResponse.getNodesMap().values()) {
            totSizeIgnoreIdle += node.getHotThreads().length();
            assertThat(node.getHotThreads(), not(containsCachedTimeThreadRunMethod));
        }

        // The filtered stacks should be smaller than unfiltered ones:
        assertThat(totSizeIgnoreIdle, lessThan(totSizeAll));
    }

    public void testTimestampAndParams() {

        final NodesHotThreadsResponse response = client().execute(TransportNodesHotThreadsAction.TYPE, new NodesHotThreadsRequest())
            .actionGet(10, TimeUnit.SECONDS);

        if (Constants.FREE_BSD) {
            for (NodeHotThreads node : response.getNodesMap().values()) {
                assertThat(node.getHotThreads(), containsString("hot_threads is not supported"));
            }
        } else {
            for (NodeHotThreads node : response.getNodesMap().values()) {
                assertThat(
                    node.getHotThreads(),
                    allOf(
                        containsString("Hot threads at"),
                        containsString("interval=500ms"),
                        containsString("busiestThreads=3"),
                        containsString("ignoreIdleThreads=true")
                    )
                );
            }
        }
    }

    @TestLogging(reason = "testing logging at various levels", value = "org.elasticsearch.action.admin.HotThreadsIT:TRACE")
    public void testLogLocalHotThreads() {
        final var level = randomFrom(Level.TRACE, Level.DEBUG, Level.INFO, Level.WARN, Level.ERROR);
        assertThat(
            ChunkedLoggingStreamTests.getDecodedLoggedBody(
                logger,
                level,
                getTestName(),
                ReferenceDocs.LOGGING,
                () -> HotThreads.logLocalHotThreads(logger, level, getTestName(), ReferenceDocs.LOGGING)
            ).utf8ToString(),
            allOf(
                containsString("Hot threads at"),
                containsString("interval=500ms"),
                containsString("busiestThreads=500"),
                containsString("ignoreIdleThreads=false"),
                containsString("cpu usage by thread")
            )
        );
    }
}
