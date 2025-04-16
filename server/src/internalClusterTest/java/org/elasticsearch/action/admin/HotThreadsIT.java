/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.admin;

import org.apache.logging.log4j.Level;
import org.apache.lucene.util.Constants;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodeHotThreads;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsRequest;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsResponse;
import org.elasticsearch.action.admin.cluster.node.hotthreads.TransportNodesHotThreadsAction;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.ChunkedLoggingStreamTestUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.monitor.jvm.HotThreads;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.hamcrest.Matcher;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.MockLog.assertThatLogger;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.collection.IsEmptyCollection.empty;

public class HotThreadsIT extends ESIntegTestCase {

    public void testHotThreadsDontFail() throws InterruptedException, ExecutionException {
        // This test just checks if nothing crashes or gets stuck etc.
        createIndex("test");
        final int iters = scaledRandomIntBetween(2, 20);
        for (int i = 0; i < iters; i++) {
            final NodesHotThreadsRequest request = new NodesHotThreadsRequest(
                Strings.EMPTY_ARRAY,
                new HotThreads.RequestOptions(
                    randomBoolean() ? HotThreads.RequestOptions.DEFAULT.threads()
                        : rarely() ? randomIntBetween(500, 5000)
                        : randomIntBetween(1, 500),
                    randomBoolean()
                        ? HotThreads.RequestOptions.DEFAULT.reportType()
                        : HotThreads.ReportType.of(randomFrom("block", "mem", "cpu", "wait")),
                    HotThreads.RequestOptions.DEFAULT.sortOrder(),
                    randomBoolean()
                        ? HotThreads.RequestOptions.DEFAULT.interval()
                        : TimeValue.timeValueMillis(rarely() ? randomIntBetween(500, 5000) : randomIntBetween(20, 500)),
                    HotThreads.RequestOptions.DEFAULT.snapshots(),
                    randomBoolean()
                )
            );
            final ActionFuture<NodesHotThreadsResponse> hotThreadsFuture = client().execute(TransportNodesHotThreadsAction.TYPE, request);

            indexRandom(
                true,
                prepareIndex("test").setId("1").setSource("field1", "value1"),
                prepareIndex("test").setId("2").setSource("field1", "value2"),
                prepareIndex("test").setId("3").setSource("field1", "value3")
            );
            ensureSearchable();
            while (hotThreadsFuture.isDone() == false) {
                assertHitCount(
                    prepareSearch().setQuery(matchAllQuery())
                        .setPostFilter(
                            boolQuery().must(matchAllQuery())
                                .mustNot(boolQuery().must(termQuery("field1", "value1")).must(termQuery("field1", "value2")))
                        ),
                    3L
                );
            }
            assertResponse(hotThreadsFuture, nodeHotThreads -> {
                assertThat(nodeHotThreads, notNullValue());
                Map<String, NodeHotThreads> nodesMap = nodeHotThreads.getNodesMap();
                assertThat(nodeHotThreads.failures(), empty());
                assertThat(nodesMap.size(), equalTo(cluster().size()));
                for (NodeHotThreads ht : nodeHotThreads.getNodes()) {
                    assertNotNull(ht.getHotThreads());
                }
            });
        }
    }

    public void testIgnoreIdleThreads() {
        assumeTrue("no support for hot_threads on FreeBSD", Constants.FREE_BSD == false);

        final Matcher<String> containsCachedTimeThreadRunMethod = containsString(
            "org.elasticsearch.threadpool.ThreadPool$CachedTimeThread.run"
        );

        // First time, don't ignore idle threads:
        final var totSizeAll = safeAwait(
            SubscribableListener.<Integer>newForked(
                l -> client().execute(
                    TransportNodesHotThreadsAction.TYPE,
                    new NodesHotThreadsRequest(
                        Strings.EMPTY_ARRAY,
                        new HotThreads.RequestOptions(
                            Integer.MAX_VALUE,
                            HotThreads.RequestOptions.DEFAULT.reportType(),
                            HotThreads.RequestOptions.DEFAULT.sortOrder(),
                            HotThreads.RequestOptions.DEFAULT.interval(),
                            HotThreads.RequestOptions.DEFAULT.snapshots(),
                            false
                        )
                    ),
                    l.map(response -> {
                        int length = 0;
                        for (NodeHotThreads node : response.getNodesMap().values()) {
                            length += node.getHotThreads().length();
                            assertThat(node.getHotThreads(), containsCachedTimeThreadRunMethod);
                        }
                        return length;
                    })
                )
            )
        );

        // Second time, do ignore idle threads:
        final var request = new NodesHotThreadsRequest(
            Strings.EMPTY_ARRAY,
            new HotThreads.RequestOptions(
                Integer.MAX_VALUE,
                HotThreads.RequestOptions.DEFAULT.reportType(),
                HotThreads.RequestOptions.DEFAULT.sortOrder(),
                HotThreads.RequestOptions.DEFAULT.interval(),
                HotThreads.RequestOptions.DEFAULT.snapshots(),
                HotThreads.RequestOptions.DEFAULT.ignoreIdleThreads()
            )
        );
        // Make sure default is true:
        assertTrue(request.ignoreIdleThreads());
        final var totSizeIgnoreIdle = safeAwait(
            SubscribableListener.<Integer>newForked(l -> client().execute(TransportNodesHotThreadsAction.TYPE, request, l.map(response -> {
                int length = 0;
                for (NodeHotThreads node : response.getNodesMap().values()) {
                    length += node.getHotThreads().length();
                    assertThat(node.getHotThreads(), not(containsCachedTimeThreadRunMethod));
                }
                return length;
            })))
        );

        // The filtered stacks should be smaller than unfiltered ones:
        assertThat(totSizeIgnoreIdle, lessThan(totSizeAll));
    }

    public void testTimestampAndParams() {
        safeAwait(
            SubscribableListener.<Void>newForked(
                l -> client().execute(
                    TransportNodesHotThreadsAction.TYPE,
                    new NodesHotThreadsRequest(Strings.EMPTY_ARRAY, HotThreads.RequestOptions.DEFAULT),
                    l.map(response -> {
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
                        return null;
                    })
                )
            )
        );
    }

    @TestLogging(reason = "testing logging at various levels", value = "org.elasticsearch.action.admin.HotThreadsIT:TRACE")
    public void testLogLocalHotThreads() {
        final var level = randomFrom(Level.TRACE, Level.DEBUG, Level.INFO, Level.WARN, Level.ERROR);
        assertThat(
            ChunkedLoggingStreamTestUtils.getDecodedLoggedBody(
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

    @TestLogging(reason = "testing logging at various levels", value = "org.elasticsearch.action.admin.HotThreadsIT:TRACE")
    public void testLogLocalCurrentThreadsInPlainText() {
        final var level = randomFrom(Level.TRACE, Level.DEBUG, Level.INFO, Level.WARN, Level.ERROR);
        assertThatLogger(
            () -> HotThreads.logLocalCurrentThreads(logger, level, getTestName()),
            HotThreadsIT.class,
            new MockLog.SeenEventExpectation(
                "Should log hot threads header in plain text",
                HotThreadsIT.class.getCanonicalName(),
                level,
                "testLogLocalCurrentThreadsInPlainText: Hot threads at"
            ),
            new MockLog.SeenEventExpectation(
                "Should log hot threads cpu usage in plain text",
                HotThreadsIT.class.getCanonicalName(),
                level,
                "cpu usage by thread"
            )
        );
    }
}
