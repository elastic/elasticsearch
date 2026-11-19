/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.Build;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.eql.action.EqlSearchAction;
import org.elasticsearch.xpack.eql.action.EqlSearchRequest;
import org.elasticsearch.xpack.eql.plugin.EqlPlugin;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;

public class EqlRequestSizeIT extends AbstractEsqlIntegTestCase {

    private static final String INDEX = "eql_size";
    private static final String HEAD = " WITH {\"result_position\": \"head\"}";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(
            CollectionUtils.appendToCopy(super.nodePlugins(), EqlPlugin.class),
            EqlRequestCapturePlugin.class
        );
    }

    @Before
    public void setupIndex() {
        assumeTrue("EQL command is snapshot-only", Build.current().isSnapshot());
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(INDEX)
                .setSettings(Settings.builder().put("index.number_of_shards", 1))
                .setMapping("@timestamp", "type=date", "event.category", "type=keyword", "pid", "type=long")
        );
        client().prepareBulk()
            .add(new IndexRequest(INDEX).id("p1").source("@timestamp", "2026-07-22T10:00:00Z", "event.category", "process", "pid", 1))
            .add(new IndexRequest(INDEX).id("p2").source("@timestamp", "2026-07-22T10:00:01Z", "event.category", "process", "pid", 2))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
    }

    public void testSizeIsTheEqlDefaultWithoutWithSize() {
        assertRequestSize("", 10);
        assertRequestSize(" | LIMIT 3", 10);
        assertRequestSize(" | KEEP pid | LIMIT 3", 10);
    }

    public void testWithSizeSetsTheSize() {
        assertRequestSize(" WITH {\"size\": 50}", 50);
        assertRequestSize(" WITH {\"size\": 50} | LIMIT 3", 50);
    }

    public void testHeadLimitLowersTheSize() {
        assertRequestSize(HEAD + " | LIMIT 3", 3);
        assertRequestSize(HEAD + " | KEEP pid | LIMIT 3", 3);
        assertRequestSize(HEAD + " | EVAL next = pid + 1 | LIMIT 3", 3);
        assertRequestSize(" WITH {\"result_position\": \"head\", \"size\": 50} | LIMIT 3", 3);
    }

    public void testHeadLimitNeverRaisesTheSize() {
        assertRequestSize(HEAD + " | LIMIT 20", 10);
        assertRequestSize(" WITH {\"result_position\": \"head\", \"size\": 2} | LIMIT 3", 2);
    }

    public void testImplicitLimitLowersTheSizeLikeAnyOther() {
        assertRequestSize(" WITH {\"result_position\": \"head\", \"size\": 5000}", 1000);
        assertRequestSize(" WITH {\"result_position\": \"head\", \"size\": 5000} | KEEP pid", 1000);
        assertRequestSize(" WITH {\"result_position\": \"head\", \"size\": 5000} | LIMIT 1000", 1000);
        assertRequestSize(" WITH {\"result_position\": \"head\", \"size\": 5000} | LIMIT 2000", 2000);
    }

    public void testImplicitLimitOfATailQueryKeepsTheSize() {
        assertRequestSize(" WITH {\"size\": 5000}", 5000);
    }

    public void testLimitBehindACommandThatChangesRowsKeepsTheSize() {
        String headWithLargeSize = " WITH {\"result_position\": \"head\", \"size\": 5000}";
        assertRequestSize(headWithLargeSize + " | WHERE pid > 0 | LIMIT 3", 5000);
        assertRequestSize(headWithLargeSize + " | STATS c = COUNT(*)", 5000);
        assertRequestSize(headWithLargeSize + " | SORT pid | LIMIT 3", 5000);
    }

    public void testExplicitEqlPipeKeepsTheSize() {
        assertRequestSizes("EQL " + INDEX + " \"process where true | head 1\"" + HEAD + " | LIMIT 3", List.of(10));
    }

    public void testHeadLimitOfASequenceKeepsTheSize() {
        String sequence = "EQL " + INDEX + " \"sequence by pid [process where true] [process where true]\"";
        assertRequestSizes(sequence + HEAD + " | LIMIT 3", List.of(10));
        assertRequestSizes(sequence + " WITH {\"result_position\": \"head\", \"size\": 5000}", List.of(5000));
    }

    public void testLimitNeverLowersTheSizeOfASample() {
        String sample = "EQL " + INDEX + " \"sample by pid [process where true] [process where true]\"";
        assertRequestSizes(sample, List.of(10));
        assertRequestSizes(sample + HEAD + " | LIMIT 3", List.of(10));
        assertRequestSizes(sample + " WITH {\"size\": 50} | LIMIT 3", List.of(50));
    }

    public void testHeadLimitLowersTheSizeOnlyOfTheForkBranchItReaches() {
        String fork = "EQL " + INDEX + " \"process where true\"" + HEAD + " | FORK (LIMIT 1) (WHERE pid > 1 | LIMIT 1)";
        assertRequestSizes(fork, List.of(1, 10));
    }

    public void testHeadLimitLowersTheSizeInsideAnInSubquery() {
        String subquery = "EQL " + INDEX + " \"process where true\"" + HEAD + " | LIMIT 1 | KEEP pid";
        assertRequestSizes("FROM " + INDEX + " | WHERE pid IN (" + subquery + ")", List.of(1));
    }

    public void testHeadLimitKeepsTheFirstRowsOfSequencesCompletedByOneEvent() {
        String index = "eql_multi_valued_keys";
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(Settings.builder().put("index.number_of_shards", 1))
                .setMapping("@timestamp", "type=date", "event.category", "type=keyword", "host", "type=keyword", "user", "type=keyword")
        );
        client().prepareBulk()
            .add(
                new IndexRequest(index).id("p1")
                    .source("@timestamp", "2026-07-22T10:00:00Z", "event.category", "process", "host", "a", "user", "y")
            )
            .add(
                new IndexRequest(index).id("p2")
                    .source("@timestamp", "2026-07-22T10:00:01Z", "event.category", "process", "host", "b", "user", "x")
            )
            .add(
                new IndexRequest(index).id("n1")
                    .source(
                        "@timestamp",
                        "2026-07-22T10:00:02Z",
                        "event.category",
                        "network",
                        "host",
                        List.of("a", "b"),
                        "user",
                        List.of("x", "y")
                    )
            )
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        String sequences = "EQL " + index + " \"sequence by host, user [process where true] [network where true]\" METADATA _id" + HEAD;
        List<List<Object>> allRows = List.of(List.of("p1"), List.of("n1"), List.of("p2"), List.of("n1"));
        try (EsqlQueryResponse resp = run(sequences + " | KEEP _id")) {
            assertThat(getValuesList(resp), equalTo(allRows));
        }
        try (EsqlQueryResponse resp = run(sequences + " | LIMIT 1 | KEEP _id")) {
            assertThat(getValuesList(resp), equalTo(allRows.subList(0, 1)));
        }
    }

    private void assertRequestSize(String suffix, int expectedSize) {
        assertRequestSizes("EQL " + INDEX + " \"process where true\"" + suffix, List.of(expectedSize));
    }

    private void assertRequestSizes(String query, List<Integer> expectedSizes) {
        EqlRequestCapturePlugin.SIZES.clear();
        try (EsqlQueryResponse ignored = run(query)) {
            List<Integer> sizes = EqlRequestCapturePlugin.SIZES.stream().sorted().toList();
            assertThat("request sizes for [" + query + "]", sizes, equalTo(expectedSizes));
        }
    }

    public static class EqlRequestCapturePlugin extends Plugin implements ActionPlugin {
        static final List<Integer> SIZES = new CopyOnWriteArrayList<>();

        @Override
        public Collection<ActionFilter> getActionFilters() {
            return List.of(new ActionFilter() {
                @Override
                public int order() {
                    return 0;
                }

                @Override
                public <Request extends ActionRequest, Response extends ActionResponse> void apply(
                    Task task,
                    String action,
                    Request request,
                    ActionListener<Response> listener,
                    ActionFilterChain<Request, Response> chain
                ) {
                    if (EqlSearchAction.NAME.equals(action) && request instanceof EqlSearchRequest eql) {
                        SIZES.add(eql.size());
                    }
                    chain.proceed(task, action, request, listener);
                }
            });
        }
    }
}
