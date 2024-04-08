/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class CanMatchIT extends AbstractEsqlIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockTransportService.TestPlugin.class);
    }

    /**
     * Make sure that we don't send data-node requests to the target shards which won't match the query
     */
    public void testCanMatch() {
        ElasticsearchAssertions.assertAcked(
            client().admin()
                .indices()
                .prepareCreate("events_2022")
                .setMapping("@timestamp", "type=date,format=yyyy-MM-dd", "uid", "type=keyword")
        );
        client().prepareBulk("events_2022")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .add(new IndexRequest().source("@timestamp", "2022-02-15", "uid", "u1"))
            .add(new IndexRequest().source("@timestamp", "2022-05-02", "uid", "u1"))
            .add(new IndexRequest().source("@timestamp", "2022-12-15", "uid", "u1"))
            .get();
        ElasticsearchAssertions.assertAcked(
            client().admin().indices().prepareCreate("events_2023").setMapping("@timestamp", "type=date", "uid", "type=keyword")
        );
        client().prepareBulk("events_2023")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .add(new IndexRequest().source("@timestamp", "2023-01-15", "uid", "u2"))
            .add(new IndexRequest().source("@timestamp", "2023-02-01", "uid", "u2"))
            .add(new IndexRequest().source("@timestamp", "2023-02-11", "uid", "u1"))
            .add(new IndexRequest().source("@timestamp", "2023-03-25", "uid", "u1"))
            .get();
        try {
            Set<String> queriedIndices = ConcurrentCollections.newConcurrentSet();
            for (TransportService ts : internalCluster().getInstances(TransportService.class)) {
                MockTransportService transportService = (MockTransportService) ts;
                transportService.addRequestHandlingBehavior(ComputeService.DATA_ACTION_NAME, (handler, request, channel, task) -> {
                    DataNodeRequest dataNodeRequest = (DataNodeRequest) request;
                    for (ShardId shardId : dataNodeRequest.shardIds()) {
                        queriedIndices.add(shardId.getIndexName());
                    }
                    handler.messageReceived(request, channel, task);
                });
            }
            try (EsqlQueryResponse resp = run("from events_*", randomPragmas(), new RangeQueryBuilder("@timestamp").gte("2023-01-01"))) {
                assertThat(getValuesList(resp), hasSize(4));
                assertThat(queriedIndices, equalTo(Set.of("events_2023")));
                queriedIndices.clear();
            }

            try (EsqlQueryResponse resp = run("from events_*", randomPragmas(), new RangeQueryBuilder("@timestamp").lt("2023-01-01"))) {
                assertThat(getValuesList(resp), hasSize(3));
                assertThat(queriedIndices, equalTo(Set.of("events_2022")));
                queriedIndices.clear();
            }

            try (
                EsqlQueryResponse resp = run(
                    "from events_*",
                    randomPragmas(),
                    new RangeQueryBuilder("@timestamp").gt("2022-01-01").lt("2023-12-31")
                )
            ) {
                assertThat(getValuesList(resp), hasSize(7));
                assertThat(queriedIndices, equalTo(Set.of("events_2022", "events_2023")));
                queriedIndices.clear();
            }

            try (
                EsqlQueryResponse resp = run(
                    "from events_*",
                    randomPragmas(),
                    new RangeQueryBuilder("@timestamp").gt("2021-01-01").lt("2021-12-31")
                )
            ) {
                assertThat(getValuesList(resp), hasSize(0));
                assertThat(queriedIndices, empty());
                queriedIndices.clear();
            }
        } finally {
            for (TransportService ts : internalCluster().getInstances(TransportService.class)) {
                ((MockTransportService) ts).clearAllRules();
            }
        }
    }

    public void testAliasFilters() {
        ElasticsearchAssertions.assertAcked(
            client().admin()
                .indices()
                .prepareCreate("employees")
                .setMapping("emp_no", "type=long", "dept", "type=keyword", "hired", "type=date,format=yyyy-MM-dd", "salary", "type=double")
        );
        client().prepareBulk("employees")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .add(new IndexRequest().source("emp_no", 101, "dept", "engineering", "hired", "2012-02-05", "salary", 20))
            .add(new IndexRequest().source("emp_no", 102, "dept", "sales", "hired", "2012-03-15", "salary", 25))
            .add(new IndexRequest().source("emp_no", 103, "dept", "engineering", "hired", "2012-03-27", "salary", 22))
            .add(new IndexRequest().source("emp_no", 104, "dept", "engineering", "hired", "2012-04-20", "salary", 39.6))
            .add(new IndexRequest().source("emp_no", 105, "dept", "engineering", "hired", "2012-06-30", "salary", 25))
            .add(new IndexRequest().source("emp_no", 106, "dept", "sales", "hired", "2012-08-09", "salary", 30.1))
            .get();

        ElasticsearchAssertions.assertAcked(
            client().admin()
                .indices()
                .prepareAliases()
                .addAlias("employees", "engineers", new MatchQueryBuilder("dept", "engineering"))
                .addAlias("employees", "sales", new MatchQueryBuilder("dept", "sales"))
        );
        // employees index
        try (var resp = run("from employees | stats count(emp_no)", randomPragmas())) {
            assertThat(getValuesList(resp).get(0), equalTo(List.of(6L)));
        }
        try (var resp = run("from employees | stats avg(salary)", randomPragmas())) {
            assertThat(getValuesList(resp).get(0), equalTo(List.of(26.95d)));
        }

        try (var resp = run("from employees | stats count(emp_no)", randomPragmas(), new RangeQueryBuilder("hired").lt("2012-04-30"))) {
            assertThat(getValuesList(resp).get(0), equalTo(List.of(4L)));
        }
        try (var resp = run("from employees | stats avg(salary)", randomPragmas(), new RangeQueryBuilder("hired").lt("2012-04-30"))) {
            assertThat(getValuesList(resp).get(0), equalTo(List.of(26.65d)));
        }

        // match both employees index and engineers alias -> employees
        try (var resp = run("from e* | stats count(emp_no)", randomPragmas())) {
            assertThat(getValuesList(resp).get(0), equalTo(List.of(6L)));
        }
        try (var resp = run("from employees | stats avg(salary)", randomPragmas())) {
            assertThat(getValuesList(resp).get(0), equalTo(List.of(26.95d)));
        }

        try (var resp = run("from e* | stats count(emp_no)", randomPragmas(), new RangeQueryBuilder("hired").lt("2012-04-30"))) {
            assertThat(getValuesList(resp).get(0), equalTo(List.of(4L)));
        }
        try (var resp = run("from e* | stats avg(salary)", randomPragmas(), new RangeQueryBuilder("hired").lt("2012-04-30"))) {
            assertThat(getValuesList(resp).get(0), equalTo(List.of(26.65d)));
        }

        // engineers alias
        try (var resp = run("from engineer* | stats count(emp_no)", randomPragmas())) {
            assertThat(getValuesList(resp).get(0), equalTo(List.of(4L)));
        }
        try (var resp = run("from engineer* | stats avg(salary)", randomPragmas())) {
            assertThat(getValuesList(resp).get(0), equalTo(List.of(26.65d)));
        }

        try (var resp = run("from engineer* | stats count(emp_no)", randomPragmas(), new RangeQueryBuilder("hired").lt("2012-04-30"))) {
            assertThat(getValuesList(resp).get(0), equalTo(List.of(3L)));
        }
        try (var resp = run("from engineer* | stats avg(salary)", randomPragmas(), new RangeQueryBuilder("hired").lt("2012-04-30"))) {
            assertThat(getValuesList(resp).get(0), equalTo(List.of(27.2d)));
        }

        // sales alias
        try (var resp = run("from sales | stats count(emp_no)", randomPragmas())) {
            assertThat(getValuesList(resp).get(0), equalTo(List.of(2L)));
        }
        try (var resp = run("from sales | stats avg(salary)", randomPragmas())) {
            assertThat(getValuesList(resp).get(0), equalTo(List.of(27.55d)));
        }

        try (var resp = run("from sales | stats count(emp_no)", randomPragmas(), new RangeQueryBuilder("hired").lt("2012-04-30"))) {
            assertThat(getValuesList(resp).get(0), equalTo(List.of(1L)));
        }
        try (var resp = run("from sales | stats avg(salary)", randomPragmas(), new RangeQueryBuilder("hired").lt("2012-04-30"))) {
            assertThat(getValuesList(resp).get(0), equalTo(List.of(25.0d)));
        }
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/103749")
    public void testFailOnUnavailableShards() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        String logsOnlyNode = internalCluster().startDataOnlyNode();
        ElasticsearchAssertions.assertAcked(
            client().admin()
                .indices()
                .prepareCreate("events")
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put("index.routing.allocation.exclude._name", logsOnlyNode)
                )
                .setMapping("timestamp", "type=long", "message", "type=keyword")
        );
        client().prepareBulk("events")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .add(new IndexRequest().source("timestamp", 1, "message", "a"))
            .add(new IndexRequest().source("timestamp", 2, "message", "b"))
            .add(new IndexRequest().source("timestamp", 3, "message", "c"))
            .get();
        ElasticsearchAssertions.assertAcked(
            client().admin()
                .indices()
                .prepareCreate("logs")
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put("index.routing.allocation.include._name", logsOnlyNode)
                )
                .setMapping("timestamp", "type=long", "message", "type=keyword")
        );
        client().prepareBulk("logs")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .add(new IndexRequest().source("timestamp", 10, "message", "aa"))
            .add(new IndexRequest().source("timestamp", 11, "message", "bb"))
            .get();
        try (EsqlQueryResponse resp = run("from events,logs | KEEP timestamp,message")) {
            assertThat(getValuesList(resp), hasSize(5));
            internalCluster().stopNode(logsOnlyNode);
            ensureClusterSizeConsistency();
            Exception error = expectThrows(Exception.class, () -> run("from events,logs | KEEP timestamp,message"));
            assertThat(error.getMessage(), containsString("no shard copies found"));
        }
    }
}
