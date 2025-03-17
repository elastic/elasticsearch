/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.FailingFieldPlugin;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

/**
 * Make sure the failures on the data node come back as failures over the wire.
 */
@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class EsqlNodeFailureIT extends AbstractEsqlIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(FailingFieldPlugin.class);
        plugins.add(InternalExchangePlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings settings = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(ExchangeService.INACTIVE_SINKS_INTERVAL_SETTING, TimeValue.timeValueMillis(between(3000, 4000)))
            .build();
        logger.info("settings {}", settings);
        return settings;
    }

    public Set<String> populateIndices() throws Exception {
        XContentBuilder mapping = JsonXContent.contentBuilder().startObject();
        mapping.startObject("runtime");
        {
            mapping.startObject("fail_me");
            {
                mapping.field("type", "long");
                mapping.startObject("script").field("source", "").field("lang", "failing_field").endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();
        client().admin().indices().prepareCreate("fail").setMapping(mapping.endObject()).get();
        int okCount = between(1, 50);
        Set<String> okIds = new HashSet<>();
        List<IndexRequestBuilder> docs = new ArrayList<>(okCount);
        for (int d = 0; d < okCount; d++) {
            String id = "ok-" + d;
            okIds.add(id);
            docs.add(client().prepareIndex("ok").setId(id).setSource("foo", d));
        }
        int failCount = between(1, 50);
        for (int d = 0; d < failCount; d++) {
            docs.add(client().prepareIndex("fail").setId("fail-" + d).setSource("foo", d));
        }
        indexRandom(true, docs);
        return okIds;
    }

    /**
     * Use a runtime field that fails when loading field values to fail the entire query.
     */
    public void testFailureLoadingFields() throws Exception {
        populateIndices();
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> run("FROM fail,ok | LIMIT 100").close());
        assertThat(e.getMessage(), equalTo("Accessing failing field"));
    }

    public void testPartialResults() throws Exception {
        Set<String> okIds = populateIndices();
        {
            EsqlQueryRequest request = new EsqlQueryRequest();
            request.query("FROM fail,ok | LIMIT 100");
            request.allowPartialResults(true);
            request.pragmas(randomPragmas());
            try (EsqlQueryResponse resp = run(request)) {
                assertTrue(resp.isPartial());
                List<List<Object>> rows = EsqlTestUtils.getValuesList(resp);
                assertThat(rows.size(), lessThanOrEqualTo(okIds.size()));
            }
        }
        {
            EsqlQueryRequest request = new EsqlQueryRequest();
            request.query("FROM fail,ok METADATA _id | KEEP _id, fail_me | LIMIT 100");
            request.allowPartialResults(true);
            request.pragmas(randomPragmas());
            try (EsqlQueryResponse resp = run(request)) {
                assertTrue(resp.isPartial());
                List<List<Object>> rows = EsqlTestUtils.getValuesList(resp);
                assertThat(rows.size(), lessThanOrEqualTo(okIds.size()));
                Set<String> actualIds = new HashSet<>();
                for (List<Object> row : rows) {
                    assertThat(row.size(), equalTo(2));
                    String id = (String) row.getFirst();
                    assertThat(id, in(okIds));
                    assertTrue(actualIds.add(id));
                }
                EsqlExecutionInfo.Cluster localInfo = resp.getExecutionInfo().getCluster(RemoteClusterService.LOCAL_CLUSTER_GROUP_KEY);
                assertThat(localInfo.getFailures(), not(empty()));
                assertThat(localInfo.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.PARTIAL));
                assertThat(localInfo.getFailures().get(0).reason(), containsString("Accessing failing field"));
            }
        }
    }

    public void testDefaultPartialResults() throws Exception {
        Set<String> okIds = populateIndices();
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS)
                .setPersistentSettings(Settings.builder().put(EsqlPlugin.QUERY_ALLOW_PARTIAL_RESULTS.getKey(), true))
        );
        try {
            // allow_partial_results = default
            {
                EsqlQueryRequest request = new EsqlQueryRequest();
                request.query("FROM fail,ok | LIMIT 100");
                request.pragmas(randomPragmas());
                if (randomBoolean()) {
                    request.allowPartialResults(true);
                }
                try (EsqlQueryResponse resp = run(request)) {
                    assertTrue(resp.isPartial());
                    List<List<Object>> rows = EsqlTestUtils.getValuesList(resp);
                    assertThat(rows.size(), lessThanOrEqualTo(okIds.size()));
                }
            }
            // allow_partial_results = false
            {
                EsqlQueryRequest request = new EsqlQueryRequest();
                request.query("FROM fail,ok | LIMIT 100");
                request.pragmas(randomPragmas());
                request.allowPartialResults(false);
                IllegalStateException e = expectThrows(IllegalStateException.class, () -> run(request).close());
                assertThat(e.getMessage(), equalTo("Accessing failing field"));
            }
        } finally {
            assertAcked(
                client().admin()
                    .cluster()
                    .prepareUpdateSettings(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS)
                    .setPersistentSettings(Settings.builder().putNull(EsqlPlugin.QUERY_ALLOW_PARTIAL_RESULTS.getKey()))
            );
        }
    }
}
