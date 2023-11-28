/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class CrossClustersQueryIT extends AbstractMultiClustersTestCase {
    private static final String REMOTE_CLUSTER = "cluster-a";

    @Override
    protected Collection<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        List<Class<? extends Plugin>> plugins = new ArrayList<>();
        plugins.addAll(super.nodePlugins(clusterAlias));
        plugins.add(EsqlPlugin.class);
        plugins.add(InternalExchangePlugin.class);
        return CollectionUtils.appendToCopy(super.nodePlugins(clusterAlias), EsqlPlugin.class);
    }

    public static class InternalExchangePlugin extends Plugin {
        @Override
        public List<Setting<?>> getSettings() {
            return List.of(
                Setting.timeSetting(
                    ExchangeService.INACTIVE_SINKS_INTERVAL_SETTING,
                    TimeValue.timeValueSeconds(30),
                    Setting.Property.NodeScope
                )
            );
        }
    }

    public void testUnsupported() {
        int numDocs = between(1, 10);
        for (String cluster : List.of(LOCAL_CLUSTER, REMOTE_CLUSTER)) {
            Client client = client(cluster);
            assertAcked(
                client.admin()
                    .indices()
                    .prepareCreate("events")
                    .setSettings(Settings.builder().put("index.number_of_shards", randomIntBetween(1, 5)))
                    .setMapping("tag", "type=keyword", "v", "type=long")
            );
            for (int i = 0; i < numDocs; i++) {
                client.prepareIndex("events").setSource("tag", cluster, "v", i).get();
            }
            client.admin().indices().prepareRefresh("events").get();
        }
        var emptyQueries = List.of(
            "from *:* | LIMIT 0",
            "from *,*:* | LIMIT 0",
            "from *:events* | LIMIT 0",
            "from events,*:events* | LIMIT 0"
        );
        for (String q : emptyQueries) {
            try (EsqlQueryResponse resp = runQuery(q)) {
                assertThat(resp.columns(), hasSize(2));
                assertFalse(resp.values().hasNext());
            }
        }
        var remotePatterns = List.of("*:*", "*, *:*", "*:events*", "events, *:events*");
        for (String pattern : remotePatterns) {
            var query = "FROM " + pattern + " | LIMIT " + between(1, 100);
            IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> runQuery(query).close());
            assertThat(error.getMessage(), equalTo("ES|QL does not yet support querying remote indices [" + pattern + "]"));
        }
        int limit = between(1, numDocs);
        var localQueries = List.of("from events* | LIMIT " + limit, "from * | LIMIT " + limit);
        for (String q : localQueries) {
            try (EsqlQueryResponse resp = runQuery(q)) {
                assertThat(resp.columns(), hasSize(2));
                int rows = 0;
                Iterator<Iterator<Object>> values = resp.values();
                while (values.hasNext()) {
                    values.next();
                    ++rows;
                }
                assertThat(rows, equalTo(limit));
            }
        }
    }

    protected EsqlQueryResponse runQuery(String query) {
        logger.info("--> query [{}]", query);
        EsqlQueryRequest request = new EsqlQueryRequest();
        request.query(query);
        request.pragmas(AbstractEsqlIntegTestCase.randomPragmas());
        return client(LOCAL_CLUSTER).execute(EsqlQueryAction.INSTANCE, request).actionGet(30, TimeUnit.SECONDS);
    }
}
