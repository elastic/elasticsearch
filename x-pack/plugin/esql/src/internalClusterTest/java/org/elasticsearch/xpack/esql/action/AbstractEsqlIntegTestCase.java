/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.plugin.TransportEsqlQueryAction;
import org.junit.After;

import java.util.Collection;
import java.util.List;

@TestLogging(value = "org.elasticsearch.xpack.esql.session:DEBUG", reason = "to better understand planning")
public abstract class AbstractEsqlIntegTestCase extends ESIntegTestCase {

    @After
    public void ensureExchangesAreReleased() throws Exception {
        for (String node : internalCluster().getNodeNames()) {
            TransportEsqlQueryAction esqlQueryAction = internalCluster().getInstance(TransportEsqlQueryAction.class, node);
            ExchangeService exchangeService = esqlQueryAction.exchangeService();
            assertBusy(() -> assertTrue("Leftover exchanges " + exchangeService + " on node " + node, exchangeService.isEmpty()));
        }
    }

    public static class InternalExchangePlugin extends Plugin {
        @Override
        public List<Setting<?>> getSettings() {
            return List.of(
                Setting.timeSetting(
                    ExchangeService.INACTIVE_SINKS_INTERVAL_SETTING,
                    TimeValue.timeValueSeconds(5),
                    Setting.Property.NodeScope
                )
            );
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), EsqlPlugin.class);
    }

    protected EsqlQueryResponse run(String esqlCommands) {
        return run(esqlCommands, randomPragmas());
    }

    protected EsqlQueryResponse run(String esqlCommands, QueryPragmas pragmas) {
        return run(esqlCommands, pragmas, null);
    }

    protected EsqlQueryResponse run(String esqlCommands, QueryPragmas pragmas, QueryBuilder filter) {
        EsqlQueryRequest request = new EsqlQueryRequest();
        request.query(esqlCommands);
        request.pragmas(pragmas);
        if (filter != null) {
            request.filter(filter);
        }
        return run(request);
    }

    protected EsqlQueryResponse run(EsqlQueryRequest request) {
        return client().execute(EsqlQueryAction.INSTANCE, request).actionGet();
    }

    protected static QueryPragmas randomPragmas() {
        Settings.Builder settings = Settings.builder();
        // pragmas are only enabled on snapshot builds
        if (Build.current().isSnapshot()) {
            if (randomBoolean()) {
                settings.put("task_concurrency", randomLongBetween(1, 10));
            }
            if (randomBoolean()) {
                final int exchangeBufferSize;
                if (frequently()) {
                    exchangeBufferSize = randomIntBetween(1, 10);
                } else {
                    exchangeBufferSize = randomIntBetween(5, 5000);
                }
                settings.put("exchange_buffer_size", exchangeBufferSize);
            }
            if (randomBoolean()) {
                settings.put("exchange_concurrent_clients", randomIntBetween(1, 10));
            }
            if (randomBoolean()) {
                settings.put("data_partitioning", randomFrom("shard", "segment", "doc"));
            }
            if (randomBoolean()) {
                final int pageSize = switch (between(0, 2)) {
                    case 0 -> between(1, 16);
                    case 1 -> between(1, 1024);
                    case 2 -> between(64, 10 * 1024);
                    default -> throw new AssertionError("unknown");
                };
                settings.put("page_size", pageSize);
            }
        }
        return new QueryPragmas(settings.build());
    }

}
