/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
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

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        if (randomBoolean()) {
            Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
            if (randomBoolean()) {
                settings.put(ExchangeService.INACTIVE_TIMEOUT_SETTING.getKey(), TimeValue.timeValueMillis(between(1, 100)));
            }
            return settings.build();
        } else {
            return super.nodeSettings(nodeOrdinal, otherSettings);
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), EsqlPlugin.class);
    }

    protected static EsqlQueryResponse run(String esqlCommands) {
        return new EsqlQueryRequestBuilder(client(), EsqlQueryAction.INSTANCE).query(esqlCommands).pragmas(randomPragmas()).get();
    }

    protected static EsqlQueryResponse run(String esqlCommands, QueryPragmas pragmas) {
        return new EsqlQueryRequestBuilder(client(), EsqlQueryAction.INSTANCE).query(esqlCommands).pragmas(pragmas).get();
    }

    protected static EsqlQueryResponse run(String esqlCommands, QueryPragmas pragmas, QueryBuilder filter) {
        return new EsqlQueryRequestBuilder(client(), EsqlQueryAction.INSTANCE).query(esqlCommands).pragmas(pragmas).filter(filter).get();
    }

    protected static QueryPragmas randomPragmas() {
        Settings.Builder settings = Settings.builder();
        // pragmas are only enabled on snapshot builds
        if (Build.CURRENT.isSnapshot()) {
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
        }
        return new QueryPragmas(settings.build());
    }
}
