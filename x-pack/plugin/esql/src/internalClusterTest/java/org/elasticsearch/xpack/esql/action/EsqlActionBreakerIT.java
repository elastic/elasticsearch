/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Makes sure that the circuit breaker is "plugged in" to ESQL by configuring an
 * unreasonably small breaker and tripping it.
 */
@ESIntegTestCase.ClusterScope(scope = SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class EsqlActionBreakerIT extends AbstractEsqlIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(InternalExchangePlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "1kb")
            /*
             * Force standard settings for the request breaker or we may not break at all.
             * Without this we can randomly decide to use the `noop` breaker for request
             * and it won't break.....
             */
            .put(
                HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING.getKey(),
                HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING.getDefault(Settings.EMPTY)
            )
            .put(
                HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getKey(),
                HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getDefault(Settings.EMPTY)
            )
            .put(ExchangeService.INACTIVE_SINKS_INTERVAL_SETTING, TimeValue.timeValueMillis(between(500, 2000)))
            .build();
    }

    public void testBreaker() {
        for (int i = 0; i < 5000; i++) {
            IndexResponse response = client().prepareIndex("test").setId(Integer.toString(i)).setSource("foo", i, "bar", i * 2).get();
            if (response.getResult() != DocWriteResponse.Result.CREATED) {
                fail("failure: " + response);
            }
        }
        client().admin().indices().prepareRefresh("test").get();
        ensureYellow("test");
        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> run("from test | stats avg(foo) by bar", QueryPragmas.EMPTY)
        );
        logger.info("expected error", e);
        if (e instanceof CircuitBreakingException) {
            // The failure occurred before starting the drivers
            assertThat(e.getMessage(), containsString("Data too large"));
        } else {
            // The failure occurred after starting the drivers
            assertThat(e.getMessage(), containsString("Compute engine failure"));
            assertThat(e.getMessage(), containsString("Data too large"));
            assertThat(e.getCause(), instanceOf(CircuitBreakingException.class));
            assertThat(e.getCause().getMessage(), containsString("Data too large"));
        }
    }
}
