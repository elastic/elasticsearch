/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging.activity;

import org.elasticsearch.action.search.SearchCaptureContext;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;

/** Proof-of-concept plugin loaded through SPI and invoked by the real search transport action. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class QuerySinkProviderIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(CapturePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(QueryLogger.QUERY_LOGGER_ENABLED.getKey(), false)
            .build();
    }

    public void testPluginSinkReceivesSearchesWithQueryLoggingDisabled() {
        var plugin = internalCluster().getInstance(PluginsService.class).filterPlugins(CapturePlugin.class).findFirst().orElseThrow();
        assertEquals(1, plugin.providerCalls.get());
        createIndex("test");
        ensureGreen("test");

        assertResponse(prepareSearch("test"), response -> assertEquals(0, response.getHits().getHits().length));
        assertThat(plugin.captures, contains("dsl:success"));

        expectThrows(IndexNotFoundException.class, () -> prepareSearch("missing-index").get());
        assertThat(plugin.captures, contains("dsl:success", "dsl:failure"));

        plugin.enabled = false;
        assertResponse(prepareSearch("test"), response -> assertEquals(0, response.getHits().getHits().length));
        assertThat(plugin.captures, hasSize(2));
        assertEquals(1, plugin.providerCalls.get());
    }

    /** Owns the sink state so its enablement is independent of the built-in query logger. */
    public static class CapturePlugin extends Plugin {
        private final List<String> captures = new CopyOnWriteArrayList<>();
        private final AtomicInteger providerCalls = new AtomicInteger();
        private volatile boolean enabled = true;
    }

    /** The plugin constructor also confines this SPI provider to nodes with CapturePlugin installed. */
    public static class CaptureSinkProvider implements QuerySinkProvider {
        private final CapturePlugin plugin;

        public CaptureSinkProvider(CapturePlugin plugin) {
            this.plugin = plugin;
        }

        @Override
        public List<QuerySink<QueryCaptureContext>> getQuerySinks() {
            plugin.providerCalls.incrementAndGet();
            return List.of(new QuerySink<>(() -> "proof-of-concept query capture") {
                @Override
                public boolean isEnabled() {
                    return plugin.enabled;
                }

                @Override
                public void accept(QueryCaptureContext context) {
                    if (context instanceof SearchCaptureContext) {
                        // Retain only copied metadata, never the borrowed context or response.
                        plugin.captures.add(context.getType() + (context.isSuccess() ? ":success" : ":failure"));
                    }
                }
            });
        }
    }
}
