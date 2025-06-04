/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TransportListTasksAction;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockFactoryProvider;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.exception.ElasticsearchTimeoutException;
import org.elasticsearch.health.node.selection.HealthNode;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.plugin.TransportEsqlQueryAction;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

@TestLogging(value = "org.elasticsearch.xpack.esql.session:DEBUG", reason = "to better understand planning")
public abstract class AbstractEsqlIntegTestCase extends ESIntegTestCase {
    @After
    public void ensureExchangesAreReleased() throws Exception {
        for (String node : internalCluster().getNodeNames()) {
            TransportEsqlQueryAction esqlQueryAction = internalCluster().getInstance(TransportEsqlQueryAction.class, node);
            ExchangeService exchangeService = esqlQueryAction.exchangeService();
            assertBusy(() -> {
                if (exchangeService.lifecycleState() == Lifecycle.State.STARTED) {
                    assertTrue("Leftover exchanges " + exchangeService + " on node " + node, exchangeService.isEmpty());
                }
            });
        }
    }

    public void ensureBlocksReleased() {
        for (String node : internalCluster().getNodeNames()) {
            BlockFactoryProvider blockFactoryProvider = internalCluster().getInstance(BlockFactoryProvider.class, node);
            try {
                if (blockFactoryProvider.blockFactory() instanceof MockBlockFactory mockBlockFactory) {
                    assertBusy(() -> {
                        try {
                            mockBlockFactory.ensureAllBlocksAreReleased();
                        } catch (Exception e) {
                            throw new AssertionError(e);
                        }
                    });
                }
            } catch (Exception e) {
                throw new RuntimeException("failed to check mock factory", e);
            }
        }
        for (String node : internalCluster().getNodeNames()) {
            CircuitBreakerService breakerService = internalCluster().getInstance(CircuitBreakerService.class, node);
            CircuitBreaker reqBreaker = breakerService.getBreaker(CircuitBreaker.REQUEST);
            try {
                assertBusy(() -> {
                    logger.info(
                        "running tasks: {}",
                        client().admin()
                            .cluster()
                            .prepareListTasks()
                            .get()
                            .getTasks()
                            .stream()
                            .filter(
                                // Skip the tasks we that'd get in the way while debugging
                                t -> false == t.action().contains(TransportListTasksAction.TYPE.name())
                                    && false == t.action().contains(HealthNode.TASK_NAME)
                            )
                            .toList()
                    );
                    assertThat("Request breaker not reset to 0 on node: " + node, reqBreaker.getUsed(), equalTo(0L));
                });
            } catch (Exception e) {
                throw new RuntimeException("failed waiting for breakers to clear", e);
            }
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
                ),
                Setting.byteSizeSetting(
                    BlockFactory.LOCAL_BREAKER_OVER_RESERVED_SIZE_SETTING,
                    ByteSizeValue.ofBytes(randomIntBetween(0, 4096)),
                    Setting.Property.NodeScope
                ),
                Setting.byteSizeSetting(
                    BlockFactory.LOCAL_BREAKER_OVER_RESERVED_MAX_SIZE_SETTING,
                    ByteSizeValue.ofBytes(randomIntBetween(0, 16 * 1024)),
                    Setting.Property.NodeScope
                ),
                Setting.byteSizeSetting(
                    BlockFactory.MAX_BLOCK_PRIMITIVE_ARRAY_SIZE_SETTING,
                    ByteSizeValue.ofBytes(randomLongBetween(1, BlockFactory.DEFAULT_MAX_BLOCK_PRIMITIVE_ARRAY_SIZE.getBytes())),
                    Setting.Property.NodeScope
                )
            );
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), EsqlPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(EsqlPlugin.QUERY_ALLOW_PARTIAL_RESULTS.getKey(), false)
            .build();
    }

    protected void setRequestCircuitBreakerLimit(ByteSizeValue limit) {
        if (limit != null) {
            assertAcked(
                clusterAdmin().prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .setPersistentSettings(
                        Settings.builder().put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), limit).build()
                    )
            );
        } else {
            assertAcked(
                clusterAdmin().prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .setPersistentSettings(
                        Settings.builder().putNull(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey()).build()
                    )
            );
        }
    }

    protected final EsqlQueryResponse run(String esqlCommands) {
        return run(esqlCommands, randomPragmas());
    }

    protected final EsqlQueryResponse run(String esqlCommands, QueryPragmas pragmas) {
        return run(esqlCommands, pragmas, null);
    }

    protected EsqlQueryResponse run(String esqlCommands, QueryPragmas pragmas, QueryBuilder filter) {
        return run(esqlCommands, pragmas, filter, null);
    }

    protected EsqlQueryResponse run(String esqlCommands, QueryPragmas pragmas, QueryBuilder filter, Boolean allowPartialResults) {
        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest();
        request.query(esqlCommands);
        if (pragmas != null) {
            request.pragmas(pragmas);
        }
        if (filter != null) {
            request.filter(filter);
        }
        if (allowPartialResults != null) {
            request.allowPartialResults(allowPartialResults);
        }
        return run(request);
    }

    protected EsqlQueryResponse run(EsqlQueryRequest request) {
        try {
            return client().execute(EsqlQueryAction.INSTANCE, request).actionGet(30, TimeUnit.SECONDS);
        } catch (ElasticsearchTimeoutException e) {
            throw new AssertionError("timeout", e);
        }
    }

    protected static QueryPragmas randomPragmas() {
        Settings.Builder settings = Settings.builder();
        if (canUseQueryPragmas()) {
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
            if (randomBoolean()) {
                settings.put("max_concurrent_shards_per_node", randomIntBetween(1, 10));
            }
            if (randomBoolean()) {
                settings.put("node_level_reduction", randomBoolean());
            }
        }
        return new QueryPragmas(settings.build());
    }

    protected static boolean canUseQueryPragmas() {
        return Build.current().isSnapshot();
    }

    protected static void assertColumnNames(List<? extends ColumnInfo> actualColumns, List<String> expectedNames) {
        assertThat(actualColumns.stream().map(ColumnInfo::name).toList(), equalTo(expectedNames));
    }

    protected static void assertColumnTypes(List<? extends ColumnInfo> actualColumns, List<String> expectedTypes) {
        assertThat(actualColumns.stream().map(ColumnInfo::outputType).toList(), equalTo(expectedTypes));
    }

    protected static void assertValues(Iterator<Iterator<Object>> actualValues, Iterable<Iterable<Object>> expectedValues) {
        assertThat(getValuesList(actualValues), equalTo(getValuesList(expectedValues)));
    }

    protected static void assertValuesInAnyOrder(Iterator<Iterator<Object>> actualValues, Iterable<Iterable<Object>> expectedValues) {
        List<List<Object>> items = new ArrayList<>();
        for (Iterable<Object> outter : expectedValues) {
            var item = new ArrayList<>();
            for (var inner : outter) {
                item.add(inner);
            }
            items.add(item);
        }
        assertThat(getValuesList(actualValues), containsInAnyOrder(items.toArray()));
    }

    /**
    * v1: value to send to runQuery (can be null; null means use default value)
    * v2: whether to expect CCS Metadata in the response (cannot be null)
    * @return
    */
    public static Tuple<Boolean, Boolean> randomIncludeCCSMetadata() {
        return switch (randomIntBetween(1, 3)) {
            case 1 -> new Tuple<>(Boolean.TRUE, Boolean.TRUE);
            case 2 -> new Tuple<>(Boolean.FALSE, Boolean.FALSE);
            case 3 -> new Tuple<>(null, Boolean.FALSE);
            default -> throw new AssertionError("should not get here");
        };
    }
}
