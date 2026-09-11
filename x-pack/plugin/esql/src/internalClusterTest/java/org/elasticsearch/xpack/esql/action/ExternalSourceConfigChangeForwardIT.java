/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.datasources.datasource.DataSourceService;
import org.elasticsearch.xpack.esql.datasources.datasource.DeleteDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.PutDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceUsageAccumulator;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;
import org.elasticsearch.xpack.esql.execution.PlanExecutor;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.ToLongFunction;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Pins the #1865 once-only invariant that {@link ExternalSourceTelemetryIT} cannot see on one node.
 *
 * <p>{@code TransportMasterNodeAction} re-enters {@code doExecute} on the master. A listener wrapped
 * there would fire on both the forwarding coordinator and the master, and phone-home sums nodes.
 * Coord pre-check records locally and must not wrap the forwarded listener; success and CAS refusal
 * record on the master's acked listener only.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2, numClientNodes = 0, supportsDedicatedMasters = false)
public class ExternalSourceConfigChangeForwardIT extends AbstractEsqlIntegTestCase {

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);

    private static final Set<String> CREATED_DATASOURCES = Set.of("ds_fwd", "ds_fwd_max");

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(ExternalSourceTelemetryIT.TestDataSourcePlugin.class);
        plugins.add(TestTelemetryPlugin.class);
        return plugins;
    }

    @Before
    public void requireFeatureFlag() {
        assumeTrue("requires dataset-in-from-command capability", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());
    }

    @After
    public void cleanup() throws Exception {
        for (String name : CREATED_DATASOURCES) {
            try {
                client().execute(
                    DeleteDataSourceAction.INSTANCE,
                    new DeleteDataSourceAction.Request(TIMEOUT, TIMEOUT, new String[] { name })
                ).get(30, TimeUnit.SECONDS);
            } catch (ResourceNotFoundException ignored) {
                // never created by this method
            } catch (Exception e) {
                logger.warn("data source cleanup [{}] failed", name, e);
            }
        }
    }

    /**
     * A successful PUT forwarded from a non-master node must land once, on the master. Wrapping the
     * coord {@code doExecute} listener would also record on the coordinator.
     */
    public void testSuccessfulPutFromNonMasterCountsOnce() throws Exception {
        String master = internalCluster().getMasterName();
        String coord = nonMasterNode(master);
        assertThat(coord, not(equalTo(master)));

        long createdBefore = clusterTotal(
            a -> a.configChanges(DataSourceUsageAccumulator.KIND_DATASOURCE, DataSourceUsageAccumulator.OP_CREATED)
        );
        resetAllMeters();
        assertAcked(
            client(coord).execute(
                PutDataSourceAction.INSTANCE,
                new PutDataSourceAction.Request(TIMEOUT, TIMEOUT, "ds_fwd", "test", null, new HashMap<>())
            )
        );
        collectAllMeters();

        assertThat(
            "phone-home created once across the cluster",
            clusterTotal(a -> a.configChanges(DataSourceUsageAccumulator.KIND_DATASOURCE, DataSourceUsageAccumulator.OP_CREATED))
                - createdBefore,
            equalTo(1L)
        );
        assertThat("APM created once across the cluster", apmConfigChanges("datasource", "created"), equalTo(1L));
        assertThat("master recorded the created event", apmConfigChangesOn(master, "datasource", "created"), equalTo(1L));
        assertThat("coordinator must not also record created", apmConfigChangesOn(coord, "datasource", "created"), equalTo(0L));
    }

    /**
     * Max-count is thrown from the CAS task after the request has been forwarded. That is the
     * {@code recordingListener.onFailure} path; a wrapped coord listener would double-count it.
     */
    public void testMaxCountFromNonMasterCountsOnce() throws Exception {
        String master = internalCluster().getMasterName();
        String coord = nonMasterNode(master);
        long rejectedBefore = clusterTotal(
            a -> a.configChanges(DataSourceUsageAccumulator.KIND_DATASOURCE, DataSourceUsageAccumulator.OP_REJECTED)
        );
        resetAllMeters();
        assertAcked(
            clusterAdmin().prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .setPersistentSettings(Settings.builder().put(DataSourceService.MAX_DATA_SOURCES_COUNT_SETTING.getKey(), 0).build())
        );
        try {
            expectThrows(
                Exception.class,
                () -> client(coord).execute(
                    PutDataSourceAction.INSTANCE,
                    new PutDataSourceAction.Request(TIMEOUT, TIMEOUT, "ds_fwd_max", "test", null, new HashMap<>())
                ).actionGet(TIMEOUT)
            );
            collectAllMeters();
            assertThat(
                "phone-home rejected once across the cluster",
                clusterTotal(a -> a.configChanges(DataSourceUsageAccumulator.KIND_DATASOURCE, DataSourceUsageAccumulator.OP_REJECTED))
                    - rejectedBefore,
                equalTo(1L)
            );
            assertThat("APM rejected once across the cluster", apmConfigChanges("datasource", "rejected"), equalTo(1L));
            assertThat("master recorded the max_count rejection", apmConfigChangesOn(master, "datasource", "rejected"), equalTo(1L));
            assertThat("coordinator must not also record rejected", apmConfigChangesOn(coord, "datasource", "rejected"), equalTo(0L));
            assertThat(
                "reason is max_count",
                counters(ExternalSourceMetrics.CONFIG_CHANGES_TOTAL).stream()
                    .anyMatch(
                        m -> "rejected".equals(m.attributes().get(ExternalSourceMetrics.OP_ATTRIBUTE))
                            && "max_count".equals(m.attributes().get(ExternalSourceMetrics.REASON_ATTRIBUTE))
                    ),
                equalTo(true)
            );
        } finally {
            assertAcked(
                clusterAdmin().prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .setPersistentSettings(Settings.builder().putNull(DataSourceService.MAX_DATA_SOURCES_COUNT_SETTING.getKey()).build())
            );
        }
    }

    private String nonMasterNode(String master) {
        for (String node : internalCluster().getNodeNames()) {
            if (node.equals(master) == false) {
                return node;
            }
        }
        throw new AssertionError("expected a non-master node beside " + master);
    }

    private List<TestTelemetryPlugin> telemetryPlugins(String node) {
        return internalCluster().getInstance(PluginsService.class, node).filterPlugins(TestTelemetryPlugin.class).toList();
    }

    private void resetAllMeters() {
        for (String node : internalCluster().getNodeNames()) {
            for (TestTelemetryPlugin plugin : telemetryPlugins(node)) {
                plugin.resetMeter();
            }
        }
    }

    private void collectAllMeters() {
        for (String node : internalCluster().getNodeNames()) {
            for (TestTelemetryPlugin plugin : telemetryPlugins(node)) {
                plugin.collect();
            }
        }
    }

    private long apmConfigChanges(String kind, String op) {
        return counters(ExternalSourceMetrics.CONFIG_CHANGES_TOTAL).stream()
            .filter(m -> kind.equals(m.attributes().get(ExternalSourceMetrics.KIND_ATTRIBUTE)))
            .filter(m -> op.equals(m.attributes().get(ExternalSourceMetrics.OP_ATTRIBUTE)))
            .mapToLong(Measurement::getLong)
            .sum();
    }

    private long apmConfigChangesOn(String node, String kind, String op) {
        List<Measurement> all = new ArrayList<>();
        for (TestTelemetryPlugin plugin : telemetryPlugins(node)) {
            all.addAll(plugin.getLongCounterMeasurement(ExternalSourceMetrics.CONFIG_CHANGES_TOTAL));
        }
        return all.stream()
            .filter(m -> kind.equals(m.attributes().get(ExternalSourceMetrics.KIND_ATTRIBUTE)))
            .filter(m -> op.equals(m.attributes().get(ExternalSourceMetrics.OP_ATTRIBUTE)))
            .mapToLong(Measurement::getLong)
            .sum();
    }

    private List<Measurement> counters(String name) {
        List<Measurement> all = new ArrayList<>();
        for (String node : internalCluster().getNodeNames()) {
            for (TestTelemetryPlugin plugin : telemetryPlugins(node)) {
                all.addAll(plugin.getLongCounterMeasurement(name));
            }
        }
        return all;
    }

    private long clusterTotal(ToLongFunction<DataSourceUsageAccumulator> fn) {
        long total = 0;
        boolean found = false;
        for (String node : internalCluster().getNodeNames()) {
            PlanExecutor planExecutor = internalCluster().getInstance(PlanExecutor.class, node);
            if (planExecutor.dataSourceModule() == null) {
                continue;
            }
            DataSourceUsageAccumulator acc = planExecutor.dataSourceModule().externalSourceMetrics().usageAccumulator();
            if (acc == null) {
                continue;
            }
            found = true;
            total += fn.applyAsLong(acc);
        }
        assertTrue("No node has a DataSourceModule with a non-null usageAccumulator", found);
        return total;
    }
}
