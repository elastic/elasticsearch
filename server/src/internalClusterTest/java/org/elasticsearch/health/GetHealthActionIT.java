/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.metrics.Counters;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.health.node.HealthInfo;
import org.elasticsearch.health.stats.HealthApiStatsAction;
import org.elasticsearch.plugins.HealthPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

import static org.elasticsearch.common.util.CollectionUtils.appendToCopy;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class GetHealthActionIT extends ESIntegTestCase {

    private static final String ILM_INDICATOR_NAME = "ilm";
    private static final String SLM_INDICATOR_NAME = "slm";
    private static final String INSTANCE_HAS_MASTER_INDICATOR_NAME = "instance_has_master";
    private static final String NONEXISTENT_INDICATOR_NAME = "test_nonexistent_indicator";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return appendToCopy(super.nodePlugins(), TestHealthPlugin.class);
    }

    public static final Setting<HealthStatus> ILM_HEALTH_STATUS_SETTING = new Setting<>(
        "test.health.status.ilm",
        "GREEN",
        HealthStatus::valueOf,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<HealthStatus> SLM_HEALTH_STATUS_SETTING = new Setting<>(
        "test.health.status.slm",
        "GREEN",
        HealthStatus::valueOf,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<HealthStatus> CLUSTER_COORDINATION_HEALTH_STATUS_SETTING = new Setting<>(
        "test.health.status.cluster.coordination",
        "GREEN",
        HealthStatus::valueOf,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final class TestHealthPlugin extends Plugin implements HealthPlugin {

        private final List<HealthIndicatorService> healthIndicatorServices = new ArrayList<>();

        @Override
        public List<Setting<?>> getSettings() {
            return List.of(ILM_HEALTH_STATUS_SETTING, SLM_HEALTH_STATUS_SETTING, CLUSTER_COORDINATION_HEALTH_STATUS_SETTING);
        }

        @Override
        public Collection<?> createComponents(PluginServices services) {
            healthIndicatorServices.add(new IlmHealthIndicatorService(services.clusterService()));
            healthIndicatorServices.add(new SlmHealthIndicatorService(services.clusterService()));
            healthIndicatorServices.add(new ClusterCoordinationHealthIndicatorService(services.clusterService()));
            return new ArrayList<>(healthIndicatorServices);
        }

        @Override
        public Collection<HealthIndicatorService> getHealthIndicatorServices() {
            return healthIndicatorServices;
        }
    }

    /**
     * This indicator pulls its status from the statusSetting Setting.
     */
    public static class TestHealthIndicatorService implements HealthIndicatorService {

        private final ClusterService clusterService;
        private final String indicatorName;
        private final Setting<HealthStatus> statusSetting;

        public TestHealthIndicatorService(ClusterService clusterService, String indicatorName, Setting<HealthStatus> statusSetting) {
            this.clusterService = clusterService;
            this.indicatorName = indicatorName;
            this.statusSetting = statusSetting;
        }

        @Override
        public String name() {
            return indicatorName;
        }

        @Override
        public HealthIndicatorResult calculate(boolean verbose, int maxAffectedResourcesCount, HealthInfo healthInfo) {
            var status = clusterService.getClusterSettings().get(statusSetting);
            return createIndicator(
                status,
                "Health is set to [" + status + "] by test plugin",
                new SimpleHealthIndicatorDetails(Map.of("verbose", verbose)),
                Collections.emptyList(),
                Collections.emptyList()
            );
        }
    }

    public static final class IlmHealthIndicatorService extends TestHealthIndicatorService {
        public IlmHealthIndicatorService(ClusterService clusterService) {
            super(clusterService, ILM_INDICATOR_NAME, ILM_HEALTH_STATUS_SETTING);
        }
    }

    public static final class SlmHealthIndicatorService extends TestHealthIndicatorService {
        public SlmHealthIndicatorService(ClusterService clusterService) {
            super(clusterService, SLM_INDICATOR_NAME, SLM_HEALTH_STATUS_SETTING);
        }
    }

    public static final class ClusterCoordinationHealthIndicatorService extends TestHealthIndicatorService {
        public ClusterCoordinationHealthIndicatorService(ClusterService clusterService) {
            super(clusterService, INSTANCE_HAS_MASTER_INDICATOR_NAME, CLUSTER_COORDINATION_HEALTH_STATUS_SETTING);
        }
    }

    public void testGetHealth() throws Exception {

        var client = client();
        var ilmIndicatorStatus = randomFrom(HealthStatus.values());
        var slmIndicatorStatus = randomFrom(HealthStatus.values());
        var clusterCoordinationIndicatorStatus = randomFrom(HealthStatus.values());

        try {
            updateClusterSettings(
                Settings.builder()
                    .put(ILM_HEALTH_STATUS_SETTING.getKey(), ilmIndicatorStatus)
                    .put(SLM_HEALTH_STATUS_SETTING.getKey(), slmIndicatorStatus)
                    .put(CLUSTER_COORDINATION_HEALTH_STATUS_SETTING.getKey(), clusterCoordinationIndicatorStatus)
            );

            // First, test that we don't request any indicators, and get back everything (but no details):
            testRootLevel(client, ilmIndicatorStatus, slmIndicatorStatus, clusterCoordinationIndicatorStatus, false);
            // Now, test the same thing but get back details):
            testRootLevel(client, ilmIndicatorStatus, slmIndicatorStatus, clusterCoordinationIndicatorStatus, true);

            // Next, test that if we ask for a specific indicator, we get only those back (without details):
            testIndicator(client, ilmIndicatorStatus, false);
            // And now with details:
            testIndicator(client, ilmIndicatorStatus, true);

            // Next, test that if we ask for a nonexistent indicator, we get an exception
            expectThrows(
                ResourceNotFoundException.class,
                client.execute(GetHealthAction.INSTANCE, new GetHealthAction.Request(NONEXISTENT_INDICATOR_NAME, randomBoolean(), 1000))
            );

            // Check health api stats
            {
                HealthApiStatsAction.Response response = client.execute(HealthApiStatsAction.INSTANCE, new HealthApiStatsAction.Request())
                    .get();
                Counters stats = response.getStats();
                assertThat(stats.get("invocations.total"), equalTo(4L));
                assertThat(stats.get("invocations.verbose_true"), equalTo(2L));
                assertThat(stats.get("invocations.verbose_false"), equalTo(2L));
                assertThat(
                    stats.get("invocations.verbose_true") + stats.get("invocations.verbose_false"),
                    equalTo(stats.get("invocations.total"))
                );
                HealthStatus mostSevereHealthStatus = HealthStatus.merge(
                    Stream.of(ilmIndicatorStatus, slmIndicatorStatus, clusterCoordinationIndicatorStatus)
                );
                assertThat(stats.get("statuses." + mostSevereHealthStatus.xContentValue()), greaterThanOrEqualTo(2L));
                assertThat(stats.get("statuses." + ilmIndicatorStatus.xContentValue()), greaterThanOrEqualTo(2L));
                String label = "indicators." + ilmIndicatorStatus.xContentValue() + ".ilm";
                if (ilmIndicatorStatus != HealthStatus.GREEN) {
                    assertThat(stats.get(label), greaterThanOrEqualTo(4L));
                } else {
                    expectThrows(IllegalArgumentException.class, () -> stats.get(label));
                }
            }

        } finally {
            updateClusterSettings(
                Settings.builder()
                    .putNull(ILM_HEALTH_STATUS_SETTING.getKey())
                    .putNull(SLM_HEALTH_STATUS_SETTING.getKey())
                    .putNull(CLUSTER_COORDINATION_HEALTH_STATUS_SETTING.getKey())
            );
        }
    }

    private void testRootLevel(
        Client client,
        HealthStatus ilmIndicatorStatus,
        HealthStatus slmIndicatorStatus,
        HealthStatus clusterCoordinationIndicatorStatus,
        boolean verbose
    ) throws Exception {
        var response = client.execute(GetHealthAction.INSTANCE, new GetHealthAction.Request(verbose, 1000)).get();

        assertThat(
            response.getStatus(),
            equalTo(HealthStatus.merge(Stream.of(ilmIndicatorStatus, slmIndicatorStatus, clusterCoordinationIndicatorStatus)))
        );
        assertThat(response.getClusterName(), equalTo(new ClusterName(cluster().getClusterName())));
        assertThat(
            response.findIndicator(ILM_INDICATOR_NAME),
            equalTo(
                new HealthIndicatorResult(
                    ILM_INDICATOR_NAME,
                    ilmIndicatorStatus,
                    "Health is set to [" + ilmIndicatorStatus + "] by test plugin",
                    new SimpleHealthIndicatorDetails(Map.of("verbose", verbose)),
                    Collections.emptyList(),
                    Collections.emptyList()
                )
            )
        );
        assertThat(
            response.findIndicator(INSTANCE_HAS_MASTER_INDICATOR_NAME),
            equalTo(
                new HealthIndicatorResult(
                    INSTANCE_HAS_MASTER_INDICATOR_NAME,
                    clusterCoordinationIndicatorStatus,
                    "Health is set to [" + clusterCoordinationIndicatorStatus + "] by test plugin",
                    new SimpleHealthIndicatorDetails(Map.of("verbose", verbose)),
                    Collections.emptyList(),
                    Collections.emptyList()
                )
            )
        );
    }

    private void testIndicator(Client client, HealthStatus ilmIndicatorStatus, boolean verbose) throws Exception {
        var response = client.execute(GetHealthAction.INSTANCE, new GetHealthAction.Request(ILM_INDICATOR_NAME, verbose, 1000)).get();
        assertNull(response.getStatus());
        assertThat(response.getClusterName(), equalTo(new ClusterName(cluster().getClusterName())));
        assertThat(
            response.findIndicator(ILM_INDICATOR_NAME),
            equalTo(
                new HealthIndicatorResult(
                    ILM_INDICATOR_NAME,
                    ilmIndicatorStatus,
                    "Health is set to [" + ilmIndicatorStatus + "] by test plugin",
                    new SimpleHealthIndicatorDetails(Map.of("verbose", verbose)),
                    Collections.emptyList(),
                    Collections.emptyList()
                )
            )
        );
        expectThrows(NoSuchElementException.class, () -> response.findIndicator(SLM_INDICATOR_NAME));
    }
}
