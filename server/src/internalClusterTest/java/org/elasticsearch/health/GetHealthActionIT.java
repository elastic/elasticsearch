/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.HealthPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.elasticsearch.common.util.CollectionUtils.appendToCopy;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class GetHealthActionIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return appendToCopy(super.nodePlugins(), TestHealthPlugin.class);
    }

    public static final Setting<HealthStatus> TEST_HEALTH_STATUS_1 = new Setting<>(
        "test.health.status.1",
        "GREEN",
        HealthStatus::valueOf,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<HealthStatus> TEST_HEALTH_STATUS_2 = new Setting<>(
        "test.health.status.2",
        "GREEN",
        HealthStatus::valueOf,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<HealthStatus> TEST_HEALTH_STATUS_3 = new Setting<>(
        "test.health.status.3",
        "GREEN",
        HealthStatus::valueOf,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final class TestHealthPlugin extends Plugin implements HealthPlugin {

        private final List<HealthIndicatorService> healthIndicatorServices = new ArrayList<>();

        @Override
        public List<Setting<?>> getSettings() {
            return List.of(TEST_HEALTH_STATUS_1, TEST_HEALTH_STATUS_2, TEST_HEALTH_STATUS_3);
        }

        @Override
        public Collection<Object> createComponents(
            Client client,
            ClusterService clusterService,
            ThreadPool threadPool,
            ResourceWatcherService resourceWatcherService,
            ScriptService scriptService,
            NamedXContentRegistry xContentRegistry,
            Environment environment,
            NodeEnvironment nodeEnvironment,
            NamedWriteableRegistry namedWriteableRegistry,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<RepositoriesService> repositoriesServiceSupplier
        ) {
            healthIndicatorServices.add(new FixedStatusHealthIndicatorService1(clusterService));
            healthIndicatorServices.add(new FixedStatusHealthIndicatorService2(clusterService));
            healthIndicatorServices.add(new FixedStatusHealthIndicatorService3(clusterService));
            return new ArrayList<>(healthIndicatorServices);
        }

        @Override
        public Collection<HealthIndicatorService> getHealthIndicatorServices() {
            return healthIndicatorServices;
        }
    }

    /**
     * This indicator could be used to pre-define health of the cluster with {@code TEST_HEALTH_STATUS} property
     * and return it via health API.
     */
    public static final class FixedStatusHealthIndicatorService1 implements HealthIndicatorService {

        private final ClusterService clusterService;

        public FixedStatusHealthIndicatorService1(ClusterService clusterService) {
            this.clusterService = clusterService;
        }

        @Override
        public String name() {
            return "test_indicator_1";
        }

        @Override
        public String component() {
            return "test_component_1";
        }

        @Override
        public HealthIndicatorResult calculate(boolean includeDetails) {
            var status = clusterService.getClusterSettings().get(TEST_HEALTH_STATUS_1);
            return createIndicator(
                status,
                "Health is set to [" + status + "] by test plugin",
                new SimpleHealthIndicatorDetails(Map.of("include_details", includeDetails)),
                includeDetails
            );
        }
    }

    public static final class FixedStatusHealthIndicatorService2 implements HealthIndicatorService {

        private final ClusterService clusterService;

        public FixedStatusHealthIndicatorService2(ClusterService clusterService) {
            this.clusterService = clusterService;
        }

        @Override
        public String name() {
            return "test_indicator_2";
        }

        @Override
        public String component() {
            return "test_component_1";
        }

        @Override
        public HealthIndicatorResult calculate(boolean includeDetails) {
            var status = clusterService.getClusterSettings().get(TEST_HEALTH_STATUS_2);
            return createIndicator(
                status,
                "Health is set to [" + status + "] by test plugin",
                new SimpleHealthIndicatorDetails(Map.of("include_details", includeDetails)),
                includeDetails
            );
        }
    }

    public static final class FixedStatusHealthIndicatorService3 implements HealthIndicatorService {

        private final ClusterService clusterService;

        public FixedStatusHealthIndicatorService3(ClusterService clusterService) {
            this.clusterService = clusterService;
        }

        @Override
        public String name() {
            return "test_indicator_3";
        }

        @Override
        public String component() {
            return "test_component_2";
        }

        @Override
        public HealthIndicatorResult calculate(boolean includeDetails) {
            var status = clusterService.getClusterSettings().get(TEST_HEALTH_STATUS_3);
            return createIndicator(
                status,
                "Health is set to [" + status + "] by test plugin",
                new SimpleHealthIndicatorDetails(Map.of("include_details", includeDetails)),
                includeDetails
            );
        }
    }

    public void testGetHealth() throws Exception {

        var client = client();
        var indicator1Status = randomFrom(HealthStatus.values());
        var indicator2Status = randomFrom(HealthStatus.values());
        var indicator3Status = randomFrom(HealthStatus.values());

        try {
            updateClusterSettings(
                Settings.builder()
                    .put(TEST_HEALTH_STATUS_1.getKey(), indicator1Status)
                    .put(TEST_HEALTH_STATUS_2.getKey(), indicator2Status)
                    .put(TEST_HEALTH_STATUS_3.getKey(), indicator3Status)
            );

            // First, test that we don't request any components or indicators, and get back everything (but no details):
            {
                var response = client.execute(GetHealthAction.INSTANCE, new GetHealthAction.Request()).get();

                assertThat(
                    response.getStatus(),
                    equalTo(HealthStatus.merge(Stream.of(indicator1Status, indicator2Status, indicator3Status)))
                );
                assertThat(response.getClusterName(), equalTo(new ClusterName(cluster().getClusterName())));
                assertThat(
                    response.findComponent("test_component_1"),
                    equalTo(
                        new HealthComponentResult(
                            "test_component_1",
                            HealthStatus.merge(Stream.of(indicator1Status, indicator2Status)),
                            List.of(
                                new HealthIndicatorResult(
                                    "test_indicator_1",
                                    "test_component_1",
                                    indicator1Status,
                                    "Health is set to [" + indicator1Status + "] by test plugin",
                                    new SimpleHealthIndicatorDetails(Map.of("include_details", false)),
                                    false
                                ),
                                new HealthIndicatorResult(
                                    "test_indicator_2",
                                    "test_component_1",
                                    indicator2Status,
                                    "Health is set to [" + indicator2Status + "] by test plugin",
                                    new SimpleHealthIndicatorDetails(Map.of("include_details", false)),
                                    false
                                )
                            ),
                            true
                        )
                    )
                );
                assertThat(
                    response.findComponent("test_component_2"),
                    equalTo(
                        new HealthComponentResult(
                            "test_component_2",
                            indicator3Status,
                            List.of(
                                new HealthIndicatorResult(
                                    "test_indicator_3",
                                    "test_component_2",
                                    indicator3Status,
                                    "Health is set to [" + indicator3Status + "] by test plugin",
                                    new SimpleHealthIndicatorDetails(Map.of("include_details", false)),
                                    false
                                )
                            ),
                            true
                        )
                    )
                );
            }

            // Next, test that if we ask for a specific component and indicator, we get only those back (with details):
            {
                var response = client.execute(GetHealthAction.INSTANCE, new GetHealthAction.Request("test_component_1", "test_indicator_1"))
                    .get();
                assertNull(response.getStatus());
                assertThat(response.getClusterName(), equalTo(new ClusterName(cluster().getClusterName())));
                assertThat(
                    response.findComponent("test_component_1"),
                    equalTo(
                        new HealthComponentResult(
                            "test_component_1",
                            null,
                            List.of(
                                new HealthIndicatorResult(
                                    "test_indicator_1",
                                    "test_component_1",
                                    indicator1Status,
                                    "Health is set to [" + indicator1Status + "] by test plugin",
                                    new SimpleHealthIndicatorDetails(Map.of("include_details", true)),
                                    true
                                )
                            ),
                            false
                        )
                    )
                );
                expectThrows(NoSuchElementException.class, () -> response.findComponent("test_component_2"));
            }

            // Test that if we specify a component name and no indicator name that we get all indicators for that component:
            {
                var response = client.execute(GetHealthAction.INSTANCE, new GetHealthAction.Request("test_component_1", null)).get();
                assertNull(response.getStatus());
                assertThat(response.getClusterName(), equalTo(new ClusterName(cluster().getClusterName())));
                assertThat(
                    response.findComponent("test_component_1"),
                    equalTo(
                        new HealthComponentResult(
                            "test_component_1",
                            HealthStatus.merge(Stream.of(indicator1Status, indicator2Status)),
                            List.of(
                                new HealthIndicatorResult(
                                    "test_indicator_1",
                                    "test_component_1",
                                    indicator1Status,
                                    "Health is set to [" + indicator1Status + "] by test plugin",
                                    new SimpleHealthIndicatorDetails(Map.of("include_details", true)),
                                    true
                                ),
                                new HealthIndicatorResult(
                                    "test_indicator_2",
                                    "test_component_1",
                                    indicator2Status,
                                    "Health is set to [" + indicator2Status + "] by test plugin",
                                    new SimpleHealthIndicatorDetails(Map.of("include_details", true)),
                                    true
                                )
                            ),
                            true
                        )
                    )
                );
                expectThrows(NoSuchElementException.class, () -> response.findComponent("test_component_2"));
            }

        } finally {
            updateClusterSettings(
                Settings.builder()
                    .putNull(TEST_HEALTH_STATUS_1.getKey())
                    .putNull(TEST_HEALTH_STATUS_2.getKey())
                    .putNull(TEST_HEALTH_STATUS_3.getKey())
            );
        }
    }
}
