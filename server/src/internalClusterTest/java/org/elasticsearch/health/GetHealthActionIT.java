/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.ResourceNotFoundException;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.elasticsearch.common.util.CollectionUtils.appendToCopy;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class GetHealthActionIT extends ESIntegTestCase {

    private static final String DATA_COMPONENT_NAME = "test_data"; // prefixing with "test_" to avoid collisions with the real component
    private static final String CLUSTER_COORDINATION_COMPONENT_NAME = "test_cluster_coordination";
    private static final String NONEXISTENT_COMPONENT_NAME = "test_nonexistent";

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
            healthIndicatorServices.add(new IlmHealthIndicatorService(clusterService));
            healthIndicatorServices.add(new SlmHealthIndicatorService(clusterService));
            healthIndicatorServices.add(new ClusterCoordinationHealthIndicatorService(clusterService));
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
        private final String componentName;
        private final String indicatorName;
        private final Setting<HealthStatus> statusSetting;

        public TestHealthIndicatorService(
            ClusterService clusterService,
            String componentName,
            String indicatorName,
            Setting<HealthStatus> statusSetting
        ) {
            this.clusterService = clusterService;
            this.componentName = componentName;
            this.indicatorName = indicatorName;
            this.statusSetting = statusSetting;
        }

        @Override
        public String name() {
            return indicatorName;
        }

        @Override
        public String component() {
            return componentName;
        }

        @Override
        public HealthIndicatorResult calculate(boolean includeDetails) {
            var status = clusterService.getClusterSettings().get(statusSetting);
            return createIndicator(
                status,
                "Health is set to [" + status + "] by test plugin",
                new SimpleHealthIndicatorDetails(Map.of("include_details", includeDetails)),
                Collections.emptyList(),
                Collections.emptyList()
            );
        }
    }

    public static final class IlmHealthIndicatorService extends TestHealthIndicatorService {
        public IlmHealthIndicatorService(ClusterService clusterService) {
            super(clusterService, DATA_COMPONENT_NAME, ILM_INDICATOR_NAME, ILM_HEALTH_STATUS_SETTING);
        }
    }

    public static final class SlmHealthIndicatorService extends TestHealthIndicatorService {
        public SlmHealthIndicatorService(ClusterService clusterService) {
            super(clusterService, DATA_COMPONENT_NAME, SLM_INDICATOR_NAME, SLM_HEALTH_STATUS_SETTING);
        }
    }

    public static final class ClusterCoordinationHealthIndicatorService extends TestHealthIndicatorService {
        public ClusterCoordinationHealthIndicatorService(ClusterService clusterService) {
            super(
                clusterService,
                CLUSTER_COORDINATION_COMPONENT_NAME,
                INSTANCE_HAS_MASTER_INDICATOR_NAME,
                CLUSTER_COORDINATION_HEALTH_STATUS_SETTING
            );
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

            // First, test that we don't request any components or indicators, and get back everything (but no details):
            {
                var response = client.execute(GetHealthAction.INSTANCE, new GetHealthAction.Request()).get();

                assertThat(
                    response.getStatus(),
                    equalTo(HealthStatus.merge(Stream.of(ilmIndicatorStatus, slmIndicatorStatus, clusterCoordinationIndicatorStatus)))
                );
                assertThat(response.getClusterName(), equalTo(new ClusterName(cluster().getClusterName())));
                assertThat(
                    response.findComponent(DATA_COMPONENT_NAME),
                    equalTo(
                        new HealthComponentResult(
                            DATA_COMPONENT_NAME,
                            HealthStatus.merge(Stream.of(ilmIndicatorStatus, slmIndicatorStatus)),
                            List.of(
                                new HealthIndicatorResult(
                                    ILM_INDICATOR_NAME,
                                    DATA_COMPONENT_NAME,
                                    ilmIndicatorStatus,
                                    "Health is set to [" + ilmIndicatorStatus + "] by test plugin",
                                    new SimpleHealthIndicatorDetails(Map.of("include_details", false)),
                                    Collections.emptyList(),
                                    Collections.emptyList()
                                ),
                                new HealthIndicatorResult(
                                    SLM_INDICATOR_NAME,
                                    DATA_COMPONENT_NAME,
                                    slmIndicatorStatus,
                                    "Health is set to [" + slmIndicatorStatus + "] by test plugin",
                                    new SimpleHealthIndicatorDetails(Map.of("include_details", false)),
                                    Collections.emptyList(),
                                    Collections.emptyList()
                                )
                            )
                        )
                    )
                );
                assertThat(
                    response.findComponent(CLUSTER_COORDINATION_COMPONENT_NAME),
                    equalTo(
                        new HealthComponentResult(
                            CLUSTER_COORDINATION_COMPONENT_NAME,
                            clusterCoordinationIndicatorStatus,
                            List.of(
                                new HealthIndicatorResult(
                                    INSTANCE_HAS_MASTER_INDICATOR_NAME,
                                    CLUSTER_COORDINATION_COMPONENT_NAME,
                                    clusterCoordinationIndicatorStatus,
                                    "Health is set to [" + clusterCoordinationIndicatorStatus + "] by test plugin",
                                    new SimpleHealthIndicatorDetails(Map.of("include_details", false)),
                                    Collections.emptyList(),
                                    Collections.emptyList()
                                )
                            )
                        )
                    )
                );
            }

            // Next, test that if we ask for a specific component and indicator, we get only those back (with details):
            {
                var response = client.execute(
                    GetHealthAction.INSTANCE,
                    new GetHealthAction.Request(DATA_COMPONENT_NAME, ILM_INDICATOR_NAME)
                ).get();
                assertNull(response.getStatus());
                assertThat(response.getClusterName(), equalTo(new ClusterName(cluster().getClusterName())));
                assertThat(
                    response.findComponent(DATA_COMPONENT_NAME),
                    equalTo(
                        new HealthComponentResult(
                            DATA_COMPONENT_NAME,
                            null,
                            List.of(
                                new HealthIndicatorResult(
                                    ILM_INDICATOR_NAME,
                                    DATA_COMPONENT_NAME,
                                    ilmIndicatorStatus,
                                    "Health is set to [" + ilmIndicatorStatus + "] by test plugin",
                                    new SimpleHealthIndicatorDetails(Map.of("include_details", true)),
                                    Collections.emptyList(),
                                    Collections.emptyList()
                                )
                            )
                        )
                    )
                );
                expectThrows(NoSuchElementException.class, () -> response.findComponent(CLUSTER_COORDINATION_COMPONENT_NAME));
            }

            // Test that if we specify a component name and no indicator name that we get all indicators for that component:
            {
                var response = client.execute(GetHealthAction.INSTANCE, new GetHealthAction.Request(DATA_COMPONENT_NAME, null)).get();
                assertNull(response.getStatus());
                assertThat(response.getClusterName(), equalTo(new ClusterName(cluster().getClusterName())));
                assertThat(
                    response.findComponent(DATA_COMPONENT_NAME),
                    equalTo(
                        new HealthComponentResult(
                            DATA_COMPONENT_NAME,
                            HealthStatus.merge(Stream.of(ilmIndicatorStatus, slmIndicatorStatus)),
                            List.of(
                                new HealthIndicatorResult(
                                    ILM_INDICATOR_NAME,
                                    DATA_COMPONENT_NAME,
                                    ilmIndicatorStatus,
                                    "Health is set to [" + ilmIndicatorStatus + "] by test plugin",
                                    new SimpleHealthIndicatorDetails(Map.of("include_details", true)),
                                    Collections.emptyList(),
                                    Collections.emptyList()
                                ),
                                new HealthIndicatorResult(
                                    SLM_INDICATOR_NAME,
                                    DATA_COMPONENT_NAME,
                                    slmIndicatorStatus,
                                    "Health is set to [" + slmIndicatorStatus + "] by test plugin",
                                    new SimpleHealthIndicatorDetails(Map.of("include_details", true)),
                                    Collections.emptyList(),
                                    Collections.emptyList()
                                )
                            )
                        )
                    )
                );
                expectThrows(NoSuchElementException.class, () -> response.findComponent(CLUSTER_COORDINATION_COMPONENT_NAME));
            }

            // Next, test that if we ask for a nonexistent component and indicator, we get an exception
            {
                ExecutionException exception = expectThrows(
                    ExecutionException.class,
                    () -> client.execute(
                        GetHealthAction.INSTANCE,
                        new GetHealthAction.Request(NONEXISTENT_COMPONENT_NAME, NONEXISTENT_INDICATOR_NAME)
                    ).get()
                );
                assertThat(exception.getCause(), instanceOf(ResourceNotFoundException.class));
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
}
