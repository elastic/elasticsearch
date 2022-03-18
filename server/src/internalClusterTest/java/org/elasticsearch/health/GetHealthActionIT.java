/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.apache.lucene.util.SetOnce;
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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.common.util.CollectionUtils.appendToCopy;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class GetHealthActionIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return appendToCopy(super.nodePlugins(), TestHealthPlugin.class);
    }

    public static final Setting<HealthStatus> TEST_HEALTH_STATUS = new Setting<>(
        "test.health.status",
        "GREEN",
        HealthStatus::valueOf,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final class TestHealthPlugin extends Plugin implements HealthPlugin {

        private final SetOnce<FixedStatusHealthIndicatorService> healthIndicatorService = new SetOnce<>();

        @Override
        public List<Setting<?>> getSettings() {
            return List.of(TEST_HEALTH_STATUS);
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
            var service = new FixedStatusHealthIndicatorService(clusterService);
            healthIndicatorService.set(service);
            return List.of(service);
        }

        @Override
        public Collection<HealthIndicatorService> getHealthIndicatorServices() {
            return List.of(healthIndicatorService.get());
        }
    }

    /**
     * This indicator could be used to pre-define health of the cluster with {@code TEST_HEALTH_STATUS} property
     * and return it via health API.
     */
    public static final class FixedStatusHealthIndicatorService implements HealthIndicatorService {

        private final ClusterService clusterService;

        public FixedStatusHealthIndicatorService(ClusterService clusterService) {
            this.clusterService = clusterService;
        }

        @Override
        public String name() {
            return "test_indicator";
        }

        @Override
        public String component() {
            return "test_component";
        }

        @Override
        public HealthIndicatorResult calculate() {
            var status = clusterService.getClusterSettings().get(TEST_HEALTH_STATUS);
            return createIndicator(
                status,
                "Health is set to [" + status + "] by test plugin",
                HealthIndicatorDetails.EMPTY,
                Collections.emptyList()
            );
        }
    }

    public void testGetHealth() throws Exception {

        var client = client();
        var status = randomFrom(HealthStatus.values());

        try {
            updateClusterSettings(Settings.builder().put(TEST_HEALTH_STATUS.getKey(), status));

            var response = client.execute(GetHealthAction.INSTANCE, new GetHealthAction.Request()).get();

            assertThat(response.getStatus(), equalTo(status));
            assertThat(response.getClusterName(), equalTo(new ClusterName(cluster().getClusterName())));
            assertThat(
                response.findComponent("test_component"),
                equalTo(
                    new HealthComponentResult(
                        "test_component",
                        status,
                        List.of(
                            new HealthIndicatorResult(
                                "test_indicator",
                                "test_component",
                                status,
                                "Health is set to [" + status + "] by test plugin",
                                HealthIndicatorDetails.EMPTY,
                                Collections.emptyList()
                            )
                        )
                    )
                )
            );
        } finally {
            updateClusterSettings(Settings.builder().putNull(TEST_HEALTH_STATUS.getKey()));
        }
    }
}
