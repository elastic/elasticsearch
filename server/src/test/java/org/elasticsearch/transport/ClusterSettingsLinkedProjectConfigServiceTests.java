/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.transport.LinkedProjectConfig.ProxyLinkedProjectConfigBuilder;
import static org.elasticsearch.transport.LinkedProjectConfigService.LinkedProjectConfigListener;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class ClusterSettingsLinkedProjectConfigServiceTests extends ESTestCase {

    private record RemovedConfig(ProjectId originProjectId, ProjectId linkedProjectId, String linkedProjectAlias) {}

    private static class StubLinkedProjectConfigListener implements LinkedProjectConfigListener {
        LinkedProjectConfig updatedConfig;
        RemovedConfig removedConfig;

        @Override
        public void updateLinkedProject(LinkedProjectConfig config) {
            updatedConfig = config;
        }

        @Override
        public void remove(ProjectId originProjectId, ProjectId linkedProjectId, String linkedProjectAlias) {
            removedConfig = new RemovedConfig(originProjectId, linkedProjectId, linkedProjectAlias);
        }

        void reset() {
            updatedConfig = null;
            removedConfig = null;
        }
    }

    /**
     * A simple test to exercise the callback registration and notification mechanism.
     * Note that {@link RemoteClusterServiceTests} uses {@link ClusterSettingsLinkedProjectConfigService}
     * and contains more thorough tests of all the settings being monitored.
     */
    public void testListenersReceiveUpdates() {
        final var alias = randomAlphaOfLength(10);

        final var initialProxyAddress = "localhost:9400";
        final var initialSkipUnavailable = randomBoolean();
        final var initialSettings = Settings.builder()
            .put("cluster.remote." + alias + ".mode", "proxy")
            .put("cluster.remote." + alias + ".proxy_address", initialProxyAddress)
            .put("cluster.remote." + alias + ".skip_unavailable", initialSkipUnavailable)
            .build();
        final var clusterSettings = ClusterSettings.createBuiltInClusterSettings(initialSettings);
        final var service = new ClusterSettingsLinkedProjectConfigService(
            initialSettings,
            clusterSettings,
            DefaultProjectResolver.INSTANCE
        );
        final var config = new ProxyLinkedProjectConfigBuilder(alias).proxyAddress(initialProxyAddress)
            .skipUnavailable(initialSkipUnavailable)
            .build();

        // Verify we can get the linked projects on startup.
        assertThat(service.getInitialLinkedProjectConfigs(), equalTo(List.of(config)));

        final int numListeners = randomIntBetween(1, 10);
        final var listeners = new ArrayList<StubLinkedProjectConfigListener>(numListeners);
        for (int i = 0; i < numListeners; ++i) {
            listeners.add(new StubLinkedProjectConfigListener());
            service.register(listeners.getLast());
        }

        // Expect no updates when applying the same settings.
        clusterSettings.applySettings(initialSettings);
        for (int i = 0; i < numListeners; ++i) {
            assertThat(listeners.get(i).updatedConfig, sameInstance(null));
            listeners.get(i).reset();
        }

        // Change the proxy address and skip_unavailable values.
        final var newProxyAddress = "localhost:9401";
        final var newSkipUnavailable = config.skipUnavailable() == false;
        clusterSettings.applySettings(
            Settings.builder()
                .put(initialSettings)
                .put("cluster.remote." + alias + ".proxy_address", newProxyAddress)
                .put("cluster.remote." + alias + ".skip_unavailable", newSkipUnavailable)
                .build()
        );
        for (int i = 0; i < numListeners; ++i) {
            assertNotNull("expected non-null updatedConfig for listener " + i, listeners.get(i).updatedConfig);
            assertThat(listeners.get(i).updatedConfig.proxyAddress(), equalTo(newProxyAddress));
            assertThat(listeners.get(i).updatedConfig.skipUnavailable(), equalTo(newSkipUnavailable));
            assertNull("removedConfig callback should not be invoked", listeners.get(i).removedConfig);
            listeners.get(i).reset();
        }

        // Unset the proxy address, should invoke the remove callback.
        clusterSettings.applySettings(
            Settings.builder()
                .put(initialSettings)
                .put("cluster.remote." + alias + ".proxy_address", "")
                .put("cluster.remote." + alias + ".skip_unavailable", newSkipUnavailable)
                .build()
        );
        for (int i = 0; i < numListeners; ++i) {
            assertNotNull("expected non-null removed config for listener " + i, listeners.get(i).removedConfig);
            assertThat(
                listeners.get(i).removedConfig,
                equalTo(new RemovedConfig(DefaultProjectResolver.INSTANCE.getProjectId(), ProjectId.DEFAULT, alias))
            );
            assertNull("updateConfig callback should not be invoked", listeners.get(i).updatedConfig);
        }
    }
}
