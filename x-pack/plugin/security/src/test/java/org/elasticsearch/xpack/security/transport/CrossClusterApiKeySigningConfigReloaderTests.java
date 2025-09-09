/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.After;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CrossClusterApiKeySigningConfigReloaderTests extends ESTestCase {
    private CrossClusterApiKeySigner crossClusterApiKeySigner;
    private ResourceWatcherService resourceWatcherService;
    private ThreadPool threadPool;
    private Settings.Builder settingsBuilder;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        crossClusterApiKeySigner = mock(CrossClusterApiKeySigner.class);
        when(crossClusterApiKeySigner.loadSigningConfig(any(), any(), anyBoolean())).thenReturn(
            new CrossClusterApiKeySigner.SigningConfig(null, null, null)
        );
        Settings settings = Settings.builder().put("resource.reload.interval.high", TimeValue.timeValueMillis(100)).build();
        threadPool = new TestThreadPool(getTestName());
        resourceWatcherService = new ResourceWatcherService(settings, threadPool);
        settingsBuilder = Settings.builder()
            .put("path.home", createTempDir())
            .put(Node.NODE_NAME_SETTING.getKey(), randomAlphaOfLengthBetween(3, 8));
    }

    public void testSimpleDynamicSettingsUpdate() throws IOException {
        Settings settings = settingsBuilder.put("cluster.remote.my_remote.signing.keystore.alias", "mykey").build();

        var clusterSettings = new ClusterSettings(settings, new HashSet<>(CrossClusterApiKeySignerSettings.getDynamicSettings()));

        var crossClusterApiKeySigningConfigReloader = new CrossClusterApiKeySigningConfigReloader(
            TestEnvironment.newEnvironment(settings),
            resourceWatcherService,
            clusterSettings
        );
        crossClusterApiKeySigningConfigReloader.setApiKeySigner(crossClusterApiKeySigner);
        clusterSettings.applySettings(Settings.builder().put("cluster.remote.my_remote.signing.keystore.alias", "anotherkey").build());
        verify(crossClusterApiKeySigner, times(1)).loadSigningConfig(
            "my_remote",
            Settings.builder()
                .put("cluster.remote.my_remote.signing.keystore.alias", "anotherkey")
                .build()
                .getByPrefix("cluster.remote.my_remote."),
            false
        );
    }

    public void testDynamicSettingsUpdateWithAddedFiles() throws Exception {
        var filesToMonitor = new Path[] { createTempFile(), createTempFile(), createTempFile() };
        Settings settings = settingsBuilder.build();

        var clusterSettings = new ClusterSettings(settings, new HashSet<>(CrossClusterApiKeySignerSettings.getDynamicSettings()));
        var crossClusterApiKeySigningConfigReloader = new CrossClusterApiKeySigningConfigReloader(
            TestEnvironment.newEnvironment(settings),
            resourceWatcherService,
            clusterSettings
        );
        var crossClusterApiKeySigner = mock(CrossClusterApiKeySigner.class);
        when(crossClusterApiKeySigner.loadSigningConfig(any(), any(), anyBoolean())).thenReturn(
            new CrossClusterApiKeySigner.SigningConfig(null, Set.of(filesToMonitor), null)
        );

        crossClusterApiKeySigningConfigReloader.setApiKeySigner(crossClusterApiKeySigner);
        clusterSettings.applySettings(
            Settings.builder()
                .put("cluster.remote.my_remote0.signing.keystore.alias", "mykey")
                .put("cluster.remote.my_remote0.signing.keystore.path", filesToMonitor[0])
                .put("cluster.remote.my_remote1.signing.keystore.alias", "mykey")
                .put("cluster.remote.my_remote1.signing.keystore.path", filesToMonitor[1])
                .put("cluster.remote.my_remote2.signing.keystore.alias", "mykey")
                .put("cluster.remote.my_remote2.signing.keystore.path", filesToMonitor[2])
                .build()
        );

        for (int i = 0; i < 3; i++) {
            final String clusterName = "my_remote" + i;
            verify(crossClusterApiKeySigner, times(1)).loadSigningConfig(
                clusterName,
                Settings.builder()
                    .put("cluster.remote." + clusterName + ".signing.keystore.alias", "mykey")
                    .put("cluster.remote." + clusterName + ".signing.keystore.path", filesToMonitor[i])
                    .build()
                    .getByPrefix("cluster.remote." + clusterName + "."),
                false
            );
            Files.writeString(filesToMonitor[i], "some content");
            assertBusy(() -> verify(crossClusterApiKeySigner, times(1)).loadSigningConfig(clusterName, null, false));
        }
    }

    public void testSimpleSecureSettingsReload() {
        var clusterSettings = new ClusterSettings(Settings.EMPTY, new HashSet<>(CrossClusterApiKeySignerSettings.getDynamicSettings()));
        var crossClusterApiKeySigningConfigReloader = new CrossClusterApiKeySigningConfigReloader(
            TestEnvironment.newEnvironment(settingsBuilder.build()),
            resourceWatcherService,
            clusterSettings
        );

        crossClusterApiKeySigningConfigReloader.setApiKeySigner(crossClusterApiKeySigner);

        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("cluster.remote.my_remote.signing.keystore.secure_password", "secret");
        Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
        crossClusterApiKeySigningConfigReloader.reload(settings);

        verify(crossClusterApiKeySigner).loadSigningConfig("my_remote", settings.getByPrefix("cluster.remote.my_remote."), true);
    }

    public void testSecureSettingsReloadNoMatchingSecureSettings() {
        var clusterSettings = new ClusterSettings(Settings.EMPTY, new HashSet<>(CrossClusterApiKeySignerSettings.getDynamicSettings()));
        var crossClusterApiKeySigningConfigReloader = new CrossClusterApiKeySigningConfigReloader(
            TestEnvironment.newEnvironment(settingsBuilder.build()),
            resourceWatcherService,
            clusterSettings
        );
        crossClusterApiKeySigningConfigReloader.setApiKeySigner(crossClusterApiKeySigner);

        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("not.a.setting", "secret");
        Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
        crossClusterApiKeySigningConfigReloader.reload(settings);

        verify(crossClusterApiKeySigner, times(0)).loadSigningConfig(any(), any(), anyBoolean());
    }

    public void testFileUpdatedReloaded() throws Exception {
        var fileToMonitor = createTempFile();
        var clusterSettings = new ClusterSettings(Settings.EMPTY, new HashSet<>(CrossClusterApiKeySignerSettings.getDynamicSettings()));

        var crossClusterApiKeySigningConfigReloader = new CrossClusterApiKeySigningConfigReloader(
            TestEnvironment.newEnvironment(settingsBuilder.put("cluster.remote.my_remote.signing.keystore.path", fileToMonitor).build()),
            resourceWatcherService,
            clusterSettings
        );

        crossClusterApiKeySigningConfigReloader.setApiKeySigner(crossClusterApiKeySigner);

        verify(crossClusterApiKeySigner, times(0)).loadSigningConfig(anyString(), any(), anyBoolean());
        Files.writeString(fileToMonitor, "some content");
        assertBusy(() -> verify(crossClusterApiKeySigner, times(1)).loadSigningConfig("my_remote", null, false));
    }

    public void testFileDeletedReloaded() throws Exception {
        var fileToMonitor = createTempFile();
        var clusterSettings = new ClusterSettings(Settings.EMPTY, new HashSet<>(CrossClusterApiKeySignerSettings.getDynamicSettings()));

        var crossClusterApiKeySigningConfigReloader = new CrossClusterApiKeySigningConfigReloader(
            TestEnvironment.newEnvironment(settingsBuilder.put("cluster.remote.my_remote.signing.keystore.path", fileToMonitor).build()),
            resourceWatcherService,
            clusterSettings
        );

        crossClusterApiKeySigningConfigReloader.setApiKeySigner(crossClusterApiKeySigner);

        verify(crossClusterApiKeySigner, times(0)).loadSigningConfig(anyString(), any(), anyBoolean());
        Files.delete(fileToMonitor);
        assertBusy(() -> verify(crossClusterApiKeySigner, times(1)).loadSigningConfig("my_remote", null, false));
    }

    @After
    public void tearDownThreadPool() {
        terminate(threadPool);
    }

}
