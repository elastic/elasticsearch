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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.After;

import java.io.IOException;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CrossClusterApiKeySignerReloaderTests extends ESTestCase {
    private CrossClusterApiKeySigner crossClusterApiKeySigner;
    private ResourceWatcherService resourceWatcherService;
    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        crossClusterApiKeySigner = mock(CrossClusterApiKeySigner.class);
        Settings settings = Settings.builder().put("resource.reload.interval.high", TimeValue.timeValueMillis(100)).build();
        threadPool = new TestThreadPool(getTestName());
        resourceWatcherService = new ResourceWatcherService(settings, threadPool);
    }

    public void testSimpleDynamicSettingsUpdate() throws IOException {
        Settings settings = Settings.builder().put("cluster.remote.my_remote.signing.keystore.alias", "mykey").build();
        when(crossClusterApiKeySigner.getDependentFilesToClusterAliases()).thenReturn(Map.of());
        var clusterSettings = new ClusterSettings(settings, new HashSet<>(CrossClusterApiKeySignerSettings.getDynamicSettings()));

        new CrossClusterApiKeySignerReloader(resourceWatcherService, clusterSettings, crossClusterApiKeySigner);
        clusterSettings.applySettings(Settings.builder().put("cluster.remote.my_remote.signing.keystore.alias", "anotherkey").build());
        verify(crossClusterApiKeySigner).loadSigningConfig(
            "my_remote",
            Settings.builder()
                .put("cluster.remote.my_remote.signing.keystore.alias", "anotherkey")
                .build()
                .getByPrefix("cluster.remote.my_remote."),
            false
        );
        verify(crossClusterApiKeySigner, times(2)).getDependentFilesToClusterAliases();
    }

    public void testDynamicSettingsUpdateWithAddedFile() throws Exception {
        var fileToMonitor = createTempFile();
        Settings settings = Settings.builder().put("cluster.remote.my_remote.signing.keystore.alias", "mykey").build();
        when(crossClusterApiKeySigner.getDependentFilesToClusterAliases()).thenReturn(Map.of())
            .thenReturn(Map.of(fileToMonitor, Set.of("my_remote")));

        var clusterSettings = new ClusterSettings(settings, new HashSet<>(CrossClusterApiKeySignerSettings.getDynamicSettings()));
        new CrossClusterApiKeySignerReloader(resourceWatcherService, clusterSettings, crossClusterApiKeySigner);

        clusterSettings.applySettings(
            Settings.builder()
                .put("cluster.remote.my_remote.signing.keystore.alias", "mykey")
                .put("cluster.remote.my_remote.signing.keystore.path", fileToMonitor)
                .build()
        );

        verify(crossClusterApiKeySigner).loadSigningConfig(
            "my_remote",
            Settings.builder()
                .put("cluster.remote.my_remote.signing.keystore.alias", "mykey")
                .put("cluster.remote.my_remote.signing.keystore.path", fileToMonitor)
                .build()
                .getByPrefix("cluster.remote.my_remote."),
            false
        );
        verify(crossClusterApiKeySigner, times(2)).getDependentFilesToClusterAliases();
        verify(crossClusterApiKeySigner, times(0)).reloadSigningConfigs(Set.of("my_remote"));
        Files.writeString(fileToMonitor, "some content");
        assertBusy(() -> verify(crossClusterApiKeySigner, times(1)).reloadSigningConfigs(Set.of("my_remote")));
    }

    public void testSimpleSecureSettingsReload() {
        when(crossClusterApiKeySigner.getDependentFilesToClusterAliases()).thenReturn(Map.of());
        var clusterSettings = new ClusterSettings(Settings.EMPTY, new HashSet<>(CrossClusterApiKeySignerSettings.getDynamicSettings()));
        var reloader = new CrossClusterApiKeySignerReloader(resourceWatcherService, clusterSettings, crossClusterApiKeySigner);

        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("cluster.remote.my_remote.signing.keystore.secure_password", "secret");
        Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
        reloader.reload(settings);

        verify(crossClusterApiKeySigner).loadSigningConfig("my_remote", settings.getByPrefix("cluster.remote.my_remote."), true);
        verify(crossClusterApiKeySigner, times(1)).getDependentFilesToClusterAliases();
    }

    public void testSecureSettingsReloadNoMatchingSecureSettings() {
        when(crossClusterApiKeySigner.getDependentFilesToClusterAliases()).thenReturn(Map.of());
        var clusterSettings = new ClusterSettings(Settings.EMPTY, new HashSet<>(CrossClusterApiKeySignerSettings.getDynamicSettings()));
        var reloader = new CrossClusterApiKeySignerReloader(resourceWatcherService, clusterSettings, crossClusterApiKeySigner);

        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("not.a.setting", "secret");
        Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
        reloader.reload(settings);

        verify(crossClusterApiKeySigner, times(0)).loadSigningConfig(any(), any(), anyBoolean());
        verify(crossClusterApiKeySigner, times(1)).getDependentFilesToClusterAliases();
    }

    public void testFileUpdatedReloaded() throws Exception {
        var fileToMonitor = createTempFile();
        Settings settings = Settings.builder().put("cluster.remote.my_remote.signing.keystore.path", fileToMonitor).build();
        when(crossClusterApiKeySigner.getDependentFilesToClusterAliases()).thenReturn(Map.of(fileToMonitor, Set.of("my_remote")));

        var clusterSettings = new ClusterSettings(settings, new HashSet<>(CrossClusterApiKeySignerSettings.getDynamicSettings()));
        new CrossClusterApiKeySignerReloader(resourceWatcherService, clusterSettings, crossClusterApiKeySigner);

        verify(crossClusterApiKeySigner, times(0)).loadSigningConfig(anyString(), any(), anyBoolean());
        verify(crossClusterApiKeySigner, times(1)).getDependentFilesToClusterAliases();
        Files.writeString(fileToMonitor, "some content");
        assertBusy(() -> verify(crossClusterApiKeySigner, times(1)).reloadSigningConfigs(Set.of("my_remote")));
    }

    public void testFileDeletedReloaded() throws Exception {
        var fileToMonitor = createTempFile();
        Settings settings = Settings.builder().put("cluster.remote.my_remote.signing.keystore.path", fileToMonitor).build();
        when(crossClusterApiKeySigner.getDependentFilesToClusterAliases()).thenReturn(Map.of(fileToMonitor, Set.of("my_remote")));

        var clusterSettings = new ClusterSettings(settings, new HashSet<>(CrossClusterApiKeySignerSettings.getDynamicSettings()));
        new CrossClusterApiKeySignerReloader(resourceWatcherService, clusterSettings, crossClusterApiKeySigner);

        verify(crossClusterApiKeySigner, times(0)).loadSigningConfig(anyString(), any(), anyBoolean());
        verify(crossClusterApiKeySigner, times(1)).getDependentFilesToClusterAliases();
        Files.delete(fileToMonitor);
        assertBusy(() -> verify(crossClusterApiKeySigner, times(1)).reloadSigningConfigs(Set.of("my_remote")));
    }

    @After
    public void tearDownThreadPool() {
        terminate(threadPool);
    }

}
