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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CrossClusterApiKeySigningConfigReloaderTests extends ESTestCase {
    private CrossClusterApiKeySignatureManager crossClusterApiKeySignatureManager;
    private ResourceWatcherService resourceWatcherService;
    private ThreadPool threadPool;
    private Settings.Builder settingsBuilder;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        crossClusterApiKeySignatureManager = mock(CrossClusterApiKeySignatureManager.class);
        when(crossClusterApiKeySignatureManager.getDependentSigningFiles(any())).thenReturn(List.of());
        Settings settings = Settings.builder().put("resource.reload.interval.high", TimeValue.timeValueMillis(100)).build();
        threadPool = new TestThreadPool(getTestName());
        resourceWatcherService = new ResourceWatcherService(settings, threadPool);
        settingsBuilder = Settings.builder()
            .put("path.home", createTempDir())
            .put(Node.NODE_NAME_SETTING.getKey(), randomAlphaOfLengthBetween(3, 8));
    }

    public void testSimpleDynamicSettingsUpdate() {
        Settings settings = settingsBuilder.put("cluster.remote.my_remote.signing.keystore.alias", "mykey").build();
        var clusterSettings = new ClusterSettings(settings, new HashSet<>(CrossClusterApiKeySigningSettings.getDynamicSigningSettings()));

        var crossClusterApiKeySigningConfigReloader = new CrossClusterApiKeySigningConfigReloader(
            TestEnvironment.newEnvironment(settings),
            resourceWatcherService,
            clusterSettings
        );
        crossClusterApiKeySigningConfigReloader.setSigningConfigLoader(crossClusterApiKeySignatureManager);
        clusterSettings.applySettings(Settings.builder().put("cluster.remote.my_remote.signing.keystore.alias", "anotherkey").build());

        verify(crossClusterApiKeySignatureManager, times(1)).reload(
            "my_remote",
            Settings.builder()
                .put("cluster.remote.my_remote.signing.keystore.alias", "anotherkey")
                .build()
                .getByPrefix("cluster.remote.my_remote.")
        );
    }

    public void testDynamicSettingsUpdateWithAddedFiles() throws Exception {
        var clusterNames = new String[] { "my_remote0", "my_remote1", "my_remote2" };
        var filesToMonitor = new Path[] { createTempFile(), createTempFile(), createTempFile() };
        var trustFile = createTempFile();
        var remoteClusterSettings = new Settings[filesToMonitor.length];
        var environment = TestEnvironment.newEnvironment(settingsBuilder.build());
        var dynamicSettingsUpdate = Settings.builder()
            .put("cluster.remote.my_remote0.signing.keystore.alias", "mykey")
            .put("cluster.remote.my_remote0.signing.keystore.path", filesToMonitor[0])
            .put("cluster.remote.my_remote1.signing.keystore.alias", "mykey")
            .put("cluster.remote.my_remote1.signing.keystore.path", filesToMonitor[1])
            .put("cluster.remote.my_remote2.signing.keystore.alias", "mykey")
            .put("cluster.remote.my_remote2.signing.keystore.path", filesToMonitor[2])
            .put("cluster.remote.signing.truststore.path", trustFile)
            .build();

        var clusterSettings = new ClusterSettings(
            settingsBuilder.build(),
            new HashSet<>(CrossClusterApiKeySigningSettings.getDynamicSigningSettings())
        );

        var crossClusterApiKeySigningConfigReloader = new CrossClusterApiKeySigningConfigReloader(
            environment,
            resourceWatcherService,
            clusterSettings
        );
        var crossClusterApiKeySigner = mock(CrossClusterApiKeySignatureManager.class);

        for (int i = 0; i < clusterNames.length; i++) {
            remoteClusterSettings[i] = Settings.builder()
                .put("cluster.remote." + clusterNames[i] + ".signing.keystore.alias", "mykey")
                .put("cluster.remote." + clusterNames[i] + ".signing.keystore.path", filesToMonitor[i])
                .build();
            when(crossClusterApiKeySigner.getDependentSigningFiles(clusterNames[i])).thenReturn(List.of(filesToMonitor[i]));
            when(crossClusterApiKeySigner.getDependentSigningFiles(clusterNames[i])).thenReturn(List.of(filesToMonitor[i]));
        }

        when(crossClusterApiKeySigner.getDependentTrustFiles()).thenReturn(List.of(trustFile));

        crossClusterApiKeySigningConfigReloader.setSigningConfigLoader(crossClusterApiKeySigner);
        clusterSettings.applySettings(dynamicSettingsUpdate);

        for (int i = 0; i < clusterNames.length; i++) {
            final String clusterName = "my_remote" + i;
            var remoteClusterSetting = remoteClusterSettings[i].getByPrefix("cluster.remote." + clusterName + ".");
            verify(crossClusterApiKeySigner, times(1)).reload(clusterName, remoteClusterSetting);
            Files.writeString(filesToMonitor[i], "some content");
            assertBusy(() -> verify(crossClusterApiKeySigner, times(2)).reload(clusterName, remoteClusterSetting));
        }

        verify(crossClusterApiKeySigner, times(1)).reload(Settings.builder().put("signing.truststore.path", trustFile).build());

        Files.writeString(trustFile, "some content");
        assertBusy(
            () -> verify(crossClusterApiKeySigner, times(2)).reload(Settings.builder().put("signing.truststore.path", trustFile).build())
        );
    }

    public void testSimpleSecureSettingsReload() {
        var clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            new HashSet<>(CrossClusterApiKeySigningSettings.getDynamicSigningSettings())
        );
        var environment = TestEnvironment.newEnvironment(settingsBuilder.build());
        var crossClusterApiKeySigningConfigReloader = new CrossClusterApiKeySigningConfigReloader(
            environment,
            resourceWatcherService,
            clusterSettings
        );

        crossClusterApiKeySigningConfigReloader.setSigningConfigLoader(crossClusterApiKeySignatureManager);

        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("cluster.remote.my_remote.signing.keystore.secure_password", "secret");
        Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
        crossClusterApiKeySigningConfigReloader.reload(settings);

        verify(crossClusterApiKeySignatureManager).reload("my_remote", settings.getByPrefix("cluster.remote.my_remote."));
    }

    public void testSecureSettingsReloadNoMatchingSecureSettings() {
        var clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            new HashSet<>(CrossClusterApiKeySigningSettings.getDynamicSigningSettings())
        );
        var environment = TestEnvironment.newEnvironment(settingsBuilder.build());
        var crossClusterApiKeySigningConfigReloader = new CrossClusterApiKeySigningConfigReloader(
            environment,
            resourceWatcherService,
            clusterSettings
        );
        crossClusterApiKeySigningConfigReloader.setSigningConfigLoader(crossClusterApiKeySignatureManager);

        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("not.a.setting", "secret");
        Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
        crossClusterApiKeySigningConfigReloader.reload(settings);

        verify(crossClusterApiKeySignatureManager, times(0)).reload(any(), any());
    }

    public void testFileUpdatedReloaded() throws Exception {
        var fileToMonitor = createTempFile();
        var clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            new HashSet<>(CrossClusterApiKeySigningSettings.getDynamicSigningSettings())
        );
        var initialSettings = settingsBuilder.put("cluster.remote.my_remote.signing.keystore.path", fileToMonitor).build();
        var environment = TestEnvironment.newEnvironment(initialSettings);
        var crossClusterApiKeySigningConfigReloader = new CrossClusterApiKeySigningConfigReloader(
            environment,
            resourceWatcherService,
            clusterSettings
        );

        crossClusterApiKeySigningConfigReloader.setSigningConfigLoader(crossClusterApiKeySignatureManager);

        verify(crossClusterApiKeySignatureManager, times(0)).reload(anyString(), any());
        Files.writeString(fileToMonitor, "some content");
        assertBusy(
            () -> verify(crossClusterApiKeySignatureManager, times(1)).reload(
                "my_remote",
                initialSettings.getByPrefix("cluster.remote.my_remote.")
            )
        );
    }

    public void testFileDeletedReloaded() throws Exception {
        var fileToMonitor = createTempFile();
        var clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            new HashSet<>(CrossClusterApiKeySigningSettings.getDynamicSigningSettings())
        );
        var initialSettings = settingsBuilder.put("cluster.remote.my_remote.signing.keystore.path", fileToMonitor).build();
        var environment = TestEnvironment.newEnvironment(initialSettings);
        var crossClusterApiKeySigningConfigReloader = new CrossClusterApiKeySigningConfigReloader(
            environment,
            resourceWatcherService,
            clusterSettings
        );

        crossClusterApiKeySigningConfigReloader.setSigningConfigLoader(crossClusterApiKeySignatureManager);

        verify(crossClusterApiKeySignatureManager, times(0)).reload(anyString(), any());
        Files.delete(fileToMonitor);
        assertBusy(
            () -> verify(crossClusterApiKeySignatureManager, times(1)).reload(
                "my_remote",
                initialSettings.getByPrefix("cluster.remote.my_remote.")
            )
        );
    }

    @After
    public void tearDownThreadPool() {
        terminate(threadPool);
    }

}
