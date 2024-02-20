/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.SetUpgradeModeAction;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class MachineLearningTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    public void testPrePostSystemIndexUpgrade_givenNotInUpgradeMode() {
        ThreadPool threadpool = new TestThreadPool("test");
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        Client client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadpool);
        when(client.settings()).thenReturn(Settings.EMPTY);
        doAnswer(invocationOnMock -> {
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocationOnMock.getArguments()[2];
            listener.onResponse(AcknowledgedResponse.TRUE);
            return null;
        }).when(client).execute(same(SetUpgradeModeAction.INSTANCE), any(SetUpgradeModeAction.Request.class), any(ActionListener.class));

        MachineLearning machineLearning = createMachineLearning(Settings.EMPTY);

        SetOnce<Map<String, Object>> response = new SetOnce<>();
        machineLearning.prepareForIndicesMigration(clusterService, client, ActionListener.wrap(response::set, e -> fail(e.getMessage())));

        assertThat(response.get(), equalTo(Collections.singletonMap("already_in_upgrade_mode", false)));
        verify(client).execute(same(SetUpgradeModeAction.INSTANCE), eq(new SetUpgradeModeAction.Request(true)), any(ActionListener.class));

        machineLearning.indicesMigrationComplete(
            response.get(),
            clusterService,
            client,
            ActionListener.wrap(ESTestCase::assertTrue, e -> fail(e.getMessage()))
        );

        verify(client).execute(same(SetUpgradeModeAction.INSTANCE), eq(new SetUpgradeModeAction.Request(false)), any(ActionListener.class));

        threadpool.shutdown();
    }

    public void testPrePostSystemIndexUpgrade_givenAlreadyInUpgradeMode() {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(
            ClusterState.builder(ClusterName.DEFAULT)
                .metadata(Metadata.builder().putCustom(MlMetadata.TYPE, new MlMetadata.Builder().isUpgradeMode(true).build()))
                .build()
        );
        Client client = mock(Client.class);

        MachineLearning machineLearning = createMachineLearning(Settings.EMPTY);

        SetOnce<Map<String, Object>> response = new SetOnce<>();
        machineLearning.prepareForIndicesMigration(clusterService, client, ActionListener.wrap(response::set, e -> fail(e.getMessage())));

        assertThat(response.get(), equalTo(Collections.singletonMap("already_in_upgrade_mode", true)));
        verifyNoMoreInteractions(client);

        machineLearning.indicesMigrationComplete(
            response.get(),
            clusterService,
            client,
            ActionListener.wrap(ESTestCase::assertTrue, e -> fail(e.getMessage()))
        );

        // Neither pre nor post should have called any action
        verifyNoMoreInteractions(client);
    }

    public void testMaxOpenWorkersSetting_givenDefault() {
        int maxOpenWorkers = MachineLearning.MAX_OPEN_JOBS_PER_NODE.get(Settings.EMPTY);
        assertEquals(512, maxOpenWorkers);
    }

    public void testMaxOpenWorkersSetting_givenSetting() {
        Settings.Builder settings = Settings.builder();
        settings.put(MachineLearning.MAX_OPEN_JOBS_PER_NODE.getKey(), 7);
        int maxOpenWorkers = MachineLearning.MAX_OPEN_JOBS_PER_NODE.get(settings.build());
        assertEquals(7, maxOpenWorkers);
    }

    public void testMaxMachineMemoryPercent_givenDefault() {
        int maxMachineMemoryPercent = MachineLearning.MAX_MACHINE_MEMORY_PERCENT.get(Settings.EMPTY);
        assertEquals(30, maxMachineMemoryPercent);
    }

    public void testMaxMachineMemoryPercent_givenValidSetting() {
        Settings.Builder settings = Settings.builder();
        int expectedMaxMachineMemoryPercent = randomIntBetween(5, 200);
        settings.put(MachineLearning.MAX_MACHINE_MEMORY_PERCENT.getKey(), expectedMaxMachineMemoryPercent);
        int maxMachineMemoryPercent = MachineLearning.MAX_MACHINE_MEMORY_PERCENT.get(settings.build());
        assertEquals(expectedMaxMachineMemoryPercent, maxMachineMemoryPercent);
    }

    public void testMaxMachineMemoryPercent_givenInvalidSetting() {
        Settings.Builder settings = Settings.builder();
        int invalidMaxMachineMemoryPercent = randomFrom(4, 201);
        settings.put(MachineLearning.MAX_MACHINE_MEMORY_PERCENT.getKey(), invalidMaxMachineMemoryPercent);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> MachineLearning.MAX_MACHINE_MEMORY_PERCENT.get(settings.build())
        );
        assertThat(
            e.getMessage(),
            startsWith(
                "Failed to parse value [" + invalidMaxMachineMemoryPercent + "] for setting [xpack.ml.max_machine_memory_percent] must be"
            )
        );
    }

    public void testNoAttributes_givenNoClash() {
        Settings.Builder builder = Settings.builder();
        if (randomBoolean()) {
            builder.put("xpack.ml.enabled", randomBoolean());
        }
        if (randomBoolean()) {
            builder.put("xpack.ml.max_open_jobs", randomIntBetween(9, 12));
        }
        builder.put("node.attr.foo", "abc");
        builder.put("node.attr.ml.bar", "def");
        MachineLearning machineLearning = createMachineLearning(builder.put("path.home", createTempDir()).build());
        assertNotNull(machineLearning.additionalSettings());
    }

    public void testNoAttributes_givenSameAndMlEnabled() {
        Settings.Builder builder = Settings.builder();
        if (randomBoolean()) {
            builder.put("xpack.ml.enabled", randomBoolean());
        }
        if (randomBoolean()) {
            int maxOpenJobs = randomIntBetween(5, 15);
            builder.put("xpack.ml.max_open_jobs", maxOpenJobs);
        }
        MachineLearning machineLearning = createMachineLearning(builder.put("path.home", createTempDir()).build());
        assertNotNull(machineLearning.additionalSettings());
    }

    public void testNoAttributes_givenClash() {
        Settings.Builder builder = Settings.builder();
        if (randomBoolean()) {
            builder.put("node.attr.ml.enabled", randomBoolean());
        } else {
            builder.put("node.attr.ml.max_open_jobs", randomIntBetween(13, 15));
        }
        MachineLearning machineLearning = createMachineLearning(builder.put("path.home", createTempDir()).build());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, machineLearning::additionalSettings);
        assertThat(e.getMessage(), startsWith("Directly setting [node.attr.ml."));
        assertThat(
            e.getMessage(),
            containsString(
                "] is not permitted - "
                    + "it is reserved for machine learning. If your intention was to customize machine learning, set the [xpack.ml."
            )
        );
    }

    public void testMachineMemory_givenStatsFailure() throws IOException {
        OsStats stats = mock(OsStats.class);
        when(stats.getMem()).thenReturn(new OsStats.Mem(0, 0));
        assertEquals(0L, MachineLearning.machineMemoryFromStats(stats));
    }

    public void testMachineMemory_givenNoCgroup() throws IOException {
        OsStats stats = mock(OsStats.class);
        when(stats.getMem()).thenReturn(new OsStats.Mem(10_737_418_240L, 5_368_709_120L));
        assertEquals(10_737_418_240L, MachineLearning.machineMemoryFromStats(stats));
    }

    public void testMachineMemory_givenCgroupNullLimit() throws IOException {
        OsStats stats = mock(OsStats.class);
        when(stats.getMem()).thenReturn(new OsStats.Mem(10_737_418_240L, 5_368_709_120L));
        when(stats.getCgroup()).thenReturn(new OsStats.Cgroup("a", 1, "b", 2, 3, new OsStats.Cgroup.CpuStat(4, 5, 6), null, null, null));
        assertEquals(10_737_418_240L, MachineLearning.machineMemoryFromStats(stats));
    }

    public void testMachineMemory_givenCgroupNoLimit() throws IOException {
        OsStats stats = mock(OsStats.class);
        when(stats.getMem()).thenReturn(new OsStats.Mem(10_737_418_240L, 5_368_709_120L));
        when(stats.getCgroup()).thenReturn(
            new OsStats.Cgroup("a", 1, "b", 2, 3, new OsStats.Cgroup.CpuStat(4, 5, 6), "c", "18446744073709551615", "4796416")
        );
        assertEquals(10_737_418_240L, MachineLearning.machineMemoryFromStats(stats));
    }

    public void testMachineMemory_givenCgroupLowLimit() throws IOException {
        OsStats stats = mock(OsStats.class);
        when(stats.getMem()).thenReturn(new OsStats.Mem(10_737_418_240L, 5_368_709_120L));
        when(stats.getCgroup()).thenReturn(
            new OsStats.Cgroup("a", 1, "b", 2, 3, new OsStats.Cgroup.CpuStat(4, 5, 6), "c", "7516192768", "4796416")
        );
        assertEquals(7_516_192_768L, MachineLearning.machineMemoryFromStats(stats));
    }

    public void testIsMlNode_given812MlNode() {
        DiscoveryNode mlNode812 = new DiscoveryNode(
            "name",
            "id",
            "ephemeralId",
            "hostName",
            "hostAddress",
            new TransportAddress(InetAddress.getLoopbackAddress(), randomIntBetween(1024, 65535)),
            Collections.emptyMap(),
            Collections.singleton(MachineLearning.ML_ROLE),
            Version.fromString("8.12.0")
        );
        assertTrue(MachineLearning.isMlNode(mlNode812));
    }

    public void testIsMlNode_given717MlNode() {
        DiscoveryNode mlNode717 = new DiscoveryNode(
            "name",
            "id",
            "ephemeralId",
            "hostName",
            "hostAddress",
            new TransportAddress(InetAddress.getLoopbackAddress(), randomIntBetween(1024, 65535)),
            Collections.singletonMap(MachineLearning.MAX_OPEN_JOBS_NODE_ATTR, "512"),
            Collections.singleton(MachineLearning.ML_ROLE),
            Version.V_7_17_0
        );
        assertTrue(MachineLearning.isMlNode(mlNode717));
    }

    public void testIsMlNode_given70MlNode() {
        DiscoveryNode mlNode70 = new DiscoveryNode(
            "name",
            "id",
            "ephemeralId",
            "hostName",
            "hostAddress",
            new TransportAddress(InetAddress.getLoopbackAddress(), randomIntBetween(1024, 65535)),
            Collections.singletonMap(MachineLearning.MAX_OPEN_JOBS_NODE_ATTR, "512"),
            Collections.emptySet(),
            Version.V_7_0_0
        );
        assertTrue(MachineLearning.isMlNode(mlNode70));
    }

    private MachineLearning createMachineLearning(Settings settings) {
        XPackLicenseState licenseState = mock(XPackLicenseState.class);

        return new MachineLearning(settings, null) {
            @Override
            protected XPackLicenseState getLicenseState() {
                return licenseState;
            }
        };
    }
}
