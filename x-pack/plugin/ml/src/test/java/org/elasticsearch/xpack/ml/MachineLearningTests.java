/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.SetUpgradeModeAction;

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
        builder.put("node.attr.ml.max_open_jobs", randomIntBetween(13, 15));
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

    private MachineLearning createMachineLearning(Settings settings) {
        XPackLicenseState licenseState = mock(XPackLicenseState.class);

        return new MachineLearning(settings) {
            @Override
            protected XPackLicenseState getLicenseState() {
                return licenseState;
            }
        };
    }
}
