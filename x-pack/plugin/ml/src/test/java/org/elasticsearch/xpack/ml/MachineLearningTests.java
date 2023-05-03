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
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsAction;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.action.MlInfoAction;
import org.elasticsearch.xpack.core.ml.action.SetUpgradeModeAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.ml.rest.RestMlInfoAction;
import org.elasticsearch.xpack.ml.rest.dataframe.RestGetDataFrameAnalyticsAction;
import org.elasticsearch.xpack.ml.rest.inference.RestGetTrainedModelsAction;
import org.elasticsearch.xpack.ml.rest.inference.RestStartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.ml.rest.job.RestGetJobsAction;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
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
    public void testPrePostSystemIndexUpgrade_givenNotInUpgradeMode() throws IOException {
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

        try (MachineLearning machineLearning = createTrialLicensedMachineLearning(Settings.EMPTY)) {

            SetOnce<Map<String, Object>> response = new SetOnce<>();
            machineLearning.prepareForIndicesMigration(
                clusterService,
                client,
                ActionListener.wrap(response::set, e -> fail(e.getMessage()))
            );

            assertThat(response.get(), equalTo(Collections.singletonMap("already_in_upgrade_mode", false)));
            verify(client).execute(
                same(SetUpgradeModeAction.INSTANCE),
                eq(new SetUpgradeModeAction.Request(true)),
                any(ActionListener.class)
            );

            machineLearning.indicesMigrationComplete(
                response.get(),
                clusterService,
                client,
                ActionListener.wrap(ESTestCase::assertTrue, e -> fail(e.getMessage()))
            );

            verify(client).execute(
                same(SetUpgradeModeAction.INSTANCE),
                eq(new SetUpgradeModeAction.Request(false)),
                any(ActionListener.class)
            );
        } finally {
            threadpool.shutdown();
        }
    }

    public void testPrePostSystemIndexUpgrade_givenAlreadyInUpgradeMode() throws IOException {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(
            ClusterState.builder(ClusterName.DEFAULT)
                .metadata(Metadata.builder().putCustom(MlMetadata.TYPE, new MlMetadata.Builder().isUpgradeMode(true).build()))
                .build()
        );
        Client client = mock(Client.class);

        try (MachineLearning machineLearning = createTrialLicensedMachineLearning(Settings.EMPTY)) {

            SetOnce<Map<String, Object>> response = new SetOnce<>();
            machineLearning.prepareForIndicesMigration(
                clusterService,
                client,
                ActionListener.wrap(response::set, e -> fail(e.getMessage()))
            );

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

    public void testNoAttributes_givenNoClash() throws IOException {
        Settings.Builder builder = Settings.builder();
        if (randomBoolean()) {
            builder.put("xpack.ml.enabled", randomBoolean());
        }
        if (randomBoolean()) {
            builder.put("xpack.ml.max_open_jobs", randomIntBetween(9, 12));
        }
        builder.put("node.attr.foo", "abc");
        builder.put("node.attr.ml.bar", "def");
        try (MachineLearning machineLearning = createTrialLicensedMachineLearning(builder.put("path.home", createTempDir()).build())) {
            assertNotNull(machineLearning.additionalSettings());
        }
    }

    public void testNoAttributes_givenSameAndMlEnabled() throws IOException {
        Settings.Builder builder = Settings.builder();
        if (randomBoolean()) {
            builder.put("xpack.ml.enabled", randomBoolean());
        }
        if (randomBoolean()) {
            int maxOpenJobs = randomIntBetween(5, 15);
            builder.put("xpack.ml.max_open_jobs", maxOpenJobs);
        }
        try (MachineLearning machineLearning = createTrialLicensedMachineLearning(builder.put("path.home", createTempDir()).build())) {
            assertNotNull(machineLearning.additionalSettings());
        }
    }

    public void testNoAttributes_givenClash() throws IOException {
        Settings.Builder builder = Settings.builder();
        builder.put("node.attr.ml.max_open_jobs", randomIntBetween(13, 15));
        try (MachineLearning machineLearning = createTrialLicensedMachineLearning(builder.put("path.home", createTempDir()).build())) {
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
    }

    public void testAnomalyDetectionOnly() throws IOException {
        Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        try (MachineLearning machineLearning = createTrialLicensedMachineLearning(settings)) {
            MlTestExtensionLoader loader = new MlTestExtensionLoader(new MlTestExtension(false, false, true, false, false));
            machineLearning.loadExtensions(loader);
            List<RestHandler> restHandlers = machineLearning.getRestHandlers(settings, null, null, null, null, null, null);
            assertThat(restHandlers, hasItem(instanceOf(RestMlInfoAction.class)));
            assertThat(restHandlers, hasItem(instanceOf(RestGetJobsAction.class)));
            assertThat(restHandlers, not(hasItem(instanceOf(RestGetTrainedModelsAction.class))));
            assertThat(restHandlers, not(hasItem(instanceOf(RestGetDataFrameAnalyticsAction.class))));
            assertThat(restHandlers, not(hasItem(instanceOf(RestStartTrainedModelDeploymentAction.class))));
            List<?> actions = machineLearning.getActions().stream().map(ActionPlugin.ActionHandler::getAction).toList();
            assertThat(actions, hasItem(instanceOf(XPackUsageFeatureAction.class)));
            assertThat(actions, hasItem(instanceOf(MlInfoAction.class)));
            assertThat(actions, hasItem(instanceOf(GetJobsAction.class)));
            assertThat(actions, not(hasItem(instanceOf(GetTrainedModelsAction.class))));
            assertThat(actions, not(hasItem(instanceOf(GetDataFrameAnalyticsAction.class))));
            assertThat(actions, not(hasItem(instanceOf(StartTrainedModelDeploymentAction.class))));
        }
    }

    public void testDataFrameAnalyticsOnly() throws IOException {
        Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        try (MachineLearning machineLearning = createTrialLicensedMachineLearning(settings)) {
            MlTestExtensionLoader loader = new MlTestExtensionLoader(new MlTestExtension(false, false, false, true, false));
            machineLearning.loadExtensions(loader);
            List<RestHandler> restHandlers = machineLearning.getRestHandlers(settings, null, null, null, null, null, null);
            assertThat(restHandlers, hasItem(instanceOf(RestMlInfoAction.class)));
            assertThat(restHandlers, not(hasItem(instanceOf(RestGetJobsAction.class))));
            assertThat(restHandlers, hasItem(instanceOf(RestGetTrainedModelsAction.class)));
            assertThat(restHandlers, hasItem(instanceOf(RestGetDataFrameAnalyticsAction.class)));
            assertThat(restHandlers, not(hasItem(instanceOf(RestStartTrainedModelDeploymentAction.class))));
            List<?> actions = machineLearning.getActions().stream().map(ActionPlugin.ActionHandler::getAction).toList();
            assertThat(actions, hasItem(instanceOf(XPackUsageFeatureAction.class)));
            assertThat(actions, hasItem(instanceOf(MlInfoAction.class)));
            assertThat(actions, not(hasItem(instanceOf(GetJobsAction.class))));
            assertThat(actions, hasItem(instanceOf(GetTrainedModelsAction.class)));
            assertThat(actions, hasItem(instanceOf(GetDataFrameAnalyticsAction.class)));
            assertThat(actions, not(hasItem(instanceOf(StartTrainedModelDeploymentAction.class))));
        }
    }

    public void testNlpOnly() throws IOException {
        Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        try (MachineLearning machineLearning = createTrialLicensedMachineLearning(settings)) {
            MlTestExtensionLoader loader = new MlTestExtensionLoader(new MlTestExtension(false, false, false, false, true));
            machineLearning.loadExtensions(loader);
            List<RestHandler> restHandlers = machineLearning.getRestHandlers(settings, null, null, null, null, null, null);
            assertThat(restHandlers, hasItem(instanceOf(RestMlInfoAction.class)));
            assertThat(restHandlers, not(hasItem(instanceOf(RestGetJobsAction.class))));
            assertThat(restHandlers, hasItem(instanceOf(RestGetTrainedModelsAction.class)));
            assertThat(restHandlers, not(hasItem(instanceOf(RestGetDataFrameAnalyticsAction.class))));
            assertThat(restHandlers, hasItem(instanceOf(RestStartTrainedModelDeploymentAction.class)));
            List<?> actions = machineLearning.getActions().stream().map(ActionPlugin.ActionHandler::getAction).toList();
            assertThat(actions, hasItem(instanceOf(XPackUsageFeatureAction.class)));
            assertThat(actions, hasItem(instanceOf(MlInfoAction.class)));
            assertThat(actions, not(hasItem(instanceOf(GetJobsAction.class))));
            assertThat(actions, hasItem(instanceOf(GetTrainedModelsAction.class)));
            assertThat(actions, not(hasItem(instanceOf(GetDataFrameAnalyticsAction.class))));
            assertThat(actions, hasItem(instanceOf(StartTrainedModelDeploymentAction.class)));
        }
    }

    public static class MlTestExtension implements MachineLearningExtension {

        private final boolean useIlm;
        private final boolean includeNodeInfo;
        private final boolean isAnomalyDetectionEnabled;
        private final boolean isDataFrameAnalyticsEnabled;
        private final boolean isNlpEnabled;

        MlTestExtension(
            boolean useIlm,
            boolean includeNodeInfo,
            boolean isAnomalyDetectionEnabled,
            boolean isDataFrameAnalyticsEnabled,
            boolean isNlpEnabled
        ) {
            this.useIlm = useIlm;
            this.includeNodeInfo = includeNodeInfo;
            this.isAnomalyDetectionEnabled = isAnomalyDetectionEnabled;
            this.isDataFrameAnalyticsEnabled = isDataFrameAnalyticsEnabled;
            this.isNlpEnabled = isNlpEnabled;
        }

        @Override
        public boolean useIlm() {
            return useIlm;
        }

        @Override
        public boolean includeNodeInfo() {
            return includeNodeInfo;
        }

        @Override
        public boolean isAnomalyDetectionEnabled() {
            return isAnomalyDetectionEnabled;
        }

        @Override
        public boolean isDataFrameAnalyticsEnabled() {
            return isDataFrameAnalyticsEnabled;
        }

        @Override
        public boolean isNlpEnabled() {
            return isNlpEnabled;
        }
    }

    public static class MlTestExtensionLoader implements ExtensiblePlugin.ExtensionLoader {

        private final MachineLearningExtension extension;

        MlTestExtensionLoader(MachineLearningExtension extension) {
            this.extension = extension;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> List<T> loadExtensions(Class<T> extensionPointType) {
            if (extensionPointType.isAssignableFrom(MachineLearningExtension.class)) {
                return List.of((T) extension);
            } else {
                return List.of();
            }
        }
    }

    public static class TrialLicensedMachineLearning extends MachineLearning {

        // A license state constructed like this is considered a trial license
        XPackLicenseState licenseState = new XPackLicenseState(() -> 0L);

        public TrialLicensedMachineLearning(Settings settings) {
            super(settings);
        }

        @Override
        protected XPackLicenseState getLicenseState() {
            return licenseState;
        }
    }

    public static MachineLearning createTrialLicensedMachineLearning(Settings settings) {
        return new TrialLicensedMachineLearning(settings);
    }
}
