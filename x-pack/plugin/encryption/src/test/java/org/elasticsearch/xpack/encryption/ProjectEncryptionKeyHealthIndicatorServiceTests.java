/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.health.node.HealthInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceState;

import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProjectEncryptionKeyHealthIndicatorServiceTests extends ESTestCase {

    private static final String PASSWORD_ID = "v1";
    private static final ProjectEncryptionKeyMetadata.PekEncryption NO_OP_ENCRYPTION = TestPekEncryption.NO_OP;

    private static byte[] randomKeyBytes() {
        return randomByteArrayOfLength(PasswordBasedEncryption.PEK_LENGTH_BYTES);
    }

    private static ProjectEncryptionKeyMetadata pekMetadata(String passwordId) {
        String keyId = ProjectEncryptionKeyMetadata.generateKeyId();
        return new ProjectEncryptionKeyMetadata(
            Map.of(keyId, new ProjectEncryptionKeyMetadata.KeyEntry(randomKeyBytes(), 0L)),
            keyId,
            passwordId,
            Map.of(),
            NO_OP_ENCRYPTION
        );
    }

    private static ClusterState stateWith(ProjectEncryptionKeyMetadata metadata) {
        ProjectMetadata.Builder project = ProjectMetadata.builder(Metadata.DEFAULT_PROJECT_ID);
        if (metadata != null) {
            project.putCustom(ProjectEncryptionKeyMetadata.TYPE, metadata);
        }
        return ClusterState.builder(new ClusterName("test")).metadata(Metadata.builder().put(project.build()).build()).build();
    }

    private static ClusterService mockClusterService(ClusterState state) {
        ClusterService cs = mock(ClusterService.class);
        when(cs.state()).thenReturn(state);
        return cs;
    }

    private static ProjectEncryptionKeyService mockPekService(EncryptionServiceState state, String activePasswordId) {
        return mockPekService(state, activePasswordId, true, true);
    }

    private static ProjectEncryptionKeyService mockPekService(EncryptionServiceState state, String activePasswordId, boolean required) {
        return mockPekService(state, activePasswordId, required, true);
    }

    private static ProjectEncryptionKeyService mockPekService(
        EncryptionServiceState state,
        String activePasswordId,
        boolean required,
        boolean canWrapForDisk
    ) {
        ProjectEncryptionKeyService svc = mock(ProjectEncryptionKeyService.class);
        when(svc.state()).thenReturn(state);
        when(svc.getActivePasswordId()).thenReturn(activePasswordId);
        when(svc.isEncryptionRequired()).thenReturn(required);
        when(svc.canWrapForDisk()).thenReturn(canWrapForDisk);
        return svc;
    }

    public void testGreenWhenNoMetadataAndNoSettings() {
        ClusterState state = stateWith(null);

        ProjectEncryptionKeyHealthIndicatorService indicator = new ProjectEncryptionKeyHealthIndicatorService(
            mockClusterService(state),
            DefaultProjectResolver.INSTANCE,
            mockPekService(EncryptionServiceState.DISABLED, null)
        );

        HealthIndicatorResult result = indicator.calculate(false, 1, HealthInfo.EMPTY_HEALTH_INFO);
        assertEquals(HealthStatus.GREEN, result.status());
        assertEquals(ProjectEncryptionKeyHealthIndicatorService.GREEN_NOT_CONFIGURED, result.symptom());
    }

    public void testGreenWhenMetadataAndPekUnlockable() {
        ProjectEncryptionKeyMetadata metadata = pekMetadata(PASSWORD_ID);
        ClusterState state = stateWith(metadata);

        ProjectEncryptionKeyHealthIndicatorService indicator = new ProjectEncryptionKeyHealthIndicatorService(
            mockClusterService(state),
            DefaultProjectResolver.INSTANCE,
            mockPekService(EncryptionServiceState.READY, PASSWORD_ID)
        );

        HealthIndicatorResult result = indicator.calculate(false, 1, HealthInfo.EMPTY_HEALTH_INFO);
        assertEquals(HealthStatus.GREEN, result.status());
        assertEquals(ProjectEncryptionKeyHealthIndicatorService.GREEN_HEALTHY, result.symptom());
    }

    public void testGreenWhenSettingsConfiguredButMetadataNotYetInstalled() {
        ClusterState state = stateWith(null);

        ProjectEncryptionKeyHealthIndicatorService indicator = new ProjectEncryptionKeyHealthIndicatorService(
            mockClusterService(state),
            DefaultProjectResolver.INSTANCE,
            mockPekService(EncryptionServiceState.READY, PASSWORD_ID)
        );

        HealthIndicatorResult result = indicator.calculate(false, 1, HealthInfo.EMPTY_HEALTH_INFO);
        assertEquals(HealthStatus.GREEN, result.status());
        // Password is configured; we just haven't installed the PEK yet.
        assertTrue(result.symptom().contains("awaiting first key install"));
    }

    public void testYellowWhenDisabledAndEncryptionNotRequired() {
        ClusterState state = stateWith(null);

        ProjectEncryptionKeyHealthIndicatorService indicator = new ProjectEncryptionKeyHealthIndicatorService(
            mockClusterService(state),
            DefaultProjectResolver.INSTANCE,
            mockPekService(EncryptionServiceState.DISABLED, null, false)
        );

        HealthIndicatorResult result = indicator.calculate(true, 1, HealthInfo.EMPTY_HEALTH_INFO);
        assertEquals(HealthStatus.YELLOW, result.status());
        assertEquals(ProjectEncryptionKeyHealthIndicatorService.YELLOW_OPT_OUT, result.symptom());
        assertEquals(1, result.diagnosisList().size());
        assertEquals(ProjectEncryptionKeyHealthIndicatorService.ENCRYPTION_OPT_OUT_DEFINITION, result.diagnosisList().get(0).definition());
        assertEquals(false, detailsMap(result).get("encryption_required"));
    }

    public void testGreenWhenDisabledAndEncryptionRequired() {
        ClusterState state = stateWith(null);

        ProjectEncryptionKeyHealthIndicatorService indicator = new ProjectEncryptionKeyHealthIndicatorService(
            mockClusterService(state),
            DefaultProjectResolver.INSTANCE,
            mockPekService(EncryptionServiceState.DISABLED, null, true)
        );

        HealthIndicatorResult result = indicator.calculate(false, 1, HealthInfo.EMPTY_HEALTH_INFO);
        assertEquals(HealthStatus.GREEN, result.status());
        assertEquals(ProjectEncryptionKeyHealthIndicatorService.GREEN_NOT_CONFIGURED, result.symptom());
    }

    public void testYellowWhenCannotWrapForDisk() {
        ProjectEncryptionKeyMetadata metadata = pekMetadata(PASSWORD_ID);
        ClusterState state = stateWith(metadata);

        ProjectEncryptionKeyHealthIndicatorService indicator = new ProjectEncryptionKeyHealthIndicatorService(
            mockClusterService(state),
            DefaultProjectResolver.INSTANCE,
            mockPekService(EncryptionServiceState.READY, null, true, false)
        );

        HealthIndicatorResult result = indicator.calculate(false, 1, HealthInfo.EMPTY_HEALTH_INFO);
        assertEquals(HealthStatus.YELLOW, result.status());
        assertEquals(ProjectEncryptionKeyHealthIndicatorService.YELLOW_CANNOT_PERSIST, result.symptom());
        assertEquals(1, result.diagnosisList().size());
        assertEquals(
            ProjectEncryptionKeyHealthIndicatorService.MISSING_WRAP_CREDENTIALS_DEFINITION,
            result.diagnosisList().get(0).definition()
        );
        assertEquals(ProjectEncryptionKeyHealthIndicatorService.WRAP_FAILURE_IMPACTS, result.impacts());
    }

    public void testYellowWhenDecryptionFailed() {
        ProjectEncryptionKeyMetadata metadata = pekMetadata(PASSWORD_ID);
        ClusterState state = stateWith(metadata);

        ProjectEncryptionKeyHealthIndicatorService indicator = new ProjectEncryptionKeyHealthIndicatorService(
            mockClusterService(state),
            DefaultProjectResolver.INSTANCE,
            mockPekService(EncryptionServiceState.UNAVAILABLE_DECRYPTION_FAILED, PASSWORD_ID)
        );

        HealthIndicatorResult result = indicator.calculate(true, 1, HealthInfo.EMPTY_HEALTH_INFO);
        assertEquals(HealthStatus.YELLOW, result.status());
        assertEquals(ProjectEncryptionKeyHealthIndicatorService.YELLOW_DECRYPTION_FAILED, result.symptom());
        assertEquals(1, result.diagnosisList().size());
        assertEquals(ProjectEncryptionKeyHealthIndicatorService.DECRYPTION_FAILED_DEFINITION, result.diagnosisList().get(0).definition());
        assertEquals(ProjectEncryptionKeyHealthIndicatorService.ENCRYPTION_UNAVAILABLE_IMPACTS, result.impacts());
    }

    public void testYellowWhenMissingPassword() {
        ClusterState state = stateWith(null);

        ProjectEncryptionKeyHealthIndicatorService indicator = new ProjectEncryptionKeyHealthIndicatorService(
            mockClusterService(state),
            DefaultProjectResolver.INSTANCE,
            mockPekService(EncryptionServiceState.UNAVAILABLE_MISSING_PASSWORD, null)
        );

        HealthIndicatorResult result = indicator.calculate(true, 1, HealthInfo.EMPTY_HEALTH_INFO);
        assertEquals(HealthStatus.YELLOW, result.status());
        assertEquals(ProjectEncryptionKeyHealthIndicatorService.YELLOW_MISSING_PASSWORD, result.symptom());
        assertEquals(1, result.diagnosisList().size());
        assertEquals(ProjectEncryptionKeyHealthIndicatorService.MISSING_PASSWORD_DEFINITION, result.diagnosisList().get(0).definition());
        assertEquals(ProjectEncryptionKeyHealthIndicatorService.ENCRYPTION_UNAVAILABLE_IMPACTS, result.impacts());
    }

    public void testStateDisplayValueAppearsInDetails() {
        ProjectEncryptionKeyMetadata metadata = pekMetadata(PASSWORD_ID);
        ClusterState state = stateWith(metadata);

        ProjectEncryptionKeyHealthIndicatorService indicator = new ProjectEncryptionKeyHealthIndicatorService(
            mockClusterService(state),
            DefaultProjectResolver.INSTANCE,
            mockPekService(EncryptionServiceState.READY, PASSWORD_ID)
        );

        HealthIndicatorResult result = indicator.calculate(true, 1, HealthInfo.EMPTY_HEALTH_INFO);
        assertEquals(EncryptionServiceState.READY.displayValue(), detailsMap(result).get("state"));
    }

    private static Map<String, Object> detailsMap(HealthIndicatorResult result) {
        return ((SimpleHealthIndicatorDetails) result.details()).details();
    }
}
