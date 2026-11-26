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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.health.node.HealthInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceState;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProjectEncryptionKeyHealthIndicatorServiceTests extends ESTestCase {

    private static final String PASSWORD_ID = "v1";
    private static final ProjectEncryptionKeyMetadata.PekEncryption NO_OP_ENCRYPTION = TestPekEncryption.NO_OP;
    // at least 14 characters, as FIPS requires
    private static final Settings PASSWORD_SETTINGS = EncryptionIntegTestUtils.passwordSettings(PASSWORD_ID, "test-password-fips");

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

    public void testYellowWhenDecryptionFailed() {
        ClusterState state = stateWith(pekMetadata(PASSWORD_ID));
        ProjectEncryptionKeyHealthIndicatorService indicator = indicator(
            state,
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
        ProjectEncryptionKeyHealthIndicatorService indicator = indicator(
            stateWith(null),
            mockPekService(EncryptionServiceState.UNAVAILABLE_MISSING_PASSWORD, null)
        );

        HealthIndicatorResult result = indicator.calculate(true, 1, HealthInfo.EMPTY_HEALTH_INFO);
        assertEquals(HealthStatus.YELLOW, result.status());
        assertEquals(ProjectEncryptionKeyHealthIndicatorService.YELLOW_MISSING_PASSWORD, result.symptom());
        assertEquals(1, result.diagnosisList().size());
        assertEquals(ProjectEncryptionKeyHealthIndicatorService.MISSING_PASSWORD_DEFINITION, result.diagnosisList().get(0).definition());
        assertEquals(ProjectEncryptionKeyHealthIndicatorService.ENCRYPTION_UNAVAILABLE_IMPACTS, result.impacts());
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
        return mockPekService(state, activePasswordId, true);
    }

    private static ProjectEncryptionKeyService mockPekService(EncryptionServiceState state, String activePasswordId, boolean required) {
        ProjectEncryptionKeyService svc = mock(ProjectEncryptionKeyService.class);
        when(svc.state()).thenReturn(state);
        when(svc.getActivePasswordId()).thenReturn(activePasswordId);
        when(svc.isEncryptionRequired()).thenReturn(required);
        return svc;
    }

    private static ProjectEncryptionKeyHealthIndicatorService indicator(ClusterState state, ProjectEncryptionKeyService pekService) {
        return indicator(state, pekService, new PasswordPekEncryption(() -> PASSWORD_SETTINGS), Set.of());
    }

    private static ProjectEncryptionKeyHealthIndicatorService indicator(
        ClusterState state,
        ProjectEncryptionKeyService pekService,
        PasswordPekEncryption pekEncryption,
        Set<String> nodesWithoutPassword
    ) {
        return indicator(state, pekService, pekEncryption, nodesWithoutPassword, true);
    }

    private static ProjectEncryptionKeyHealthIndicatorService indicator(
        ClusterState state,
        ProjectEncryptionKeyService pekService,
        PasswordPekEncryption pekEncryption,
        Set<String> nodesWithoutPassword,
        boolean persistsClusterState
    ) {
        return new ProjectEncryptionKeyHealthIndicatorService(
            mockClusterService(state),
            DefaultProjectResolver.INSTANCE,
            pekService,
            pekEncryption,
            () -> nodesWithoutPassword,
            persistsClusterState
        );
    }

    /** On the elected master, an install waiting for nodes without a password is YELLOW and names them. */
    public void testYellowWhileInstallWaitsForNodesWithoutPassword() {
        ProjectEncryptionKeyHealthIndicatorService indicator = indicator(
            stateWith(null),
            mockPekService(EncryptionServiceState.READY, PASSWORD_ID),
            new PasswordPekEncryption(() -> PASSWORD_SETTINGS),
            Set.of("node-b", "node-a")
        );

        HealthIndicatorResult result = indicator.calculate(true, 1, HealthInfo.EMPTY_HEALTH_INFO);
        assertEquals(HealthStatus.YELLOW, result.status());
        assertEquals(ProjectEncryptionKeyHealthIndicatorService.YELLOW_WAITING_FOR_NODES, result.symptom());
        assertEquals(
            ProjectEncryptionKeyHealthIndicatorService.NODES_WITHOUT_PASSWORD_DEFINITION,
            result.diagnosisList().get(0).definition()
        );
        assertEquals(new TreeSet<>(Set.of("node-a", "node-b")), detailsMap(result).get("nodes_without_password"));
    }

    public void testGreenWhenNoMetadataAndNoSettings() {
        ClusterState state = stateWith(null);

        ProjectEncryptionKeyHealthIndicatorService indicator = indicator(state, mockPekService(EncryptionServiceState.DISABLED, null));

        HealthIndicatorResult result = indicator.calculate(false, 1, HealthInfo.EMPTY_HEALTH_INFO);
        assertEquals(HealthStatus.GREEN, result.status());
        assertEquals(ProjectEncryptionKeyHealthIndicatorService.GREEN_NOT_CONFIGURED, result.symptom());
    }

    public void testGreenWhenMetadataAndPekUnlockable() {
        ProjectEncryptionKeyMetadata metadata = pekMetadata(PASSWORD_ID);
        ClusterState state = stateWith(metadata);

        ProjectEncryptionKeyHealthIndicatorService indicator = indicator(state, mockPekService(EncryptionServiceState.READY, PASSWORD_ID));

        HealthIndicatorResult result = indicator.calculate(false, 1, HealthInfo.EMPTY_HEALTH_INFO);
        assertEquals(HealthStatus.GREEN, result.status());
        assertEquals(ProjectEncryptionKeyHealthIndicatorService.GREEN_HEALTHY, result.symptom());
    }

    public void testGreenWhenSettingsConfiguredButMetadataNotYetInstalled() {
        ClusterState state = stateWith(null);

        ProjectEncryptionKeyHealthIndicatorService indicator = indicator(state, mockPekService(EncryptionServiceState.READY, PASSWORD_ID));

        HealthIndicatorResult result = indicator.calculate(false, 1, HealthInfo.EMPTY_HEALTH_INFO);
        assertEquals(HealthStatus.GREEN, result.status());
        // Password is configured; we just haven't installed the PEK yet.
        assertTrue(result.symptom().contains("awaiting first key install"));
    }

    public void testYellowWhenDisabledAndEncryptionNotRequired() {
        ClusterState state = stateWith(null);

        ProjectEncryptionKeyHealthIndicatorService indicator = indicator(
            state,
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

        ProjectEncryptionKeyHealthIndicatorService indicator = indicator(
            state,
            mockPekService(EncryptionServiceState.DISABLED, null, true)
        );

        HealthIndicatorResult result = indicator.calculate(false, 1, HealthInfo.EMPTY_HEALTH_INFO);
        assertEquals(HealthStatus.GREEN, result.status());
        assertEquals(ProjectEncryptionKeyHealthIndicatorService.GREEN_NOT_CONFIGURED, result.symptom());
    }

    public void testYellowWhenCannotWrapForDisk() {
        ProjectEncryptionKeyMetadata metadata = pekMetadata(PASSWORD_ID);
        ClusterState state = stateWith(metadata);

        ProjectEncryptionKeyHealthIndicatorService indicator = indicator(
            state,
            mockPekService(EncryptionServiceState.READY, null),
            new PasswordPekEncryption(() -> Settings.EMPTY),
            Set.of()
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

    /** A node that doesn't persist cluster state, such as a coordinating-only node, doesn't need a password. */
    public void testGreenWithoutPasswordOnNodeThatDoesNotPersistClusterState() {
        ProjectEncryptionKeyHealthIndicatorService indicator = indicator(
            stateWith(pekMetadata(PASSWORD_ID)),
            mockPekService(EncryptionServiceState.READY, null),
            new PasswordPekEncryption(() -> Settings.EMPTY),
            Set.of(),
            false
        );

        HealthIndicatorResult result = indicator.calculate(true, 1, HealthInfo.EMPTY_HEALTH_INFO);
        assertEquals(HealthStatus.GREEN, result.status());
        assertEquals(ProjectEncryptionKeyHealthIndicatorService.GREEN_HEALTHY, result.symptom());
        assertFalse(detailsMap(result).containsKey("key_on_disk"));
    }

    public void testStateDisplayValueAppearsInDetails() {
        ProjectEncryptionKeyMetadata metadata = pekMetadata(PASSWORD_ID);
        ClusterState state = stateWith(metadata);

        ProjectEncryptionKeyHealthIndicatorService indicator = indicator(state, mockPekService(EncryptionServiceState.READY, PASSWORD_ID));

        HealthIndicatorResult result = indicator.calculate(true, 1, HealthInfo.EMPTY_HEALTH_INFO);
        assertEquals(EncryptionServiceState.READY.displayValue(), detailsMap(result).get("state"));
    }

    private static Map<String, Object> detailsMap(HealthIndicatorResult result) {
        return ((SimpleHealthIndicatorDetails) result.details()).details();
    }
}
