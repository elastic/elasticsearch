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
import org.elasticsearch.xpack.encryption.KeyRotationCoordinatorTests.TestCustom;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandler;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceState;

import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

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

    private static final EncryptedDataHandler<TestCustom> HANDLER = new EncryptedDataHandler<>() {
        @Override
        public String customName() {
            return TestCustom.TYPE;
        }

        @Override
        public TestCustom reEncrypt(TestCustom current, UnaryOperator<EncryptedData> rewrapper) {
            return current;
        }
    };

    private static ClusterState stateWith(ProjectEncryptionKeyMetadata metadata) {
        return stateWith(metadata, false);
    }

    private static ClusterState stateWith(ProjectEncryptionKeyMetadata metadata, boolean withEncryptedData) {
        ProjectMetadata.Builder project = ProjectMetadata.builder(Metadata.DEFAULT_PROJECT_ID);
        if (metadata != null) {
            project.putCustom(ProjectEncryptionKeyMetadata.TYPE, metadata);
        }
        if (withEncryptedData) {
            project.putCustom(TestCustom.TYPE, TestCustom.encryptedUnder("k1"));
        }
        return ClusterState.builder(new ClusterName("test")).metadata(Metadata.builder().put(project.build()).build()).build();
    }

    private static ClusterService mockClusterService(ClusterState state) {
        ClusterService cs = mock(ClusterService.class);
        when(cs.state()).thenReturn(state);
        return cs;
    }

    private static ProjectEncryptionKeyHealthIndicatorService indicator(ClusterState state, ProjectEncryptionKeyService pekService) {
        return indicator(state, pekService, null);
    }

    private static ProjectEncryptionKeyHealthIndicatorService indicator(
        ClusterState state,
        ProjectEncryptionKeyService pekService,
        Boolean keyInGateway
    ) {
        return new ProjectEncryptionKeyHealthIndicatorService(
            mockClusterService(state),
            DefaultProjectResolver.INSTANCE,
            pekService,
            () -> keyInGateway,
            () -> Boolean.TRUE.equals(keyInGateway),
            new EncryptedDataHandlerRegistry(List.of(HANDLER))
        );
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

        // a node that persisted no key, for example since a reset, has nothing to persist until the next key is installed
        ProjectEncryptionKeyHealthIndicatorService indicator = indicator(
            state,
            mockPekService(EncryptionServiceState.READY, PASSWORD_ID),
            randomBoolean() ? null : false
        );

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

    /** No password with a key installed, no password before the first install, and a password configured but not yet persisted. */
    public void testYellowWhenKeyCannotBePersisted() {
        for (ProjectEncryptionKeyHealthIndicatorService indicator : List.of(
            indicator(stateWith(pekMetadata(PASSWORD_ID)), mockPekService(EncryptionServiceState.READY, null, true, false)),
            indicator(stateWith(null), mockPekService(EncryptionServiceState.READY, PASSWORD_ID, true, false)),
            indicator(stateWith(pekMetadata(PASSWORD_ID)), mockPekService(EncryptionServiceState.READY, PASSWORD_ID), false)
        )) {
            HealthIndicatorResult result = indicator.calculate(true, 1, HealthInfo.EMPTY_HEALTH_INFO);
            assertEquals(HealthStatus.YELLOW, result.status());
            assertEquals(ProjectEncryptionKeyHealthIndicatorService.YELLOW_CANNOT_PERSIST, result.symptom());
            assertEquals(1, result.diagnosisList().size());
            assertEquals(
                ProjectEncryptionKeyHealthIndicatorService.MISSING_WRAP_CREDENTIALS_DEFINITION,
                result.diagnosisList().get(0).definition()
            );
            assertEquals(ProjectEncryptionKeyHealthIndicatorService.WRAP_FAILURE_IMPACTS, result.impacts());
        }
    }

    public void testRedWhenElectedMasterHoldsNoKeyAndDataIsEncrypted() {
        assertMasterHasNoKey(true, HealthStatus.RED);
    }

    public void testYellowWhenElectedMasterHoldsNoKeyAndNoDataIsEncrypted() {
        assertMasterHasNoKey(false, HealthStatus.YELLOW);
    }

    private static void assertMasterHasNoKey(boolean withEncryptedData, HealthStatus expected) {
        ClusterState state = stateWith(
            ProjectEncryptionKeyMetadata.degraded("k1", PASSWORD_ID, NO_OP_ENCRYPTION, "no key material found on disk"),
            withEncryptedData
        );
        boolean keyOnDisk = randomBoolean();
        ProjectEncryptionKeyHealthIndicatorService indicator = indicator(
            state,
            mockPekService(EncryptionServiceState.UNAVAILABLE_DECRYPTION_FAILED, PASSWORD_ID),
            keyOnDisk
        );

        HealthIndicatorResult result = indicator.calculate(true, 1, HealthInfo.EMPTY_HEALTH_INFO);
        assertEquals(expected, result.status());
        assertEquals(keyOnDisk, detailsMap(result).get("key_on_disk"));
        assertEquals(keyOnDisk ? true : null, detailsMap(result).get("can_read_key_on_disk"));
        assertEquals(ProjectEncryptionKeyHealthIndicatorService.MASTER_HAS_NO_KEY, result.symptom());
        assertEquals(1, result.diagnosisList().size());
        assertEquals(ProjectEncryptionKeyHealthIndicatorService.MASTER_HAS_NO_KEY_DEFINITION, result.diagnosisList().get(0).definition());
        assertEquals(ProjectEncryptionKeyHealthIndicatorService.MASTER_HAS_NO_KEY_IMPACTS, result.impacts());
    }

    public void testYellowWhenMissingPassword() {
        ClusterState state = stateWith(null);

        ProjectEncryptionKeyHealthIndicatorService indicator = indicator(
            state,
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

        ProjectEncryptionKeyHealthIndicatorService indicator = indicator(state, mockPekService(EncryptionServiceState.READY, PASSWORD_ID));

        HealthIndicatorResult result = indicator.calculate(true, 1, HealthInfo.EMPTY_HEALTH_INFO);
        assertEquals(EncryptionServiceState.READY.displayValue(), detailsMap(result).get("state"));
        assertFalse("unknown until this node persists cluster state", detailsMap(result).containsKey("key_on_disk"));
    }

    private static Map<String, Object> detailsMap(HealthIndicatorResult result) {
        return ((SimpleHealthIndicatorDetails) result.details()).details();
    }
}
