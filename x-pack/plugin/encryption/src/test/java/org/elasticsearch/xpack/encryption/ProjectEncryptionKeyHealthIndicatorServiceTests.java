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
import org.elasticsearch.health.node.HealthInfo;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProjectEncryptionKeyHealthIndicatorServiceTests extends ESTestCase {

    private static final String PASSWORD_ID = "v1";

    private static byte[] randomWrappedBytes() {
        // Opaque blob at the canonical wrap length — the health indicator doesn't unwrap, it only reads metadata fields.
        return randomByteArrayOfLength(
            PasswordBasedEncryption.SALT_LENGTH_BYTES + AesGcm.OVERHEAD_BYTES + PasswordBasedEncryption.PEK_LENGTH_BYTES
        );
    }

    private static ProjectEncryptionKeyMetadata pekMetadata(String passwordId) {
        String keyId = ProjectEncryptionKeyMetadata.generateKeyId();
        return new ProjectEncryptionKeyMetadata(
            Map.of(keyId, new ProjectEncryptionKeyMetadata.KeyEntry(randomWrappedBytes(), 0L)),
            keyId,
            passwordId
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

    private static ProjectEncryptionKeyService mockPekService(AesGcmEncryptionService.ActiveKey activeKey, String activePasswordId) {
        ProjectEncryptionKeyService svc = mock(ProjectEncryptionKeyService.class);
        when(svc.getActiveKey()).thenReturn(activeKey);
        when(svc.getActivePasswordId()).thenReturn(activePasswordId);
        return svc;
    }

    public void testGreenWhenNoMetadataAndNoSettings() {
        ClusterState state = stateWith(null);

        ProjectEncryptionKeyHealthIndicatorService indicator = new ProjectEncryptionKeyHealthIndicatorService(
            mockClusterService(state),
            DefaultProjectResolver.INSTANCE,
            mockPekService(null, null)
        );

        HealthIndicatorResult result = indicator.calculate(false, 1, HealthInfo.EMPTY_HEALTH_INFO);
        assertEquals(HealthStatus.GREEN, result.status());
        assertEquals(ProjectEncryptionKeyHealthIndicatorService.GREEN_NOT_CONFIGURED, result.symptom());
    }

    public void testGreenWhenMetadataAndPekUnlockable() {
        ProjectEncryptionKeyMetadata metadata = pekMetadata(PASSWORD_ID);
        ClusterState state = stateWith(metadata);
        AesGcmEncryptionService.ActiveKey activeKey = new AesGcmEncryptionService.ActiveKey(
            metadata.getActiveKeyId(),
            new javax.crypto.spec.SecretKeySpec(new byte[PasswordBasedEncryption.PEK_LENGTH_BYTES], "AES")
        );

        ProjectEncryptionKeyHealthIndicatorService indicator = new ProjectEncryptionKeyHealthIndicatorService(
            mockClusterService(state),
            DefaultProjectResolver.INSTANCE,
            mockPekService(activeKey, PASSWORD_ID)
        );

        HealthIndicatorResult result = indicator.calculate(false, 1, HealthInfo.EMPTY_HEALTH_INFO);
        assertEquals(HealthStatus.GREEN, result.status());
        assertEquals(ProjectEncryptionKeyHealthIndicatorService.GREEN_HEALTHY, result.symptom());
    }

    public void testYellowWhenMetadataPresentButPekLockedAndPasswordIdMissing() {
        ProjectEncryptionKeyMetadata metadata = pekMetadata(PASSWORD_ID);
        ClusterState state = stateWith(metadata);

        ProjectEncryptionKeyHealthIndicatorService indicator = new ProjectEncryptionKeyHealthIndicatorService(
            mockClusterService(state),
            DefaultProjectResolver.INSTANCE,
            mockPekService(null, null)  // no active_password_id snapshotted
        );

        HealthIndicatorResult result = indicator.calculate(false, 1, HealthInfo.EMPTY_HEALTH_INFO);
        assertEquals(HealthStatus.YELLOW, result.status());
        assertEquals(ProjectEncryptionKeyHealthIndicatorService.YELLOW_LOCKED, result.symptom());
        assertEquals(1, result.diagnosisList().size());
        assertEquals(ProjectEncryptionKeyHealthIndicatorService.MISSING_PASSWORD_DEFINITION, result.diagnosisList().get(0).definition());
    }

    public void testYellowWhenMetadataPresentButPekLockedAndPasswordWrong() {
        ProjectEncryptionKeyMetadata metadata = pekMetadata(PASSWORD_ID);
        ClusterState state = stateWith(metadata);

        ProjectEncryptionKeyHealthIndicatorService indicator = new ProjectEncryptionKeyHealthIndicatorService(
            mockClusterService(state),
            DefaultProjectResolver.INSTANCE,
            // active_password_id matches metadata's passwordId but the configured password is wrong (pekService returns null for the
            // active key); the diagnosis is then "undecryptable" rather than "missing".
            mockPekService(null, PASSWORD_ID)
        );

        HealthIndicatorResult result = indicator.calculate(false, 1, HealthInfo.EMPTY_HEALTH_INFO);
        assertEquals(HealthStatus.YELLOW, result.status());
        assertEquals(ProjectEncryptionKeyHealthIndicatorService.UNDECRYPTABLE_DEFINITION, result.diagnosisList().get(0).definition());
    }

    public void testGreenWhenSettingsConfiguredButMetadataNotYetInstalled() {
        ClusterState state = stateWith(null);

        ProjectEncryptionKeyHealthIndicatorService indicator = new ProjectEncryptionKeyHealthIndicatorService(
            mockClusterService(state),
            DefaultProjectResolver.INSTANCE,
            mockPekService(null, PASSWORD_ID)
        );

        HealthIndicatorResult result = indicator.calculate(false, 1, HealthInfo.EMPTY_HEALTH_INFO);
        assertEquals(HealthStatus.GREEN, result.status());
        // Settings are configured; we just haven't installed the PEK yet.
        assertTrue(result.symptom().contains("awaiting first key install"));
    }
}
