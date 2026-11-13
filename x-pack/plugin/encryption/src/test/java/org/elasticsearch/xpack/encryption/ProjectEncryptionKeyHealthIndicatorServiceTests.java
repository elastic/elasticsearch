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
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandler;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceState;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProjectEncryptionKeyHealthIndicatorServiceTests extends ESTestCase {

    private static final String PASSWORD_ID = "v1";
    private static final ProjectEncryptionKeyMetadata.PekEncryption NO_OP_ENCRYPTION = TestPekEncryption.NO_OP;
    // at least 14 characters, as FIPS requires
    private static final Settings PASSWORD_SETTINGS = EncryptionIntegTestUtils.passwordSettings(PASSWORD_ID, "test-password-fips");
    private static final ToXContent.Params GATEWAY = new ToXContent.MapParams(
        Map.of(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_GATEWAY)
    );

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
        return indicator(state, pekService, new PasswordPekEncryption(() -> PASSWORD_SETTINGS), List.of(), Set.of());
    }

    private static ProjectEncryptionKeyHealthIndicatorService indicator(
        ClusterState state,
        ProjectEncryptionKeyService pekService,
        PasswordPekEncryption pekEncryption,
        List<EncryptedDataHandler<?>> handlers,
        Set<String> nodesWithoutPassword
    ) {
        return indicator(state, pekService, pekEncryption, handlers, nodesWithoutPassword, true);
    }

    private static ProjectEncryptionKeyHealthIndicatorService indicator(
        ClusterState state,
        ProjectEncryptionKeyService pekService,
        PasswordPekEncryption pekEncryption,
        List<EncryptedDataHandler<?>> handlers,
        Set<String> nodesWithoutPassword,
        boolean persistsClusterState
    ) {
        return new ProjectEncryptionKeyHealthIndicatorService(
            mockClusterService(state),
            DefaultProjectResolver.INSTANCE,
            pekService,
            pekEncryption,
            new EncryptedDataHandlerRegistry(handlers),
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
            List.of(),
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

    /** A restore waiting for nodes without a password says so instead of how to give the master its password. */
    public void testRestoreWaitingForNodesWithoutPasswordNamesThem() {
        ProjectEncryptionKeyHealthIndicatorService indicator = indicator(
            stateWith(ProjectEncryptionKeyMetadata.degraded("k1", PASSWORD_ID, NO_OP_ENCRYPTION, "no key")),
            mockPekService(EncryptionServiceState.UNAVAILABLE_DECRYPTION_FAILED, PASSWORD_ID),
            new PasswordPekEncryption(() -> PASSWORD_SETTINGS),
            List.of(),
            Set.of("node-b")
        );

        HealthIndicatorResult result = indicator.calculate(true, 1, HealthInfo.EMPTY_HEALTH_INFO);
        assertEquals(ProjectEncryptionKeyHealthIndicatorService.MASTER_HAS_NO_KEY, result.symptom());
        assertEquals(
            ProjectEncryptionKeyHealthIndicatorService.NODES_WITHOUT_PASSWORD_DEFINITION,
            result.diagnosisList().get(0).definition()
        );
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
            List.of(),
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
            List.of(),
            Set.of(),
            false
        );

        HealthIndicatorResult result = indicator.calculate(false, 1, HealthInfo.EMPTY_HEALTH_INFO);
        assertEquals(HealthStatus.GREEN, result.status());
        assertEquals(ProjectEncryptionKeyHealthIndicatorService.GREEN_HEALTHY, result.symptom());
    }

    /** The elected master holds no key: red if any handler holds encrypted data, which can't be decrypted until the key is back. */
    public void testElectedMasterHoldsNoKey() {
        ProjectEncryptionKeyMetadata metadata = pekMetadata(PASSWORD_ID);
        ClusterState state = stateWith(metadata);
        boolean hasEncryptedData = randomBoolean();
        EncryptedDataHandler<Metadata.ProjectCustom> handler = new EncryptedDataHandler<>() {
            @Override
            public String customName() {
                return "test_custom";
            }

            @Override
            public Metadata.ProjectCustom reEncrypt(Metadata.ProjectCustom current, UnaryOperator<EncryptedData> rewrapper) {
                return current;
            }

            @Override
            public boolean hasData(Metadata.ProjectCustom current) {
                return hasEncryptedData;
            }
        };

        ProjectEncryptionKeyHealthIndicatorService indicator = indicator(
            state,
            mockPekService(EncryptionServiceState.UNAVAILABLE_DECRYPTION_FAILED, PASSWORD_ID),
            new PasswordPekEncryption(() -> PASSWORD_SETTINGS),
            List.of(handler),
            Set.of()
        );

        HealthIndicatorResult result = indicator.calculate(true, 1, HealthInfo.EMPTY_HEALTH_INFO);
        assertEquals(hasEncryptedData ? HealthStatus.RED : HealthStatus.YELLOW, result.status());
        assertEquals(ProjectEncryptionKeyHealthIndicatorService.MASTER_HAS_NO_KEY, result.symptom());
        assertEquals(1, result.diagnosisList().size());
        assertEquals(ProjectEncryptionKeyHealthIndicatorService.DECRYPTION_FAILED_DEFINITION, result.diagnosisList().get(0).definition());
        assertEquals(ProjectEncryptionKeyHealthIndicatorService.ENCRYPTION_UNAVAILABLE_IMPACTS, result.impacts());
    }

    public void testStateDisplayValueAppearsInDetails() {
        ProjectEncryptionKeyMetadata metadata = pekMetadata(PASSWORD_ID);
        ClusterState state = stateWith(metadata);

        ProjectEncryptionKeyHealthIndicatorService indicator = indicator(state, mockPekService(EncryptionServiceState.READY, PASSWORD_ID));

        HealthIndicatorResult result = indicator.calculate(true, 1, HealthInfo.EMPTY_HEALTH_INFO);
        assertEquals(EncryptionServiceState.READY.displayValue(), detailsMap(result).get("state"));
    }

    /** Reports whether this node holds a copy of the key on disk, and whether it has the password to read it. */
    public void testKeyOnDiskDetails() {
        AtomicReference<Settings> settings = new AtomicReference<>(PASSWORD_SETTINGS);
        PasswordPekEncryption pekEncryption = new PasswordPekEncryption(settings::get);
        String keyId = ProjectEncryptionKeyMetadata.generateKeyId();
        ProjectEncryptionKeyMetadata metadata = new ProjectEncryptionKeyMetadata(
            Map.of(keyId, new ProjectEncryptionKeyMetadata.KeyEntry(randomKeyBytes(), 0L)),
            keyId,
            PASSWORD_ID,
            Map.of(),
            pekEncryption
        );
        ProjectEncryptionKeyHealthIndicatorService indicator = indicator(
            stateWith(metadata),
            mockPekService(EncryptionServiceState.READY, PASSWORD_ID),
            pekEncryption,
            List.of(),
            Set.of()
        );
        assertEquals(
            "nothing read or written yet",
            false,
            detailsMap(indicator.calculate(true, 1, HealthInfo.EMPTY_HEALTH_INFO)).get("key_on_disk")
        );

        ProjectEncryptionKeyMetadata.degraded("other", PASSWORD_ID, pekEncryption, "no key").toXContentChunked(GATEWAY);
        Map<String, Object> details = detailsMap(indicator.calculate(true, 1, HealthInfo.EMPTY_HEALTH_INFO));
        assertEquals(false, details.get("key_on_disk"));
        assertFalse(details.containsKey("can_read_key_on_disk"));

        metadata.toXContentChunked(GATEWAY);
        details = detailsMap(indicator.calculate(true, 1, HealthInfo.EMPTY_HEALTH_INFO));
        assertEquals(true, details.get("key_on_disk"));
        assertEquals(true, details.get("can_read_key_on_disk"));
        assertEquals(PASSWORD_ID, details.get("key_on_disk_password_id"));

        settings.set(EncryptionIntegTestUtils.passwordSettings(PASSWORD_ID, "another-password-fips"));
        assertEquals(
            "a password with the same id that doesn't decrypt the copy",
            false,
            detailsMap(indicator.calculate(true, 1, HealthInfo.EMPTY_HEALTH_INFO)).get("can_read_key_on_disk")
        );

        settings.set(Settings.EMPTY);
        assertEquals(false, detailsMap(indicator.calculate(true, 1, HealthInfo.EMPTY_HEALTH_INFO)).get("can_read_key_on_disk"));
    }

    private static Map<String, Object> detailsMap(HealthIndicatorResult result) {
        return ((SimpleHealthIndicatorDetails) result.details()).details();
    }
}
