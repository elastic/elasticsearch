/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.health.node.HealthInfo;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceState;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.RED;
import static org.elasticsearch.health.HealthStatus.YELLOW;

/**
 * Reports health for the project encryption key (PEK). Since PEK bytes travel in plaintext over the TLS-protected transport channel,
 * the node can always encrypt and decrypt at runtime once a key is installed. This indicator surfaces two operational concerns:
 * <ul>
 *   <li>YELLOW opt-out: no password is configured and {@code cluster.state.encryption.required} is {@code false}, meaning
 *       secrets are stored unencrypted in cluster state.</li>
 *   <li>YELLOW cannot-persist: a PEK is installed but this node, which persists cluster state, lacks a usable password to wrap it for
 *       disk. The next cluster-state persist on this node will fail, and the node will not survive a restart without the password.</li>
 * </ul>
 * If the elected master holds no key, encryption is unavailable on every node: RED if encrypted data exists, YELLOW otherwise.
 *
 * <p>On the elected master it also reports the nodes that can't persist the key while installing or restoring the key waits for them:
 * YELLOW while the first key waits, and an extra diagnosis while a restore waits. Only the elected master knows these nodes.
 */
class ProjectEncryptionKeyHealthIndicatorService implements HealthIndicatorService {

    static final String NAME = "cluster_state_encryption";

    // TODO add URL when available
    private static final String HELP_URL = null;

    static final String GREEN_NOT_CONFIGURED = "Cluster state encryption is not configured.";
    static final String GREEN_HEALTHY = "Cluster state encryption is healthy.";
    static final String YELLOW_CANNOT_PERSIST = "Cluster state encryption cannot be persisted to disk on this node.";
    static final String YELLOW_OPT_OUT = "Cluster state encryption is disabled; secrets are stored unencrypted.";
    static final String MASTER_HAS_NO_KEY = "The elected master holds no cluster state encryption key; encryption is unavailable.";
    static final String YELLOW_WAITING_FOR_NODES =
        "The cluster state encryption key is not created yet: some nodes that persist cluster state can't persist it.";

    static final List<HealthIndicatorImpact> WRAP_FAILURE_IMPACTS = List.of(
        new HealthIndicatorImpact(
            NAME,
            "encryption_key_not_persisted",
            2,
            "The cluster state encryption key cannot be persisted to disk. This node will fail to start after a restart.",
            List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)
        )
    );
    static final List<HealthIndicatorImpact> ENCRYPTION_UNAVAILABLE_IMPACTS = List.of(
        new HealthIndicatorImpact(
            NAME,
            "encryption_unavailable",
            1,
            "Cluster state encryption is unavailable on this node. Encrypt and decrypt operations will fail.",
            List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)
        )
    );
    static final List<HealthIndicatorImpact> WAITING_FOR_NODES_IMPACTS = List.of(
        new HealthIndicatorImpact(
            NAME,
            "encryption_key_waiting_for_nodes",
            2,
            "The cluster state encryption key is not created or restored until every node that persists cluster state can persist it."
                + " Until then, encrypted secrets can't be stored or read.",
            List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)
        )
    );
    static final List<HealthIndicatorImpact> OPT_OUT_IMPACTS = List.of(
        new HealthIndicatorImpact(
            NAME,
            "secrets_stored_unencrypted",
            3,
            "Data source credentials are stored unencrypted in cluster state.",
            List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)
        )
    );

    static final Diagnosis.Definition MISSING_WRAP_CREDENTIALS_DEFINITION = new Diagnosis.Definition(
        NAME,
        "missing_wrap_credentials",
        "A cluster state encryption key is installed but this node lacks the password material to wrap it for disk.",
        "Provision cluster.state.encryption.active_password_id and the matching cluster.state.encryption.password.<id> in the keystore"
            + " (stateful) or operator file settings (serverless). If the password was added to the keystore call"
            + " POST /_nodes/reload_secure_settings (stateful).",
        HELP_URL
    );
    static final Diagnosis.Definition DECRYPTION_FAILED_DEFINITION = new Diagnosis.Definition(
        NAME,
        "decryption_failed",
        "The elected master could not decrypt the cluster state encryption key on its disk.",
        "Configure the password the key on the elected master's disk is wrapped with, as cluster.state.encryption.password.<id> with"
            + " cluster.state.encryption.active_password_id, and reload secure settings; it then reads the key from its disk. If that"
            + " password is lost, fail the master over to a master-eligible node that can read its copy (can_read_key_on_disk), for"
            + " example by excluding the elected master from the voting configuration. Only if no node can, call"
            + " POST /_encryption/_reset?accept_data_loss=true, which permanently deletes all encrypted data.",
        HELP_URL
    );
    static final Diagnosis.Definition NODES_WITHOUT_PASSWORD_DEFINITION = new Diagnosis.Definition(
        NAME,
        "nodes_without_password",
        "The elected master waits to create or restore the cluster state encryption key until every node that persists cluster state"
            + " can persist it, and the nodes in nodes_without_password can't.",
        "Configure cluster.state.encryption.active_password_id and the matching cluster.state.encryption.password.<id> on the nodes in"
            + " nodes_without_password and reload their secure settings.",
        HELP_URL
    );
    static final Diagnosis.Definition ENCRYPTION_OPT_OUT_DEFINITION = new Diagnosis.Definition(
        NAME,
        "encryption_opt_out",
        "Cluster state encryption is not configured and cluster.state.encryption.required is false.",
        "Configure cluster.state.encryption.active_password_id and the matching cluster.state.encryption.password.<id> to enable"
            + " encryption, or set cluster.state.encryption.required: true to reject credential writes until encryption is configured.",
        HELP_URL
    );

    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;
    private final ProjectEncryptionKeyService pekService;
    private final PasswordPekEncryption pekEncryption;
    private final EncryptedDataHandlerRegistry handlerRegistry;
    private final Supplier<Set<String>> nodesWithoutPassword;
    private final boolean persistsClusterState;

    ProjectEncryptionKeyHealthIndicatorService(
        ClusterService clusterService,
        ProjectResolver projectResolver,
        ProjectEncryptionKeyService pekService,
        PasswordPekEncryption pekEncryption,
        EncryptedDataHandlerRegistry handlerRegistry,
        Supplier<Set<String>> nodesWithoutPassword,
        boolean persistsClusterState
    ) {
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
        this.pekService = pekService;
        this.pekEncryption = pekEncryption;
        this.handlerRegistry = handlerRegistry;
        this.nodesWithoutPassword = nodesWithoutPassword;
        this.persistsClusterState = persistsClusterState;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public HealthIndicatorResult calculate(boolean verbose, int maxAffectedResourcesCount, HealthInfo healthInfo) {
        ClusterState clusterState = clusterService.state();
        ProjectMetadata project = clusterState.metadata().getProject(projectResolver.getProjectId());
        ProjectEncryptionKeyMetadata metadata = project.custom(ProjectEncryptionKeyMetadata.TYPE);
        EncryptionServiceState serviceState = pekService.state();
        Set<String> waitingFor = nodesWithoutPassword.get();
        HealthIndicatorDetails details = details(metadata, serviceState, waitingFor);

        return switch (serviceState) {
            case READY -> {
                if (metadata == null) {
                    if (waitingFor.isEmpty()) {
                        yield createIndicator(
                            GREEN,
                            "Cluster state encryption is configured; awaiting first key install.",
                            details,
                            List.of(),
                            List.of()
                        );
                    }
                    yield createIndicator(
                        YELLOW,
                        YELLOW_WAITING_FOR_NODES,
                        details,
                        WAITING_FOR_NODES_IMPACTS,
                        List.of(new Diagnosis(NODES_WITHOUT_PASSWORD_DEFINITION, List.of()))
                    );
                }
                if (persistsClusterState && pekEncryption.canWrapForGateway() == false) {
                    yield createIndicator(
                        YELLOW,
                        YELLOW_CANNOT_PERSIST,
                        details,
                        WRAP_FAILURE_IMPACTS,
                        List.of(new Diagnosis(MISSING_WRAP_CREDENTIALS_DEFINITION, List.of()))
                    );
                }
                yield createIndicator(GREEN, GREEN_HEALTHY, details, List.of(), List.of());
            }
            case DISABLED -> {
                if (pekService.isEncryptionRequired() == false) {
                    yield createIndicator(
                        YELLOW,
                        YELLOW_OPT_OUT,
                        details,
                        OPT_OUT_IMPACTS,
                        List.of(new Diagnosis(ENCRYPTION_OPT_OUT_DEFINITION, List.of()))
                    );
                }
                yield createIndicator(GREEN, GREEN_NOT_CONFIGURED, details, List.of(), List.of());
            }
            case UNAVAILABLE_DECRYPTION_FAILED -> createIndicator(
                hasEncryptedData(project) ? RED : YELLOW,
                MASTER_HAS_NO_KEY,
                details,
                ENCRYPTION_UNAVAILABLE_IMPACTS,
                waitingFor.isEmpty()
                    ? List.of(new Diagnosis(DECRYPTION_FAILED_DEFINITION, List.of()))
                    : List.of(new Diagnosis(NODES_WITHOUT_PASSWORD_DEFINITION, List.of()))
            );
        };
    }

    private boolean hasEncryptedData(ProjectMetadata project) {
        return handlerRegistry.handlers().stream().anyMatch(handler -> EncryptedDataHandlerRegistry.hasData(handler, project));
    }

    private HealthIndicatorDetails details(
        @Nullable ProjectEncryptionKeyMetadata metadata,
        EncryptionServiceState serviceState,
        Set<String> waitingFor
    ) {
        String activePasswordId = pekService.getActivePasswordId();
        Map<String, Object> details = new HashMap<>();
        details.put("state", serviceState.displayValue());
        details.put("encryption_required", pekService.isEncryptionRequired());
        details.put("active_password_id", activePasswordId == null ? "(unset)" : activePasswordId);
        details.put("key_on_disk", pekEncryption.gatewayKeys().holdsKeys());
        putIfNotNull(details, "can_read_key_on_disk", pekEncryption.canReadGatewayKeys());
        putIfNotNull(details, "key_on_disk_password_id", pekEncryption.gatewayKeys().passwordId());
        if (waitingFor.isEmpty() == false) {
            details.put("nodes_without_password", waitingFor);
        }
        if (metadata != null) {
            details.put("metadata_password_id", metadata.getPasswordId());
            details.put("active_key_id", metadata.getActiveKeyId());
            details.put("key_count", metadata.getKeys().size());
        }
        return new SimpleHealthIndicatorDetails(details);
    }

    private static void putIfNotNull(Map<String, Object> details, String name, @Nullable Object value) {
        if (value != null) {
            details.put(name, value);
        }
    }
}
