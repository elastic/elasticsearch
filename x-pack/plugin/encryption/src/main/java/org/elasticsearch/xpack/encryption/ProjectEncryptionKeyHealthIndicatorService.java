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
 *   <li>YELLOW cannot-persist: this node lacks the password material to wrap the PEK for disk, or has not persisted it since the
 *       password was configured.</li>
 * </ul>
 * If the elected master holds no key material, encryption is unavailable on every node: RED if encrypted data exists, YELLOW otherwise.
 */
class ProjectEncryptionKeyHealthIndicatorService implements HealthIndicatorService {

    static final String NAME = "cluster_state_encryption";

    // TODO add URL when available
    private static final String HELP_URL = null;

    static final String GREEN_NOT_CONFIGURED = "Cluster state encryption is not configured.";
    static final String GREEN_HEALTHY = "Cluster state encryption is healthy.";
    static final String YELLOW_CANNOT_PERSIST = "Cluster state encryption cannot be persisted to disk on this node.";
    static final String MASTER_HAS_NO_KEY = "The elected master holds no cluster state encryption key; encryption is unavailable.";
    static final String YELLOW_OPT_OUT = "Cluster state encryption is disabled; secrets are stored unencrypted.";
    static final String YELLOW_MISSING_PASSWORD = "Cluster state encryption is degraded: no active password is configured on this node.";

    static final List<HealthIndicatorImpact> WRAP_FAILURE_IMPACTS = List.of(
        new HealthIndicatorImpact(
            NAME,
            "encryption_key_not_persisted",
            2,
            "This node's on-disk cluster state has no usable copy of the cluster state encryption key. While this node is the elected"
                + " master no key can be installed, and if it is elected master after a restart, cluster state encryption becomes"
                + " unavailable.",
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
    static final List<HealthIndicatorImpact> MASTER_HAS_NO_KEY_IMPACTS = List.of(
        new HealthIndicatorImpact(
            NAME,
            "master_has_no_encryption_key",
            1,
            "Encrypted secrets cannot be decrypted and new secrets cannot be stored on any node. The cluster state encryption key only"
                + " survives on the disks of nodes that persisted it, and is lost if all of those nodes are removed from the cluster.",
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
        "This node lacks the password material to persist the cluster state encryption key, or has not persisted it since the password"
            + " was configured.",
        "Provision cluster.state.encryption.active_password_id and the matching cluster.state.encryption.password.<id> in the keystore"
            + " (stateful) or operator file settings (serverless). If the password was added to the keystore call"
            + " POST /_nodes/reload_secure_settings (stateful). The key is then persisted with the next change to cluster-wide metadata;"
            + " to persist it now, change a persistent cluster setting or restart this node.",
        HELP_URL
    );
    static final Diagnosis.Definition MASTER_HAS_NO_KEY_DEFINITION = new Diagnosis.Definition(
        NAME,
        "master_has_no_key",
        "The elected master started without a usable copy of the cluster state encryption key and published cluster state without it.",
        "Configure the encryption password on the elected master and reload secure settings; if it persisted the key with that password,"
            + " it restores the key. Otherwise use voting configuration exclusions to elect a master-eligible node whose"
            + " cluster_state_encryption health details report key_on_disk: true; it restores the key. Only reset"
            + " encryption if no node holds the key: a reset permanently deletes all encrypted data.",
        HELP_URL
    );
    static final Diagnosis.Definition MISSING_PASSWORD_DEFINITION = new Diagnosis.Definition(
        NAME,
        "missing_password",
        "No active password is configured on this node; the cluster state encryption key cannot be decrypted.",
        "Provision cluster.state.encryption.active_password_id and the matching cluster.state.encryption.password.<id> in the keystore"
            + " (stateful) or operator file settings (serverless). If the password was added to the keystore call"
            + " POST /_nodes/reload_secure_settings (stateful).",
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
    private final Supplier<Boolean> keyInGateway;
    private final EncryptedDataHandlerRegistry handlerRegistry;

    ProjectEncryptionKeyHealthIndicatorService(
        ClusterService clusterService,
        ProjectResolver projectResolver,
        ProjectEncryptionKeyService pekService,
        Supplier<Boolean> keyInGateway,
        EncryptedDataHandlerRegistry handlerRegistry
    ) {
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
        this.pekService = pekService;
        this.keyInGateway = keyInGateway;
        this.handlerRegistry = handlerRegistry;
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

        return switch (serviceState) {
            case READY -> {
                if (pekService.canWrapForDisk() == false || Boolean.FALSE.equals(keyInGateway.get())) {
                    yield createIndicator(
                        YELLOW,
                        YELLOW_CANNOT_PERSIST,
                        detailsBuilder(metadata, serviceState),
                        WRAP_FAILURE_IMPACTS,
                        List.of(new Diagnosis(MISSING_WRAP_CREDENTIALS_DEFINITION, List.of()))
                    );
                }
                if (metadata == null) {
                    yield createIndicator(
                        GREEN,
                        "Cluster state encryption is configured; awaiting first key install.",
                        detailsBuilder(null, serviceState),
                        List.of(),
                        List.of()
                    );
                }
                yield createIndicator(GREEN, GREEN_HEALTHY, detailsBuilder(metadata, serviceState), List.of(), List.of());
            }
            case DISABLED -> {
                if (pekService.isEncryptionRequired() == false) {
                    yield createIndicator(
                        YELLOW,
                        YELLOW_OPT_OUT,
                        detailsBuilder(metadata, serviceState),
                        OPT_OUT_IMPACTS,
                        List.of(new Diagnosis(ENCRYPTION_OPT_OUT_DEFINITION, List.of()))
                    );
                }
                yield createIndicator(GREEN, GREEN_NOT_CONFIGURED, detailsBuilder(metadata, serviceState), List.of(), List.of());
            }
            case UNAVAILABLE_DECRYPTION_FAILED -> createIndicator(
                hasEncryptedData(project) ? RED : YELLOW,
                MASTER_HAS_NO_KEY,
                detailsBuilder(metadata, serviceState),
                MASTER_HAS_NO_KEY_IMPACTS,
                List.of(new Diagnosis(MASTER_HAS_NO_KEY_DEFINITION, List.of()))
            );
            case UNAVAILABLE_MISSING_PASSWORD -> createIndicator(
                YELLOW,
                YELLOW_MISSING_PASSWORD,
                detailsBuilder(metadata, serviceState),
                ENCRYPTION_UNAVAILABLE_IMPACTS,
                List.of(new Diagnosis(MISSING_PASSWORD_DEFINITION, List.of()))
            );
        };
    }

    private boolean hasEncryptedData(ProjectMetadata project) {
        return handlerRegistry.handlers().stream().anyMatch(handler -> EncryptedDataHandlerRegistry.hasData(handler, project));
    }

    private HealthIndicatorDetails detailsBuilder(@Nullable ProjectEncryptionKeyMetadata metadata, EncryptionServiceState serviceState) {
        String activePasswordId = pekService.getActivePasswordId();
        Map<String, Object> details = new HashMap<>();
        details.put("state", serviceState.displayValue());
        details.put("encryption_required", pekService.isEncryptionRequired());
        details.put("active_password_id", activePasswordId == null ? "(unset)" : activePasswordId);
        Boolean keyOnDisk = keyInGateway.get();
        if (keyOnDisk != null) {
            details.put("key_on_disk", keyOnDisk);
        }
        if (metadata != null) {
            details.put("metadata_password_id", metadata.getPasswordId());
            details.put("active_key_id", metadata.getActiveKeyId());
            details.put("key_count", metadata.getKeys().size());
        }
        return new SimpleHealthIndicatorDetails(details);
    }
}
