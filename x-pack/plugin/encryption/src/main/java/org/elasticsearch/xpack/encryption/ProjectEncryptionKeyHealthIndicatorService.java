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

import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.YELLOW;

/**
 * Reports health for the project encryption key (PEK). Since PEK bytes travel in plaintext over the TLS-protected transport channel,
 * the node can always encrypt and decrypt at runtime once a key is installed. This indicator surfaces two operational concerns:
 * <ul>
 *   <li>YELLOW opt-out: no password is configured and {@code cluster.state.encryption.required} is {@code false}, meaning
 *       secrets are stored unencrypted in cluster state.</li>
 *   <li>YELLOW password-not-configured: an active password id is configured but its password is missing on this node, before any
 *       PEK is installed. On the elected master this prevents the install; on other nodes the PEK will not be persisted once
 *       installed.</li>
 *   <li>YELLOW cannot-persist: a PEK is installed but this node lacks the password material to wrap it for disk, so its on-disk
 *       cluster state carries no usable copy of the key.</li>
 *   <li>YELLOW not-yet-persisted: this node can wrap the PEK again, but has not rewritten its on-disk copy since it last failed to.</li>
 *   <li>YELLOW master-has-no-keys: the elected master published the PEK without key material; this node keeps using its own copy.</li>
 * </ul>
 */
class ProjectEncryptionKeyHealthIndicatorService implements HealthIndicatorService {

    static final String NAME = "cluster_state_encryption";

    // TODO add URL when available
    private static final String HELP_URL = null;

    static final String GREEN_NOT_CONFIGURED = "Cluster state encryption is not configured.";
    static final String GREEN_HEALTHY = "Cluster state encryption is healthy.";
    static final String YELLOW_CANNOT_PERSIST = "Cluster state encryption cannot be persisted to disk on this node.";
    static final String YELLOW_NOT_YET_PERSISTED = "The cluster state encryption key is not yet persisted to disk on this node.";
    static final String YELLOW_PASSWORD_NOT_CONFIGURED =
        "Cluster state encryption is configured but the password for the active password id is missing on this node.";
    static final String YELLOW_MASTER_HAS_NO_KEYS =
        "The elected master holds no cluster state encryption key; this node is using its own copy of the key.";
    static final String YELLOW_OPT_OUT = "Cluster state encryption is disabled; secrets are stored unencrypted.";
    static final String YELLOW_DECRYPTION_FAILED =
        "Cluster state encryption is degraded: the encryption key could not be decrypted on startup.";
    static final String YELLOW_MISSING_PASSWORD = "Cluster state encryption is degraded: no active password is configured on this node.";

    static final List<HealthIndicatorImpact> WRAP_FAILURE_IMPACTS = List.of(
        new HealthIndicatorImpact(
            NAME,
            "encryption_key_not_persisted",
            2,
            "This node's on-disk cluster state has no usable copy of the cluster state encryption key. If the whole cluster restarts and"
                + " this node is elected master, cluster state encryption becomes unavailable.",
            List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)
        )
    );
    static final List<HealthIndicatorImpact> PASSWORD_NOT_CONFIGURED_IMPACTS = List.of(
        new HealthIndicatorImpact(
            NAME,
            "encryption_password_not_configured",
            2,
            "If this node is the elected master, no cluster state encryption key is installed and secrets cannot be encrypted. Otherwise"
                + " this node cannot persist the key to disk once it is installed.",
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
    static final List<HealthIndicatorImpact> MASTER_HAS_NO_KEYS_IMPACTS = List.of(
        new HealthIndicatorImpact(
            NAME,
            "master_has_no_encryption_key",
            2,
            "Encrypt and decrypt operations fail on the elected master, and the cluster state encryption key is not rotated.",
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
    static final Diagnosis.Definition KEY_NOT_YET_PERSISTED_DEFINITION = new Diagnosis.Definition(
        NAME,
        "key_not_yet_persisted",
        "This node could not persist the cluster state encryption key earlier and has not rewritten it since its password was configured.",
        "The key is written to disk with the next change to persistent cluster settings, index templates or other cluster-wide metadata."
            + " Changes to indices do not trigger it. To persist it now, change a persistent cluster setting or restart this node.",
        HELP_URL
    );
    static final Diagnosis.Definition PASSWORD_NOT_CONFIGURED_DEFINITION = new Diagnosis.Definition(
        NAME,
        "password_not_configured",
        "cluster.state.encryption.active_password_id is set but the matching cluster.state.encryption.password.<id> is not configured"
            + " on this node.",
        "Provision cluster.state.encryption.password.<id> for the active password id in the keystore (stateful) or operator file settings"
            + " (serverless). If the password was added to the keystore call POST /_nodes/reload_secure_settings (stateful).",
        HELP_URL
    );
    static final Diagnosis.Definition DECRYPTION_FAILED_DEFINITION = new Diagnosis.Definition(
        NAME,
        "decryption_failed",
        "The cluster state encryption key could not be decrypted on startup, likely due to a wrong or missing password.",
        "Fix the password configuration and restart the node. Other nodes may still hold the key, so only reset encryption via"
            + " POST /_encryption/_reset?accept_data_loss=true if no node reports a usable key.",
        HELP_URL
    );
    static final Diagnosis.Definition MASTER_HAS_NO_KEYS_DEFINITION = new Diagnosis.Definition(
        NAME,
        "master_has_no_key",
        "The elected master started without a usable copy of the cluster state encryption key and published cluster state without it.",
        "Configure the encryption password on the elected master and restart it, so that a master-eligible node that holds the key is"
            + " elected and restores it. If the newly elected master holds no key either, repeat this for that node. Do not reset"
            + " encryption: this node still holds the key.",
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
    private final BooleanSupplier keyMissingOnDisk;

    ProjectEncryptionKeyHealthIndicatorService(
        ClusterService clusterService,
        ProjectResolver projectResolver,
        ProjectEncryptionKeyService pekService,
        BooleanSupplier keyMissingOnDisk
    ) {
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
        this.pekService = pekService;
        this.keyMissingOnDisk = keyMissingOnDisk;
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
                if (metadata == null) {
                    if (pekService.canWrapForDisk() == false) {
                        yield createIndicator(
                            YELLOW,
                            YELLOW_PASSWORD_NOT_CONFIGURED,
                            detailsBuilder(null, serviceState),
                            PASSWORD_NOT_CONFIGURED_IMPACTS,
                            List.of(new Diagnosis(PASSWORD_NOT_CONFIGURED_DEFINITION, List.of()))
                        );
                    }
                    yield createIndicator(
                        GREEN,
                        "Cluster state encryption is configured; awaiting first key install.",
                        detailsBuilder(null, serviceState),
                        List.of(),
                        List.of()
                    );
                }
                if (pekService.isUsingLocalKeys()) {
                    yield createIndicator(
                        YELLOW,
                        YELLOW_MASTER_HAS_NO_KEYS,
                        detailsBuilder(metadata, serviceState),
                        MASTER_HAS_NO_KEYS_IMPACTS,
                        List.of(new Diagnosis(MASTER_HAS_NO_KEYS_DEFINITION, List.of()))
                    );
                }
                if (pekService.canWrapForDisk() == false) {
                    yield createIndicator(
                        YELLOW,
                        YELLOW_CANNOT_PERSIST,
                        detailsBuilder(metadata, serviceState),
                        WRAP_FAILURE_IMPACTS,
                        List.of(new Diagnosis(MISSING_WRAP_CREDENTIALS_DEFINITION, List.of()))
                    );
                }
                if (keyMissingOnDisk.getAsBoolean()) {
                    yield createIndicator(
                        YELLOW,
                        YELLOW_NOT_YET_PERSISTED,
                        detailsBuilder(metadata, serviceState),
                        WRAP_FAILURE_IMPACTS,
                        List.of(new Diagnosis(KEY_NOT_YET_PERSISTED_DEFINITION, List.of()))
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
                YELLOW,
                YELLOW_DECRYPTION_FAILED,
                detailsBuilder(metadata, serviceState),
                ENCRYPTION_UNAVAILABLE_IMPACTS,
                List.of(new Diagnosis(DECRYPTION_FAILED_DEFINITION, List.of()))
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

    private HealthIndicatorDetails detailsBuilder(@Nullable ProjectEncryptionKeyMetadata metadata, EncryptionServiceState serviceState) {
        String activePasswordId = pekService.getActivePasswordId();
        if (metadata == null) {
            return new SimpleHealthIndicatorDetails(
                Map.of(
                    "state",
                    serviceState.displayValue(),
                    "encryption_required",
                    pekService.isEncryptionRequired(),
                    "active_password_id",
                    activePasswordId == null ? "(unset)" : activePasswordId
                )
            );
        }
        return new SimpleHealthIndicatorDetails(
            Map.of(
                "state",
                serviceState.displayValue(),
                "encryption_required",
                pekService.isEncryptionRequired(),
                "active_password_id",
                activePasswordId == null ? "(unset)" : activePasswordId,
                "metadata_password_id",
                metadata.getPasswordId(),
                "active_key_id",
                metadata.getActiveKeyId(),
                "key_count",
                metadata.getKeys().size()
            )
        );
    }
}
