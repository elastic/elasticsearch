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

import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.YELLOW;

/**
 * Reports health for the project encryption key (PEK). Since PEK bytes travel in plaintext over the TLS-protected transport channel,
 * the node can always encrypt and decrypt at runtime once a key is installed. This indicator surfaces two operational concerns:
 * <ul>
 *   <li>YELLOW opt-out: no password is configured and {@code cluster.state.encryption.required} is {@code false}, meaning
 *       secrets are stored unencrypted in cluster state.</li>
 *   <li>YELLOW cannot-persist: a PEK is installed but this node lacks the password material to wrap it for disk. The next
 *       cluster-state persist on this node will fail, and the node will not survive a restart without the password.</li>
 * </ul>
 */
class ProjectEncryptionKeyHealthIndicatorService implements HealthIndicatorService {

    static final String NAME = "project_encryption_key";

    // TODO add URL when available
    private static final String HELP_URL = null;

    static final String GREEN_NOT_CONFIGURED = "Cluster state encryption is not configured.";
    static final String GREEN_HEALTHY = "Cluster state encryption is healthy.";
    static final String YELLOW_CANNOT_PERSIST = "Cluster state encryption cannot be persisted to disk on this node.";
    static final String YELLOW_OPT_OUT = "Cluster state encryption is disabled; secrets are stored unencrypted.";
    static final String YELLOW_DECRYPTION_FAILED =
        "Cluster state encryption is degraded: the encryption key could not be decrypted on startup.";
    static final String YELLOW_MISSING_PASSWORD = "Cluster state encryption is degraded: no active password is configured on this node.";

    static final List<HealthIndicatorImpact> WRAP_FAILURE_IMPACTS = List.of(
        new HealthIndicatorImpact(
            NAME,
            "encryption_key_not_persisted",
            2,
            "The project encryption key cannot be persisted to disk. This node will fail to start after a restart.",
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
        "A project encryption key is installed but this node lacks the password material to wrap it for disk.",
        "Provision cluster.state.encryption.active_password_id and the matching cluster.state.encryption.password.<id> in the keystore"
            + " (stateful) or operator file settings (serverless). If the password was added to the keystore call"
            + " POST /_nodes/reload_secure_settings (stateful).",
        HELP_URL
    );
    static final Diagnosis.Definition DECRYPTION_FAILED_DEFINITION = new Diagnosis.Definition(
        NAME,
        "decryption_failed",
        "The project encryption key could not be decrypted on startup, likely due to a wrong or missing password.",
        "Fix the password configuration and restart the node, or reset encryption via POST /_encryption/_reset?accept_data_loss=true.",
        HELP_URL
    );
    static final Diagnosis.Definition MISSING_PASSWORD_DEFINITION = new Diagnosis.Definition(
        NAME,
        "missing_password",
        "No active password is configured on this node; the project encryption key cannot be decrypted.",
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

    ProjectEncryptionKeyHealthIndicatorService(
        ClusterService clusterService,
        ProjectResolver projectResolver,
        ProjectEncryptionKeyService pekService
    ) {
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
        this.pekService = pekService;
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
                    yield createIndicator(
                        GREEN,
                        "Cluster state encryption is configured; awaiting first key install.",
                        detailsBuilder(null, serviceState),
                        List.of(),
                        List.of()
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
