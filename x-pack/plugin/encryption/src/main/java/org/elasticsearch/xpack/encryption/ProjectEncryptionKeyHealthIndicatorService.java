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
 * Reports health for the project encryption key (PEK). The indicator is YELLOW when a PEK is
 * installed in cluster state but the local node cannot unlock it — either because the matching
 * {@code cluster.state.encryption.password.<id>} is not present in secure settings, or because
 * the configured value fails to decrypt the persisted key.
 *
 * <p>Encryption-dependent features cannot read or write encrypted data while in this state. The
 * cluster is otherwise operational; data not encrypted by features is unaffected.
 */
class ProjectEncryptionKeyHealthIndicatorService implements HealthIndicatorService {

    static final String NAME = "project_encryption_key";

    // TODO add URL when available
    private static final String HELP_URL = null;

    static final String GREEN_NOT_CONFIGURED = "Cluster state encryption is not configured.";
    static final String GREEN_HEALTHY = "Cluster state encryption is healthy.";
    static final String YELLOW_LOCKED = "Cluster state encryption is unavailable on this node.";
    static final String YELLOW_OPT_OUT = "Cluster state encryption is disabled; secrets are stored unencrypted.";

    static final List<HealthIndicatorImpact> IMPACTS = List.of(
        new HealthIndicatorImpact(
            NAME,
            "encryption_dependent_features_unavailable",
            2,
            "Encryption-dependent features cannot read or write encrypted data on this node.",
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

    static final Diagnosis.Definition MISSING_PASSWORD_DEFINITION = new Diagnosis.Definition(
        NAME,
        "missing_password",
        "The active password id is set, but no matching secure setting was found.",
        "Provision the matching cluster.state.encryption.password.<id> in the keystore (stateful) or operator file settings (serverless)."
            + " If the password was added to the keystore call POST /_nodes/reload_secure_settings (stateful).",
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
    static final Diagnosis.Definition ENCRYPTION_OPT_OUT_MISCONFIGURED_DEFINITION = new Diagnosis.Definition(
        NAME,
        "encryption_opt_out_misconfigured",
        "Cluster state encryption is misconfigured and cluster.state.encryption.required is false;"
            + " secrets are stored unencrypted.",
        "Fix the encryption configuration (the 'state' field in details describes the specific problem),"
            + " or set cluster.state.encryption.required: true to reject credential writes until encryption is restored.",
        HELP_URL
    );
    static final Diagnosis.Definition UNDECRYPTABLE_DEFINITION = new Diagnosis.Definition(
        NAME,
        "undecryptable_pek",
        "A project encryption key is installed in cluster state but the local node cannot unlock it.",
        "Verify the configured cluster.state.encryption.password.<id> matches the password id stored alongside the encrypted key."
            + " If unrecoverable, run POST /_encryption/_reset?accept_data_loss=true to drop all encrypted state.",
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
            case UNAVAILABLE_MISSING_PASSWORD -> {
                if (pekService.isEncryptionRequired() == false) {
                    yield createIndicator(
                        YELLOW,
                        YELLOW_OPT_OUT,
                        detailsBuilder(metadata, serviceState),
                        OPT_OUT_IMPACTS,
                        List.of(new Diagnosis(ENCRYPTION_OPT_OUT_MISCONFIGURED_DEFINITION, List.of()))
                    );
                }
                yield createIndicator(
                    YELLOW,
                    YELLOW_LOCKED,
                    detailsBuilder(metadata, serviceState),
                    IMPACTS,
                    List.of(new Diagnosis(MISSING_PASSWORD_DEFINITION, List.of()))
                );
            }
            case UNAVAILABLE_DECRYPTION_FAILED -> {
                if (pekService.isEncryptionRequired() == false) {
                    yield createIndicator(
                        YELLOW,
                        YELLOW_OPT_OUT,
                        detailsBuilder(metadata, serviceState),
                        OPT_OUT_IMPACTS,
                        List.of(new Diagnosis(ENCRYPTION_OPT_OUT_MISCONFIGURED_DEFINITION, List.of()))
                    );
                }
                yield createIndicator(
                    YELLOW,
                    YELLOW_LOCKED,
                    detailsBuilder(metadata, serviceState),
                    IMPACTS,
                    List.of(new Diagnosis(UNDECRYPTABLE_DEFINITION, List.of()))
                );
            }
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
