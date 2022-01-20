/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.snapshotbasedrecoveries.health;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.HealthIndicator;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class RepositoryCorruptionHealthIndicatorService implements HealthIndicatorService, ClusterStateApplier {

    private volatile List<HealthIndicator> indicators = List.of();

    public RepositoryCorruptionHealthIndicatorService(ClusterService clusterService) {
        clusterService.addStateApplier(this);
    }

    @Override
    public List<HealthIndicator> getIndicators() {
        return indicators;
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        if (event.localNodeMaster() == false) {
            this.indicators = List.of();
        }
        if (repositoriesChanged(event)) {
            RepositoriesMetadata repositories = event.state().metadata().custom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY);
            final List<RepositoryCorruptionHealthIndicator> updated = new ArrayList<>();
            boolean changed = false;
            Long now = null;
            for (RepositoryMetadata repository : repositories.repositories()) {
                if (repository.generation() == RepositoryData.CORRUPTED_REPO_GEN) {
                    if (now == null) {
                        now = Instant.now().toEpochMilli();
                    }
                    updated.add(new RepositoryCorruptionHealthIndicator(repository.name(), repository.uuid(), now));
                    changed = true;
                }
            }
            if (changed) {
                this.indicators = Collections.unmodifiableList(updated);
            } else if (repositories.repositories().isEmpty()) {
                this.indicators = List.of();
            }
        }
    }

    private boolean repositoriesChanged(ClusterChangedEvent event) {
        RepositoriesMetadata previous = event.previousState().metadata().custom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY);
        RepositoriesMetadata current = event.state().metadata().custom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY);
        return Objects.equals(previous, current) == false;
    }

    static class RepositoryCorruptionHealthIndicator extends HealthIndicator {

        private static final String NAME = "repository-corruption";

        private final String repositoryName;
        private final String repositoryUuid;
        private final long creationTime;

        RepositoryCorruptionHealthIndicator(String repositoryName, String repositoryUuid, long creationTime) {
            super(NAME, COMPONENT_SNAPSHOTS, Status.RED, "Repository is marked as corrupted");
            this.repositoryName = Objects.requireNonNull(repositoryName);
            this.repositoryUuid = Objects.requireNonNull(repositoryUuid);
            this.creationTime = creationTime;
        }

        public XContentBuilder metaToXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("repository_name", repositoryName);
            builder.field("repository_uuid", repositoryUuid);
            builder.field("corrupted_at", Instant.ofEpochMilli(creationTime).atZone(ZoneOffset.UTC));
            return builder;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.V_8_1_0;
        }
    }
}
