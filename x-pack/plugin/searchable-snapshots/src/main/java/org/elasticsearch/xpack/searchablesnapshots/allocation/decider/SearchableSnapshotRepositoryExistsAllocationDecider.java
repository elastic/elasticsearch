/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.allocation.decider;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;

import java.util.List;

import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_REPOSITORY_NAME_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_REPOSITORY_UUID_SETTING;

public class SearchableSnapshotRepositoryExistsAllocationDecider extends AllocationDecider {

    private static final String NAME = "searchable_snapshot_repository_exists";

    private static final Decision YES_INAPPLICABLE = Decision.single(
        Decision.Type.YES,
        NAME,
        "this decider only applies to indices backed by searchable snapshots"
    );

    private static final Decision YES_REPOSITORY_EXISTS = Decision.single(
        Decision.Type.YES,
        NAME,
        "the repository containing the data for this index exists"
    );

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return allowAllocation(allocation.metadata().indexMetadata(shardRouting.index()), allocation);
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
        return allowAllocation(allocation.metadata().indexMetadata(shardRouting.index()), allocation);
    }

    @Override
    public Decision canAllocate(IndexMetadata indexMetadata, RoutingNode node, RoutingAllocation allocation) {
        return allowAllocation(indexMetadata, allocation);
    }

    private static Decision allowAllocation(IndexMetadata indexMetadata, RoutingAllocation allocation) {
        if (indexMetadata.isSearchableSnapshot()) {
            final Settings settings = indexMetadata.getSettings();

            final var repositoriesMetadata = RepositoriesMetadata.get(allocation.getClusterState());
            if (repositoriesMetadata.repositories().isEmpty()) {
                return allocation.decision(Decision.NO, NAME, "there are no repositories registered in this cluster");
            }

            final String repositoryUuid = SNAPSHOT_REPOSITORY_UUID_SETTING.get(settings);
            if (Strings.hasLength(repositoryUuid)) {
                final List<RepositoryMetadata> repositories = repositoriesMetadata.repositories();
                if (repositories.stream().anyMatch(r -> repositoryUuid.equals(r.uuid()))) {
                    return YES_REPOSITORY_EXISTS;
                }

                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "this index is backed by a searchable snapshot in a repository with UUID [%s] but no such repository is registered "
                        + "with this cluster; the required repository was originally named [%s]",
                    repositoryUuid,
                    SNAPSHOT_REPOSITORY_NAME_SETTING.get(settings)
                );

            } else {
                final String repositoryName = SNAPSHOT_REPOSITORY_NAME_SETTING.get(settings);
                if (repositoriesMetadata.repository(repositoryName) != null) {
                    return YES_REPOSITORY_EXISTS;
                }

                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "this index is backed by a searchable snapshot in a repository named [%s] but no such repository is registered "
                        + "with this cluster",
                    repositoryName
                );
            }
        } else {
            return YES_INAPPLICABLE;
        }
    }
}
