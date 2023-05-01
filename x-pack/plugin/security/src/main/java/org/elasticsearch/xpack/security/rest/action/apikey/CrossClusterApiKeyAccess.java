/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.apikey;

import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.util.List;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class CrossClusterApiKeyAccess {

    private final List<RoleDescriptor.IndicesPrivileges> search;
    private final List<RoleDescriptor.IndicesPrivileges> replication;

    private CrossClusterApiKeyAccess(List<RoleDescriptor.IndicesPrivileges> search, List<RoleDescriptor.IndicesPrivileges> replication) {
        this.search = search == null ? List.of() : search;
        this.replication = replication == null ? List.of() : replication;
    }

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<CrossClusterApiKeyAccess, Void> PARSER = new ConstructingObjectParser<>(
        "cross_cluster_api_key_request_access",
        false,
        (args, v) -> new CrossClusterApiKeyAccess(
            (List<RoleDescriptor.IndicesPrivileges>) args[0],
            (List<RoleDescriptor.IndicesPrivileges>) args[1]
        )
    );

    static {
        PARSER.declareObjectArray(
            optionalConstructorArg(),
            (p, c) -> RoleDescriptor.parseIndexWithPrivileges(
                "cross_cluster",
                new String[] { "read", "read_cross_cluster", "view_index_metadata" },
                p
            ),
            new ParseField("search")
        );
        PARSER.declareObjectArray(
            optionalConstructorArg(),
            (p, c) -> RoleDescriptor.parseIndexWithPrivileges(
                "cross_cluster",
                new String[] {
                    "manage",
                    "read",
                    "indices:internal/admin/ccr/restore/*",
                    "internal:transport/proxy/indices:internal/admin/ccr/restore/*" },
                p
            ),
            new ParseField("replication")
        );
    }

    RoleDescriptor toRoleDescriptor(String name) {
        final String[] clusterPrivileges;
        if (search.isEmpty() && replication.isEmpty()) {
            throw new IllegalArgumentException("must specify non-empty access for either [search] or [replication]");
        } else if (search.isEmpty()) {
            clusterPrivileges = new String[] { "cross_cluster_access", "cluster:monitor/state" };
        } else if (replication.isEmpty()) {
            clusterPrivileges = new String[] { "cross_cluster_access" };
        } else {
            clusterPrivileges = new String[] { "cross_cluster_access", "cluster:monitor/state" };
        }

        if (replication.stream().anyMatch(RoleDescriptor.IndicesPrivileges::isUsingDocumentOrFieldLevelSecurity)) {
            throw new IllegalArgumentException("replication does not support document or field level security");
        }

        return new RoleDescriptor(
            name,
            clusterPrivileges,
            CollectionUtils.concatLists(search, replication).toArray(RoleDescriptor.IndicesPrivileges[]::new),
            null
        );
    }
}
