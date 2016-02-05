/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.permission;

import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.shield.authz.accesscontrol.IndicesAccessControl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A composite permission that combines {@code cluster}, {@code indices} and {@code run_as} permissions
 */
public class GlobalPermission implements Permission {

    public static final GlobalPermission NONE = new GlobalPermission(ClusterPermission.Core.NONE, IndicesPermission.Core.NONE,
            RunAsPermission.Core.NONE);

    private final ClusterPermission cluster;
    private final IndicesPermission indices;
    private final RunAsPermission runAs;

    GlobalPermission(ClusterPermission cluster, IndicesPermission indices, RunAsPermission runAs) {
        this.cluster = cluster;
        this.indices = indices;
        this.runAs = runAs;
    }

    public ClusterPermission cluster() {
        return cluster;
    }

    public IndicesPermission indices() {
        return indices;
    }

    public RunAsPermission runAs() {
        return runAs;
    }

    @Override
    public boolean isEmpty() {
        return (cluster == null || cluster.isEmpty()) && (indices == null || indices.isEmpty()) && (runAs == null || runAs.isEmpty());
    }

    /**
     * Returns whether at least one group encapsulated by this indices permissions is authorized to execute the
     * specified action with the requested indices/aliases. At the same time if field and/or document level security
     * is configured for any group also the allowed fields and role queries are resolved.
     */
    public IndicesAccessControl authorize(String action, Set<String> requestedIndicesOrAliases, MetaData metaData) {
        Map<String, IndicesAccessControl.IndexAccessControl> indexPermissions = indices.authorize(
                action, requestedIndicesOrAliases, metaData
        );

        // At least one role / indices permission set need to match with all the requested indices/aliases:
        boolean granted = true;
        for (Map.Entry<String, IndicesAccessControl.IndexAccessControl> entry : indexPermissions.entrySet()) {
            if (!entry.getValue().isGranted()) {
                granted = false;
                break;
            }
        }
        return new IndicesAccessControl(granted, indexPermissions);
    }

    public static class Compound extends GlobalPermission {

        public Compound(List<GlobalPermission> globals) {
            super(new ClusterPermission.Globals(globals), new IndicesPermission.Globals(globals), new RunAsPermission.Globals(globals));
        }

        public static Compound.Builder builder() {
            return new Compound.Builder();
        }

        public static class Builder {

            private List<GlobalPermission> globals = new ArrayList<>();

            private Builder() {
            }

            public Compound.Builder add(GlobalPermission global) {
                globals.add(global);
                return this;
            }

            public Compound build() {
                return new Compound(Collections.unmodifiableList(globals));
            }
        }
    }
}
