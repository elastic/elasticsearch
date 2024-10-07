/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.support.mapper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.core.security.authz.RoleMappingMetadata;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.security.SecurityExtension.SecurityComponents;

/**
 * A role mapper the reads the role mapping rules (i.e. {@link ExpressionRoleMapping}s) from the cluster state
 * (i.e. {@link RoleMappingMetadata}). This is not enabled by default.
 */
public class ClusterStateRoleMapper extends AbstractRoleMapperClearRealmCache implements ClusterStateListener {
    /**
     * This setting is never registered by the xpack security plugin - in order to enable the
     * cluster-state based role mapper another plugin must register it as a boolean setting
     * and set it to `true`.
     * If this setting is set to <code>true</code> then:
     * <ul>
     *     <li>Realms that make use role mappings (all realms but file and native) will,
     *          in addition, observe the role mappings set in the cluster state.</li>
     *     <li>Similarly, xpack security's {@link SecurityComponents} extensions will,
     *          additionally, observe the cluster state role mappings too.</li>
     *     <li>{@link UserRoleMapper} class will be guice-bound to a {@link CompositeRoleMapper}
     *          of the {@link NativeRoleMappingStore} and this mapper.</li>
     * </ul>
     */
    public static final String CLUSTER_STATE_ROLE_MAPPINGS_ENABLED = "xpack.security.authc.cluster_state_role_mappings.enabled";
    public static final String RESERVED_ROLE_MAPPING_SUFFIX = "-read-only-operator-config";
    private static final Logger logger = LogManager.getLogger(ClusterStateRoleMapper.class);

    private final ScriptService scriptService;
    private final ClusterService clusterService;
    private final boolean enabled;

    public ClusterStateRoleMapper(Settings settings, ScriptService scriptService, ClusterService clusterService) {
        this.scriptService = scriptService;
        this.clusterService = clusterService;
        // this role mapper is enabled by default and only code in other plugins can disable it
        this.enabled = settings.getAsBoolean(CLUSTER_STATE_ROLE_MAPPINGS_ENABLED, true);
        if (this.enabled) {
            clusterService.addListener(this);
        }
    }

    @Override
    public void resolveRoles(UserData user, ActionListener<Set<String>> listener) {
        listener.onResponse(ExpressionRoleMapping.resolveRoles(user, getMappings(), scriptService, logger));
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // The cluster state (which contains the new role mappings) is already applied when this listener is called,
        // such that {@link #resolveRoles} will be returning the new role mappings when called after this is called
        if (enabled
            && false == Objects.equals(
                RoleMappingMetadata.getFromClusterState(event.previousState()),
                RoleMappingMetadata.getFromClusterState(event.state())
            )) {
            // trigger realm cache clear, even if only disabled role mappings have changed
            // ideally disabled role mappings should not be published in the cluster state
            clearRealmCachesOnLocalNode();
        }
    }

    public boolean hasMapping(String name) {
        return getMappings().stream().map(ExpressionRoleMapping::getName).anyMatch(name::equals);
    }

    public Set<ExpressionRoleMapping> getMappings(@Nullable Set<String> names) {
        if (enabled == false) {
            return Set.of();
        }
        final Set<ExpressionRoleMapping> mappings = getMappings();
        if (names == null || names.isEmpty()) {
            return mappings;
        }
        return mappings.stream().filter(it -> names.contains(it.getName())).collect(Collectors.toSet());
    }

    private Set<ExpressionRoleMapping> getMappings() {
        if (enabled == false) {
            return Set.of();
        } else {
            final Set<ExpressionRoleMapping> mappings = RoleMappingMetadata.getFromClusterState(clusterService.state()).getRoleMappings();
            logger.trace(
                "Retrieved mapping(s) {} from cluster state",
                Arrays.toString(mappings.stream().map(ExpressionRoleMapping::getName).toArray(String[]::new))
            );
            return mappings;
        }
    }
}
