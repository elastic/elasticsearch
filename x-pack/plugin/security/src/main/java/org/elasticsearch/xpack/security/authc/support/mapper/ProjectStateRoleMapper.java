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
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.core.security.authz.RoleMappingMetadata;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.security.SecurityExtension.SecurityComponents;

/**
 * A role mapper the reads the role mapping rules (i.e. {@link ExpressionRoleMapping}s) from the cluster state
 * (i.e. {@link RoleMappingMetadata}). This is not enabled by default.
 */
public class ProjectStateRoleMapper extends AbstractRoleMapperClearRealmCache implements ClusterStateListener {
    /**
     * This setting is never registered by the xpack security plugin - in order to disable the
     * cluster-state based role mapper another plugin must register it as a boolean setting
     * and set it to `false`.
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
    private static final Logger logger = LogManager.getLogger(ProjectStateRoleMapper.class);

    private final ScriptService scriptService;
    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;
    private final boolean enabled;

    public ProjectStateRoleMapper(
        Settings settings,
        ScriptService scriptService,
        ClusterService clusterService,
        ProjectResolver projectResolver
    ) {
        this.scriptService = scriptService;
        this.clusterService = clusterService;
        // this role mapper is enabled by default and only code in other plugins can disable it
        this.enabled = settings.getAsBoolean(CLUSTER_STATE_ROLE_MAPPINGS_ENABLED, true);
        this.projectResolver = projectResolver;
        if (this.enabled) {
            clusterService.addListener(this);
        }
    }

    @Override
    public void resolveRoles(UserData user, ActionListener<Set<String>> listener) {
        listener.onResponse(ExpressionRoleMapping.resolveRoles(user, getMappings(), scriptService, logger));
    }

    @Override
    @FixForMultiProject(description = "It would be better to clear a project specific realm cache, rather than the cache for all projects")
    public void clusterChanged(ClusterChangedEvent event) {
        // The cluster state (which contains the new role mappings) is already applied when this listener is called,
        // such that {@link #resolveRoles} will be returning the new role mappings when called after this is called
        if (enabled) {
            if (roleMappingsChanged(event)) {
                // trigger realm cache clear, even if only disabled role mappings have changed
                // ideally disabled role mappings should not be published in the cluster state
                clearRealmCachesOnLocalNode();
            }
        }
    }

    private boolean roleMappingsChanged(ClusterChangedEvent event) {
        final Map<ProjectId, ProjectMetadata> previousProjects = event.previousState().metadata().projects();
        for (ProjectMetadata currentProject : event.state().metadata().projects().values()) {
            final RoleMappingMetadata currentMapping = RoleMappingMetadata.getFromProject(currentProject);
            final ProjectMetadata previousProject = previousProjects.get(currentProject.id());
            if (previousProject == null) {
                if (currentMapping != null) {
                    return true;
                } else {
                    continue;
                }
            }
            final RoleMappingMetadata previousMapping = RoleMappingMetadata.getFromProject(previousProject);
            if (false == Objects.equals(previousMapping, currentMapping)) {
                return true;
            }
        }
        return false;
    }

    public boolean hasMapping(String name) {
        if (enabled == false) {
            return false;
        }
        return false == getMappings(Set.of(name)).isEmpty();
    }

    public Set<ExpressionRoleMapping> getMappings() {
        return getMappings(null);
    }

    public Set<ExpressionRoleMapping> getMappings(@Nullable Set<String> names) {
        if (enabled == false) {
            return Set.of();
        }
        final ProjectMetadata project = projectResolver.getProjectMetadata(clusterService.state());
        final Set<ExpressionRoleMapping> mappings = RoleMappingMetadata.getFromProject(project).getRoleMappings();
        logger.trace("Retrieved [{}] mapping(s) from cluster state", mappings.size());
        if (names == null || names.isEmpty()) {
            return mappings;
        }
        return mappings.stream().filter(roleMapping -> names.contains(roleMapping.getName())).collect(Collectors.toSet());
    }
}
