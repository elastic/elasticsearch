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
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xpack.core.security.authc.support.CachingRealm;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.ExpressionModel;
import org.elasticsearch.xpack.core.security.authz.RoleMappingMetadata;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static org.elasticsearch.common.Strings.format;

public final class ClusterStateRoleMapper implements UserRoleMapper, ClusterStateListener {

    // TODO Is this a good name??
    public static final String CLUSTER_STATE_ROLE_MAPPINGS_ENABLED = "xpack.security.authc.cluster_state_role_mappings.enabled";
    private static final Logger logger = LogManager.getLogger(ClusterStateRoleMapper.class);

    private final ScriptService scriptService;
    private final ClusterService clusterService;
    private final boolean enabled;
    private final CopyOnWriteArrayList<Runnable> clearCacheListeners = new CopyOnWriteArrayList<>();

    public ClusterStateRoleMapper(Settings settings, ScriptService scriptService, ClusterService clusterService) {
        this.scriptService = scriptService;
        this.clusterService = clusterService;
        this.enabled = settings.getAsBoolean(CLUSTER_STATE_ROLE_MAPPINGS_ENABLED, false);
        if (this.enabled) {
            clusterService.addListener(this);
        }
    }

    @Override
    public void resolveRoles(UserData user, ActionListener<Set<String>> listener) {
        ExpressionModel model = user.asModel();
        Set<String> roles = getMappings().stream()
            .filter(ExpressionRoleMapping::isEnabled)
            .filter(m -> m.getExpression().match(model))
            .flatMap(m -> {
                Set<String> roleNames = m.getRoleNames(scriptService, model);
                logger.trace(
                    () -> format("Applying role-mapping [{}] to user-model [{}] produced role-names [{}]", m.getName(), model, roleNames)
                );
                return roleNames.stream();
            })
            .collect(Collectors.toSet());
        logger.debug(() -> format("Mapping user [{}] to roles [{}]", user, roles));
        listener.onResponse(roles);
    }

    @Override
    public void refreshRealmOnChange(CachingRealm realm) {
        clearCacheListeners.add(realm::expireAll);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // Here, it's just simpler to also trigger a realm cache clear when only disabled role mapping expressions changed,
        // even though disable role mapping expressions are ultimately ignored.
        // Instead, it's better to ensure disabled role mapping expressions are not published in the cluster state in the first place.
        if (enabled
            && false == Objects.equals(
                RoleMappingMetadata.getFromClusterState(event.previousState()),
                RoleMappingMetadata.getFromClusterState(event.state())
            )) {
            notifyClearCache();
        }
    }

    private Set<ExpressionRoleMapping> getMappings() {
        if (enabled == false) {
            return Set.of();
        } else {
            return RoleMappingMetadata.getFromClusterState(clusterService.state()).getRoleMappings();
        }
    }

    private void notifyClearCache() {
        clearCacheListeners.forEach(Runnable::run);
    }
}
