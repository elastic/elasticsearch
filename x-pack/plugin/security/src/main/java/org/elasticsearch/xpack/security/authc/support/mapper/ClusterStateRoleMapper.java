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
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.ExpressionModel;
import org.elasticsearch.xpack.core.security.authz.RoleMappingMetadata;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public final class ClusterStateRoleMapper extends AbstractRoleMapperClearRealmCache implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(ClusterStateRoleMapper.class);

    private final ScriptService scriptService;
    private final ClusterService clusterService;

    public ClusterStateRoleMapper(ScriptService scriptService, ClusterService clusterService) {
        this.scriptService = scriptService;
        this.clusterService = clusterService;
        clusterService.addListener(this);
    }

    @Override
    public void resolveRoles(UserData user, ActionListener<Set<String>> listener) {
        ExpressionModel model = user.asModel();
        Set<String> roles = getMappings().stream()
            .filter(ExpressionRoleMapping::isEnabled)
            .filter(m -> m.getExpression().match(model))
            .flatMap(m -> {
                Set<String> roleNames = m.getRoleNames(scriptService, model);
                logger.trace("Applying role-mapping [{}] to user-model [{}] produced role-names [{}]", m.getName(), model, roleNames);
                return roleNames.stream();
            })
            .collect(Collectors.toSet());
        logger.debug("Mapping user [{}] to roles [{}]", user, roles);
        listener.onResponse(roles);
    }

    private Set<ExpressionRoleMapping> getMappings() {
        return RoleMappingMetadata.getFromClusterState(clusterService.state()).getRoleMappings();
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (false == Objects.equals(
            RoleMappingMetadata.getFromClusterState(event.previousState()),
            RoleMappingMetadata.getFromClusterState(event.state())
        )) {
            clearRealmCachesOnLocalNode();
        }
    }
}
