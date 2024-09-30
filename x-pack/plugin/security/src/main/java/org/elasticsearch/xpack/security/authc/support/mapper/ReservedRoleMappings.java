/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.support.mapper;

import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ReservedRoleMappings {
    private final ClusterStateRoleMapper clusterStateRoleMapper;

    public ReservedRoleMappings(ClusterStateRoleMapper clusterStateRoleMapper) {
        this.clusterStateRoleMapper = clusterStateRoleMapper;
    }

    public List<ExpressionRoleMapping> combineWithReserved(List<ExpressionRoleMapping> roleMappings) {
        final Set<ExpressionRoleMapping> reservedRoleMappings = clusterStateRoleMapper.getMappings();
        if (reservedRoleMappings.isEmpty()) {
            return roleMappings;
        }
        if (roleMappings.isEmpty()) {
            return List.copyOf(reservedRoleMappings);
        }
        final Map<String, ExpressionRoleMapping> combinedMappings = new LinkedHashMap<>();
        for (ExpressionRoleMapping mapping : reservedRoleMappings) {
            combinedMappings.put(mapping.getName(), mapping);
        }
        for (ExpressionRoleMapping mapping : roleMappings) {
            combinedMappings.putIfAbsent(mapping.getName(), mapping);
        }
        return List.copyOf(combinedMappings.values());
    }

    public boolean isReserved(String roleMappingName) {
        return clusterStateRoleMapper.getMappings().stream().anyMatch(roleMapping -> roleMapping.getName().equals(roleMappingName));
    }
}
