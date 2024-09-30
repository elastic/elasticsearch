/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.support.mapper;

import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ReservedRoleMappings {
    private final ClusterStateRoleMapper clusterStateRoleMapper;

    public ReservedRoleMappings(ClusterStateRoleMapper clusterStateRoleMapper) {
        this.clusterStateRoleMapper = clusterStateRoleMapper;
    }

    // TODO don't need this?
    public List<ExpressionRoleMapping> filterOutExcluded(List<ExpressionRoleMapping> roleMappings) {
        if (roleMappings.isEmpty()) {
            return roleMappings;
        }
        final Set<ExpressionRoleMapping> excludedRoleMappings = clusterStateRoleMapper.getMappings();
        if (excludedRoleMappings.isEmpty()) {
            return roleMappings;
        }
        final Set<String> namesToExclude = excludedRoleMappings.stream().map(ExpressionRoleMapping::getName).collect(Collectors.toSet());
        return roleMappings.stream().filter(it -> false == namesToExclude.contains(it.getName())).toList();
    }

    public List<ExpressionRoleMapping> combineWithReserved(List<ExpressionRoleMapping> roleMappings) {
        final Set<ExpressionRoleMapping> excludedRoleMappings = clusterStateRoleMapper.getMappings();
        if (excludedRoleMappings.isEmpty()) {
            return roleMappings;
        }
        final Set<String> namesToExclude = excludedRoleMappings.stream().map(ExpressionRoleMapping::getName).collect(Collectors.toSet());
        final List<ExpressionRoleMapping> filteredNativeRoleMappings = roleMappings.stream()
            .filter(it -> false == namesToExclude.contains(it.getName()))
            .toList();
        // TODO optimize
        final var combined = new ArrayList<>(excludedRoleMappings);
        combined.addAll(filteredNativeRoleMappings);
        return List.copyOf(combined);
    }

    public boolean isReserved(String roleMappingName) {
        return clusterStateRoleMapper.getMappings().stream().anyMatch(roleMapping -> roleMapping.getName().equals(roleMappingName));
    }
}
