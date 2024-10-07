/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.rolemapping;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.rolemapping.GetRoleMappingsAction;
import org.elasticsearch.xpack.core.security.action.rolemapping.GetRoleMappingsRequest;
import org.elasticsearch.xpack.core.security.action.rolemapping.GetRoleMappingsResponse;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.security.authc.support.mapper.ClusterStateRoleMapper;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.security.authc.support.mapper.ClusterStateRoleMapper.RESERVED_ROLE_MAPPING_SUFFIX;

public class TransportGetRoleMappingsAction extends HandledTransportAction<GetRoleMappingsRequest, GetRoleMappingsResponse> {

    private final NativeRoleMappingStore roleMappingStore;
    private final ClusterStateRoleMapper clusterStateRoleMapper;

    @Inject
    public TransportGetRoleMappingsAction(
        ActionFilters actionFilters,
        TransportService transportService,
        NativeRoleMappingStore nativeRoleMappingStore,
        ClusterStateRoleMapper clusterStateRoleMapper
    ) {
        super(
            GetRoleMappingsAction.NAME,
            transportService,
            actionFilters,
            GetRoleMappingsRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.roleMappingStore = nativeRoleMappingStore;
        this.clusterStateRoleMapper = clusterStateRoleMapper;
    }

    @Override
    protected void doExecute(Task task, final GetRoleMappingsRequest request, final ActionListener<GetRoleMappingsResponse> listener) {
        final Set<String> names;
        if (request.getNames() == null || request.getNames().length == 0) {
            names = null;
        } else {
            names = new HashSet<>(Arrays.asList(request.getNames()));
        }
        roleMappingStore.getRoleMappings(names, ActionListener.wrap(mappings -> {
            List<ExpressionRoleMapping> combinedRoleMappings = Stream.concat(
                mappings.stream(),
                clusterStateRoleMapper.getMappings(names == null ? null : names.stream().map(name -> {
                    // If a read-only role is fetched by name including suffix, remove suffix
                    return name.endsWith(RESERVED_ROLE_MAPPING_SUFFIX)
                        ? name.substring(0, name.length() - RESERVED_ROLE_MAPPING_SUFFIX.length())
                        : name;
                }).collect(Collectors.toSet()))
                    .stream()
                    .map(this::cloneAndMarkAsReadOnly)
                    .sorted(Comparator.comparing(ExpressionRoleMapping::getName))
            ).toList();
            listener.onResponse(new GetRoleMappingsResponse(combinedRoleMappings));
        }, listener::onFailure));
    }

    private ExpressionRoleMapping cloneAndMarkAsReadOnly(ExpressionRoleMapping mapping) {
        // Mark role mappings from cluster state as "read only" by adding a suffix to their name
        return new ExpressionRoleMapping(
            mapping.getName() + RESERVED_ROLE_MAPPING_SUFFIX,
            mapping.getExpression(),
            mapping.getRoles(),
            mapping.getRoleTemplates(),
            mapping.getMetadata(),
            mapping.isEnabled()
        );
    }
}
