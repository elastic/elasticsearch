/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.rolemapping;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.elasticsearch.xpack.core.security.authc.support.mapper.ReservedRoleMappingXContentNameFieldHelper;
import org.elasticsearch.xpack.security.authc.support.mapper.ClusterStateRoleMapper;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TransportGetRoleMappingsAction extends HandledTransportAction<GetRoleMappingsRequest, GetRoleMappingsResponse> {
    private static final Logger logger = LogManager.getLogger(TransportGetRoleMappingsAction.class);

    static final String READ_ONLY_ROLE_MAPPING_SUFFIX = " (read only)";
    static final String READ_ONLY_METADATA_FLAG = "_read_only";

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
            names = Set.of();
        } else {
            names = new HashSet<>(Arrays.asList(request.getNames()));
        }
        roleMappingStore.getRoleMappings(names, ActionListener.wrap(nativeRoleMappings -> {
            final Collection<ExpressionRoleMapping> clusterStateRoleMappings = clusterStateRoleMapper.getMappings(
                // if the API was queried with a reserved suffix for any of the names, we need to remove it because role mappings are
                // stored without it in cluster-state
                removeReservedSuffix(names)
            );
            listener.onResponse(buildResponse(clusterStateRoleMappings, nativeRoleMappings));
        }, listener::onFailure));
    }

    private GetRoleMappingsResponse buildResponse(
        Collection<ExpressionRoleMapping> clusterStateRoleMappings,
        Collection<ExpressionRoleMapping> nativeRoleMappings
    ) {
        return new GetRoleMappingsResponse(
            Stream.concat(nativeRoleMappings.stream(), clusterStateRoleMappings.stream().map(this::toResponseModel))
                .toArray(ExpressionRoleMapping[]::new)
        );
    }

    private ExpressionRoleMapping toResponseModel(ExpressionRoleMapping mapping) {
        Map<String, Object> metadata = new HashMap<>(mapping.getMetadata());
        metadata.remove(ReservedRoleMappingXContentNameFieldHelper.METADATA_NAME_FIELD);
        if (metadata.put(READ_ONLY_METADATA_FLAG, true) != null) {
            logger.error(
                "Metadata field [{}] is reserved and will be overwritten with an internal system value. "
                    + "Please rename this field in your role mapping configuration.",
                READ_ONLY_METADATA_FLAG
            );
        }
        return new ExpressionRoleMapping(
            addReservedSuffix(mapping),
            mapping.getExpression(),
            mapping.getRoles(),
            mapping.getRoleTemplates(),
            metadata,
            mapping.isEnabled()
        );
    }

    private String addReservedSuffix(ExpressionRoleMapping mapping) {
        return mapping.getName() + READ_ONLY_ROLE_MAPPING_SUFFIX;
    }

    private Set<String> removeReservedSuffix(Set<String> names) {
        return names.stream().map(this::removeReservedSuffix).collect(Collectors.toSet());
    }

    private String removeReservedSuffix(String name) {
        if (name.endsWith(READ_ONLY_ROLE_MAPPING_SUFFIX)) {
            return name.substring(0, name.length() - READ_ONLY_ROLE_MAPPING_SUFFIX.length());
        }
        return name;
    }
}
