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
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

public class TransportGetRoleMappingsAction extends HandledTransportAction<GetRoleMappingsRequest, GetRoleMappingsResponse> {
    private static final Logger logger = LogManager.getLogger(TransportGetRoleMappingsAction.class);

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
                TransportClusterStateRoleMappingTranslator.removeReadOnlySuffixIfPresent(names)
            );
            listener.onResponse(buildResponse(clusterStateRoleMappings, nativeRoleMappings));
        }, listener::onFailure));
    }

    private GetRoleMappingsResponse buildResponse(
        Collection<ExpressionRoleMapping> clusterStateMappings,
        Collection<ExpressionRoleMapping> nativeMappings
    ) {
        Stream<ExpressionRoleMapping> translatedClusterStateMappings = clusterStateMappings.stream().filter(roleMapping -> {
            if (ReservedRoleMappingXContentNameFieldHelper.hasFallbackName(roleMapping)) {
                logger.warn(
                    "Role mapping retrieved from cluster-state with an ambiguous name. It will be omitted from the API response."
                        + "This is likely a transient issue during node start-up."
                );
                return false;
            }
            return true;
        }).map(TransportClusterStateRoleMappingTranslator::translate);
        return new GetRoleMappingsResponse(
            Stream.concat(nativeMappings.stream(), translatedClusterStateMappings).toArray(ExpressionRoleMapping[]::new)
        );
    }
}
