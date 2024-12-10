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
import org.elasticsearch.xpack.core.security.authz.RoleMappingMetadata;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;
import org.elasticsearch.xpack.security.authc.support.mapper.ProjectStateRoleMapper;

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

    private final NativeRoleMappingStore roleMappingStore;
    private final ProjectStateRoleMapper projectStateRoleMapper;

    @Inject
    public TransportGetRoleMappingsAction(
        ActionFilters actionFilters,
        TransportService transportService,
        NativeRoleMappingStore nativeRoleMappingStore,
        ProjectStateRoleMapper projectStateRoleMapper
    ) {
        super(
            GetRoleMappingsAction.NAME,
            transportService,
            actionFilters,
            GetRoleMappingsRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.roleMappingStore = nativeRoleMappingStore;
        this.projectStateRoleMapper = projectStateRoleMapper;
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
            final Collection<ExpressionRoleMapping> clusterStateRoleMappings = projectStateRoleMapper.getMappings(
                // if the API was queried with a reserved suffix for any of the names, we need to remove it because role mappings are
                // stored without it in cluster-state
                removeReadOnlySuffixIfPresent(names)
            );
            listener.onResponse(buildResponse(clusterStateRoleMappings, nativeRoleMappings));
        }, listener::onFailure));
    }

    private GetRoleMappingsResponse buildResponse(
        Collection<ExpressionRoleMapping> clusterStateMappings,
        Collection<ExpressionRoleMapping> nativeMappings
    ) {
        Stream<ExpressionRoleMapping> translatedClusterStateMappings = clusterStateMappings.stream().filter(roleMapping -> {
            if (RoleMappingMetadata.hasFallbackName(roleMapping)) {
                logger.warn(
                    "Role mapping retrieved from cluster-state with an ambiguous name. It will be omitted from the API response."
                        + "This is likely a transient issue during node start-up."
                );
                return false;
            }
            return true;
        }).map(this::translateClusterStateMapping);
        return new GetRoleMappingsResponse(
            Stream.concat(nativeMappings.stream(), translatedClusterStateMappings).toArray(ExpressionRoleMapping[]::new)
        );
    }

    private Set<String> removeReadOnlySuffixIfPresent(Set<String> names) {
        return names.stream().map(ExpressionRoleMapping::removeReadOnlySuffixIfPresent).collect(Collectors.toSet());
    }

    /**
     * Translator method for ensuring unique API names and marking cluster-state role mappings as read-only.
     * Role mappings retrieved from cluster-state are surfaced through both the transport and REST layers,
     * along with native role mappings. Unlike native role mappings, cluster-state role mappings are
     * read-only and cannot be modified via APIs. It is possible for cluster-state and native role mappings
     * to have overlapping names.
     *
     * <p>
     * This does the following:
     * </p>
     *
     * <ol>
     *   <li>Appends a reserved suffix to cluster-state role mapping names to avoid conflicts with native role mappings.</li>
     *   <li>Marks the metadata of cluster-state role mappings with a reserved read-only flag.</li>
     *   <li>Removes internal metadata flag used in processing (see {@link RoleMappingMetadata#METADATA_NAME_FIELD}).</li>
     * </ol>
     */
    private ExpressionRoleMapping translateClusterStateMapping(ExpressionRoleMapping mapping) {
        Map<String, Object> metadata = new HashMap<>(mapping.getMetadata());
        if (metadata.put(ExpressionRoleMapping.READ_ONLY_ROLE_MAPPING_METADATA_FLAG, true) != null) {
            logger.error(
                "Metadata field [{}] is reserved and will be overwritten with an internal system value. "
                    + "Rename this field in your role mapping configuration.",
                ExpressionRoleMapping.READ_ONLY_ROLE_MAPPING_METADATA_FLAG
            );
        }
        metadata.remove(RoleMappingMetadata.METADATA_NAME_FIELD);
        return new ExpressionRoleMapping(
            ExpressionRoleMapping.addReadOnlySuffix(mapping.getName()),
            mapping.getExpression(),
            mapping.getRoles(),
            mapping.getRoleTemplates(),
            metadata,
            mapping.isEnabled()
        );
    }
}
