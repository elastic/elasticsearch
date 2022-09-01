/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.rolemapping;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.reservedstate.TransformState;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.core.security.RoleMappingMetadata;
import org.elasticsearch.xpack.core.security.RoleMappingsMetadata;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingRequest;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.core.security.authc.support.mapper.TemplateRoleName;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentHelper.mapToXContentParser;

/**
 * This Action is the reserved state save version of RestPutRoleMappingAction/RestDeleteRoleMappingAction
 * <p>
 * It is used by the ReservedClusterStateService to add/update or remove role mappings. Typical usage
 * for this action is in the context of file based settings.
 */
public class ReservedRoleMappingAction implements ReservedClusterStateHandler<List<ExpressionRoleMapping>> {
    private static final Logger LOGGER = LogManager.getLogger(ReservedRoleMappingAction.class);

    public static final String NAME = "role_mappings";

    private final NativeRoleMappingStore roleMappingStore;
    private final ScriptService scriptService;

    /**
     * Creates a ReservedRoleMappingAction
     *
     * @param scriptService requires {@link NativeRoleMappingStore} for validating the mappings
     */
    public ReservedRoleMappingAction(NativeRoleMappingStore roleMappingStore, ScriptService scriptService) {
        this.roleMappingStore = roleMappingStore;
        this.scriptService = scriptService;
    }

    @Override
    public String name() {
        return NAME;
    }

    @SuppressWarnings("unchecked")
    public Collection<PutRoleMappingRequest> prepare(Object input) {
        List<ExpressionRoleMapping> roleMappings = (List<ExpressionRoleMapping>) input;
        List<PutRoleMappingRequest> requests = roleMappings.stream().map(rm -> PutRoleMappingRequest.fromMapping(rm)).toList();

        var exceptions = new ArrayList<String>();
        for (var request : requests) {
            var exception = request.validate();
            if (exception != null) {
                exceptions.add(exception.getMessage());
            }
            try {
                for (TemplateRoleName templateRoleName : request.getRoleTemplates()) {
                    templateRoleName.validate(scriptService);
                }
            } catch (Exception e) {
                exceptions.add(e.getMessage());
            }
        }

        if (exceptions.isEmpty() == false) {
            throw new IllegalStateException(String.join(", ", exceptions));
        }

        return requests;
    }

    @Override
    public TransformState transform(Object source, TransformState prevState) throws Exception {
        var requests = prepare(source);

        ClusterState state = prevState.state();

        for (var request : requests) {
            state = putRoleMappingTransform(state, request);
        }

        Set<String> entities = requests.stream().map(r -> r.getName()).collect(Collectors.toSet());

        Set<String> toDelete = new HashSet<>(prevState.keys());
        toDelete.removeAll(entities);

        for (var mappingToDelete : toDelete) {
            state = deleteRoleMappingTransform(state, mappingToDelete);
        }

        return new TransformState(state, entities);
    }

    @Override
    public List<ExpressionRoleMapping> fromXContent(XContentParser parser) throws IOException {
        List<ExpressionRoleMapping> result = new ArrayList<>();

        Map<String, ?> source = parser.map();

        for (String name : source.keySet()) {
            @SuppressWarnings("unchecked")
            Map<String, ?> content = (Map<String, ?>) source.get(name);
            try (XContentParser mappingParser = mapToXContentParser(XContentParserConfiguration.EMPTY, content)) {
                ExpressionRoleMapping mapping = ExpressionRoleMapping.parse(name, mappingParser);
                result.add(mapping);
            }
        }

        return result;
    }

    @Override
    public void postTransform(ClusterState clusterState) {
        roleMappingStore.onClusterStateRolesUpdated();
    }

    static boolean isNoopUpdate(@Nullable RoleMappingMetadata existingMapping, ExpressionRoleMapping newRoleMapping) {
        if (existingMapping == null) {
            return false;
        } else {
            return newRoleMapping.equals(existingMapping.getRoleMapping());
        }
    }

    public ClusterState putRoleMappingTransform(ClusterState currentState, PutRoleMappingRequest request) {
        final RoleMappingsMetadata currentMetadata = currentState.metadata().custom(RoleMappingsMetadata.TYPE, RoleMappingsMetadata.EMPTY);
        final var existingMappingsMetadata = currentMetadata.getRoleMappingMetadatas().get(request.getMapping().getName());

        // Double-check for no-op in the state update task, in case it was changed/reset in the meantime
        if (isNoopUpdate(existingMappingsMetadata, request.getMapping())) {
            return currentState;
        }

        ClusterState.Builder stateBuilder = ClusterState.builder(currentState);
        SortedMap<String, RoleMappingMetadata> newMappings = new TreeMap<>(currentMetadata.getRoleMappingMetadatas());
        RoleMappingMetadata mappingMetadata = new RoleMappingMetadata(request.getMapping());
        RoleMappingMetadata oldMappingMetadata = newMappings.put(mappingMetadata.getRoleMapping().getName(), mappingMetadata);
        if (oldMappingMetadata == null) {
            LOGGER.info("adding role mapping to cluster state [{}]", request.getMapping().getName());
        } else {
            LOGGER.info("updating role mapping in cluster state [{}]", request.getMapping().getName());
        }
        RoleMappingsMetadata newMetadata = new RoleMappingsMetadata(newMappings);
        stateBuilder.metadata(Metadata.builder(currentState.getMetadata()).putCustom(RoleMappingsMetadata.TYPE, newMetadata).build());
        return stateBuilder.build();
    }

    public ClusterState deleteRoleMappingTransform(ClusterState currentState, String roleMappingName) {
        ClusterState.Builder newState = ClusterState.builder(currentState);
        RoleMappingsMetadata currentMetadata = currentState.metadata().custom(RoleMappingsMetadata.TYPE);
        if (currentMetadata == null || currentMetadata.getRoleMappingMetadatas().containsKey(roleMappingName) == false) {
            throw new ResourceNotFoundException("Role mapping not found: {}", roleMappingName);
        }
        SortedMap<String, RoleMappingMetadata> newMappings = new TreeMap<>(currentMetadata.getRoleMappingMetadatas());
        newMappings.remove(roleMappingName);
        RoleMappingsMetadata newMetadata = new RoleMappingsMetadata(newMappings);
        newState.metadata(Metadata.builder(currentState.getMetadata()).putCustom(RoleMappingsMetadata.TYPE, newMetadata).build());
        return newState.build();
    }
}
