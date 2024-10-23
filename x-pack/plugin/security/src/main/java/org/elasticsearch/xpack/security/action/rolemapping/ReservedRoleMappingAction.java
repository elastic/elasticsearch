/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.rolemapping;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.reservedstate.TransformState;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingRequest;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingRequestBuilder;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.core.security.authz.RoleMappingMetadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentHelper.mapToXContentParser;

/**
 * This Action is the reserved state save version of RestPutRoleMappingAction/RestDeleteRoleMappingAction
 * <p>
 * It is used by the ReservedClusterStateService to add/update or remove role mappings. Typical usage
 * for this action is in the context of file based settings.
 */
public class ReservedRoleMappingAction implements ReservedClusterStateHandler<List<PutRoleMappingRequest>> {
    public static final String NAME = "role_mappings";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public TransformState transform(Object source, TransformState prevState) throws Exception {
        @SuppressWarnings("unchecked")
        Set<ExpressionRoleMapping> roleMappings = validateAndTranslate((List<PutRoleMappingRequest>) source);
        RoleMappingMetadata newRoleMappingMetadata = new RoleMappingMetadata(roleMappings);
        if (newRoleMappingMetadata.equals(RoleMappingMetadata.getFromClusterState(prevState.state()))) {
            return prevState;
        } else {
            ClusterState newState = newRoleMappingMetadata.updateClusterState(prevState.state());
            Set<String> entities = newRoleMappingMetadata.getRoleMappings()
                .stream()
                .map(ExpressionRoleMapping::getName)
                .collect(Collectors.toSet());
            return new TransformState(newState, entities);
        }
    }

    @Override
    public List<PutRoleMappingRequest> fromXContent(XContentParser parser) throws IOException {
        List<PutRoleMappingRequest> result = new ArrayList<>();
        Map<String, ?> source = parser.map();
        for (String name : source.keySet()) {
            @SuppressWarnings("unchecked")
            Map<String, ?> content = (Map<String, ?>) source.get(name);
            try (XContentParser mappingParser = mapToXContentParser(XContentParserConfiguration.EMPTY, content)) {
                result.add(new PutRoleMappingRequestBuilder(null).source(name, mappingParser).request());
            }
        }
        return result;
    }

    private Set<ExpressionRoleMapping> validateAndTranslate(List<PutRoleMappingRequest> roleMappings) {
        var exceptions = new ArrayList<Exception>();
        for (var roleMapping : roleMappings) {
            // File based defined role mappings are allowed to use MetadataUtils.RESERVED_PREFIX
            var exception = roleMapping.validate(false);
            if (exception != null) {
                exceptions.add(exception);
            }
        }
        if (exceptions.isEmpty() == false) {
            var illegalArgumentException = new IllegalArgumentException("error on validating put role mapping requests");
            exceptions.forEach(illegalArgumentException::addSuppressed);
            throw illegalArgumentException;
        }
        return roleMappings.stream()
            .map(r -> RoleMappingMetadata.copyWithNameInMetadata(r.getMapping()))
            .collect(Collectors.toUnmodifiableSet());
    }
}
