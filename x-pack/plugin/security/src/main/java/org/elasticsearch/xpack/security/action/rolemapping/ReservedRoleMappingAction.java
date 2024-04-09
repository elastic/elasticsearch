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
 * This is used by the ReservedClusterStateService to add/update or remove role mappings.
 * Typical usage for this action is in the context of file based settings.
 */
public class ReservedRoleMappingAction implements ReservedClusterStateHandler<List<ExpressionRoleMapping>> {
    public static final String NAME = "role_mappings";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public TransformState transform(Object source, TransformState prevState) throws Exception {
        // We execute the prepare() call to catch any errors in the transform phase.
        // Since we store the role mappings outside the cluster state, we do the actual save with a
        // non cluster state transform call.
        @SuppressWarnings("unchecked")
        List<ExpressionRoleMapping> roleMappings = validate((List<ExpressionRoleMapping>) source);
        ClusterState newState = prevState.state()
            .copyAndUpdate(b -> b.putCustom(RoleMappingMetadata.TYPE, new RoleMappingMetadata(roleMappings)));
        Set<String> entities = roleMappings.stream().map(ExpressionRoleMapping::getName).collect(Collectors.toSet());
        return new TransformState(newState, entities);
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

    private List<ExpressionRoleMapping> validate(List<ExpressionRoleMapping> roleMappings) {
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
        return roleMappings;
    }
}
