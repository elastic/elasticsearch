/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.rolemapping;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.reservedstate.TransformState;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingRequest;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingRequestBuilder;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.core.security.authz.RoleMappingMetadata;
import org.elasticsearch.xpack.core.security.support.RoleMappingCleanupTaskParams;

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

    private final PersistentTasksService persistentTasksService;

    private static final Logger logger = LogManager.getLogger(ReservedRoleMappingAction.class);

    public ReservedRoleMappingAction(PersistentTasksService persistentTasksService) {
        this.persistentTasksService = persistentTasksService;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public TransformState transform(Object source, TransformState prevState) throws Exception {
        @SuppressWarnings("unchecked")
        Set<ExpressionRoleMapping> roleMappings = validate((List<PutRoleMappingRequest>) source);
        RoleMappingMetadata newRoleMappingMetadata = new RoleMappingMetadata(roleMappings);
        RoleMappingMetadata currentRoleMappingMetadata = RoleMappingMetadata.getFromClusterState(prevState.state());
        int version = currentRoleMappingMetadata.getVersion();
        logger.info("Checking role mappings version!");
        if (version == 0) {
            submitCleanupTask();
        }
        // TODO INC VERSION AND REWRITE IN NEW FORMAT
        if (newRoleMappingMetadata.equals(currentRoleMappingMetadata)) {
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

    private void submitCleanupTask() {
        persistentTasksService.sendStartRequest(
            RoleMappingCleanupTaskParams.TASK_NAME,
            RoleMappingCleanupTaskParams.TASK_NAME,
            new RoleMappingCleanupTaskParams(),
            null,
            ActionListener.wrap((response) -> {
                logger.info("Role mapping cleanup task submitted");
            }, (exception) -> {
                // Do nothing if the task is already in progress
                if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                    logger.info("Cleanup task already started Already started");
                } else {
                    logger.warn("Role mapping cleanup task failed: " + exception);
                }
            })
        );
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

    private Set<ExpressionRoleMapping> validate(List<PutRoleMappingRequest> roleMappings) {
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
        return roleMappings.stream().map(PutRoleMappingRequest::getMapping).collect(Collectors.toUnmodifiableSet());
    }
}
