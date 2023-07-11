/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.rolemapping;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.reservedstate.NonStateTransformResult;
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.reservedstate.TransformState;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.core.security.action.rolemapping.DeleteRoleMappingRequest;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingRequest;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
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
public class ReservedRoleMappingAction implements ReservedClusterStateHandler<List<ExpressionRoleMapping>> {
    public static final String NAME = "role_mappings";

    private final NativeRoleMappingStore roleMappingStore;
    private final ListenableFuture<Void> securityIndexRecoveryListener = new ListenableFuture<>();

    /**
     * Creates a ReservedRoleMappingAction
     *
     * @param roleMappingStore requires {@link NativeRoleMappingStore} for storing/deleting the mappings
     */
    public ReservedRoleMappingAction(NativeRoleMappingStore roleMappingStore) {
        this.roleMappingStore = roleMappingStore;
    }

    @Override
    public String name() {
        return NAME;
    }

    private Collection<PutRoleMappingRequest> prepare(List<ExpressionRoleMapping> roleMappings) {
        List<PutRoleMappingRequest> requests = roleMappings.stream().map(rm -> PutRoleMappingRequest.fromMapping(rm)).toList();

        var exceptions = new ArrayList<Exception>();
        for (var request : requests) {
            // File based defined role mappings are allowed to use MetadataUtils.RESERVED_PREFIX
            var exception = request.validate(false);
            if (exception != null) {
                exceptions.add(exception);
            }
        }

        if (exceptions.isEmpty() == false) {
            var illegalArgumentException = new IllegalArgumentException("error on validating put role mapping requests");
            exceptions.forEach(illegalArgumentException::addSuppressed);
            throw illegalArgumentException;
        }

        return requests;
    }

    @Override
    public TransformState transform(Object source, TransformState prevState) throws Exception {
        // We execute the prepare() call to catch any errors in the transform phase.
        // Since we store the role mappings outside the cluster state, we do the actual save with a
        // non cluster state transform call.
        @SuppressWarnings("unchecked")
        var requests = prepare((List<ExpressionRoleMapping>) source);
        return new TransformState(
            prevState.state(),
            prevState.keys(),
            l -> securityIndexRecoveryListener.addListener(
                l.delegateFailureAndWrap((ll, ignored) -> nonStateTransform(requests, prevState, ll))
            )
        );
    }

    // Exposed for testing purposes
    protected void nonStateTransform(
        Collection<PutRoleMappingRequest> requests,
        TransformState prevState,
        ActionListener<NonStateTransformResult> listener
    ) {
        Set<String> entities = requests.stream().map(r -> r.getName()).collect(Collectors.toSet());
        Set<String> toDelete = new HashSet<>(prevState.keys());
        toDelete.removeAll(entities);

        final int tasksCount = requests.size() + toDelete.size();

        // Nothing to do, don't start a group listener with 0 actions
        if (tasksCount == 0) {
            listener.onResponse(new NonStateTransformResult(ReservedRoleMappingAction.NAME, Set.of()));
            return;
        }

        GroupedActionListener<Boolean> taskListener = new GroupedActionListener<>(tasksCount, new ActionListener<>() {
            @Override
            public void onResponse(Collection<Boolean> booleans) {
                listener.onResponse(new NonStateTransformResult(ReservedRoleMappingAction.NAME, Collections.unmodifiableSet(entities)));
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });

        for (var request : requests) {
            roleMappingStore.putRoleMapping(request, taskListener);
        }

        for (var mappingToDelete : toDelete) {
            var deleteRequest = new DeleteRoleMappingRequest();
            deleteRequest.setName(mappingToDelete);
            roleMappingStore.deleteRoleMapping(deleteRequest, taskListener);
        }
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

    public void securityIndexRecovered() {
        securityIndexRecoveryListener.onResponse(null);
    }
}
