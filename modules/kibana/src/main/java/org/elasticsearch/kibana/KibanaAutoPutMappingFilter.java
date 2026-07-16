/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.kibana;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.TransportAutoPutMappingAction;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.MappedActionFilter;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.Index;
import org.elasticsearch.tasks.Task;

import java.util.Map;

/**
 * Guards the dynamic-mapping-update path against resurrecting dropped fields. When a document introduces unmapped
 * fields under {@code dynamic: true}/{@code runtime}, the shard submits an internal auto-put-mapping through
 * {@link TransportAutoPutMappingAction} — a separate action from the public put-mapping API that does <em>not</em>
 * consult {@code mappingRequestValidators}, so {@link KibanaDroppedFieldsMappingValidator} never sees it. This filter
 * closes that path: if the dynamic update targets a Kibana system index and touches a tombstoned field, the mapping
 * update — and therefore the document write that triggered it — fails with an explanatory error.
 * <p>
 * Kibana's indices run {@code dynamic: strict}/{@code false}, so this filter should never fire in practice; it exists
 * so that tombstone enforcement does not silently depend on that invariant.
 */
public class KibanaAutoPutMappingFilter implements MappedActionFilter {

    private final ClusterService clusterService;

    public KibanaAutoPutMappingFilter(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public String actionName() {
        return TransportAutoPutMappingAction.TYPE.name();
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        if (request instanceof PutMappingRequest putMappingRequest) {
            try {
                validate(putMappingRequest);
            } catch (IllegalArgumentException e) {
                listener.onFailure(e);
                return;
            }
        }
        chain.proceed(task, action, request, listener);
    }

    private void validate(PutMappingRequest request) {
        Index index = request.getConcreteIndex();
        if (index == null || KibanaPlugin.KIBANA_INDEX_DESCRIPTOR.matchesIndexPattern(index.getName()) == false) {
            return;
        }
        IndexMetadata indexMetadata = clusterService.state().metadata().indexMetadata(index);
        if (indexMetadata == null) {
            return;
        }
        Map<String, String> tombstones = indexMetadata.getCustomData(TransportReplaceKibanaIndexMappingAction.DROPPED_FIELDS_METADATA_KEY);
        if (tombstones == null || tombstones.isEmpty()) {
            return;
        }
        for (String field : KibanaDroppedFieldsMappingValidator.leafFieldPaths(request.source())) {
            String droppedType = tombstones.get(field);
            if (droppedType != null) {
                throw new IllegalArgumentException(
                    "field ["
                        + field
                        + "] of ["
                        + index.getName()
                        + "] was previously dropped (as type ["
                        + droppedType
                        + "]) and cannot be re-introduced by a dynamic mapping update; re-introduce it with the same"
                        + " type via the Kibana replace mappings API, or use a new (versioned) field name"
                );
            }
        }
    }
}
