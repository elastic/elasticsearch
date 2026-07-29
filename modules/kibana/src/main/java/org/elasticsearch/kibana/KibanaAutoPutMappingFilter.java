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
import org.elasticsearch.index.Index;
import org.elasticsearch.tasks.Task;

/**
 * Blocks dynamic mapping updates on Kibana saved-objects system indices. When a document introduces unmapped fields
 * under {@code dynamic: true} or {@code runtime}, the shard submits an internal auto-put-mapping through
 * {@link TransportAutoPutMappingAction} — a path that does not consult {@code mappingRequestValidators}, bypassing
 * {@link KibanaDroppedFieldsMappingValidator}. This filter closes that path.
 * <p>
 * Kibana's saved-objects indices run {@code dynamic: false} or {@code dynamic: strict}, so this filter should never
 * fire in practice. It exists as a defence-in-depth guardrail: if dynamic mapping somehow fires on a Kibana index,
 * the resulting failure surfaces the misconfiguration rather than silently corrupting the field history.
 */
public class KibanaAutoPutMappingFilter implements MappedActionFilter {

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
            Index index = putMappingRequest.getConcreteIndex();
            if (index != null && KibanaPlugin.KIBANA_INDEX_DESCRIPTOR.matchesIndexPattern(index.getName())) {
                listener.onFailure(
                    new IllegalArgumentException(
                        "dynamic mapping updates are not allowed on Kibana system index ["
                            + index.getName()
                            + "]; use the Kibana replace-mappings API to manage field additions"
                    )
                );
                return;
            }
        }
        chain.proceed(task, action, request, listener);
    }
}
