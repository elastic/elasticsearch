/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.client.internal;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.util.Map;

public interface ClientHelperService {
    Map<String, String> getPersistableSafeSecurityHeaders(ThreadContext threadContext, ClusterState clusterState);

    <Request extends ActionRequest, Response extends ActionResponse> void executeWithHeadersAsync(
        Map<String, String> headers,
        String origin,
        Client client,
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    );
}
