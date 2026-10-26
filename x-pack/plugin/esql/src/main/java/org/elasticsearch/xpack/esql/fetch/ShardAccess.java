/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;

/**
 * The check of every shard that a request of the fetch phase names. Security authorizes such a request for the index
 * expressions it carries, not for the shards it names, so the node checks each shard against the access control of the
 * request.
 */
final class ShardAccess {
    private ShardAccess() {}

    /**
     * The access control that security resolved for the request in {@code threadContext}, {@code null} without security.
     */
    @Nullable
    static IndicesAccessControl of(ThreadContext threadContext) {
        return AuthorizationServiceField.INDICES_PERMISSIONS_VALUE.get(threadContext);
    }

    /**
     * Without security there is no access control. With it, the index of every shard is in the access control of the
     * request unless authorization dropped it, and reading such a shard would skip its document and field level security.
     *
     * @param action the action of the request, for the error
     */
    static void check(@Nullable IndicesAccessControl accessControl, ShardId shardId, String action) {
        if (accessControl != null
            && (accessControl.isGranted() == false || accessControl.hasIndexPermissions(shardId.getIndexName()) == false)) {
            throw new ElasticsearchSecurityException(
                "action [" + action + "] is unauthorized for index [" + shardId.getIndexName() + "]",
                RestStatus.FORBIDDEN
            );
        }
    }
}
