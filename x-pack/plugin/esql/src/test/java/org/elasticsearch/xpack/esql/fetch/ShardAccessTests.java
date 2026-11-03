/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * The check of the shards that a request of the fetch phase names, against the access control of the request.
 */
public class ShardAccessTests extends ESTestCase {
    private static final ShardId LOGS = new ShardId("logs", "logs-uuid", 0);
    private static final ShardId SECRET = new ShardId("secret", "secret-uuid", 1);
    private static final String ACTION = FetchService.FETCH_ACTION_NAME;

    /**
     * Without security there is no access control, so every shard passes.
     */
    public void testWithoutSecurityEveryShardPasses() {
        assertThat(ShardAccess.of(new ThreadContext(Settings.EMPTY)), nullValue());
        ShardAccess.check(null, LOGS, ACTION);
        ShardAccess.check(null, SECRET, ACTION);
    }

    /**
     * The access control comes from the thread context of the request, where security put it.
     */
    public void testAShardOfAnIndexTheRequestMayReadPasses() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        IndicesAccessControl logsOnly = logsOnly();
        AuthorizationServiceField.INDICES_PERMISSIONS_VALUE.set(threadContext, logsOnly);

        assertThat(ShardAccess.of(threadContext), sameInstance(logsOnly));
        ShardAccess.check(ShardAccess.of(threadContext), LOGS, ACTION);
    }

    /**
     * Authorization dropped the index of the shard, and reading it would skip its document and field level security.
     */
    public void testAShardOfAnIndexTheRequestMayNotReadIsForbidden() {
        ElasticsearchSecurityException e = expectThrows(
            ElasticsearchSecurityException.class,
            () -> ShardAccess.check(logsOnly(), SECRET, ACTION)
        );
        assertThat(e.status(), equalTo(RestStatus.FORBIDDEN));
    }

    public void testARequestWhoseAuthorizationWasDeniedReadsNoShard() {
        IndicesAccessControl denied = randomFrom(
            IndicesAccessControl.DENIED,
            new IndicesAccessControl(false, Map.of(LOGS.getIndexName(), IndicesAccessControl.IndexAccessControl.ALLOW_ALL))
        );
        ElasticsearchSecurityException e = expectThrows(
            ElasticsearchSecurityException.class,
            () -> ShardAccess.check(denied, LOGS, ACTION)
        );
        assertThat(e.status(), equalTo(RestStatus.FORBIDDEN));
    }

    /**
     * The fetch and the keep-alive each report their own action.
     */
    public void testTheErrorNamesTheAction() {
        String action = randomFrom(FetchService.FETCH_ACTION_NAME, FetchContextService.KEEP_ALIVE_ACTION_NAME);
        ElasticsearchSecurityException e = expectThrows(
            ElasticsearchSecurityException.class,
            () -> ShardAccess.check(logsOnly(), SECRET, action)
        );
        assertThat(e.getMessage(), equalTo("action [" + action + "] is unauthorized for index [secret]"));
    }

    private static IndicesAccessControl logsOnly() {
        return new IndicesAccessControl(true, Map.of(LOGS.getIndexName(), IndicesAccessControl.IndexAccessControl.ALLOW_ALL));
    }
}
