/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.support.AuthenticationContextSerializer;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

public class RemoteAccessAuthenticator {

    public static final RoleDescriptor CROSS_CLUSTER_SEARCH_ROLE = new RoleDescriptor(
        "_cross_cluster_search",
        new String[] { ClusterStateAction.NAME },
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );
    private static final Logger logger = LogManager.getLogger(RemoteAccessAuthenticator.class);

    private final ApiKeyService apiKeyService;
    public final String nodeName;
    private final AuthenticationContextSerializer authenticationSerializer;

    public RemoteAccessAuthenticator(ApiKeyService apiKeyService, String nodeName) {
        this.apiKeyService = apiKeyService;
        this.nodeName = nodeName;
        this.authenticationSerializer = new AuthenticationContextSerializer();
    }

    AuthenticationToken extractFromContext(ThreadContext threadContext) {
        final ApiKeyService.ApiKeyCredentials apiKeyCredentials = apiKeyService.getCredentialsFromRemoteAccessHeader(threadContext);
        if (apiKeyCredentials == null) {
            throw new IllegalStateException("failed to extract credentials");
        }
        return apiKeyCredentials;
    }

}
