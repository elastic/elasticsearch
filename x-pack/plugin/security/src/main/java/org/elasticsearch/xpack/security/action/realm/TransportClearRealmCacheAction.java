/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.realm;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheAction;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheRequest;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheResponse;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.support.CachingRealm;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.Realms;

import java.io.IOException;
import java.util.List;

public class TransportClearRealmCacheAction extends TransportNodesAction<
    ClearRealmCacheRequest,
    ClearRealmCacheResponse,
    ClearRealmCacheRequest.Node,
    ClearRealmCacheResponse.Node> {

    private final Realms realms;
    private final AuthenticationService authenticationService;

    @Inject
    public TransportClearRealmCacheAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        Realms realms,
        AuthenticationService authenticationService
    ) {
        super(
            ClearRealmCacheAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            ClearRealmCacheRequest.Node::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.realms = realms;
        this.authenticationService = authenticationService;
    }

    @Override
    protected ClearRealmCacheResponse newResponse(
        ClearRealmCacheRequest request,
        List<ClearRealmCacheResponse.Node> responses,
        List<FailedNodeException> failures
    ) {
        return new ClearRealmCacheResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected ClearRealmCacheRequest.Node newNodeRequest(ClearRealmCacheRequest request) {
        return new ClearRealmCacheRequest.Node(request);
    }

    @Override
    protected ClearRealmCacheResponse.Node newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new ClearRealmCacheResponse.Node(in);
    }

    @Override
    protected ClearRealmCacheResponse.Node nodeOperation(ClearRealmCacheRequest.Node nodeRequest, Task task) throws ElasticsearchException {
        if (nodeRequest.getRealms() == null || nodeRequest.getRealms().length == 0) {
            for (Realm realm : realms) {
                clearCache(realm, nodeRequest.getUsernames());
            }
            clearAuthenticationServiceCache(nodeRequest.getUsernames());
            return new ClearRealmCacheResponse.Node(clusterService.localNode());
        }

        for (String realmName : nodeRequest.getRealms()) {
            Realm realm = realms.realm(realmName);
            if (realm == null) {
                throw new IllegalArgumentException("could not find active realm [" + realmName + "]");
            }
            clearCache(realm, nodeRequest.getUsernames());
        }
        clearAuthenticationServiceCache(nodeRequest.getUsernames());
        return new ClearRealmCacheResponse.Node(clusterService.localNode());
    }

    private void clearAuthenticationServiceCache(String[] usernames) {
        // this is heavy handed since we could also take realm into account but that would add
        // complexity since we would need to iterate over the cache under a lock to remove all
        // entries that referenced the specific realm
        if (usernames != null && usernames.length != 0) {
            for (String username : usernames) {
                authenticationService.expire(username);
            }
        } else {
            authenticationService.expireAll();
        }
    }

    private static void clearCache(Realm realm, String[] usernames) {
        if ((realm instanceof CachingRealm) == false) {
            return;
        }
        CachingRealm cachingRealm = (CachingRealm) realm;

        if (usernames != null && usernames.length != 0) {
            for (String username : usernames) {
                cachingRealm.expire(username);
            }
        } else {
            cachingRealm.expireAll();
        }
    }

}
