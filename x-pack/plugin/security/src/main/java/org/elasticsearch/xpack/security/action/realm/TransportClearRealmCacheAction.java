/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.realm;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheAction;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheRequest;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheResponse;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.support.CachingRealm;

import java.util.List;

public class TransportClearRealmCacheAction extends TransportNodesAction<ClearRealmCacheRequest, ClearRealmCacheResponse,
        ClearRealmCacheRequest.Node, ClearRealmCacheResponse.Node> {

    private final Realms realms;

    @Inject
    public TransportClearRealmCacheAction(ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                                          ActionFilters actionFilters, Realms realms) {
        super(ClearRealmCacheAction.NAME, threadPool, clusterService, transportService, actionFilters,
            ClearRealmCacheRequest::new, ClearRealmCacheRequest.Node::new, ThreadPool.Names.MANAGEMENT,
              ClearRealmCacheResponse.Node.class);
        this.realms = realms;
    }

    @Override
    protected ClearRealmCacheResponse newResponse(ClearRealmCacheRequest request,
                                                  List<ClearRealmCacheResponse.Node> responses, List<FailedNodeException> failures) {
        return new ClearRealmCacheResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected ClearRealmCacheRequest.Node newNodeRequest(String nodeId, ClearRealmCacheRequest request) {
        return new ClearRealmCacheRequest.Node(request, nodeId);
    }

    @Override
    protected ClearRealmCacheResponse.Node newNodeResponse() {
        return new ClearRealmCacheResponse.Node();
    }

    @Override
    protected ClearRealmCacheResponse.Node nodeOperation(ClearRealmCacheRequest.Node nodeRequest) throws ElasticsearchException {
        if (nodeRequest.getRealms() == null || nodeRequest.getRealms().length == 0) {
            for (Realm realm : realms) {
                clearCache(realm, nodeRequest.getUsernames());
            }
            return new ClearRealmCacheResponse.Node(clusterService.localNode());
        }

        for (String realmName : nodeRequest.getRealms()) {
            Realm realm = realms.realm(realmName);
            if (realm == null) {
                throw new IllegalArgumentException("could not find active realm [" + realmName + "]");
            }
            clearCache(realm, nodeRequest.getUsernames());
        }
        return new ClearRealmCacheResponse.Node(clusterService.localNode());
    }

    private void clearCache(Realm realm, String[] usernames) {
        if (!(realm instanceof CachingRealm)) {
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
