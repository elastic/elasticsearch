/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.action.authc.cache;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesOperationAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.authc.Realm;
import org.elasticsearch.shield.authc.RealmMissingException;
import org.elasticsearch.shield.authc.Realms;
import org.elasticsearch.shield.authc.support.CachingUsernamePasswordRealm;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 *
 */
public class TransportClearRealmCacheAction extends TransportNodesOperationAction<ClearRealmCacheRequest, ClearRealmCacheResponse, ClearRealmCacheRequest.Node, ClearRealmCacheResponse.Node> {

    private final Realms realms;

    @Inject
    public TransportClearRealmCacheAction(Settings settings, ClusterName clusterName, ThreadPool threadPool,
                                          ClusterService clusterService, TransportService transportService,
                                          ActionFilters actionFilters, Realms realms) {
        super(settings, ClearRealmCacheAction.NAME, clusterName, threadPool, clusterService, transportService, actionFilters);
        this.realms = realms;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected ClearRealmCacheRequest newRequest() {
        return new ClearRealmCacheRequest();
    }

    @Override
    protected ClearRealmCacheResponse newResponse(ClearRealmCacheRequest request, AtomicReferenceArray responses) {
        final List<ClearRealmCacheResponse.Node> nodes = Lists.newArrayList();
        for (int i = 0; i < responses.length(); i++) {
            Object resp = responses.get(i);
            if (resp instanceof ClearRealmCacheResponse.Node) {
                nodes.add((ClearRealmCacheResponse.Node) resp);
            }
        }
        return new ClearRealmCacheResponse(clusterName, nodes.toArray(new ClearRealmCacheResponse.Node[nodes.size()]));
    }

    @Override
    protected ClearRealmCacheRequest.Node newNodeRequest() {
        return new ClearRealmCacheRequest.Node();
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
        if (nodeRequest.realms == null || nodeRequest.realms.length == 0) {
            for (Realm realm : realms) {
                clearCache(realm, nodeRequest.usernames);
            }
            return new ClearRealmCacheResponse.Node(clusterService.localNode());
        }

        for (String realmName : nodeRequest.realms) {
            Realm realm = realms.realm(realmName);
            if (realm == null) {
                throw new RealmMissingException("Could not find active realm [" + realmName + "]");
            }
            clearCache(realm, nodeRequest.usernames);
        }
        return new ClearRealmCacheResponse.Node(clusterService.localNode());
    }

    private void clearCache(Realm realm, String[] usernames) {
        if (!(realm instanceof CachingUsernamePasswordRealm)) {
            return;
        }
        CachingUsernamePasswordRealm cachingRealm = (CachingUsernamePasswordRealm) realm;

        if (usernames != null && usernames.length != 0) {
            for (String username : usernames) {
                cachingRealm.expire(username);
            }
        } else {
            cachingRealm.expireAll();
        }
    }

    @Override
    protected boolean accumulateExceptions() {
        return false;
    }

}
