/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.example;

import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexAbstraction.ConcreteIndex;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationResult;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.IndexAuthorizationResult;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.RequestInfo;
import org.elasticsearch.xpack.core.security.authz.ResolvedIndices;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

/**
 * Unit tests for the custom authorization engine. These are basic tests that validate the
 * engine's functionality outside of being used by the AuthorizationService
 */
public class CustomAuthorizationEngineTests extends ESTestCase {

    public void testGetAuthorizationInfo() {
        PlainActionFuture<AuthorizationInfo> future = new PlainActionFuture<>();
        CustomAuthorizationEngine engine = new CustomAuthorizationEngine();
        engine.resolveAuthorizationInfo(getRequestInfo(), future);
        assertNotNull(future.actionGet());
    }

    public void testAuthorizeRunAs() {
        final String action = "cluster:monitor/foo";
        final TransportRequest request = new TransportRequest() {};
        CustomAuthorizationEngine engine = new CustomAuthorizationEngine();
        // unauthorized
        {
            Authentication authentication = Authentication
                .newRealmAuthentication(new User("bar", "not_superuser"), new RealmRef("test", "test", "node"))
                .runAs(new User("joe", "custom_superuser"), new RealmRef("test", "test", "node"));
            RequestInfo info = new RequestInfo(authentication, request, action, null);
            PlainActionFuture<AuthorizationInfo> future = new PlainActionFuture<>();
            engine.resolveAuthorizationInfo(info, future);
            AuthorizationInfo authzInfo = future.actionGet();

            PlainActionFuture<AuthorizationResult> resultFuture = new PlainActionFuture<>();
            engine.authorizeRunAs(info, authzInfo, resultFuture);
            AuthorizationResult result = resultFuture.actionGet();
            assertThat(result.isGranted(), is(false));
        }

        // authorized
        {
            Authentication authentication = Authentication
                .newRealmAuthentication(new User("bar", "custom_superuser"), new RealmRef("test", "test", "node"))
                .runAs(new User("joe", "not_superuser"), new RealmRef("test", "test", "node"));
            RequestInfo info = new RequestInfo(authentication, request, action, null);
            PlainActionFuture<AuthorizationInfo> future = new PlainActionFuture<>();
            engine.resolveAuthorizationInfo(info, future);
            AuthorizationInfo authzInfo = future.actionGet();
            PlainActionFuture<AuthorizationResult> resultFuture = new PlainActionFuture<>();
            engine.authorizeRunAs(info, authzInfo, resultFuture);
            AuthorizationResult result = resultFuture.actionGet();
            assertThat(result.isGranted(), is(true));
        }
    }

    public void testAuthorizeClusterAction() {
        CustomAuthorizationEngine engine = new CustomAuthorizationEngine();
        RequestInfo requestInfo = getRequestInfo();
        // authorized
        {
            PlainActionFuture<AuthorizationInfo> future = new PlainActionFuture<>();
            engine.resolveAuthorizationInfo(requestInfo, future);
            AuthorizationInfo authzInfo = future.actionGet();

            PlainActionFuture<AuthorizationResult> resultFuture = new PlainActionFuture<>();
            engine.authorizeClusterAction(requestInfo, authzInfo, resultFuture);
            AuthorizationResult result = resultFuture.actionGet();
            assertThat(result.isGranted(), is(true));
        }

        // unauthorized
        {
            RequestInfo unauthReqInfo =
                new RequestInfo(
                    Authentication.newRealmAuthentication(new User("joe", "not_superuser"), new RealmRef("test", "test", "node")),
                    requestInfo.getRequest(), requestInfo.getAction(), null);
            PlainActionFuture<AuthorizationInfo> future = new PlainActionFuture<>();
            engine.resolveAuthorizationInfo(unauthReqInfo, future);
            AuthorizationInfo authzInfo = future.actionGet();

            PlainActionFuture<AuthorizationResult> resultFuture = new PlainActionFuture<>();
            engine.authorizeClusterAction(unauthReqInfo, authzInfo, resultFuture);
            AuthorizationResult result = resultFuture.actionGet();
            assertThat(result.isGranted(), is(false));
        }
    }

    public void testAuthorizeIndexAction() {
        CustomAuthorizationEngine engine = new CustomAuthorizationEngine();
        Map<String, IndexAbstraction> indicesMap = new HashMap<>();
        indicesMap.put("index", new ConcreteIndex(IndexMetadata.builder("index")
            .settings(Settings.builder().put("index.version.created", Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build(), null));
        // authorized
        {
            RequestInfo requestInfo =
                new RequestInfo(
                    Authentication.newRealmAuthentication(new User("joe", "custom_superuser"), new RealmRef("test", "test", "node")),
                    new SearchRequest(), "indices:data/read/search", null);
            PlainActionFuture<AuthorizationInfo> future = new PlainActionFuture<>();
            engine.resolveAuthorizationInfo(requestInfo, future);
            AuthorizationInfo authzInfo = future.actionGet();

            PlainActionFuture<IndexAuthorizationResult> resultFuture = new PlainActionFuture<>();
            engine.authorizeIndexAction(requestInfo, authzInfo,
                listener -> listener.onResponse(new ResolvedIndices(Collections.singletonList("index"), Collections.emptyList())),
                indicesMap, resultFuture);
            IndexAuthorizationResult result = resultFuture.actionGet();
            assertThat(result.isGranted(), is(true));
            IndicesAccessControl indicesAccessControl = result.getIndicesAccessControl();
            assertNotNull(indicesAccessControl.getIndexPermissions("index"));
        }

        // unauthorized
        {
            RequestInfo requestInfo =
                new RequestInfo(
                    Authentication.newRealmAuthentication(new User("joe", "not_superuser"), new RealmRef("test", "test", "node")),
                    new SearchRequest(), "indices:data/read/search", null);
            PlainActionFuture<AuthorizationInfo> future = new PlainActionFuture<>();
            engine.resolveAuthorizationInfo(requestInfo, future);
            AuthorizationInfo authzInfo = future.actionGet();

            PlainActionFuture<IndexAuthorizationResult> resultFuture = new PlainActionFuture<>();
            engine.authorizeIndexAction(requestInfo, authzInfo,
                listener -> listener.onResponse(new ResolvedIndices(Collections.singletonList("index"), Collections.emptyList())),
                indicesMap, resultFuture);
            IndexAuthorizationResult result = resultFuture.actionGet();
            assertThat(result.isGranted(), is(false));
            IndicesAccessControl indicesAccessControl = result.getIndicesAccessControl();
            assertNull(indicesAccessControl.getIndexPermissions("index"));
        }
    }

    private RequestInfo getRequestInfo() {
        final String action = "cluster:monitor/foo";
        final TransportRequest request = new TransportRequest() {};
        final Authentication authentication =
            Authentication.newRealmAuthentication(new User("joe", "custom_superuser"), new RealmRef("test", "test", "node"));
        return new RequestInfo(authentication, request, action, null);
    }
}
