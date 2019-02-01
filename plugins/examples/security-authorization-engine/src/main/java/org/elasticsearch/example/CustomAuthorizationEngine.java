/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.example;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.ResolvedIndices;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl.IndexAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * A custom implementation of an authorization engine. This engine is extremely basic in that it
 * authorizes based upon the name of a single role. If users have this role they are granted access.
 */
public class CustomAuthorizationEngine implements AuthorizationEngine {

    @Override
    public void resolveAuthorizationInfo(RequestInfo requestInfo, ActionListener<AuthorizationInfo> listener) {
        final Authentication authentication = requestInfo.getAuthentication();
        if (authentication.getUser().isRunAs()) {
            final CustomAuthorizationInfo authenticatedUserAuthzInfo =
                new CustomAuthorizationInfo(authentication.getUser().authenticatedUser().roles(), null);
            listener.onResponse(new CustomAuthorizationInfo(authentication.getUser().roles(), authenticatedUserAuthzInfo));
        } else {
            listener.onResponse(new CustomAuthorizationInfo(authentication.getUser().roles(), null));
        }
    }

    @Override
    public void authorizeRunAs(RequestInfo requestInfo, AuthorizationInfo authorizationInfo, ActionListener<AuthorizationResult> listener) {
        if (isSuperuser(requestInfo.getAuthentication().getUser().authenticatedUser())) {
            listener.onResponse(AuthorizationResult.granted());
        } else {
            listener.onResponse(AuthorizationResult.deny());
        }
    }

    @Override
    public void authorizeClusterAction(RequestInfo requestInfo, AuthorizationInfo authorizationInfo,
                                       ActionListener<AuthorizationResult> listener) {
        if (isSuperuser(requestInfo.getAuthentication().getUser())) {
            listener.onResponse(AuthorizationResult.granted());
        } else {
            listener.onResponse(AuthorizationResult.deny());
        }
    }

    @Override
    public void authorizeIndexAction(RequestInfo requestInfo, AuthorizationInfo authorizationInfo,
                                     AsyncSupplier<ResolvedIndices> indicesAsyncSupplier,
                                     Function<String, AliasOrIndex> aliasOrIndexFunction,
                                     ActionListener<IndexAuthorizationResult> listener) {
        if (isSuperuser(requestInfo.getAuthentication().getUser())) {
            indicesAsyncSupplier.getAsync(ActionListener.wrap(resolvedIndices -> {
                Map<String, IndexAccessControl> indexAccessControlMap = new HashMap<>();
                for (String name : resolvedIndices.getLocal()) {
                    indexAccessControlMap.put(name, new IndexAccessControl(true, FieldPermissions.DEFAULT, null));
                }
                IndicesAccessControl indicesAccessControl =
                    new IndicesAccessControl(true, Collections.unmodifiableMap(indexAccessControlMap));
                listener.onResponse(new IndexAuthorizationResult(true, indicesAccessControl));
            }, listener::onFailure));
        } else {
            listener.onResponse(new IndexAuthorizationResult(true, IndicesAccessControl.DENIED));
        }
    }

    @Override
    public void loadAuthorizedIndices(RequestInfo requestInfo, AuthorizationInfo authorizationInfo,
                                      Map<String, AliasOrIndex> aliasAndIndexLookup, ActionListener<List<String>> listener) {
        if (isSuperuser(requestInfo.getAuthentication().getUser())) {
            listener.onResponse(new ArrayList<>(aliasAndIndexLookup.keySet()));
        } else {
            listener.onResponse(Collections.emptyList());
        }
    }

    @Override
    public void validateIndexPermissionsAreSubset(RequestInfo requestInfo, AuthorizationInfo authorizationInfo,
                                                  Map<String, List<String>> indexNameToNewNames,
                                                  ActionListener<AuthorizationResult> listener) {
        if (isSuperuser(requestInfo.getAuthentication().getUser())) {
            listener.onResponse(AuthorizationResult.granted());
        } else {
            listener.onResponse(AuthorizationResult.deny());
        }
    }

    public static class CustomAuthorizationInfo implements AuthorizationInfo {

        private final String[] roles;
        private final CustomAuthorizationInfo authenticatedAuthzInfo;

        CustomAuthorizationInfo(String[] roles, CustomAuthorizationInfo authenticatedAuthzInfo) {
            this.roles = roles;
            this.authenticatedAuthzInfo = authenticatedAuthzInfo;
        }

        @Override
        public Map<String, Object> asMap() {
            return Collections.singletonMap("roles", roles);
        }

        @Override
        public CustomAuthorizationInfo getAuthenticatedUserAuthorizationInfo() {
            return authenticatedAuthzInfo;
        }
    }

    private boolean isSuperuser(User user) {
        return Arrays.binarySearch(user.roles(), "custom_superuser") > -1;
    }
}
