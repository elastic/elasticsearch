/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesResponse;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * An AuthorizationEngine is responsible for making the core decisions about whether a request
 * should be authorized or not. The engine can and usually will be called multiple times during
 * the authorization of a request. Security categorizes requests into a few different buckets
 * and uses the action name as the indicator of what a request will perform. Internally, the
 * action name is used to map a {@link TransportRequest} to the actual
 * {@link org.elasticsearch.action.support.TransportAction} that will handle the request.
 * </p><br>
 * <p>
 * Requests can be a <em>cluster</em> request or an <em>indices</em> request. Cluster requests
 * are requests that tend to be global in nature; they could affect the whole cluster.
 * Indices requests are those that deal with specific indices; the actions could have the affect
 * of reading data, modifying data, creating an index, deleting an index, or modifying metadata.
 * </p><br>
 * <p>
 * Each call to the engine will contain a {@link RequestInfo} object that contains the request,
 * action name, and the authentication associated with the request. This data is provided by the
 * engine so that all information about the request can be used to make the authorization decision.
 * </p><br>
 * The methods of the engine will be called in the following order:
 * <ol>
 *     <li>{@link #resolveAuthorizationInfo(RequestInfo, ActionListener)} to retrieve information
 *         necessary to authorize the given user. It is important to note that the {@link RequestInfo}
 *         may contain an {@link Authentication} object that actually has two users when the
 *         <i>run as</i> feature is used and this method should resolve the information for both.
 *         To check for the presence of run as, use the {@link User#isRunAs()} method on the user
 *         retrieved using the {@link Authentication#getUser()} method.</li>
 *     <li>{@link #authorizeRunAs(RequestInfo, AuthorizationInfo, ActionListener)} if the request
 *         is making use of the run as feature. This method is used to ensure the authenticated user
 *         can actually impersonate the user running the request.</li>
 *     <li>{@link #authorizeClusterAction(RequestInfo, AuthorizationInfo, ActionListener)} if the
 *         request is a cluster level operation.</li>
 *     <li>{@link #authorizeIndexAction(RequestInfo, AuthorizationInfo, AsyncSupplier, Map, ActionListener)} if
 *         the request is a an index action. This method may be called multiple times for a single
 *         request as the request may be made up of sub-requests that also need to be authorized. The async supplier
 *         for resolved indices will invoke the
 *         {@link #loadAuthorizedIndices(RequestInfo, AuthorizationInfo, Map, ActionListener)} method
 *         if it is used as part of the authorization process.</li>
 * </ol>
 * <br><p>
 * <em>NOTE:</em> the {@link #loadAuthorizedIndices(RequestInfo, AuthorizationInfo, Map, ActionListener)}
 * method may be called prior to {@link #authorizeIndexAction(RequestInfo, AuthorizationInfo, AsyncSupplier, Map, ActionListener)}
 * in cases where wildcards need to be expanded.
 * </p><br>
 * Authorization engines can be called from various threads including network threads that should
 * not be blocked waiting for I/O. Network threads in elasticsearch are limited and we rely on
 * asynchronous processing to ensure optimal use of network threads; this is unlike many other Java
 * based servers that have a thread for each concurrent request and blocking operations could take
 * place on those threads. Given this it is imperative that the implementations used here do not
 * block when calling out to an external service or waiting on some data.
 */
public interface AuthorizationEngine {

    /**
     * Asynchronously resolves any necessary information to authorize the given user(s). This could
     * include retrieval of permissions from an index or external system.
     *
     * @param requestInfo object contain the request and associated information such as the action
     *                    and associated user(s)
     * @param listener the listener to be notified of success using {@link ActionListener#onResponse(Object)}
     *                 or failure using {@link ActionListener#onFailure(Exception)}
     */
    void resolveAuthorizationInfo(RequestInfo requestInfo, ActionListener<AuthorizationInfo> listener);

    /**
     * Asynchronously authorizes an attempt for a user to run as another user.
     *
     * @param requestInfo object contain the request and associated information such as the action
     *                    and associated user(s)
     * @param authorizationInfo information needed from authorization that was previously retrieved
     *                          from {@link #resolveAuthorizationInfo(RequestInfo, ActionListener)}
     * @param listener the listener to be notified of the authorization result
     */
    void authorizeRunAs(RequestInfo requestInfo, AuthorizationInfo authorizationInfo, ActionListener<AuthorizationResult> listener);

    /**
     * Asynchronously authorizes a cluster action.
     *
     * @param requestInfo object contain the request and associated information such as the action
     *                    and associated user(s)
     * @param authorizationInfo information needed from authorization that was previously retrieved
     *                          from {@link #resolveAuthorizationInfo(RequestInfo, ActionListener)}
     * @param listener the listener to be notified of the authorization result
     */
    void authorizeClusterAction(RequestInfo requestInfo, AuthorizationInfo authorizationInfo, ActionListener<AuthorizationResult> listener);

    /**
     * Asynchronously authorizes an action that operates on an index. The indices and aliases that
     * the request is attempting to operate on can be retrieved using the {@link AsyncSupplier} for
     * {@link ResolvedIndices}. The resolved indices will contain the exact list of indices and aliases
     * that the request is attempting to take action on; in other words this supplier handles wildcard
     * expansion and datemath expressions.
     *
     * @param requestInfo object contain the request and associated information such as the action
     *                    and associated user(s)
     * @param authorizationInfo information needed from authorization that was previously retrieved
     *                          from {@link #resolveAuthorizationInfo(RequestInfo, ActionListener)}
     * @param indicesAsyncSupplier the asynchronous supplier for the indices that this request is
     *                             attempting to operate on
     * @param aliasOrIndexLookup a map of a string name to the cluster metadata specific to that
     *                            alias or index
     * @param listener the listener to be notified of the authorization result
     */
    void authorizeIndexAction(RequestInfo requestInfo, AuthorizationInfo authorizationInfo,
                              AsyncSupplier<ResolvedIndices> indicesAsyncSupplier, Map<String, AliasOrIndex> aliasOrIndexLookup,
                              ActionListener<IndexAuthorizationResult> listener);

    /**
     * Asynchronously loads a list of alias and index names for which the user is authorized
     * to execute the requested action.
     *
     * @param requestInfo object contain the request and associated information such as the action
     *                    and associated user(s)
     * @param authorizationInfo information needed from authorization that was previously retrieved
     *                          from {@link #resolveAuthorizationInfo(RequestInfo, ActionListener)}
     * @param aliasOrIndexLookup a map of a string name to the cluster metadata specific to that
     *                            alias or index
     * @param listener the listener to be notified of the authorization result
     */
    void loadAuthorizedIndices(RequestInfo requestInfo, AuthorizationInfo authorizationInfo,
                               Map<String, AliasOrIndex> aliasOrIndexLookup, ActionListener<List<String>> listener);


    /**
     * Asynchronously checks that the permissions a user would have for a given list of names do
     * not exceed their permissions for a given name. This is used to ensure that a user cannot
     * perform operations that would escalate their privileges over the data. Some examples include
     * adding an alias to gain more permissions to a given index and/or resizing an index in order
     * to gain more privileges on the data since the index name changes.
     *
     * @param requestInfo object contain the request and associated information such as the action
     *                    and associated user(s)
     * @param authorizationInfo information needed from authorization that was previously retrieved
     *                          from {@link #resolveAuthorizationInfo(RequestInfo, ActionListener)}
     * @param indexNameToNewNames A map of an existing index/alias name to a one or more names of
     *                            an index/alias that the user is requesting to create. The method
     *                            should validate that none of the names have more permissions than
     *                            the name in the key would have.
     * @param listener the listener to be notified of the authorization result
     */
    void validateIndexPermissionsAreSubset(RequestInfo requestInfo, AuthorizationInfo authorizationInfo,
                                           Map<String, List<String>> indexNameToNewNames, ActionListener<AuthorizationResult> listener);

    /**
     * Checks the current user's privileges against those that being requested to check in the
     * request. This provides a way for an application to ask if a user has permission to perform
     * an action or if they have permissions to an application resource.
     *
     * @param authentication the authentication that is associated with this request
     * @param authorizationInfo information needed from authorization that was previously retrieved
     *                          from {@link #resolveAuthorizationInfo(RequestInfo, ActionListener)}
     * @param hasPrivilegesRequest the request that contains the privileges to check for the user
     * @param applicationPrivilegeDescriptors a collection of application privilege descriptors
     * @param listener the listener to be notified of the has privileges response
     */
    void checkPrivileges(Authentication authentication, AuthorizationInfo authorizationInfo, HasPrivilegesRequest hasPrivilegesRequest,
                         Collection<ApplicationPrivilegeDescriptor> applicationPrivilegeDescriptors,
                         ActionListener<HasPrivilegesResponse> listener);

    /**
     * Retrieve's the current user's privileges in a standard format that can be rendered via an
     * API for an application to understand the privileges that the current user has.
     *
     * @param authentication the authentication that is associated with this request
     * @param authorizationInfo information needed from authorization that was previously retrieved
     *                          from {@link #resolveAuthorizationInfo(RequestInfo, ActionListener)}
     * @param request the request for retrieving the user's privileges
     * @param listener the listener to be notified of the has privileges response
     */
    void getUserPrivileges(Authentication authentication, AuthorizationInfo authorizationInfo, GetUserPrivilegesRequest request,
                           ActionListener<GetUserPrivilegesResponse> listener);

    /**
     * Interface for objects that contains the information needed to authorize a request
     */
    interface AuthorizationInfo {

        /**
         * @return a map representation of the authorization information. This map will be used to
         * augment the data that is audited, so in the case of RBAC this map could contain the
         * role names.
         */
        Map<String, Object> asMap();

        /**
         * This method should be overridden in case of run as. Authorization info is only retrieved
         * a single time and should represent the information to authorize both run as and the
         * operation being performed.
         */
        default AuthorizationInfo getAuthenticatedUserAuthorizationInfo() {
            return this;
        }
    }

    /**
     * Implementation of authorization info that is used in cases where we were not able to resolve
     * the authorization info
     */
    final class EmptyAuthorizationInfo implements AuthorizationInfo {

        public static final EmptyAuthorizationInfo INSTANCE = new EmptyAuthorizationInfo();

        private EmptyAuthorizationInfo() {}

        @Override
        public Map<String, Object> asMap() {
            return Collections.emptyMap();
        }
    }

    /**
     * A class that encapsulates information about the request that is being authorized including
     * the actual transport request, the authentication, and the action being invoked.
     */
    final class RequestInfo {

        private final Authentication authentication;
        private final TransportRequest request;
        private final String action;

        public RequestInfo(Authentication authentication, TransportRequest request, String action) {
            this.authentication = authentication;
            this.request = request;
            this.action = action;
        }

        public String getAction() {
            return action;
        }

        public Authentication getAuthentication() {
            return authentication;
        }

        public TransportRequest getRequest() {
            return request;
        }
    }

    /**
     * Represents the result of authorization. This includes whether the actions should be granted
     * and if this should be considered an auditable event.
     */
    class AuthorizationResult {

        private final boolean granted;
        private final boolean auditable;

        /**
         * Create an authorization result with the provided granted value that is auditable
         */
        public AuthorizationResult(boolean granted) {
            this(granted, true);
        }

        public AuthorizationResult(boolean granted, boolean auditable) {
            this.granted = granted;
            this.auditable = auditable;
        }

        public boolean isGranted() {
            return granted;
        }

        public boolean isAuditable() {
            return auditable;
        }

        /**
         * Returns a new authorization result that is granted and auditable
         */
        public static AuthorizationResult granted() {
            return new AuthorizationResult(true);
        }

        /**
         * Returns a new authorization result that is denied and auditable
         */
        public static AuthorizationResult deny() {
            return new AuthorizationResult(false);
        }
    }

    /**
     * An extension of {@link AuthorizationResult} that is specific to index requests. Index requests
     * need to return a {@link IndicesAccessControl} object representing the users permissions to indices
     * that are being operated on.
     */
    class IndexAuthorizationResult extends AuthorizationResult {

        private final IndicesAccessControl indicesAccessControl;

        public IndexAuthorizationResult(boolean auditable, IndicesAccessControl indicesAccessControl) {
            super(indicesAccessControl == null || indicesAccessControl.isGranted(), auditable);
            this.indicesAccessControl = indicesAccessControl;
        }

        public IndicesAccessControl getIndicesAccessControl() {
            return indicesAccessControl;
        }
    }

    @FunctionalInterface
    interface AsyncSupplier<V> {

        /**
         * Asynchronously retrieves the value that is being supplied and notifies the listener upon
         * completion.
         */
        void getAsync(ActionListener<V> listener);
    }
}
