/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.ResourcePrivileges;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;

import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.action.ValidateActions.addValidationError;

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
 *         To check for the presence of run as, use the {@link Authentication#isRunAs()} method.</li>
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
     * Asynchronously resolves the information necessary to authorize the given request, which has
     * already been authenticated. This could include retrieval of permissions from an index or external system.
     * See also {@link #resolveAuthorizationInfo(Subject, ActionListener)}, for which this method is the more
     * specific sibling. This returns the specific {@code AuthorizationInfo} used to authorize only the specified request.
     *
     * @param requestInfo object containing the request and associated information such as the action name
     *                    and associated user(s)
     * @param listener the listener to be notified of success using {@link ActionListener#onResponse(Object)}
     *                 or failure using {@link ActionListener#onFailure(Exception)}
     */
    void resolveAuthorizationInfo(RequestInfo requestInfo, ActionListener<AuthorizationInfo> listener);

    /**
     * Asynchronously resolves the information necessary to authorize requests in the context of the given {@code Subject}.
     * This could include retrieval of permissions from an index or external system.
     * See also {@link #resolveAuthorizationInfo(RequestInfo, ActionListener)}, for which this method is the more general
     * sibling. This returns the {@code AuthorizationInfo} that is used for access checks outside the context of
     * authorizing a specific request, i.e.
     * {@link #checkPrivileges(AuthorizationInfo, PrivilegesToCheck, Collection, ActionListener)}
     *
     * @param subject object representing the effective user
     * @param listener the listener to be notified of success using {@link ActionListener#onResponse(Object)}
     *                 or failure using {@link ActionListener#onFailure(Exception)}
     */
    void resolveAuthorizationInfo(Subject subject, ActionListener<AuthorizationInfo> listener);

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
    void authorizeIndexAction(
        RequestInfo requestInfo,
        AuthorizationInfo authorizationInfo,
        AsyncSupplier<ResolvedIndices> indicesAsyncSupplier,
        Map<String, IndexAbstraction> aliasOrIndexLookup,
        ActionListener<IndexAuthorizationResult> listener
    );

    /**
     * Asynchronously loads a set of alias and index names for which the user is authorized
     * to execute the requested action.
     *
     * @param requestInfo object contain the request and associated information such as the action
     *                    and associated user(s)
     * @param authorizationInfo information needed from authorization that was previously retrieved
     *                          from {@link #resolveAuthorizationInfo(RequestInfo, ActionListener)}
     * @param indicesLookup a map of a string name to the cluster metadata specific to that
     *                            alias or index
     * @param listener the listener to be notified of the authorization result
     */
    void loadAuthorizedIndices(
        RequestInfo requestInfo,
        AuthorizationInfo authorizationInfo,
        Map<String, IndexAbstraction> indicesLookup,
        ActionListener<AuthorizationEngine.AuthorizedIndices> listener
    );

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
    void validateIndexPermissionsAreSubset(
        RequestInfo requestInfo,
        AuthorizationInfo authorizationInfo,
        Map<String, List<String>> indexNameToNewNames,
        ActionListener<AuthorizationResult> listener
    );

    /**
     * Checks the privileges from the provided authorization information against those that are being
     * requested to be checked. This provides a way for a client application to ask if a Subject has
     * permission to perform an action, before actually trying to perform the action,
     * or if the subject has privileges to an application resource.
     *
     * @param authorizationInfo information used for authorization, for a specific Subject, that was previously retrieved
     *                          using {@link #resolveAuthorizationInfo(Subject, ActionListener)}
     * @param privilegesToCheck the object that contains the privileges to check for the Subject
     * @param applicationPrivilegeDescriptors a collection of application privilege descriptors
     * @param listener the listener to be notified of the check privileges response
     */
    void checkPrivileges(
        AuthorizationInfo authorizationInfo,
        PrivilegesToCheck privilegesToCheck,
        Collection<ApplicationPrivilegeDescriptor> applicationPrivilegeDescriptors,
        ActionListener<PrivilegesCheckResult> listener
    );

    /**
     * Retrieve the privileges, from the provided authorization information, in a standard format that can be rendered via an
     * API for a client application to understand the privileges that the Subject has.
     *
     * @param authorizationInfo information used from authorization, for a specific Subject, that was previously retrieved
     *                          from {@link #resolveAuthorizationInfo(Subject, ActionListener)}
     * @param listener the listener to be notified of the get privileges response
     */
    void getUserPrivileges(AuthorizationInfo authorizationInfo, ActionListener<GetUserPrivilegesResponse> listener);

    /**
     * Retrieve privileges towards a remote cluster, from the provided authorization information, to be sent together
     * with a cross-cluster request (e.g. CCS) from an originating cluster to the target cluster.
     */
    default void getRoleDescriptorsIntersectionForRemoteCluster(
        final String remoteClusterAlias,
        final AuthorizationInfo authorizationInfo,
        final ActionListener<RoleDescriptorsIntersection> listener
    ) {
        throw new UnsupportedOperationException(
            "retrieving role descriptors for remote cluster is not supported by this authorization engine"
        );
    }

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
     * Used to retrieve index-like resources that the user has access to, for a specific access action type,
     * at a specific point in time (for a fixed cluster state view).
     * It can also be used to check if a specific resource name is authorized (access to the resource name
     * can be authorized even if it doesn't exist).
     */
    interface AuthorizedIndices {
        /**
         * Returns all the index-like resource names that are available and accessible for an action type by a user,
         * at a fixed point in time (for a single cluster state view).
         */
        Supplier<Set<String>> all();

        /**
         * Checks if an index-like resource name is authorized, for an action by a user. The resource might or might not exist.
         */
        boolean check(String name);
    }

    /**
     * This encapsulates the privileges that can be checked for access. It's intentional that the privileges to be checked are specified
     * in the same manner that they are granted in the {@link RoleDescriptor}. The privilege check can be detailed or not, per the
     * {@link #runDetailedCheck} parameter. The detailed response {@link PrivilegesCheckResult} of a check run, also shows which privileges
     * are NOT granted.
     */
    record PrivilegesToCheck(
        String[] cluster,
        RoleDescriptor.IndicesPrivileges[] index,
        RoleDescriptor.ApplicationResourcePrivileges[] application,
        boolean runDetailedCheck
    ) {
        public static PrivilegesToCheck readFrom(StreamInput in) throws IOException {
            return new PrivilegesToCheck(
                in.readOptionalStringArray(),
                in.readOptionalArray(RoleDescriptor.IndicesPrivileges::new, RoleDescriptor.IndicesPrivileges[]::new),
                in.readOptionalArray(
                    RoleDescriptor.ApplicationResourcePrivileges::new,
                    RoleDescriptor.ApplicationResourcePrivileges[]::new
                ),
                in.readBoolean()
            );
        }

        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalStringArray(cluster);
            out.writeOptionalArray(RoleDescriptor.IndicesPrivileges::write, index);
            out.writeOptionalArray(RoleDescriptor.ApplicationResourcePrivileges::write, application);
            out.writeBoolean(runDetailedCheck);
        }

        public ActionRequestValidationException validate(ActionRequestValidationException validationException) {
            if (cluster == null) {
                validationException = addValidationError("clusterPrivileges must not be null", validationException);
            }
            if (index == null) {
                validationException = addValidationError("indexPrivileges must not be null", validationException);
            } else {
                for (int i = 0; i < index.length; i++) {
                    BytesReference query = index[i].getQuery();
                    if (query != null) {
                        validationException = addValidationError(
                            "may only check index privileges without any DLS query [" + query.utf8ToString() + "]",
                            validationException
                        );
                    }
                }
            }
            if (application == null) {
                validationException = addValidationError("applicationPrivileges must not be null", validationException);
            } else {
                for (RoleDescriptor.ApplicationResourcePrivileges applicationPrivilege : application) {
                    try {
                        ApplicationPrivilege.validateApplicationName(applicationPrivilege.getApplication());
                    } catch (IllegalArgumentException e) {
                        validationException = addValidationError(e.getMessage(), validationException);
                    }
                }
            }
            if (cluster != null
                && cluster.length == 0
                && index != null
                && index.length == 0
                && application != null
                && application.length == 0) {
                validationException = addValidationError("must specify at least one privilege", validationException);
            }
            return validationException;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PrivilegesToCheck that = (PrivilegesToCheck) o;
            return runDetailedCheck == that.runDetailedCheck
                && Arrays.equals(cluster, that.cluster)
                && Arrays.equals(index, that.index)
                && Arrays.equals(application, that.application);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(runDetailedCheck);
            result = 31 * result + Arrays.hashCode(cluster);
            result = 31 * result + Arrays.hashCode(index);
            result = 31 * result + Arrays.hashCode(application);
            return result;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName()
                + "{"
                + "cluster="
                + Arrays.toString(cluster)
                + ","
                + "index="
                + Arrays.toString(index)
                + ","
                + "application="
                + Arrays.toString(application)
                + ","
                + "detailed="
                + runDetailedCheck
                + "}";
        }
    }

    /**
     * The result of a (has) privilege check. This is not to be used as an Elasticsearch authorization result (though clients can base their
     * authorization decisions on this response). The {@link #allChecksSuccess} field tells if all the privileges are granted over
     * all the resources. The {@link #details} field is only present (non-null) if the check has been run in a detailed mode
     * {@link PrivilegesToCheck#runDetailedCheck}, and contains a run-down of which privileges are granted over which resources or not.
     */
    final class PrivilegesCheckResult {

        public static final PrivilegesCheckResult ALL_CHECKS_SUCCESS_NO_DETAILS = new PrivilegesCheckResult(true, null);
        public static final PrivilegesCheckResult SOME_CHECKS_FAILURE_NO_DETAILS = new PrivilegesCheckResult(false, null);

        private final boolean allChecksSuccess;

        @Nullable
        private final Details details;

        public PrivilegesCheckResult(boolean allChecksSuccess, Details details) {
            this.allChecksSuccess = allChecksSuccess;
            this.details = details;
        }

        public boolean allChecksSuccess() {
            return allChecksSuccess;
        }

        public @Nullable Details getDetails() {
            return details;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PrivilegesCheckResult that = (PrivilegesCheckResult) o;
            return allChecksSuccess == that.allChecksSuccess && Objects.equals(details, that.details);
        }

        @Override
        public int hashCode() {
            return Objects.hash(allChecksSuccess, details);
        }

        public record Details(
            Map<String, Boolean> cluster,
            Map<String, ResourcePrivileges> index,
            Map<String, Collection<ResourcePrivileges>> application
        ) {
            public Details {
                Objects.requireNonNull(cluster);
                Objects.requireNonNull(index);
                Objects.requireNonNull(application);
            }
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
     * Implementation of the authorization info that is used when a resolved authorization info results
     * in an empty permissions due to denied access.
     */
    final class DeniedAuthorizationInfo implements AuthorizationInfo {

        public static final DeniedAuthorizationInfo INSTANCE = new DeniedAuthorizationInfo();

        private DeniedAuthorizationInfo() {}

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
        @Nullable
        private final AuthorizationContext originatingAuthorizationContext;
        @Nullable
        private final ParentActionAuthorization parentAuthorization;

        public RequestInfo(
            Authentication authentication,
            TransportRequest request,
            String action,
            AuthorizationContext originatingContext,
            ParentActionAuthorization parentAuthorization
        ) {
            this.authentication = Objects.requireNonNull(authentication);
            this.request = Objects.requireNonNull(request);
            this.action = Objects.requireNonNull(action);
            this.originatingAuthorizationContext = originatingContext;
            this.parentAuthorization = parentAuthorization;
        }

        public RequestInfo(
            Authentication authentication,
            TransportRequest request,
            String action,
            AuthorizationContext originatingContext
        ) {
            this(authentication, request, action, originatingContext, null);
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

        @Nullable
        public AuthorizationContext getOriginatingAuthorizationContext() {
            return originatingAuthorizationContext;
        }

        @Nullable
        public ParentActionAuthorization getParentAuthorization() {
            return parentAuthorization;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName()
                + '{'
                + "authentication=["
                + authentication
                + "], request=["
                + request
                + "], action=["
                + action
                + ']'
                + ", originating=["
                + originatingAuthorizationContext
                + "], parent=["
                + parentAuthorization
                + "]}";
        }

        @Nullable
        public static String[] indices(TransportRequest transportRequest) {
            if (transportRequest instanceof final IndicesRequest indicesRequest) {
                return indicesRequest.indices();
            }
            return null;
        }
    }

    /**
     * Represents the result of authorization to tell whether the actions should be granted
     */
    class AuthorizationResult {

        private final boolean granted;

        /**
         * Create an authorization result with the provided granted value
         */
        public AuthorizationResult(boolean granted) {
            this.granted = granted;
        }

        public boolean isGranted() {
            return granted;
        }

        /**
         * Returns additional context about an authorization failure, if {@link #isGranted()} is false.
         */
        @Nullable
        public String getFailureContext(RequestInfo requestInfo, RestrictedIndices restrictedIndices) {
            return null;
        }

        /**
         * Returns a new authorization result that is granted
         */
        public static AuthorizationResult granted() {
            return new AuthorizationResult(true);
        }

        /**
         * Returns a new authorization result that is denied
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

        public static final IndexAuthorizationResult DENIED = new IndexAuthorizationResult(IndicesAccessControl.DENIED);
        public static final IndexAuthorizationResult EMPTY = new IndexAuthorizationResult(null);
        public static final IndexAuthorizationResult ALLOW_NO_INDICES = new IndexAuthorizationResult(IndicesAccessControl.ALLOW_NO_INDICES);

        private final IndicesAccessControl indicesAccessControl;

        public IndexAuthorizationResult(IndicesAccessControl indicesAccessControl) {
            super(indicesAccessControl == null || indicesAccessControl.isGranted());
            this.indicesAccessControl = indicesAccessControl;
        }

        @Override
        public String getFailureContext(RequestInfo requestInfo, RestrictedIndices restrictedIndices) {
            if (isGranted()) {
                return null;
            } else {
                assert indicesAccessControl != null;
                String[] indices = RequestInfo.indices(requestInfo.getRequest());
                if (indices == null
                    || indices.length == 0
                    || Arrays.equals(IndicesAndAliasesResolverField.NO_INDICES_OR_ALIASES_ARRAY, indices)) {
                    return null;
                }
                Set<String> deniedIndices = Arrays.asList(indices)
                    .stream()
                    .filter(index -> false == indicesAccessControl.hasIndexPermissions(index))
                    .collect(Collectors.toSet());
                return getFailureDescription(deniedIndices, restrictedIndices);
            }
        }

        public static String getFailureDescription(Collection<String> deniedIndices, RestrictedIndices restrictedNames) {
            if (deniedIndices.isEmpty()) {
                return null;
            }
            final StringBuilder regularIndices = new StringBuilder();
            final StringBuilder restrictedIndices = new StringBuilder();
            for (String index : deniedIndices) {
                final StringBuilder builder = restrictedNames.isRestricted(index) ? restrictedIndices : regularIndices;
                if (builder.isEmpty() == false) {
                    builder.append(',');
                }
                builder.append(index);
            }
            StringBuilder message = new StringBuilder();
            if (regularIndices.isEmpty() == false) {
                message.append("on indices [").append(regularIndices).append(']');
            }
            if (restrictedIndices.isEmpty() == false) {
                message.append(message.length() == 0 ? "on" : " and").append(" restricted indices [").append(restrictedIndices).append(']');
            }
            return message.toString();
        }

        @Nullable
        public IndicesAccessControl getIndicesAccessControl() {
            return indicesAccessControl;
        }
    }

    final class AuthorizationContext {
        private final String action;
        private final AuthorizationInfo authorizationInfo;
        private final IndicesAccessControl indicesAccessControl;

        public AuthorizationContext(String action, AuthorizationInfo authorizationInfo, IndicesAccessControl accessControl) {
            this.action = action;
            this.authorizationInfo = authorizationInfo;
            this.indicesAccessControl = accessControl;
        }

        public String getAction() {
            return action;
        }

        public AuthorizationInfo getAuthorizationInfo() {
            return authorizationInfo;
        }

        public IndicesAccessControl getIndicesAccessControl() {
            return indicesAccessControl;
        }
    }

    /**
     * Holds information about authorization of a parent action which is used to pre-authorize its child actions.
     *
     *  @param action the parent action
     */
    record ParentActionAuthorization(String action) implements Writeable {

        public static final String THREAD_CONTEXT_KEY = "_xpack_security_parent_action_authz";

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(action);
        }

        /**
         * Reads an {@link ParentActionAuthorization} from a {@link StreamInput}
         *
         * @param in the {@link StreamInput} to read from
         * @return {@link ParentActionAuthorization}
         * @throws IOException if I/O operation fails
         */
        public static ParentActionAuthorization readFrom(StreamInput in) throws IOException {
            String action = in.readString();
            return new ParentActionAuthorization(action);
        }

        /**
         * Read and deserialize parent authorization from thread context.
         *
         * @param context the thread context to read from
         * @return {@link ParentActionAuthorization} or null
         * @throws IOException if reading fails due to I/O exception
         */
        @Nullable
        public static ParentActionAuthorization readFromThreadContext(ThreadContext context) throws IOException {
            final String header = context.getHeader(THREAD_CONTEXT_KEY);
            if (header == null) {
                return null;
            }

            byte[] bytes = Base64.getDecoder().decode(header);
            StreamInput input = StreamInput.wrap(bytes);
            return readFrom(input);
        }

        /**
         * Writes the authorization to the context. There must not be an existing authorization in the context and if there is an
         * {@link IllegalStateException} will be thrown.
         */
        public void writeToThreadContext(ThreadContext context) throws IOException {
            String header = this.encode();
            assert header != null : "parent authorization object encoded to null";
            context.putHeader(THREAD_CONTEXT_KEY, header);
        }

        private String encode() throws IOException {
            BytesStreamOutput output = new BytesStreamOutput();
            writeTo(output);
            return Base64.getEncoder().encodeToString(BytesReference.toBytes(output.bytes()));
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
