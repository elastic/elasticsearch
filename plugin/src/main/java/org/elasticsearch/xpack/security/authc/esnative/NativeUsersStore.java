/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.esnative;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.security.InternalClient;
import org.elasticsearch.xpack.security.SecurityLifecycleService;
import org.elasticsearch.xpack.security.action.realm.ClearRealmCacheRequest;
import org.elasticsearch.xpack.security.action.realm.ClearRealmCacheResponse;
import org.elasticsearch.xpack.security.action.user.ChangePasswordRequest;
import org.elasticsearch.xpack.security.action.user.DeleteUserRequest;
import org.elasticsearch.xpack.security.action.user.PutUserRequest;
import org.elasticsearch.xpack.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.security.authc.support.Hasher;
import org.elasticsearch.xpack.security.client.SecurityClient;
import org.elasticsearch.xpack.security.user.SystemUser;
import org.elasticsearch.xpack.security.user.User;
import org.elasticsearch.xpack.security.user.User.Fields;
import org.elasticsearch.xpack.security.user.XPackUser;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * NativeUsersStore is a store for users that reads from an Elasticsearch index. This store is responsible for fetching the full
 * {@link User} object, which includes the names of the roles assigned to the user.
 * <p>
 * No caching is done by this class, it is handled at a higher level and no polling for changes is done by this class. Modification
 * operations make a best effort attempt to clear the cache on all nodes for the user that was modified.
 */
public class NativeUsersStore extends AbstractComponent {

    public static final String INDEX_TYPE = "doc";
    static final String USER_DOC_TYPE = "user";
    public static final String RESERVED_USER_TYPE = "reserved-user";


    private final Hasher hasher = Hasher.BCRYPT;
    private final InternalClient client;
    private final boolean isTribeNode;

    private volatile SecurityLifecycleService securityLifecycleService;

    public NativeUsersStore(Settings settings, InternalClient client, SecurityLifecycleService securityLifecycleService) {
        super(settings);
        this.client = client;
        this.isTribeNode = XPackPlugin.isTribeNode(settings);
        this.securityLifecycleService = securityLifecycleService;
    }

    /**
     * Blocking version of {@code getUser} that blocks until the User is returned
     */
    public void getUser(String username, ActionListener<User> listener) {
        getUserAndPassword(username, ActionListener.wrap((uap) -> {
            listener.onResponse(uap == null ? null : uap.user());
        }, listener::onFailure));
    }

    /**
     * Retrieve a list of users, if userNames is null or empty, fetch all users
     */
    public void getUsers(String[] userNames, final ActionListener<Collection<User>> listener) {
        final Consumer<Exception> handleException = (t) -> {
            if (t instanceof IndexNotFoundException) {
                logger.trace("could not retrieve users because security index does not exist");
                // We don't invoke the onFailure listener here, instead just pass an empty list
                listener.onResponse(Collections.emptyList());
            } else {
                listener.onFailure(t);
            }
        };
        if (userNames.length == 1) { // optimization for single user lookup
            final String username = userNames[0];
            getUserAndPassword(username, ActionListener.wrap(
                    (uap) -> listener.onResponse(uap == null ? Collections.emptyList() : Collections.singletonList(uap.user())),
                    handleException::accept));
        } else {
            if (securityLifecycleService.isSecurityIndexOutOfDate()) {
                listener.onFailure(new IllegalStateException(
                    "Security index is not on the current version - the native realm will not be operational " +
                    "until the upgrade API is run on the security index"));
                return;
            }
            try {
                final QueryBuilder query;
                if (userNames == null || userNames.length == 0) {
                    query = QueryBuilders.termQuery(Fields.TYPE.getPreferredName(), USER_DOC_TYPE);
                } else {
                    final String[] users = Arrays.asList(userNames).stream()
                        .map(s -> getIdForUser(USER_DOC_TYPE, s)).toArray(String[]::new);
                    query = QueryBuilders.boolQuery().filter(QueryBuilders.idsQuery(INDEX_TYPE).addIds(users));
                }
                SearchRequest request = client.prepareSearch(SecurityLifecycleService.SECURITY_INDEX_NAME)
                        .setScroll(TimeValue.timeValueSeconds(10L))
                        .setQuery(query)
                        .setSize(1000)
                        .setFetchSource(true)
                        .request();
                request.indicesOptions().ignoreUnavailable();
                InternalClient.fetchAllByEntity(client, request, listener, (hit) -> {
                    UserAndPassword u = transformUser(hit.getId(), hit.getSourceAsMap());
                    return u != null ? u.user() : null;
                });
            } catch (Exception e) {
                logger.error((Supplier<?>) () -> new ParameterizedMessage("unable to retrieve users {}", Arrays.toString(userNames)), e);
                listener.onFailure(e);
            }
        }
    }

    /**
     * Async method to retrieve a user and their password
     */
    private void getUserAndPassword(final String user, final ActionListener<UserAndPassword> listener) {
        if (securityLifecycleService.isSecurityIndexOutOfDate()) {
            listener.onFailure(new IllegalStateException(
                "Security index is not on the current version - the native realm will not be operational until " +
                "the upgrade API is run on the security index"));
            return;
        }
        try {
            GetRequest request = client.prepareGet(SecurityLifecycleService.SECURITY_INDEX_NAME,
                INDEX_TYPE, getIdForUser(USER_DOC_TYPE, user)).request();
            client.get(request, new ActionListener<GetResponse>() {
                @Override
                public void onResponse(GetResponse response) {
                    listener.onResponse(transformUser(response.getId(), response.getSource()));
                }

                @Override
                public void onFailure(Exception t) {
                    if (t instanceof IndexNotFoundException) {
                        logger.trace(
                                (Supplier<?>) () -> new ParameterizedMessage(
                                        "could not retrieve user [{}] because security index does not exist", user), t);
                    } else {
                        logger.error((Supplier<?>) () -> new ParameterizedMessage("failed to retrieve user [{}]", user), t);
                    }
                    // We don't invoke the onFailure listener here, instead
                    // we call the response with a null user
                    listener.onResponse(null);
                }
            });
        } catch (IndexNotFoundException infe) {
            logger.trace("could not retrieve user [{}] because security index does not exist", user);
            listener.onResponse(null);
        } catch (Exception e) {
            logger.error((Supplier<?>) () -> new ParameterizedMessage("unable to retrieve user [{}]", user), e);
            listener.onFailure(e);
        }
    }

    /**
     * Async method to change the password of a native or reserved user. If a reserved user does not exist, the document will be created
     * with a hash of the provided password.
     */
    public void changePassword(final ChangePasswordRequest request, final ActionListener<Void> listener) {
        final String username = request.username();
        assert SystemUser.NAME.equals(username) == false && XPackUser.NAME.equals(username) == false : username + "is internal!";
        if (isTribeNode) {
            listener.onFailure(new UnsupportedOperationException("users may not be created or modified using a tribe node"));
            return;
        } else if (securityLifecycleService.isSecurityIndexOutOfDate()) {
            listener.onFailure(new IllegalStateException(
                "Security index is not on the current version - the native realm will not be operational until " +
                "the upgrade API is run on the security index"));
            return;
        } else if (securityLifecycleService.isSecurityIndexWriteable() == false) {
            listener.onFailure(new IllegalStateException("password cannot be changed as user service cannot write until template and " +
                    "mappings are up to date"));
            return;
        }

        final String docType;
        if (ReservedRealm.isReserved(username, settings)) {
            docType = RESERVED_USER_TYPE;
        } else {
            docType = USER_DOC_TYPE;
        }

        securityLifecycleService.createIndexIfNeededThenExecute(listener, () ->
            client.prepareUpdate(SecurityLifecycleService.SECURITY_INDEX_NAME, INDEX_TYPE, getIdForUser(docType, username))
                .setDoc(Requests.INDEX_CONTENT_TYPE, Fields.PASSWORD.getPreferredName(), String.valueOf(request.passwordHash()))
                .setRefreshPolicy(request.getRefreshPolicy())
                .execute(new ActionListener<UpdateResponse>() {
                    @Override
                    public void onResponse(UpdateResponse updateResponse) {
                        assert updateResponse.getResult() == DocWriteResponse.Result.UPDATED;
                        clearRealmCache(request.username(), listener, null);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        if (isIndexNotFoundOrDocumentMissing(e)) {
                            if (docType.equals(RESERVED_USER_TYPE)) {
                                createReservedUser(username, request.passwordHash(), request.getRefreshPolicy(), listener);
                            } else {
                                logger.debug((Supplier<?>) () ->
                                        new ParameterizedMessage("failed to change password for user [{}]", request.username()), e);
                                ValidationException validationException = new ValidationException();
                                validationException.addValidationError("user must exist in order to change password");
                                listener.onFailure(validationException);
                            }
                        } else {
                            listener.onFailure(e);
                        }
                    }
                }));
    }

    /**
     * Asynchronous method to create a reserved user with the given password hash. The cache for the user will be cleared after the document
     * has been indexed
     */
    private void createReservedUser(String username, char[] passwordHash, RefreshPolicy refresh, ActionListener<Void> listener) {
        if (securityLifecycleService.isSecurityIndexOutOfDate()) {
            listener.onFailure(new IllegalStateException(
                "Security index is not on the current version - the native realm will not be operational until " +
                "the upgrade API is run on the security index"));
            return;
        }
        securityLifecycleService.createIndexIfNeededThenExecute(listener, () ->
            client.prepareIndex(SecurityLifecycleService.SECURITY_INDEX_NAME, INDEX_TYPE, getIdForUser(RESERVED_USER_TYPE, username))
                .setSource(Fields.PASSWORD.getPreferredName(), String.valueOf(passwordHash), Fields.ENABLED.getPreferredName(), true,
                           Fields.TYPE.getPreferredName(), RESERVED_USER_TYPE)
                .setRefreshPolicy(refresh)
                .execute(new ActionListener<IndexResponse>() {
                    @Override
                    public void onResponse(IndexResponse indexResponse) {
                        clearRealmCache(username, listener, null);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }
                }));
    }

    /**
     * Asynchronous method to put a user. A put user request without a password hash is treated as an update and will fail with a
     * {@link ValidationException} if the user does not exist. If a password hash is provided, then we issue a update request with an
     * upsert document as well; the upsert document sets the enabled flag of the user to true but if the document already exists, this
     * method will not modify the enabled value.
     */
    public void putUser(final PutUserRequest request, final ActionListener<Boolean> listener) {
        if (isTribeNode) {
            listener.onFailure(new UnsupportedOperationException("users may not be created or modified using a tribe node"));
            return;
        } else if (securityLifecycleService.isSecurityIndexOutOfDate()) {
            listener.onFailure(new IllegalStateException(
                "Security index is not on the current version - the native realm will not be operational until " +
                "the upgrade API is run on the security index"));
            return;
        } else if (securityLifecycleService.isSecurityIndexWriteable() == false) {
            listener.onFailure(new IllegalStateException("user cannot be created or changed as the user service cannot write until " +
                    "template and mappings are up to date"));
            return;
        }

        try {
            if (request.passwordHash() == null) {
                updateUserWithoutPassword(request, listener);
            } else {
                indexUser(request, listener);
            }
        } catch (Exception e) {
            logger.error((Supplier<?>) () -> new ParameterizedMessage("unable to put user [{}]", request.username()), e);
            listener.onFailure(e);
        }
    }

    /**
     * Handles updating a user that should already exist where their password should not change
     */
    private void updateUserWithoutPassword(final PutUserRequest putUserRequest, final ActionListener<Boolean> listener) {
        assert putUserRequest.passwordHash() == null;
        assert !securityLifecycleService.isSecurityIndexOutOfDate() : "security index should be up to date";
        // We must have an existing document
        securityLifecycleService.createIndexIfNeededThenExecute(listener, () ->
            client.prepareUpdate(SecurityLifecycleService.SECURITY_INDEX_NAME, INDEX_TYPE,
                             getIdForUser(USER_DOC_TYPE, putUserRequest.username()))
                .setDoc(Requests.INDEX_CONTENT_TYPE,
                        Fields.USERNAME.getPreferredName(), putUserRequest.username(),
                        Fields.ROLES.getPreferredName(), putUserRequest.roles(),
                        Fields.FULL_NAME.getPreferredName(), putUserRequest.fullName(),
                        Fields.EMAIL.getPreferredName(), putUserRequest.email(),
                        Fields.METADATA.getPreferredName(), putUserRequest.metadata(),
                        Fields.ENABLED.getPreferredName(), putUserRequest.enabled(),
                        Fields.TYPE.getPreferredName(), USER_DOC_TYPE)
                .setRefreshPolicy(putUserRequest.getRefreshPolicy())
                .execute(new ActionListener<UpdateResponse>() {
                    @Override
                    public void onResponse(UpdateResponse updateResponse) {
                        assert updateResponse.getResult() == DocWriteResponse.Result.UPDATED;
                        clearRealmCache(putUserRequest.username(), listener, false);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        Exception failure = e;
                        if (isIndexNotFoundOrDocumentMissing(e)) {
                            // if the index doesn't exist we can never update a user
                            // if the document doesn't exist, then this update is not valid
                            logger.debug((Supplier<?>) () -> new ParameterizedMessage("failed to update user document with username [{}]",
                                    putUserRequest.username()), e);
                            ValidationException validationException = new ValidationException();
                            validationException.addValidationError("password must be specified unless you are updating an existing user");
                            failure = validationException;
                        }
                        listener.onFailure(failure);
                    }
                }));
    }

    private void indexUser(final PutUserRequest putUserRequest, final ActionListener<Boolean> listener) {
        assert putUserRequest.passwordHash() != null;
        assert !securityLifecycleService.isSecurityIndexOutOfDate() : "security index should be up to date";
        securityLifecycleService.createIndexIfNeededThenExecute(listener, () ->
            client.prepareIndex(SecurityLifecycleService.SECURITY_INDEX_NAME, INDEX_TYPE,
                            getIdForUser(USER_DOC_TYPE, putUserRequest.username()))
                .setSource(Fields.USERNAME.getPreferredName(), putUserRequest.username(),
                        Fields.PASSWORD.getPreferredName(), String.valueOf(putUserRequest.passwordHash()),
                        Fields.ROLES.getPreferredName(), putUserRequest.roles(),
                        Fields.FULL_NAME.getPreferredName(), putUserRequest.fullName(),
                        Fields.EMAIL.getPreferredName(), putUserRequest.email(),
                        Fields.METADATA.getPreferredName(), putUserRequest.metadata(),
                        Fields.ENABLED.getPreferredName(), putUserRequest.enabled(),
                        Fields.TYPE.getPreferredName(), USER_DOC_TYPE)
                .setRefreshPolicy(putUserRequest.getRefreshPolicy())
                .execute(new ActionListener<IndexResponse>() {
                    @Override
                    public void onResponse(IndexResponse updateResponse) {
                        clearRealmCache(putUserRequest.username(), listener, updateResponse.getResult() == DocWriteResponse.Result.CREATED);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }
                }));
    }

    /**
     * Asynchronous method that will update the enabled flag of a user. If the user is reserved and the document does not exist, a document
     * will be created. If the user is not reserved, the user must exist otherwise the operation will fail.
     */
    public void setEnabled(final String username, final boolean enabled, final RefreshPolicy refreshPolicy,
                           final ActionListener<Void> listener) {
        if (isTribeNode) {
            listener.onFailure(new UnsupportedOperationException("users may not be created or modified using a tribe node"));
            return;
        } else if (securityLifecycleService.isSecurityIndexOutOfDate()) {
            listener.onFailure(new IllegalStateException(
                "Security index is not on the current version - the native realm will not be operational until " +
                "the upgrade API is run on the security index"));
            return;
        } else if (securityLifecycleService.isSecurityIndexWriteable() == false) {
            listener.onFailure(new IllegalStateException("enabled status cannot be changed as user service cannot write until template " +
                    "and mappings are up to date"));
            return;
        }

        if (ReservedRealm.isReserved(username, settings)) {
            setReservedUserEnabled(username, enabled, refreshPolicy, true, listener);
        } else {
            setRegularUserEnabled(username, enabled, refreshPolicy, listener);
        }
    }

    private void setRegularUserEnabled(final String username, final boolean enabled, final RefreshPolicy refreshPolicy,
                            final ActionListener<Void> listener) {
        assert !securityLifecycleService.isSecurityIndexOutOfDate() : "security index should be up to date";
        try {
            securityLifecycleService.createIndexIfNeededThenExecute(listener, () ->
                client.prepareUpdate(SecurityLifecycleService.SECURITY_INDEX_NAME, INDEX_TYPE, getIdForUser(USER_DOC_TYPE, username))
                    .setDoc(Requests.INDEX_CONTENT_TYPE, Fields.ENABLED.getPreferredName(), enabled)
                    .setRefreshPolicy(refreshPolicy)
                    .execute(new ActionListener<UpdateResponse>() {
                        @Override
                        public void onResponse(UpdateResponse updateResponse) {
                            clearRealmCache(username, listener, null);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            Exception failure = e;
                            if (isIndexNotFoundOrDocumentMissing(e)) {
                                // if the index doesn't exist we can never update a user
                                // if the document doesn't exist, then this update is not valid
                                logger.debug((Supplier<?>) () ->
                                        new ParameterizedMessage("failed to {} user [{}]", enabled ? "enable" : "disable", username), e);
                                ValidationException validationException = new ValidationException();
                                validationException.addValidationError("only existing users can be " + (enabled ? "enabled" : "disabled"));
                                failure = validationException;
                            }
                            listener.onFailure(failure);
                        }
                    }));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void setReservedUserEnabled(final String username, final boolean enabled, final RefreshPolicy refreshPolicy,
                                        boolean clearCache, final ActionListener<Void> listener) {
        assert !securityLifecycleService.isSecurityIndexOutOfDate() : "security index should be up to date";
        try {
            securityLifecycleService.createIndexIfNeededThenExecute(listener, () ->
                client.prepareUpdate(SecurityLifecycleService.SECURITY_INDEX_NAME, INDEX_TYPE, getIdForUser(RESERVED_USER_TYPE, username))
                    .setDoc(Requests.INDEX_CONTENT_TYPE, Fields.ENABLED.getPreferredName(), enabled)
                    .setUpsert(XContentType.JSON,
                               Fields.PASSWORD.getPreferredName(), "",
                               Fields.ENABLED.getPreferredName(), enabled,
                               Fields.TYPE.getPreferredName(), RESERVED_USER_TYPE)
                    .setRefreshPolicy(refreshPolicy)
                    .execute(new ActionListener<UpdateResponse>() {
                        @Override
                        public void onResponse(UpdateResponse updateResponse) {
                            if (clearCache) {
                                clearRealmCache(username, listener, null);
                            } else {
                                listener.onResponse(null);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            listener.onFailure(e);
                        }
                    }));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    public void deleteUser(final DeleteUserRequest deleteUserRequest, final ActionListener<Boolean> listener) {
        if (isTribeNode) {
            listener.onFailure(new UnsupportedOperationException("users may not be deleted using a tribe node"));
            return;
        } else if (securityLifecycleService.isSecurityIndexOutOfDate()) {
            listener.onFailure(new IllegalStateException(
                "Security index is not on the current version - the native realm will not be operational until " +
                "the upgrade API is run on the security index"));
            return;
        } else if (securityLifecycleService.isSecurityIndexWriteable() == false) {
            listener.onFailure(new IllegalStateException("user cannot be deleted as user service cannot write until template and " +
                    "mappings are up to date"));
            return;
        }

        try {
            DeleteRequest request = client.prepareDelete(SecurityLifecycleService.SECURITY_INDEX_NAME,
                    INDEX_TYPE, getIdForUser(USER_DOC_TYPE, deleteUserRequest.username())).request();
            request.indicesOptions().ignoreUnavailable();
            request.setRefreshPolicy(deleteUserRequest.getRefreshPolicy());
            client.delete(request, new ActionListener<DeleteResponse>() {
                @Override
                public void onResponse(DeleteResponse deleteResponse) {
                    clearRealmCache(deleteUserRequest.username(), listener,
                            deleteResponse.getResult() == DocWriteResponse.Result.DELETED);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        } catch (Exception e) {
            logger.error("unable to remove user", e);
            listener.onFailure(e);
        }
    }

    /**
     * This method is used to verify the username and credentials against those stored in the system.
     *
     * @param username username to lookup the user by
     * @param password the plaintext password to verify
     */
    void verifyPassword(String username, final SecureString password, ActionListener<AuthenticationResult> listener) {
        getUserAndPassword(username, ActionListener.wrap((userAndPassword) -> {
            if (userAndPassword == null || userAndPassword.passwordHash() == null) {
                listener.onResponse(AuthenticationResult.notHandled());
            } else if (hasher.verify(password, userAndPassword.passwordHash())) {
                listener.onResponse(AuthenticationResult.success(userAndPassword.user()));
            } else {
                listener.onResponse(AuthenticationResult.unsuccessful("Password authentication failed for " + username, null));
            }
        }, listener::onFailure));
    }

    void getReservedUserInfo(String username, ActionListener<ReservedUserInfo> listener) {
        if (!securityLifecycleService.isSecurityIndexExisting()) {
            listener.onFailure(new IllegalStateException("Attempt to get reserved user info but the security index does not exist"));
            return;
        } else if (securityLifecycleService.isSecurityIndexOutOfDate()) {
            listener.onFailure(new IllegalStateException(
                "Security index is not on the current version - the native realm not be operational until " +
                "the upgrade API is run on the security index"));
            return;
        }
        client.prepareGet(SecurityLifecycleService.SECURITY_INDEX_NAME, INDEX_TYPE, getIdForUser(RESERVED_USER_TYPE, username))
                .execute(new ActionListener<GetResponse>() {
                    @Override
                    public void onResponse(GetResponse getResponse) {
                        if (getResponse.isExists()) {
                            Map<String, Object> sourceMap = getResponse.getSourceAsMap();
                            String password = (String) sourceMap.get(Fields.PASSWORD.getPreferredName());
                            Boolean enabled = (Boolean) sourceMap.get(Fields.ENABLED.getPreferredName());
                            if (password == null) {
                                listener.onFailure(new IllegalStateException("password hash must not be null!"));
                            } else if (enabled == null) {
                                listener.onFailure(new IllegalStateException("enabled must not be null!"));
                            } else if (password.isEmpty()) {
                                listener.onResponse((enabled ? ReservedRealm.ENABLED_DEFAULT_USER_INFO : ReservedRealm
                                        .DISABLED_DEFAULT_USER_INFO).deepClone());
                            } else {
                                listener.onResponse(new ReservedUserInfo(password.toCharArray(), enabled, false));
                            }
                        } else {
                            listener.onResponse(null);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        if (e instanceof IndexNotFoundException) {
                            logger.trace((Supplier<?>) () -> new ParameterizedMessage(
                                    "could not retrieve built in user [{}] info since security index does not exist", username), e);
                            listener.onResponse(null);
                        } else {
                            logger.error(
                                    (Supplier<?>) () -> new ParameterizedMessage(
                                            "failed to retrieve built in user [{}] info", username), e);
                            listener.onFailure(null);
                        }
                    }
                });
    }

    void getAllReservedUserInfo(ActionListener<Map<String, ReservedUserInfo>> listener) {
        if (securityLifecycleService.isSecurityIndexOutOfDate()) {
            listener.onFailure(new IllegalStateException(
                "Security index is not on the current version - the native realm will not be operational until " +
                "the upgrade API is run on the security index"));
            return;
        }
        client.prepareSearch(SecurityLifecycleService.SECURITY_INDEX_NAME)
                .setQuery(QueryBuilders.termQuery(Fields.TYPE.getPreferredName(), RESERVED_USER_TYPE))
                .setFetchSource(true)
                .execute(new ActionListener<SearchResponse>() {
                    @Override
                    public void onResponse(SearchResponse searchResponse) {
                        Map<String, ReservedUserInfo> userInfos = new HashMap<>();
                        assert searchResponse.getHits().getTotalHits() <= 10 : "there are more than 10 reserved users we need to change " +
                                "this to retrieve them all!";
                        for (SearchHit searchHit : searchResponse.getHits().getHits()) {
                            Map<String, Object> sourceMap = searchHit.getSourceAsMap();
                            String password = (String) sourceMap.get(Fields.PASSWORD.getPreferredName());
                            Boolean enabled = (Boolean) sourceMap.get(Fields.ENABLED.getPreferredName());
                            final String id = searchHit.getId();
                            assert id != null && id.startsWith(RESERVED_USER_TYPE) :
                                "id [" + id + "] does not start with reserved-user prefix";
                            final String username = id.substring(RESERVED_USER_TYPE.length() + 1);
                            if (password == null) {
                                listener.onFailure(new IllegalStateException("password hash must not be null!"));
                                return;
                            } else if (enabled == null) {
                                listener.onFailure(new IllegalStateException("enabled must not be null!"));
                                return;
                            } else {
                                userInfos.put(username, new ReservedUserInfo(password.toCharArray(), enabled, false));
                            }
                        }
                        listener.onResponse(userInfos);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        if (e instanceof IndexNotFoundException) {
                            logger.trace("could not retrieve built in users since security index does not exist", e);
                            listener.onResponse(Collections.emptyMap());
                        } else {
                            logger.error("failed to retrieve built in users", e);
                            listener.onFailure(e);
                        }
                    }
                });
    }

    private <Response> void clearRealmCache(String username, ActionListener<Response> listener, Response response) {
        SecurityClient securityClient = new SecurityClient(client);
        ClearRealmCacheRequest request = securityClient.prepareClearRealmCache()
                .usernames(username).request();
        securityClient.clearRealmCache(request, new ActionListener<ClearRealmCacheResponse>() {
            @Override
            public void onResponse(ClearRealmCacheResponse nodes) {
                listener.onResponse(response);
            }

            @Override
            public void onFailure(Exception e) {
                logger.error((Supplier<?>) () -> new ParameterizedMessage("unable to clear realm cache for user [{}]", username), e);
                ElasticsearchException exception = new ElasticsearchException("clearing the cache for [" + username
                        + "] failed. please clear the realm cache manually", e);
                listener.onFailure(exception);
            }
        });
    }

    @Nullable
    private UserAndPassword transformUser(final String id, final Map<String, Object> sourceMap) {
        if (sourceMap == null) {
            return null;
        }
        assert id != null && id.startsWith(USER_DOC_TYPE) : "id [" + id + "] does not start with user prefix";
        final String username = id.substring(USER_DOC_TYPE.length() + 1);
        try {
            String password = (String) sourceMap.get(Fields.PASSWORD.getPreferredName());
            String[] roles = ((List<String>) sourceMap.get(Fields.ROLES.getPreferredName())).toArray(Strings.EMPTY_ARRAY);
            String fullName = (String) sourceMap.get(Fields.FULL_NAME.getPreferredName());
            String email = (String) sourceMap.get(Fields.EMAIL.getPreferredName());
            Boolean enabled = (Boolean) sourceMap.get(Fields.ENABLED.getPreferredName());
            if (enabled == null) {
                // fallback mechanism as a user from 2.x may not have the enabled field
                enabled = Boolean.TRUE;
            }
            Map<String, Object> metadata = (Map<String, Object>) sourceMap.get(Fields.METADATA.getPreferredName());
            return new UserAndPassword(new User(username, roles, fullName, email, metadata, enabled), password.toCharArray());
        } catch (Exception e) {
            logger.error((Supplier<?>) () -> new ParameterizedMessage("error in the format of data for user [{}]", username), e);
            return null;
        }
    }

    private static boolean isIndexNotFoundOrDocumentMissing(Exception e) {
        if (e instanceof ElasticsearchException) {
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            if (cause instanceof IndexNotFoundException || cause instanceof DocumentMissingException) {
                return true;
            }
        }
        return false;
    }

    /**
     * Gets the document id for the given user and user type (reserved user or regular user).
     */
    public static String getIdForUser(final String docType, final String userName) {
        return docType + "-" + userName;
    }

    static final class ReservedUserInfo {

        public final char[] passwordHash;
        public final boolean enabled;
        public final boolean hasEmptyPassword;

        ReservedUserInfo(char[] passwordHash, boolean enabled, boolean hasEmptyPassword) {
            this.passwordHash = passwordHash;
            this.enabled = enabled;
            this.hasEmptyPassword = hasEmptyPassword;
        }

        ReservedUserInfo deepClone() {
            return new ReservedUserInfo(Arrays.copyOf(passwordHash, passwordHash.length), enabled, hasEmptyPassword);
        }

    }
}
