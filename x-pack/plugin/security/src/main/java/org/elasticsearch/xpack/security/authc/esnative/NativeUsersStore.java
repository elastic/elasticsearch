/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.esnative;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.Requests;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.ScrollHelper;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheAction;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheRequest;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheResponse;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordRequest;
import org.elasticsearch.xpack.core.security.action.user.DeleteUserRequest;
import org.elasticsearch.xpack.core.security.action.user.PutUserRequest;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.esnative.ClientReservedRealm;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.user.ElasticUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.user.User.Fields;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.elasticsearch.xpack.security.support.SecurityIndexManager.IndexState;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.search.SearchService.DEFAULT_KEEPALIVE_SETTING;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.Availability.PRIMARY_SHARDS;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.Availability.SEARCH_SHARDS;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;

/**
 * NativeUsersStore is a store for users that reads from an Elasticsearch index. This store is responsible for fetching the full
 * {@link User} object, which includes the names of the roles assigned to the user.
 * <p>
 * No caching is done by this class, it is handled at a higher level and no polling for changes is done by this class. Modification
 * operations make a best effort attempt to clear the cache on all nodes for the user that was modified.
 */
public class NativeUsersStore {

    public static final String USER_DOC_TYPE = "user";
    public static final String RESERVED_USER_TYPE = "reserved-user";
    private static final Logger logger = LogManager.getLogger(NativeUsersStore.class);

    private final Settings settings;
    private final Client client;

    private final SecurityIndexManager securityIndex;

    public NativeUsersStore(Settings settings, Client client, SecurityIndexManager securityIndex) {
        this.settings = settings;
        this.client = client;
        this.securityIndex = securityIndex;
    }

    /**
     * Blocking version of {@code getUser} that blocks until the User is returned
     */
    public void getUser(String username, ActionListener<User> listener) {
        getUserAndPassword(
            username,
            ActionListener.wrap((uap) -> { listener.onResponse(uap == null ? null : uap.user()); }, listener::onFailure)
        );
    }

    /**
     * Retrieve a list of users, if userNames is null or empty, fetch all users
     */
    public void getUsers(String[] userNames, final ActionListener<Collection<User>> listener) {
        final Consumer<Exception> handleException = (t) -> {
            if (TransportActions.isShardNotAvailableException(t)) {
                logger.trace("could not retrieve users because of a shard not available exception", t);
                if (t instanceof IndexNotFoundException) {
                    // We don't invoke the onFailure listener here, instead just pass an empty list
                    // as the index doesn't exist. Could have been deleted between checks and execution
                    listener.onResponse(Collections.emptyList());
                } else {
                    listener.onFailure(t);
                }
            }
            listener.onFailure(t);
        };

        final IndexState projectSecurityIndex = this.securityIndex.forCurrentProject();
        if (projectSecurityIndex.indexExists() == false) {
            listener.onResponse(Collections.emptyList());
        } else if (projectSecurityIndex.isAvailable(PRIMARY_SHARDS) == false) {
            listener.onFailure(projectSecurityIndex.getUnavailableReason(PRIMARY_SHARDS));
        } else if (userNames.length == 1) { // optimization for single user lookup
            final String username = userNames[0];
            getUserAndPassword(
                username,
                ActionListener.wrap(
                    (uap) -> listener.onResponse(uap == null ? Collections.emptyList() : Collections.singletonList(uap.user())),
                    handleException
                )
            );
        } else {
            projectSecurityIndex.checkIndexVersionThenExecute(listener::onFailure, () -> {
                final QueryBuilder query;
                if (userNames == null || userNames.length == 0) {
                    query = QueryBuilders.termQuery(Fields.TYPE.getPreferredName(), USER_DOC_TYPE);
                } else {
                    final String[] users = Arrays.stream(userNames).map(s -> getIdForUser(USER_DOC_TYPE, s)).toArray(String[]::new);
                    query = QueryBuilders.boolQuery().filter(QueryBuilders.idsQuery().addIds(users));
                }
                final Supplier<ThreadContext.StoredContext> supplier = client.threadPool().getThreadContext().newRestorableContext(false);
                try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(SECURITY_ORIGIN)) {
                    SearchRequest request = client.prepareSearch(SECURITY_MAIN_ALIAS)
                        .setScroll(DEFAULT_KEEPALIVE_SETTING.get(settings))
                        .setQuery(query)
                        .setSize(1000)
                        .setFetchSource(true)
                        .request();
                    request.indicesOptions().ignoreUnavailable();
                    ScrollHelper.fetchAllByEntity(client, request, new ContextPreservingActionListener<>(supplier, listener), (hit) -> {
                        UserAndPassword u = transformUser(hit.getId(), hit.getSourceAsMap());
                        return u != null ? u.user() : null;
                    });
                }
            });
        }
    }

    public void queryUsers(SearchRequest searchRequest, ActionListener<QueryUserResults> listener) {
        final IndexState projectSecurityIndex = securityIndex.forCurrentProject();
        if (projectSecurityIndex.indexExists() == false) {
            logger.debug("security index does not exist");
            listener.onResponse(QueryUserResults.EMPTY);
        } else if (projectSecurityIndex.isAvailable(SEARCH_SHARDS) == false) {
            listener.onFailure(projectSecurityIndex.getUnavailableReason(SEARCH_SHARDS));
        } else {
            projectSecurityIndex.checkIndexVersionThenExecute(
                listener::onFailure,
                () -> executeAsyncWithOrigin(
                    client,
                    SECURITY_ORIGIN,
                    TransportSearchAction.TYPE,
                    searchRequest,
                    ActionListener.wrap(searchResponse -> {
                        final long total = searchResponse.getHits().getTotalHits().value();
                        if (total == 0) {
                            logger.debug("No users found for query [{}]", searchRequest.source().query());
                            listener.onResponse(QueryUserResults.EMPTY);
                            return;
                        }

                        final List<QueryUserResult> userItems = Arrays.stream(searchResponse.getHits().getHits()).map(hit -> {
                            UserAndPassword userAndPassword = transformUser(hit.getId(), hit.getSourceAsMap());
                            return userAndPassword != null ? new QueryUserResult(userAndPassword.user(), hit.getSortValues()) : null;
                        }).filter(Objects::nonNull).toList();
                        listener.onResponse(new QueryUserResults(userItems, total));
                    }, listener::onFailure)
                )
            );
        }
    }

    void getUserCount(final ActionListener<Long> listener) {
        final IndexState projectSecurityIndex = this.securityIndex.forCurrentProject();
        if (projectSecurityIndex.indexExists() == false) {
            listener.onResponse(0L);
        } else if (projectSecurityIndex.isAvailable(SEARCH_SHARDS) == false) {
            listener.onFailure(projectSecurityIndex.getUnavailableReason(SEARCH_SHARDS));
        } else {
            projectSecurityIndex.checkIndexVersionThenExecute(
                listener::onFailure,
                () -> executeAsyncWithOrigin(
                    client.threadPool().getThreadContext(),
                    SECURITY_ORIGIN,
                    client.prepareSearch(SECURITY_MAIN_ALIAS)
                        .setQuery(QueryBuilders.termQuery(Fields.TYPE.getPreferredName(), USER_DOC_TYPE))
                        .setSize(0)
                        .setTrackTotalHits(true)
                        .request(),
                    listener.<SearchResponse>safeMap(response -> response.getHits().getTotalHits().value()),
                    client::search
                )
            );
        }
    }

    /**
     * Async method to retrieve a user and their password
     */
    private void getUserAndPassword(final String user, final ActionListener<UserAndPassword> listener) {
        final IndexState projectSecurityIndex = securityIndex.forCurrentProject();
        if (projectSecurityIndex.isAvailable(PRIMARY_SHARDS) == false) {
            if (projectSecurityIndex.indexExists() == false) {
                logger.trace("could not retrieve user [{}] because security index does not exist", user);
            } else {
                logger.warn("could not retrieve user [{}] because security index is not available", user);
            }
            listener.onResponse(null);
        } else {
            projectSecurityIndex.checkIndexVersionThenExecute(
                listener::onFailure,
                () -> executeAsyncWithOrigin(
                    client.threadPool().getThreadContext(),
                    SECURITY_ORIGIN,
                    client.prepareGet(SECURITY_MAIN_ALIAS, getIdForUser(USER_DOC_TYPE, user)).request(),
                    new ActionListener<GetResponse>() {
                        @Override
                        public void onResponse(GetResponse response) {
                            logger.trace(
                                "user [{}] is doc [{}] in index [{}] with primTerm [{}] and seqNo [{}]",
                                user,
                                response.getId(),
                                response.getIndex(),
                                response.getPrimaryTerm(),
                                response.getSeqNo()
                            );
                            listener.onResponse(transformUser(response.getId(), response.getSource()));
                        }

                        @Override
                        public void onFailure(Exception t) {
                            if (t instanceof IndexNotFoundException) {
                                logger.trace(() -> "could not retrieve user [" + user + "] because security index does not exist", t);
                            } else {
                                logger.error(() -> "failed to retrieve user [" + user + "]", t);
                            }
                            // We don't invoke the onFailure listener here, instead
                            // we call the response with a null user
                            listener.onResponse(null);
                        }
                    },
                    client::get
                )
            );
        }
    }

    /**
     * Async method to change the password of a native or reserved user. If a reserved user does not exist, the document will be created
     * with a hash of the provided password.
     */
    public void changePassword(final ChangePasswordRequest request, final ActionListener<Void> listener) {
        final String username = request.username();
        final String docType;
        if (ClientReservedRealm.isReserved(username, settings)) {
            docType = RESERVED_USER_TYPE;
        } else {
            docType = USER_DOC_TYPE;
        }

        securityIndex.forCurrentProject().prepareIndexIfNeededThenExecute(listener::onFailure, () -> {
            executeAsyncWithOrigin(
                client.threadPool().getThreadContext(),
                SECURITY_ORIGIN,
                client.prepareUpdate(SECURITY_MAIN_ALIAS, getIdForUser(docType, username))
                    .setDoc(Requests.INDEX_CONTENT_TYPE, Fields.PASSWORD.getPreferredName(), String.valueOf(request.passwordHash()))
                    .setRefreshPolicy(request.getRefreshPolicy())
                    .request(),
                new ActionListener<UpdateResponse>() {
                    @Override
                    public void onResponse(UpdateResponse updateResponse) {
                        assert updateResponse.getResult() == DocWriteResponse.Result.UPDATED
                            || updateResponse.getResult() == DocWriteResponse.Result.NOOP;
                        clearRealmCache(request.username(), listener, null);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        if (isIndexNotFoundOrDocumentMissing(e)) {
                            if (docType.equals(RESERVED_USER_TYPE)) {
                                updateReservedUser(
                                    username,
                                    request.passwordHash(),
                                    DocWriteRequest.OpType.INDEX,
                                    request.getRefreshPolicy(),
                                    listener
                                );
                            } else {
                                logger.debug(() -> format("failed to change password for user [%s]", request.username()), e);
                                ValidationException validationException = new ValidationException();
                                validationException.addValidationError("user must exist in order to change password");
                                listener.onFailure(validationException);
                            }
                        } else {
                            listener.onFailure(e);
                        }
                    }
                },
                client::update
            );
        });
    }

    /**
     * Asynchronous method to create the elastic superuser with the given password hash. The cache for the user will be
     * cleared after the document has been indexed.
     */
    public void createElasticUser(char[] passwordHash, ActionListener<Void> listener) {
        updateReservedUser(ElasticUser.NAME, passwordHash, DocWriteRequest.OpType.CREATE, RefreshPolicy.IMMEDIATE, listener);
    }

    /**
     * Asynchronous method to create or update a reserved user with the given password hash. The cache for the user will be
     * cleared after the document has been indexed
     */
    private void updateReservedUser(
        String username,
        char[] passwordHash,
        DocWriteRequest.OpType opType,
        RefreshPolicy refresh,
        ActionListener<Void> listener
    ) {
        securityIndex.forCurrentProject().prepareIndexIfNeededThenExecute(listener::onFailure, () -> {
            executeAsyncWithOrigin(
                client.threadPool().getThreadContext(),
                SECURITY_ORIGIN,
                client.prepareIndex(SECURITY_MAIN_ALIAS)
                    .setOpType(opType)
                    .setId(getIdForUser(RESERVED_USER_TYPE, username))
                    .setSource(
                        Fields.PASSWORD.getPreferredName(),
                        String.valueOf(passwordHash),
                        Fields.ENABLED.getPreferredName(),
                        true,
                        Fields.TYPE.getPreferredName(),
                        RESERVED_USER_TYPE
                    )
                    .setRefreshPolicy(refresh)
                    .request(),
                listener.<DocWriteResponse>delegateFailure((l, indexResponse) -> clearRealmCache(username, l, null)),
                client::index
            );
        });
    }

    /**
     * Asynchronous method to put a user. A put user request without a password hash is treated as an update and will fail with a
     * {@link ValidationException} if the user does not exist. If a password hash is provided, then we issue a update request with an
     * upsert document as well; the upsert document sets the enabled flag of the user to true but if the document already exists, this
     * method will not modify the enabled value.
     */
    public void putUser(final PutUserRequest request, final ActionListener<Boolean> listener) {
        if (request.passwordHash() == null) {
            updateUserWithoutPassword(request, listener);
        } else {
            indexUser(request, listener);
        }
    }

    /**
     * Handles updating a user that should already exist where their password should not change
     */
    private void updateUserWithoutPassword(final PutUserRequest putUserRequest, final ActionListener<Boolean> listener) {
        assert putUserRequest.passwordHash() == null;
        // We must have an existing document
        securityIndex.forCurrentProject().prepareIndexIfNeededThenExecute(listener::onFailure, () -> {
            executeAsyncWithOrigin(
                client.threadPool().getThreadContext(),
                SECURITY_ORIGIN,
                client.prepareUpdate(SECURITY_MAIN_ALIAS, getIdForUser(USER_DOC_TYPE, putUserRequest.username()))
                    .setDoc(
                        Requests.INDEX_CONTENT_TYPE,
                        Fields.USERNAME.getPreferredName(),
                        putUserRequest.username(),
                        Fields.ROLES.getPreferredName(),
                        putUserRequest.roles(),
                        Fields.FULL_NAME.getPreferredName(),
                        putUserRequest.fullName(),
                        Fields.EMAIL.getPreferredName(),
                        putUserRequest.email(),
                        Fields.METADATA.getPreferredName(),
                        putUserRequest.metadata(),
                        Fields.ENABLED.getPreferredName(),
                        putUserRequest.enabled(),
                        Fields.TYPE.getPreferredName(),
                        USER_DOC_TYPE
                    )
                    .setRefreshPolicy(putUserRequest.getRefreshPolicy())
                    .request(),
                new ActionListener<UpdateResponse>() {
                    @Override
                    public void onResponse(UpdateResponse updateResponse) {
                        assert updateResponse.getResult() == DocWriteResponse.Result.UPDATED
                            || updateResponse.getResult() == DocWriteResponse.Result.NOOP
                            : "Expected 'UPDATED' or 'NOOP' result [" + updateResponse + "] for request [" + putUserRequest + "]";
                        clearRealmCache(putUserRequest.username(), listener, false);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        Exception failure = e;
                        if (isIndexNotFoundOrDocumentMissing(e)) {
                            // if the index doesn't exist we can never update a user
                            // if the document doesn't exist, then this update is not valid
                            logger.debug(() -> format("failed to update user document with username [%s]", putUserRequest.username()), e);
                            ValidationException validationException = new ValidationException();
                            validationException.addValidationError("password must be specified unless you are updating an existing user");
                            failure = validationException;
                        }
                        listener.onFailure(failure);
                    }
                },
                client::update
            );
        });
    }

    private void indexUser(final PutUserRequest putUserRequest, final ActionListener<Boolean> listener) {
        assert putUserRequest.passwordHash() != null;
        securityIndex.forCurrentProject().prepareIndexIfNeededThenExecute(listener::onFailure, () -> {
            executeAsyncWithOrigin(
                client.threadPool().getThreadContext(),
                SECURITY_ORIGIN,
                client.prepareIndex(SECURITY_MAIN_ALIAS)
                    .setId(getIdForUser(USER_DOC_TYPE, putUserRequest.username()))
                    .setSource(
                        Fields.USERNAME.getPreferredName(),
                        putUserRequest.username(),
                        Fields.PASSWORD.getPreferredName(),
                        String.valueOf(putUserRequest.passwordHash()),
                        Fields.ROLES.getPreferredName(),
                        putUserRequest.roles(),
                        Fields.FULL_NAME.getPreferredName(),
                        putUserRequest.fullName(),
                        Fields.EMAIL.getPreferredName(),
                        putUserRequest.email(),
                        Fields.METADATA.getPreferredName(),
                        putUserRequest.metadata(),
                        Fields.ENABLED.getPreferredName(),
                        putUserRequest.enabled(),
                        Fields.TYPE.getPreferredName(),
                        USER_DOC_TYPE
                    )
                    .setRefreshPolicy(putUserRequest.getRefreshPolicy())
                    .request(),
                listener.<DocWriteResponse>delegateFailure(
                    (l, updateResponse) -> clearRealmCache(
                        putUserRequest.username(),
                        l,
                        updateResponse.getResult() == DocWriteResponse.Result.CREATED
                    )
                ),
                client::index
            );
        });
    }

    /**
     * Asynchronous method that will update the enabled flag of a user. If the user is reserved and the document does not exist, a document
     * will be created. If the user is not reserved, the user must exist otherwise the operation will fail.
     */
    public void setEnabled(
        final String username,
        final boolean enabled,
        final RefreshPolicy refreshPolicy,
        final ActionListener<Void> listener
    ) {
        if (ClientReservedRealm.isReserved(username, settings)) {
            setReservedUserEnabled(username, enabled, refreshPolicy, true, listener);
        } else {
            setRegularUserEnabled(username, enabled, refreshPolicy, listener);
        }
    }

    private void setRegularUserEnabled(
        final String username,
        final boolean enabled,
        final RefreshPolicy refreshPolicy,
        final ActionListener<Void> listener
    ) {
        securityIndex.forCurrentProject().prepareIndexIfNeededThenExecute(listener::onFailure, () -> {
            executeAsyncWithOrigin(
                client.threadPool().getThreadContext(),
                SECURITY_ORIGIN,
                client.prepareUpdate(SECURITY_MAIN_ALIAS, getIdForUser(USER_DOC_TYPE, username))
                    .setDoc(Requests.INDEX_CONTENT_TYPE, Fields.ENABLED.getPreferredName(), enabled)
                    .setRefreshPolicy(refreshPolicy)
                    .request(),
                new ActionListener<UpdateResponse>() {
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
                            logger.debug(() -> format("failed to %s user [%s]", enabled ? "enable" : "disable", username), e);
                            ValidationException validationException = new ValidationException();
                            validationException.addValidationError("only existing users can be " + (enabled ? "enabled" : "disabled"));
                            failure = validationException;
                        }
                        listener.onFailure(failure);
                    }
                },
                client::update
            );
        });
    }

    private void setReservedUserEnabled(
        final String username,
        final boolean enabled,
        final RefreshPolicy refreshPolicy,
        boolean clearCache,
        final ActionListener<Void> listener
    ) {
        securityIndex.forCurrentProject().prepareIndexIfNeededThenExecute(listener::onFailure, () -> {
            executeAsyncWithOrigin(
                client.threadPool().getThreadContext(),
                SECURITY_ORIGIN,
                client.prepareUpdate(SECURITY_MAIN_ALIAS, getIdForUser(RESERVED_USER_TYPE, username))
                    .setDoc(Requests.INDEX_CONTENT_TYPE, Fields.ENABLED.getPreferredName(), enabled)
                    .setUpsert(
                        XContentType.JSON,
                        Fields.PASSWORD.getPreferredName(),
                        "",
                        Fields.ENABLED.getPreferredName(),
                        enabled,
                        Fields.TYPE.getPreferredName(),
                        RESERVED_USER_TYPE
                    )
                    .setRefreshPolicy(refreshPolicy)
                    .request(),
                listener.<UpdateResponse>delegateFailure((l, updateResponse) -> {
                    if (clearCache) {
                        clearRealmCache(username, l, null);
                    } else {
                        l.onResponse(null);
                    }
                }),
                client::update
            );
        });
    }

    public void deleteUser(final DeleteUserRequest deleteUserRequest, final ActionListener<Boolean> listener) {
        final IndexState projectSecurityIndex = securityIndex.forCurrentProject();
        if (projectSecurityIndex.indexExists() == false) {
            listener.onResponse(false);
        } else if (projectSecurityIndex.isAvailable(PRIMARY_SHARDS) == false) {
            listener.onFailure(projectSecurityIndex.getUnavailableReason(PRIMARY_SHARDS));
        } else {
            projectSecurityIndex.checkIndexVersionThenExecute(listener::onFailure, () -> {
                DeleteRequest request = client.prepareDelete(SECURITY_MAIN_ALIAS, getIdForUser(USER_DOC_TYPE, deleteUserRequest.username()))
                    .request();
                request.setRefreshPolicy(deleteUserRequest.getRefreshPolicy());
                executeAsyncWithOrigin(
                    client.threadPool().getThreadContext(),
                    SECURITY_ORIGIN,
                    request,
                    listener.<DeleteResponse>delegateFailure(
                        (l, deleteResponse) -> clearRealmCache(
                            deleteUserRequest.username(),
                            l,
                            deleteResponse.getResult() == DocWriteResponse.Result.DELETED
                        )
                    ),
                    client::delete
                );
            });
        }
    }

    /**
     * This method is used to verify the username and credentials against those stored in the system.
     *
     * @param username username to lookup the user by
     * @param password the plaintext password to verify
     */
    void verifyPassword(String username, final SecureString password, ActionListener<AuthenticationResult<User>> listener) {
        getUserAndPassword(username, ActionListener.wrap((userAndPassword) -> {
            if (userAndPassword == null) {
                logger.trace(
                    "user [{}] does not exist in index [{}], cannot authenticate against the native realm",
                    username,
                    securityIndex.aliasName()
                );
                listener.onResponse(AuthenticationResult.notHandled());
            } else if (userAndPassword.passwordHash() == null) {
                logger.debug("user [{}] in index [{}] does not have a password, cannot authenticate", username, securityIndex.aliasName());
                listener.onResponse(AuthenticationResult.notHandled());
            } else {
                if (userAndPassword.verifyPassword(password)) {
                    logger.trace("successfully authenticated user [{}] (security index [{}])", userAndPassword, securityIndex.aliasName());
                    listener.onResponse(AuthenticationResult.success(userAndPassword.user()));
                } else {
                    logger.trace("password mismatch for user [{}] (security index [{}])", userAndPassword, securityIndex.aliasName());
                    listener.onResponse(AuthenticationResult.unsuccessful("Password authentication failed for " + username, null));
                }
            }
        }, listener::onFailure));
    }

    void getReservedUserInfo(String username, ActionListener<ReservedUserInfo> listener) {
        final IndexState projectSecurityIndex = securityIndex.forCurrentProject();
        if (projectSecurityIndex.indexExists() == false) {
            listener.onResponse(null);
        } else if (projectSecurityIndex.isAvailable(PRIMARY_SHARDS) == false) {
            listener.onFailure(projectSecurityIndex.getUnavailableReason(PRIMARY_SHARDS));
        } else {
            projectSecurityIndex.checkIndexVersionThenExecute(
                listener::onFailure,
                () -> executeAsyncWithOrigin(
                    client.threadPool().getThreadContext(),
                    SECURITY_ORIGIN,
                    client.prepareGet(SECURITY_MAIN_ALIAS, getIdForUser(RESERVED_USER_TYPE, username)).request(),
                    new ActionListener<GetResponse>() {
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
                                    listener.onResponse(
                                        enabled ? ReservedUserInfo.defaultEnabledUserInfo() : ReservedUserInfo.defaultDisabledUserInfo()
                                    );
                                } else {
                                    listener.onResponse(new ReservedUserInfo(password.toCharArray(), enabled));
                                }
                            } else {
                                listener.onResponse(null);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            if (TransportActions.isShardNotAvailableException(e)) {
                                logger.trace(
                                    () -> format("could not retrieve built in user [%s] info since security index unavailable", username),
                                    e
                                );
                            }
                            listener.onFailure(e);
                        }
                    },
                    client::get
                )
            );
        }
    }

    void getAllReservedUserInfo(ActionListener<Map<String, ReservedUserInfo>> listener) {
        final IndexState projectSecurityIndex = securityIndex.forCurrentProject();
        if (projectSecurityIndex.indexExists() == false) {
            listener.onResponse(Collections.emptyMap());
        } else if (projectSecurityIndex.isAvailable(SEARCH_SHARDS) == false) {
            listener.onFailure(projectSecurityIndex.getUnavailableReason(SEARCH_SHARDS));
        } else {
            projectSecurityIndex.checkIndexVersionThenExecute(
                listener::onFailure,
                () -> executeAsyncWithOrigin(
                    client.threadPool().getThreadContext(),
                    SECURITY_ORIGIN,
                    client.prepareSearch(SECURITY_MAIN_ALIAS)
                        .setTrackTotalHits(true)
                        .setQuery(QueryBuilders.termQuery(Fields.TYPE.getPreferredName(), RESERVED_USER_TYPE))
                        .setFetchSource(true)
                        .request(),
                    new ActionListener<SearchResponse>() {
                        @Override
                        public void onResponse(SearchResponse searchResponse) {
                            Map<String, ReservedUserInfo> userInfos = new HashMap<>();
                            assert searchResponse.getHits().getTotalHits().value() <= 10
                                : "there are more than 10 reserved users we need to change this to retrieve them all!";
                            for (SearchHit searchHit : searchResponse.getHits().getHits()) {
                                Map<String, Object> sourceMap = searchHit.getSourceAsMap();
                                String password = (String) sourceMap.get(Fields.PASSWORD.getPreferredName());
                                Boolean enabled = (Boolean) sourceMap.get(Fields.ENABLED.getPreferredName());
                                final String id = searchHit.getId();
                                assert id != null && id.startsWith(RESERVED_USER_TYPE)
                                    : "id [" + id + "] does not start with reserved-user prefix";
                                final String username = id.substring(RESERVED_USER_TYPE.length() + 1);
                                if (password == null) {
                                    listener.onFailure(new IllegalStateException("password hash must not be null!"));
                                    return;
                                } else if (enabled == null) {
                                    listener.onFailure(new IllegalStateException("enabled must not be null!"));
                                    return;
                                } else {
                                    userInfos.put(username, new ReservedUserInfo(password.toCharArray(), enabled));
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
                    },
                    client::search
                )
            );
        }
    }

    private <Response> void clearRealmCache(String username, ActionListener<Response> listener, Response response) {
        ClearRealmCacheRequest request = new ClearRealmCacheRequest().usernames(username);
        executeAsyncWithOrigin(client, SECURITY_ORIGIN, ClearRealmCacheAction.INSTANCE, request, new ActionListener<>() {
            @Override
            public void onResponse(ClearRealmCacheResponse nodes) {
                listener.onResponse(response);
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(() -> "unable to clear realm cache for user [" + username + "]", e);
                ElasticsearchException exception = new ElasticsearchException(
                    "clearing the cache for [" + username + "] failed. please clear the realm cache manually",
                    e
                );
                listener.onFailure(exception);
            }
        });
    }

    @Nullable
    private static UserAndPassword transformUser(final String id, final Map<String, Object> sourceMap) {
        if (sourceMap == null) {
            return null;
        }
        assert id != null && id.startsWith(USER_DOC_TYPE) : "id [" + id + "] does not start with user prefix";
        final String username = id.substring(USER_DOC_TYPE.length() + 1);
        try {
            String password = (String) sourceMap.get(Fields.PASSWORD.getPreferredName());
            @SuppressWarnings("unchecked")
            String[] roles = ((List<String>) sourceMap.get(Fields.ROLES.getPreferredName())).toArray(Strings.EMPTY_ARRAY);
            String fullName = (String) sourceMap.get(Fields.FULL_NAME.getPreferredName());
            String email = (String) sourceMap.get(Fields.EMAIL.getPreferredName());
            Boolean enabled = (Boolean) sourceMap.get(Fields.ENABLED.getPreferredName());
            if (enabled == null) {
                // fallback mechanism as a user from 2.x may not have the enabled field
                enabled = Boolean.TRUE;
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> metadata = (Map<String, Object>) sourceMap.get(Fields.METADATA.getPreferredName());
            return new UserAndPassword(new User(username, roles, fullName, email, metadata, enabled), password.toCharArray());
        } catch (Exception e) {
            logger.error(() -> "error in the format of data for user [" + username + "]", e);
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
        private final Hasher hasher;

        ReservedUserInfo(char[] passwordHash, boolean enabled) {
            this.passwordHash = passwordHash;
            this.enabled = enabled;
            this.hasher = Hasher.resolveFromHash(this.passwordHash);
        }

        boolean hasEmptyPassword() {
            return passwordHash.length == 0;
        }

        boolean verifyPassword(SecureString data) {
            return hasher.verify(data, this.passwordHash);
        }

        static ReservedUserInfo defaultEnabledUserInfo() {
            return new ReservedUserInfo(new char[0], true);
        }

        static ReservedUserInfo defaultDisabledUserInfo() {
            return new ReservedUserInfo(new char[0], false);
        }
    }

    /**
     * Result record for every document matching a user
     */
    public record QueryUserResult(User user, Object[] sortValues) {}

    /**
     * Total result for a Query User query
     */
    public record QueryUserResults(List<QueryUserResult> userQueryResult, long total) {
        public static final QueryUserResults EMPTY = new QueryUserResults(List.of(), 0);
    }
}
