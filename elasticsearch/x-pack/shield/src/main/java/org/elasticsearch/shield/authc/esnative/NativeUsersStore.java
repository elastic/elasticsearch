/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.esnative;

import com.carrotsearch.hppc.ObjectHashSet;
import com.carrotsearch.hppc.ObjectLongHashMap;
import com.carrotsearch.hppc.ObjectLongMap;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.shield.InternalClient;
import org.elasticsearch.shield.ShieldTemplateService;
import org.elasticsearch.shield.user.SystemUser;
import org.elasticsearch.shield.user.User;
import org.elasticsearch.shield.user.User.Fields;
import org.elasticsearch.shield.action.realm.ClearRealmCacheRequest;
import org.elasticsearch.shield.action.realm.ClearRealmCacheResponse;
import org.elasticsearch.shield.action.user.ChangePasswordRequest;
import org.elasticsearch.shield.action.user.DeleteUserRequest;
import org.elasticsearch.shield.action.user.PutUserRequest;
import org.elasticsearch.shield.authc.support.Hasher;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.client.SecurityClient;
import org.elasticsearch.shield.support.SelfReschedulingRunnable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.shield.Security.setting;

/**
 * ESNativeUsersStore is a {@code UserStore} that, instead of reading from a
 * file, reads from an Elasticsearch index instead. This {@code UserStore} in
 * particular implements both a User store and a UserRoles store, which means it
 * is responsible for fetching not only {@code User} objects, but also
 * retrieving the roles for a given username.
 * <p>
 * No caching is done by this class, it is handled at a higher level
 */
public class NativeUsersStore extends AbstractComponent implements ClusterStateListener {

    public static final Setting<Integer> SCROLL_SIZE_SETTING =
            Setting.intSetting(setting("authc.native.scroll.size"), 1000, Property.NodeScope);

    public static final Setting<TimeValue> SCROLL_KEEP_ALIVE_SETTING =
            Setting.timeSetting(setting("authc.native.scroll.keep_alive"), TimeValue.timeValueSeconds(10L), Property.NodeScope);

    public static final Setting<TimeValue> POLL_INTERVAL_SETTING =
            Setting.timeSetting(setting("authc.native.reload.interval"), TimeValue.timeValueSeconds(30L), Property.NodeScope);

    public enum State {
        INITIALIZED,
        STARTING,
        STARTED,
        STOPPING,
        STOPPED,
        FAILED
    }

    public static final String USER_DOC_TYPE = "user";
    static final String RESERVED_USER_DOC_TYPE = "reserved-user";

    private final Hasher hasher = Hasher.BCRYPT;
    private final List<ChangeListener> listeners = new CopyOnWriteArrayList<>();
    private final AtomicReference<State> state = new AtomicReference<>(State.INITIALIZED);
    private final Provider<InternalClient> clientProvider;
    private final ThreadPool threadPool;

    private SelfReschedulingRunnable userPoller;
    private Client client;
    private int scrollSize;
    private TimeValue scrollKeepAlive;

    private volatile boolean shieldIndexExists = false;

    @Inject
    public NativeUsersStore(Settings settings, Provider<InternalClient> clientProvider, ThreadPool threadPool) {
        super(settings);
        this.clientProvider = clientProvider;
        this.threadPool = threadPool;
    }

    /**
     * Blocking version of {@code getUser} that blocks until the User is returned
     */
    public User getUser(String username) {
        if (state() != State.STARTED) {
            logger.trace("attempted to get user [{}] before service was started", username);
            return null;
        }
        UserAndPassword uap = getUserAndPassword(username);
        return uap == null ? null : uap.user();
    }

    /**
     * Retrieve a single user, calling the listener when retrieved
     */
    public void getUser(String username, final ActionListener<User> listener) {
        if (state() != State.STARTED) {
            logger.trace("attempted to get user [{}] before service was started", username);
            listener.onFailure(new IllegalStateException("user cannot be retrieved as native user service has not been started"));
            return;
        }
        getUserAndPassword(username, new ActionListener<UserAndPassword>() {
            @Override
            public void onResponse(UserAndPassword uap) {
                listener.onResponse(uap == null ? null : uap.user());
            }

            @Override
            public void onFailure(Throwable t) {
                if (t instanceof IndexNotFoundException) {
                    logger.trace("failed to retrieve user [{}] since security index does not exist", username);
                    // We don't invoke the onFailure listener here, instead
                    // we call the response with a null user
                    listener.onResponse(null);
                } else {
                    logger.debug("failed to retrieve user [{}]", t, username);
                    listener.onFailure(t);
                }
            }
        });
    }

    /**
     * Retrieve a list of users, if usernames is null or empty, fetch all users
     */
    public void getUsers(String[] usernames, final ActionListener<List<User>> listener) {
        if (state() != State.STARTED) {
            logger.trace("attempted to get users before service was started");
            listener.onFailure(new IllegalStateException("users cannot be retrieved as native user service has not been started"));
            return;
        }
        try {
            final List<User> users = new ArrayList<>();
            QueryBuilder query;
            if (usernames == null || usernames.length == 0) {
                query = QueryBuilders.matchAllQuery();
            } else {
                query = QueryBuilders.boolQuery().filter(QueryBuilders.idsQuery(USER_DOC_TYPE).addIds(usernames));
            }
            SearchRequest request = client.prepareSearch(ShieldTemplateService.SECURITY_INDEX_NAME)
                    .setScroll(scrollKeepAlive)
                    .setTypes(USER_DOC_TYPE)
                    .setQuery(query)
                    .setSize(scrollSize)
                    .setFetchSource(true)
                    .request();
            request.indicesOptions().ignoreUnavailable();

            // This function is MADNESS! But it works, don't think about it too hard...
            client.search(request, new ActionListener<SearchResponse>() {

                private SearchResponse lastResponse = null;

                @Override
                public void onResponse(final SearchResponse resp) {
                    lastResponse = resp;
                    boolean hasHits = resp.getHits().getHits().length > 0;
                    if (hasHits) {
                        for (SearchHit hit : resp.getHits().getHits()) {
                            UserAndPassword u = transformUser(hit.getId(), hit.getSource());
                            if (u != null) {
                                users.add(u.user());
                            }
                        }
                        SearchScrollRequest scrollRequest = client.prepareSearchScroll(resp.getScrollId())
                                .setScroll(scrollKeepAlive).request();
                        client.searchScroll(scrollRequest, this);
                    } else {
                        if (resp.getScrollId() != null) {
                            clearScrollResponse(resp.getScrollId());
                        }
                        // Finally, return the list of users
                        listener.onResponse(Collections.unmodifiableList(users));
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    // attempt to clear scroll response
                    if (lastResponse != null && lastResponse.getScrollId() != null) {
                        clearScrollResponse(lastResponse.getScrollId());
                    }

                    if (t instanceof IndexNotFoundException) {
                        logger.trace("could not retrieve users because security index does not exist");
                        // We don't invoke the onFailure listener here, instead just pass an empty list
                        listener.onResponse(Collections.emptyList());
                    } else {
                        listener.onFailure(t);
                    }

                }
            });
        } catch (Exception e) {
            logger.error("unable to retrieve users {}", e, Arrays.toString(usernames));
            listener.onFailure(e);
        }
    }

    private UserAndPassword getUserAndPassword(final String username) {
        final AtomicReference<UserAndPassword> userRef = new AtomicReference<>(null);
        final CountDownLatch latch = new CountDownLatch(1);
        getUserAndPassword(username, new LatchedActionListener<>(new ActionListener<UserAndPassword>() {
            @Override
            public void onResponse(UserAndPassword user) {
                userRef.set(user);
            }

            @Override
            public void onFailure(Throwable t) {
                if (t instanceof IndexNotFoundException) {
                    logger.trace("failed to retrieve user [{}] since security index does not exist", t, username);
                } else {
                    logger.error("failed to retrieve user [{}]", t, username);
                }
            }
        }, latch));
        try {
            latch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("timed out retrieving user [{}]", username);
            return null;
        }
        return userRef.get();
    }

    private void getUserAndPassword(final String user, final ActionListener<UserAndPassword> listener) {
        try {
            GetRequest request = client.prepareGet(ShieldTemplateService.SECURITY_INDEX_NAME, USER_DOC_TYPE, user).request();
            client.get(request, new ActionListener<GetResponse>() {
                @Override
                public void onResponse(GetResponse response) {
                    listener.onResponse(transformUser(response.getId(), response.getSource()));
                }

                @Override
                public void onFailure(Throwable t) {
                    if (t instanceof IndexNotFoundException) {
                        logger.trace("could not retrieve user [{}] because security index does not exist", t, user);
                    } else {
                        logger.error("failed to retrieve user [{}]", t, user);
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
            logger.error("unable to retrieve user [{}]", e, user);
            listener.onFailure(e);
        }
    }

    public void changePassword(final ChangePasswordRequest request, final ActionListener<Void> listener) {
        final String username = request.username();
        if (SystemUser.NAME.equals(username)) {
            ValidationException validationException = new ValidationException();
            validationException.addValidationError("changing the password for [" + username + "] is not allowed");
            listener.onFailure(validationException);
            return;
        }

        final String docType;
        if (ReservedRealm.isReserved(username)) {
            docType = RESERVED_USER_DOC_TYPE;
        } else {
            docType = USER_DOC_TYPE;
        }

        client.prepareUpdate(ShieldTemplateService.SECURITY_INDEX_NAME, docType, username)
                .setDoc(Fields.PASSWORD.getPreferredName(), String.valueOf(request.passwordHash()))
                .setRefresh(request.refresh())
                .execute(new ActionListener<UpdateResponse>() {
                    @Override
                    public void onResponse(UpdateResponse updateResponse) {
                        assert updateResponse.isCreated() == false;
                        clearRealmCache(request.username(), listener, null);
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        Throwable cause = e;
                        if (e instanceof ElasticsearchException) {
                            cause = ExceptionsHelper.unwrapCause(e);
                            if ((cause instanceof IndexNotFoundException) == false
                                    && (cause instanceof DocumentMissingException) == false) {
                                listener.onFailure(e);
                                return;
                            }
                        }

                        if (docType.equals(RESERVED_USER_DOC_TYPE)) {
                            createReservedUser(username, request.passwordHash(), request.refresh(), listener);
                        } else {
                            logger.debug("failed to change password for user [{}]", cause, request.username());
                            ValidationException validationException = new ValidationException();
                            validationException.addValidationError("user must exist in order to change password");
                            listener.onFailure(validationException);
                        }
                    }
                });
    }

    private void createReservedUser(String username, char[] passwordHash, boolean refresh, ActionListener<Void> listener) {
        client.prepareIndex(ShieldTemplateService.SECURITY_INDEX_NAME, RESERVED_USER_DOC_TYPE, username)
                .setSource(Fields.PASSWORD.getPreferredName(), String.valueOf(passwordHash))
                .setRefresh(refresh)
                .execute(new ActionListener<IndexResponse>() {
                    @Override
                    public void onResponse(IndexResponse indexResponse) {
                        clearRealmCache(username, listener, null);
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        listener.onFailure(e);
                    }
                });
    }

    public void putUser(final PutUserRequest request, final ActionListener<Boolean> listener) {
        if (state() != State.STARTED) {
            listener.onFailure(new IllegalStateException("user cannot be added as native user service has not been started"));
            return;
        }

        try {
            if (request.passwordHash() == null) {
                updateUserWithoutPassword(request, listener);
            } else {
                indexUser(request, listener);
            }
        } catch (Exception e) {
            logger.error("unable to put user [{}]", e, request.username());
            listener.onFailure(e);
        }
    }

    private void updateUserWithoutPassword(final PutUserRequest putUserRequest, final ActionListener<Boolean> listener) {
        assert putUserRequest.passwordHash() == null;
        // We must have an existing document
        client.prepareUpdate(ShieldTemplateService.SECURITY_INDEX_NAME, USER_DOC_TYPE, putUserRequest.username())
                .setDoc(User.Fields.USERNAME.getPreferredName(), putUserRequest.username(),
                        User.Fields.ROLES.getPreferredName(), putUserRequest.roles(),
                        User.Fields.FULL_NAME.getPreferredName(), putUserRequest.fullName(),
                        User.Fields.EMAIL.getPreferredName(), putUserRequest.email(),
                        User.Fields.METADATA.getPreferredName(), putUserRequest.metadata())
                .setRefresh(putUserRequest.refresh())
                .execute(new ActionListener<UpdateResponse>() {
                    @Override
                    public void onResponse(UpdateResponse updateResponse) {
                        assert updateResponse.isCreated() == false;
                        clearRealmCache(putUserRequest.username(), listener, false);
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        Throwable cause = e;
                        if (e instanceof ElasticsearchException) {
                            cause = ExceptionsHelper.unwrapCause(e);
                            if ((cause instanceof IndexNotFoundException) == false
                                    && (cause instanceof DocumentMissingException) == false) {
                                listener.onFailure(e);
                                return;
                            }
                        }

                        // if the index doesn't exist we can never update a user
                        // if the document doesn't exist, then this update is not valid
                        logger.debug("failed to update user document with username [{}]", cause, putUserRequest.username());
                        ValidationException validationException = new ValidationException();
                        validationException.addValidationError("password must be specified unless you are updating an existing user");
                        listener.onFailure(validationException);
                    }
                });
    }

    private void indexUser(final PutUserRequest putUserRequest, final ActionListener<Boolean> listener) {
        assert putUserRequest.passwordHash() != null;
        client.prepareIndex(ShieldTemplateService.SECURITY_INDEX_NAME,
                USER_DOC_TYPE, putUserRequest.username())
                .setSource(User.Fields.USERNAME.getPreferredName(), putUserRequest.username(),
                        User.Fields.PASSWORD.getPreferredName(), String.valueOf(putUserRequest.passwordHash()),
                        User.Fields.ROLES.getPreferredName(), putUserRequest.roles(),
                        User.Fields.FULL_NAME.getPreferredName(), putUserRequest.fullName(),
                        User.Fields.EMAIL.getPreferredName(), putUserRequest.email(),
                        User.Fields.METADATA.getPreferredName(), putUserRequest.metadata())
                .setRefresh(putUserRequest.refresh())
                .execute(new ActionListener<IndexResponse>() {
                    @Override
                    public void onResponse(IndexResponse indexResponse) {
                        // if the document was just created, then we don't need to clear cache
                        if (indexResponse.isCreated()) {
                            listener.onResponse(indexResponse.isCreated());
                            return;
                        }

                        clearRealmCache(putUserRequest.username(), listener, indexResponse.isCreated());
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        listener.onFailure(e);
                    }
                });
    }

    public void deleteUser(final DeleteUserRequest deleteUserRequest, final ActionListener<Boolean> listener) {
        if (state() != State.STARTED) {
            listener.onFailure(new IllegalStateException("user cannot be deleted as native user service has not been started"));
            return;
        }

        try {
            DeleteRequest request = client.prepareDelete(ShieldTemplateService.SECURITY_INDEX_NAME,
                    USER_DOC_TYPE, deleteUserRequest.username()).request();
            request.indicesOptions().ignoreUnavailable();
            request.refresh(deleteUserRequest.refresh());
            client.delete(request, new ActionListener<DeleteResponse>() {
                @Override
                public void onResponse(DeleteResponse deleteResponse) {
                    clearRealmCache(deleteUserRequest.username(), listener, deleteResponse.isFound());
                }

                @Override
                public void onFailure(Throwable e) {
                    listener.onFailure(e);
                }
            });
        } catch (Exception e) {
            logger.error("unable to remove user", e);
            listener.onFailure(e);
        }
    }

    public boolean canStart(ClusterState clusterState, boolean master) {
        if (state() != State.INITIALIZED) {
            return false;
        }

        if (clusterState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // wait until the gateway has recovered from disk, otherwise we
            // think may not have the .security index but they it may not have
            // been restored from the cluster state on disk yet
            logger.debug("native users store waiting until gateway has recovered from disk");
            return false;
        }

        if (clusterState.metaData().templates().get(ShieldTemplateService.SECURITY_TEMPLATE_NAME) == null) {
            logger.debug("native users template [{}] does not exist, so service cannot start",
                    ShieldTemplateService.SECURITY_TEMPLATE_NAME);
            return false;
        }

        IndexMetaData metaData = clusterState.metaData().index(ShieldTemplateService.SECURITY_INDEX_NAME);
        if (metaData == null) {
            logger.debug("security index [{}] does not exist, so service can start", ShieldTemplateService.SECURITY_INDEX_NAME);
            return true;
        }

        if (clusterState.routingTable().index(ShieldTemplateService.SECURITY_INDEX_NAME).allPrimaryShardsActive()) {
            logger.debug("security index [{}] all primary shards started, so service can start",
                    ShieldTemplateService.SECURITY_INDEX_NAME);
            shieldIndexExists = true;
            return true;
        }
        return false;
    }

    public void start() {
        try {
            if (state.compareAndSet(State.INITIALIZED, State.STARTING)) {
                this.client = clientProvider.get();
                this.scrollSize = SCROLL_SIZE_SETTING.get(settings);
                this.scrollKeepAlive = SCROLL_KEEP_ALIVE_SETTING.get(settings);

                UserStorePoller poller = new UserStorePoller();
                try {
                    poller.doRun();
                } catch (Exception e) {
                    logger.warn("failed to do initial poll of users", e);
                }
                userPoller = new SelfReschedulingRunnable(poller, threadPool, POLL_INTERVAL_SETTING.get(settings), Names.GENERIC, logger);
                userPoller.start();
                state.set(State.STARTED);
            }
        } catch (Exception e) {
            logger.error("failed to start native user store", e);
            state.set(State.FAILED);
        }
    }

    public void stop() {
        if (state.compareAndSet(State.STARTED, State.STOPPING)) {
            try {
                userPoller.stop();
            } catch (Throwable t) {
                state.set(State.FAILED);
                throw t;
            } finally {
                state.set(State.STOPPED);
            }
        }
    }

    /**
     * This method is used to verify the username and credentials against those stored in the system.
     *
     * @param username username to lookup the user by
     * @param password the plaintext password to verify
     * @return {@link} User object if successful or {@code null} if verification fails
     */
    public User verifyPassword(String username, final SecuredString password) {
        if (state() != State.STARTED) {
            logger.trace("attempted to verify user credentials for [{}] but service was not started", username);
            return null;
        }

        UserAndPassword user = getUserAndPassword(username);
        if (user == null || user.passwordHash() == null) {
            return null;
        }
        if (hasher.verify(password, user.passwordHash())) {
            return user.user();
        }
        return null;
    }

    public void addListener(ChangeListener listener) {
        listeners.add(listener);
    }

    boolean started() {
        return state() == State.STARTED;
    }

    boolean shieldIndexExists() {
        return shieldIndexExists;
    }

    char[] reservedUserPassword(String username) throws Throwable {
        assert started();
        final AtomicReference<char[]> passwordHash = new AtomicReference<>();
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        client.prepareGet(ShieldTemplateService.SECURITY_INDEX_NAME, RESERVED_USER_DOC_TYPE, username)
                .execute(new LatchedActionListener<>(new ActionListener<GetResponse>() {
                    @Override
                    public void onResponse(GetResponse getResponse) {
                        if (getResponse.isExists()) {
                            Map<String, Object> sourceMap = getResponse.getSourceAsMap();
                            String password = (String) sourceMap.get(User.Fields.PASSWORD.getPreferredName());
                            if (password == null || password.isEmpty()) {
                                failure.set(new IllegalStateException("password hash must not be empty!"));
                                return;
                            }
                            passwordHash.set(password.toCharArray());
                        }
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        if (e instanceof IndexNotFoundException) {
                            logger.trace("could not retrieve built in user [{}] password since security index does not exist", e, username);
                        } else {
                            logger.error("failed to retrieve built in user [{}] password", e, username);
                            failure.set(e);
                        }
                    }
                }, latch));

        try {
            final boolean responseReceived = latch.await(30, TimeUnit.SECONDS);
            if (responseReceived == false) {
                failure.set(new TimeoutException("timed out trying to get built in user [" + username + "]"));
            }
        } catch (InterruptedException e) {
            failure.set(e);
        }

        Throwable failureCause = failure.get();
        if (failureCause != null) {
            // if there is any sort of failure we need to throw an exception to prevent the fallback to the default password...
            throw failureCause;
        }
        return passwordHash.get();
    }

    private void clearScrollResponse(String scrollId) {
        ClearScrollRequest clearScrollRequest = client.prepareClearScroll().addScrollId(scrollId).request();
        client.clearScroll(clearScrollRequest, new ActionListener<ClearScrollResponse>() {
            @Override
            public void onResponse(ClearScrollResponse response) {
                // cool, it cleared, we don't really care though...
            }

            @Override
            public void onFailure(Throwable t) {
                // Not really much to do here except for warn about it...
                logger.warn("failed to clear scroll [{}]", t, scrollId);
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
            public void onFailure(Throwable e) {
                logger.error("unable to clear realm cache for user [{}]", e, username);
                ElasticsearchException exception = new ElasticsearchException("clearing the cache for [" + username
                        + "] failed. please clear the realm cache manually", e);
                listener.onFailure(exception);
            }
        });
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        final boolean exists = event.state().metaData().indices().get(ShieldTemplateService.SECURITY_INDEX_NAME) != null;
        // make sure all the primaries are active
        if (exists && event.state().routingTable().index(ShieldTemplateService.SECURITY_INDEX_NAME).allPrimaryShardsActive()) {
            logger.debug("security index [{}] all primary shards started, so polling can start",
                    ShieldTemplateService.SECURITY_INDEX_NAME);
            shieldIndexExists = true;
        } else {
            // always set the value - it may have changed...
            shieldIndexExists = false;
        }
    }

    public State state() {
        return state.get();
    }

    // FIXME hack for testing
    public void reset() {
        final State state = state();
        if (state != State.STOPPED && state != State.FAILED) {
            throw new IllegalStateException("can only reset if stopped!!!");
        }
        this.listeners.clear();
        this.client = null;
        this.shieldIndexExists = false;
        this.state.set(State.INITIALIZED);
    }

    @Nullable
    private UserAndPassword transformUser(String username, Map<String, Object> sourceMap) {
        if (sourceMap == null) {
            return null;
        }
        try {
            String password = (String) sourceMap.get(User.Fields.PASSWORD.getPreferredName());
            String[] roles = ((List<String>) sourceMap.get(User.Fields.ROLES.getPreferredName())).toArray(Strings.EMPTY_ARRAY);
            String fullName = (String) sourceMap.get(User.Fields.FULL_NAME.getPreferredName());
            String email = (String) sourceMap.get(User.Fields.EMAIL.getPreferredName());
            Map<String, Object> metadata = (Map<String, Object>) sourceMap.get(User.Fields.METADATA.getPreferredName());
            return new UserAndPassword(new User(username, roles, fullName, email, metadata), password.toCharArray());
        } catch (Exception e) {
            logger.error("error in the format of data for user [{}]", e, username);
            return null;
        }
    }

    private class UserStorePoller extends AbstractRunnable {

        // this map contains the mapping for username -> version, which is used when polling the index to easily detect of
        // any changes that may have been missed since the last update.
        private final ObjectLongHashMap<String> userVersionMap = new ObjectLongHashMap<>();
        private final ObjectLongHashMap<String> reservedUserVersionMap = new ObjectLongHashMap<>();

        @Override
        public void doRun() {
            // hold a reference to the client since the poller may run after the class is stopped (we don't interrupt it running) and
            // we reset when we test which sets the client to null...
            final Client client = NativeUsersStore.this.client;
            if (isStopped()) {
                return;
            }
            if (shieldIndexExists == false) {
                logger.trace("cannot poll for user changes since security index [{}] does not exist", ShieldTemplateService
                        .SECURITY_INDEX_NAME);
                return;
            }

            logger.trace("starting polling of user index to check for changes");
            List<String> changedUsers = scrollForModifiedUsers(client, USER_DOC_TYPE, userVersionMap);
            if (isStopped()) {
                return;
            }

            changedUsers.addAll(scrollForModifiedUsers(client, RESERVED_USER_DOC_TYPE, reservedUserVersionMap));
            if (isStopped()) {
                return;
            }

            notifyListeners(changedUsers);
            logger.trace("finished polling of user index");
        }

        private List<String> scrollForModifiedUsers(Client client, String docType, ObjectLongMap<String> usersMap) {
            // create a copy of all known users
            ObjectHashSet<String> knownUsers = new ObjectHashSet<>(usersMap.keys());
            List<String> changedUsers = new ArrayList<>();

            SearchResponse response = null;
            try {
                client.admin().indices().prepareRefresh(ShieldTemplateService.SECURITY_INDEX_NAME).get();
                response = client.prepareSearch(ShieldTemplateService.SECURITY_INDEX_NAME)
                        .setScroll(scrollKeepAlive)
                        .setQuery(QueryBuilders.typeQuery(docType))
                        .setSize(scrollSize)
                        .setVersion(true)
                        .setFetchSource(false) // we only need id and version
                        .get();

                boolean keepScrolling = response.getHits().getHits().length > 0;
                while (keepScrolling) {
                    for (SearchHit hit : response.getHits().getHits()) {
                        final String username = hit.id();
                        final long version = hit.version();
                        if (knownUsers.contains(username)) {
                            final long lastKnownVersion = usersMap.get(username);
                            if (version != lastKnownVersion) {
                                // version is only changed by this method
                                assert version > lastKnownVersion;
                                usersMap.put(username, version);
                                // there is a chance that the user's cache has already been cleared and we'll clear it again but
                                // this should be ok in most cases as user changes should not be that frequent
                                changedUsers.add(username);
                            }
                            knownUsers.remove(username);
                        } else {
                            usersMap.put(username, version);
                        }
                    }

                    if (isStopped()) {
                        // bail here
                        return Collections.emptyList();
                    }
                    response = client.prepareSearchScroll(response.getScrollId()).setScroll(scrollKeepAlive).get();
                    keepScrolling = response.getHits().getHits().length > 0;
                }
            } catch (IndexNotFoundException e) {
                logger.trace("security index does not exist", e);
            } finally {
                if (response != null && response.getScrollId() != null) {
                    ClearScrollRequest clearScrollRequest = client.prepareClearScroll().addScrollId(response.getScrollId()).request();
                    client.clearScroll(clearScrollRequest).actionGet();
                }
            }

            // we now have a list of users that were in our version map and have been deleted
            Iterator<ObjectCursor<String>> userIter = knownUsers.iterator();
            while (userIter.hasNext()) {
                String user = userIter.next().value;
                usersMap.remove(user);
                changedUsers.add(user);
            }

            return changedUsers;
        }

        private void notifyListeners(List<String> changedUsers) {
            if (changedUsers.isEmpty()) {
                return;
            }

            // make the list unmodifiable to prevent modifications by any listeners
            changedUsers = Collections.unmodifiableList(changedUsers);
            if (logger.isDebugEnabled()) {
                logger.debug("changes detected for users [{}]", changedUsers);
            }

            // call listeners
            Throwable th = null;
            for (ChangeListener listener : listeners) {
                try {
                    listener.onUsersChanged(changedUsers);
                } catch (Throwable t) {
                    th = ExceptionsHelper.useOrSuppress(th, t);
                }
            }

            ExceptionsHelper.reThrowIfNotNull(th);
        }

        @Override
        public void onFailure(Throwable t) {
            logger.error("error occurred while checking the native users for changes", t);
        }

        private boolean isStopped() {
            State state = state();
            return state == State.STOPPED || state == State.STOPPING;
        }
    }

    interface ChangeListener {

        void onUsersChanged(List<String> username);
    }

    public static void registerSettings(SettingsModule settingsModule) {
        settingsModule.registerSetting(SCROLL_SIZE_SETTING);
        settingsModule.registerSetting(SCROLL_KEEP_ALIVE_SETTING);
        settingsModule.registerSetting(POLL_INTERVAL_SETTING);
    }
}
