/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.profile;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.search.internal.ReaderContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.security.SecurityExtension;
import org.elasticsearch.xpack.core.security.action.profile.Profile;
import org.elasticsearch.xpack.core.security.action.profile.SuggestProfilesAction;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesResponse;
import org.elasticsearch.xpack.core.security.action.user.ProfileHasPrivilegesAction;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.ResolvedIndices;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.security.LocalStateSecurity;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.SecuritySettingsSource.TEST_USER_NAME;
import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class ProfileCancellationIntegTests extends AbstractProfileIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(SearchBlockPlugin.class);
        {
            // replace the security plugin with the security plugin with the dummy authorization engine extension
            plugins.remove(LocalStateSecurity.class);
            plugins.add(LocalStateWithDummyAuthorizationEngineExtension.class);
        }
        return List.copyOf(plugins);
    }

    @Override
    protected Class<?> xpackPluginClass() {
        return LocalStateWithDummyAuthorizationEngineExtension.class;
    }

    public void testSuggestProfilesCancellation() throws Exception {
        doActivateProfile(RAC_USER_NAME, TEST_PASSWORD_SECURE_STRING);

        final String xOpaqueId = randomAlphaOfLength(10);
        final Request request = new Request("GET", "/_security/profile/_suggest");
        RequestOptions.Builder options = request.getOptions()
            .toBuilder()
            .addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(TEST_USER_NAME, TEST_PASSWORD_SECURE_STRING))
            .addHeader(Task.X_OPAQUE_ID_HTTP_HEADER, xOpaqueId);
        request.setOptions(options);

        // Stall the search
        enableSearchBlock();

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Exception> error = new AtomicReference<>();
        final Cancellable cancellable = getRestClient().performRequestAsync(request, new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                latch.countDown();
            }

            @Override
            public void onFailure(Exception exception) {
                error.set(exception);
                latch.countDown();
            }
        });

        // Assert that suggest task and search sub-tasks are initiated
        final Set<Long> taskIds = ConcurrentHashMap.newKeySet();
        assertBusy(() -> {
            final List<Task> tasks = getTasksForXOpaqueId(xOpaqueId);
            final List<String> taskActions = tasks.stream().map(Task::getAction).toList();
            assertThat(
                taskActions,
                hasItems(
                    equalTo(SuggestProfilesAction.NAME),
                    equalTo(TransportSearchAction.TYPE.name()),
                    startsWith(TransportSearchAction.TYPE.name())
                )
            );
            assertThat(isShardSearchBlocked(), is(true));
            tasks.forEach(t -> {
                logger.info("task " + t.getId() + "/" + t.getAction() + " registered");
                taskIds.add(t.getId());
            });
        }, 20, TimeUnit.SECONDS);

        // Cancel the suggest request and all tasks should be cancelled
        cancellable.cancel();
        assertBusy(() -> {
            final List<CancellableTask> cancellableTasks = getCancellableTasksForXOpaqueId(xOpaqueId);
            cancellableTasks.forEach(cancellableTask -> {
                assertThat(
                    "task " + cancellableTask.getId() + "/" + cancellableTask.getAction() + " not cancelled",
                    cancellableTask.isCancelled(),
                    is(true)
                );
                taskIds.remove(cancellableTask.getId());
            });
            assertThat(taskIds, empty());
        }, 20, TimeUnit.SECONDS);

        disableSearchBlock();
        latch.await();
        assertThat(error.get(), instanceOf(CancellationException.class));
    }

    public void testProfileHasPrivilegesCancellation() throws Exception {
        Profile racProfile = doActivateProfile(RAC_USER_NAME, TEST_PASSWORD_SECURE_STRING);
        Profile otherRacProfile = doActivateProfile(OTHER_RAC_USER_NAME, TEST_PASSWORD_SECURE_STRING);
        XContentBuilder requestBodyBuilder = JsonXContent.contentBuilder();
        requestBodyBuilder.startObject();
        requestBodyBuilder.field("uids", List.of(racProfile.uid(), otherRacProfile.uid()));
        requestBodyBuilder.startObject("privileges");
        requestBodyBuilder.field("cluster", List.of("monitor"));
        requestBodyBuilder.endObject();
        requestBodyBuilder.endObject();

        final String xOpaqueId = randomAlphaOfLength(10);
        Request request = new Request("POST", "/_security/profile/_has_privileges");
        RequestOptions.Builder options = request.getOptions()
            .toBuilder()
            .addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(TEST_USER_NAME, TEST_PASSWORD_SECURE_STRING))
            .addHeader(Task.X_OPAQUE_ID_HTTP_HEADER, xOpaqueId);
        request.setOptions(options);
        request.setJsonEntity(Strings.toString(requestBodyBuilder));

        blockCheckPrivileges();

        final CountDownLatch responseLatch = new CountDownLatch(1);
        final AtomicReference<Exception> error = new AtomicReference<>();
        final AtomicReference<Response> responseReference = new AtomicReference<>();
        final Cancellable cancellable = getRestClient().performRequestAsync(request, new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                responseReference.set(response);
                responseLatch.countDown();
            }

            @Override
            public void onFailure(Exception exception) {
                error.set(exception);
                responseLatch.countDown();
            }
        });

        // assert that the has privilege task is running
        final AtomicLong taskId = new AtomicLong();
        assertBusy(() -> {
            assertThat(isCheckPrivilegesBlocked(), is(true));
            final List<Task> tasks = getTasksForXOpaqueId(xOpaqueId);
            if (tasks.size() > 1) {
                logger.info("seen more than 1 task for the expected opaque id");
            }
            assertThat(tasks.size(), is(1));
            logger.info("seen task " + tasks.get(0).getId() + "/" + tasks.get(0).getAction());
            assertThat(tasks.get(0).getAction(), equalTo(ProfileHasPrivilegesAction.NAME));
            final List<String> taskActions = tasks.stream().map(Task::getAction).toList();
            assertThat(taskActions, contains(equalTo(ProfileHasPrivilegesAction.NAME)));
            taskId.set(tasks.get(0).getId());
        }, 20, TimeUnit.SECONDS);

        boolean cancelViaAPI = randomBoolean();

        if (cancelViaAPI) {
            request = new Request("POST", "/_tasks/_cancel?actions=cluster%3Aadmin%2Fxpack%2Fsecurity%2Fprofile%2Fhas_privileges");
            options = request.getOptions()
                .toBuilder()
                .addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(TEST_USER_NAME, TEST_PASSWORD_SECURE_STRING));
            request.setOptions(options);
            Response cancelTasksResponse = getRestClient().performRequest(request);
            assertThat(cancelTasksResponse.getStatusLine().getStatusCode(), is(200));
        } else {
            cancellable.cancel();
        }

        assertBusy(() -> {
            final List<CancellableTask> cancellableTasks = getCancellableTasksForXOpaqueId(xOpaqueId);
            assertThat(cancellableTasks.size(), is(1));
            assertThat(cancellableTasks.get(0).isCancelled(), is(true));
            assertThat(cancellableTasks.get(0).getId(), is(taskId.get()));
        }, 20, TimeUnit.SECONDS);

        unblockCheckPrivileges();

        responseLatch.await();

        if (cancelViaAPI) {
            assertThat(responseReference.get(), nullValue());
            assertThat(error.get(), instanceOf(ResponseException.class));
        } else {
            assertThat(responseReference.get(), nullValue());
            assertThat(error.get(), instanceOf(CancellationException.class));
        }
    }

    private List<Task> getTasksForXOpaqueId(String xOpaqueId) {
        final ArrayList<Task> tasks = new ArrayList<>();
        for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
            tasks.addAll(
                transportService.getTaskManager()
                    .getTasks()
                    .values()
                    .stream()
                    .filter(t -> xOpaqueId.equals(t.headers().get(Task.X_OPAQUE_ID_HTTP_HEADER)))
                    .toList()
            );
        }
        return tasks;
    }

    private List<CancellableTask> getCancellableTasksForXOpaqueId(String xOpaqueId) {
        final ArrayList<CancellableTask> cancellableTasks = new ArrayList<>();
        for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
            cancellableTasks.addAll(
                transportService.getTaskManager()
                    .getCancellableTasks()
                    .values()
                    .stream()
                    .filter(t -> xOpaqueId.equals(t.headers().get(Task.X_OPAQUE_ID_HTTP_HEADER)))
                    .toList()
            );
        }
        return cancellableTasks;
    }

    private void enableSearchBlock() {
        for (PluginsService pluginsService : internalCluster().getInstances(PluginsService.class)) {
            pluginsService.filterPlugins(SearchBlockPlugin.class).forEach(SearchBlockPlugin::enableSearchBlock);
        }
    }

    private void disableSearchBlock() {
        for (PluginsService pluginsService : internalCluster().getInstances(PluginsService.class)) {
            pluginsService.filterPlugins(SearchBlockPlugin.class).forEach(SearchBlockPlugin::disableSearchBlock);
        }
    }

    private void blockCheckPrivileges() {
        for (PluginsService pluginsService : internalCluster().getInstances(PluginsService.class)) {
            pluginsService.filterPlugins(LocalStateWithDummyAuthorizationEngineExtension.class)
                .forEach(LocalStateWithDummyAuthorizationEngineExtension::blockCheckPrivileges);
        }
    }

    private void unblockCheckPrivileges() {
        for (PluginsService pluginsService : internalCluster().getInstances(PluginsService.class)) {
            pluginsService.filterPlugins(LocalStateWithDummyAuthorizationEngineExtension.class)
                .forEach(LocalStateWithDummyAuthorizationEngineExtension::unblockCheckPrivileges);
        }
    }

    private boolean isCheckPrivilegesBlocked() {
        for (PluginsService pluginsService : internalCluster().getInstances(PluginsService.class)) {
            if (pluginsService.filterPlugins(LocalStateWithDummyAuthorizationEngineExtension.class)
                .anyMatch(LocalStateWithDummyAuthorizationEngineExtension::isBlockedOnCheckPrivileges)) {
                return true;
            }
        }
        return false;
    }

    private boolean isShardSearchBlocked() {
        for (PluginsService pluginsService : internalCluster().getInstances(PluginsService.class)) {
            if (pluginsService.filterPlugins(SearchBlockPlugin.class).anyMatch(SearchBlockPlugin::isShardSearchBlocked)) {
                return true;
            }
        }
        return false;
    }

    public static class LocalStateWithDummyAuthorizationEngineExtension extends LocalStateSecurity {

        protected static final Logger logger = LogManager.getLogger(LocalStateWithDummyAuthorizationEngineExtension.class);
        private static final AtomicBoolean shouldBlockOnCheckPrivileges = new AtomicBoolean(false);
        private static final AtomicBoolean isBlockedOnCheckPrivileges = new AtomicBoolean(false);

        public LocalStateWithDummyAuthorizationEngineExtension(Settings settings, Path configPath) throws Exception {
            super(settings, configPath);
        }

        @Override
        protected List<SecurityExtension> securityExtensions() {
            return List.of(new DummyAuthorizationEngineExtension(shouldBlockOnCheckPrivileges, isBlockedOnCheckPrivileges, logger));
        }

        void blockCheckPrivileges() {
            shouldBlockOnCheckPrivileges.set(true);
        }

        void unblockCheckPrivileges() {
            shouldBlockOnCheckPrivileges.set(false);
        }

        boolean isBlockedOnCheckPrivileges() {
            return isBlockedOnCheckPrivileges.get();
        }
    }

    /**
     * This Plugin authorizes everything
     */
    public static class DummyAuthorizationEngineExtension implements SecurityExtension {

        private final AtomicBoolean shouldBlockOnCheckPrivileges;
        private final AtomicBoolean isBlockedOnCheckPrivileges;
        private final Logger logger;

        DummyAuthorizationEngineExtension(
            AtomicBoolean shouldBlockOnCheckPrivileges,
            AtomicBoolean isBlockedOnCheckPrivileges,
            Logger logger
        ) {
            this.shouldBlockOnCheckPrivileges = shouldBlockOnCheckPrivileges;
            this.isBlockedOnCheckPrivileges = isBlockedOnCheckPrivileges;
            this.logger = logger;
        }

        @Override
        public AuthorizationEngine getAuthorizationEngine(Settings settings) {
            return new AuthorizationEngine() {
                @Override
                public void resolveAuthorizationInfo(RequestInfo requestInfo, ActionListener<AuthorizationInfo> listener) {
                    listener.onResponse(EmptyAuthorizationInfo.INSTANCE);
                }

                @Override
                public void resolveAuthorizationInfo(Subject subject, ActionListener<AuthorizationInfo> listener) {
                    listener.onResponse(EmptyAuthorizationInfo.INSTANCE);
                }

                @Override
                public void authorizeRunAs(
                    RequestInfo requestInfo,
                    AuthorizationInfo authorizationInfo,
                    ActionListener<AuthorizationResult> listener
                ) {
                    listener.onFailure(new UnsupportedOperationException("not implemented"));
                }

                @Override
                public void authorizeClusterAction(
                    RequestInfo requestInfo,
                    AuthorizationInfo authorizationInfo,
                    ActionListener<AuthorizationResult> listener
                ) {
                    listener.onResponse(AuthorizationResult.granted());
                }

                @Override
                public SubscribableListener<IndexAuthorizationResult> authorizeIndexAction(
                    RequestInfo requestInfo,
                    AuthorizationInfo authorizationInfo,
                    AsyncSupplier<ResolvedIndices> indicesAsyncSupplier,
                    ProjectMetadata metadata
                ) {
                    return SubscribableListener.newSucceeded(IndexAuthorizationResult.ALLOW_NO_INDICES);
                }

                @Override
                public void loadAuthorizedIndices(
                    RequestInfo requestInfo,
                    AuthorizationInfo authorizationInfo,
                    Map<String, IndexAbstraction> indicesLookup,
                    ActionListener<AuthorizationEngine.AuthorizedIndices> listener
                ) {
                    listener.onFailure(new UnsupportedOperationException("not implemented"));
                }

                @Override
                public void validateIndexPermissionsAreSubset(
                    RequestInfo requestInfo,
                    AuthorizationInfo authorizationInfo,
                    Map<String, List<String>> indexNameToNewNames,
                    ActionListener<AuthorizationResult> listener
                ) {
                    listener.onFailure(new UnsupportedOperationException("not implemented"));
                }

                @Override
                public void checkPrivileges(
                    AuthorizationInfo authorizationInfo,
                    PrivilegesToCheck privilegesToCheck,
                    Collection<ApplicationPrivilegeDescriptor> applicationPrivilegeDescriptors,
                    ActionListener<PrivilegesCheckResult> listener
                ) {
                    try {
                        logger.info("blocking check privileges");
                        assertBusy(() -> {
                            boolean blocked = shouldBlockOnCheckPrivileges.get();
                            isBlockedOnCheckPrivileges.set(blocked);
                            assertFalse(blocked);
                        }, 20, TimeUnit.SECONDS);
                        logger.info("unblocking check privileges");
                    } catch (Exception e) {
                        listener.onFailure(e);
                        return;
                    }
                    listener.onResponse(PrivilegesCheckResult.ALL_CHECKS_SUCCESS_NO_DETAILS);
                }

                @Override
                public void getUserPrivileges(AuthorizationInfo authorizationInfo, ActionListener<GetUserPrivilegesResponse> listener) {
                    listener.onFailure(new UnsupportedOperationException("not implemented"));
                }
            };
        }
    }

    public static class SearchBlockPlugin extends Plugin implements ActionPlugin {
        protected static final Logger logger = LogManager.getLogger(SearchBlockPlugin.class);

        private final String nodeId;
        private final AtomicBoolean shouldBlockOnSearch = new AtomicBoolean(false);
        private final AtomicBoolean shardSearchBlocked = new AtomicBoolean(false);

        public SearchBlockPlugin(Settings settings, Path configPath) throws Exception {
            nodeId = settings.get("node.name");
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            super.onIndexModule(indexModule);
            indexModule.addSearchOperationListener(new SearchOperationListener() {

                @Override
                public void onPreQueryPhase(SearchContext c) {
                    logger.info("onPreQueryPhase");
                }

                @Override
                public void onNewReaderContext(ReaderContext c) {
                    try {
                        logger.info("blocking search on " + nodeId);
                        shardSearchBlocked.set(true);
                        assertBusy(() -> assertFalse(shouldBlockOnSearch.get()), 20, TimeUnit.SECONDS);
                        logger.info("unblocking search on " + nodeId);
                        shardSearchBlocked.set(false);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }

        void enableSearchBlock() {
            shouldBlockOnSearch.set(true);
        }

        void disableSearchBlock() {
            shouldBlockOnSearch.set(false);
        }

        boolean isShardSearchBlocked() {
            return shardSearchBlocked.get();
        }
    }
}
