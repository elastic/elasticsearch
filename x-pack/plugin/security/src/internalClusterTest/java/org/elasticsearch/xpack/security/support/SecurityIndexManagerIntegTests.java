/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.support;

import org.elasticsearch.exception.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xpack.core.security.action.user.PutUserRequestBuilder;
import org.elasticsearch.xpack.core.security.action.user.PutUserResponse;
import org.elasticsearch.xpack.security.authz.store.NativePrivilegeStore;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class SecurityIndexManagerIntegTests extends SecurityIntegTestCase {

    private final int concurrentCallsToOnAvailable = 6;
    private final ExecutorService executor = Executors.newFixedThreadPool(concurrentCallsToOnAvailable);

    @After
    public void shutdownExecutor() {
        executor.shutdown();
    }

    public void testConcurrentOperationsTryingToCreateSecurityIndexAndAlias() throws Exception {
        final int processors = Runtime.getRuntime().availableProcessors();
        final int numThreads = Math.min(50, scaledRandomIntBetween((processors + 1) / 2, 4 * processors));  // up to 50 threads
        final int maxNumRequests = 50 / numThreads; // bound to a maximum of 50 requests
        final int numRequests = scaledRandomIntBetween(Math.min(4, maxNumRequests), maxNumRequests);
        logger.info("creating users with [{}] threads, each sending [{}] requests", numThreads, numRequests);

        final List<ActionFuture<PutUserResponse>> futures = new CopyOnWriteArrayList<>();
        final List<Exception> exceptions = new CopyOnWriteArrayList<>();
        final Thread[] threads = new Thread[numThreads];
        final CyclicBarrier barrier = new CyclicBarrier(threads.length);
        final AtomicInteger userNumber = new AtomicInteger(0);
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    exceptions.add(e);
                }

                @Override
                protected void doRun() throws Exception {
                    final List<PutUserRequestBuilder> requests = new ArrayList<>(numRequests);
                    final SecureString password = new SecureString("test-user-password".toCharArray());
                    for (int i = 0; i < numRequests; i++) {
                        requests.add(
                            new PutUserRequestBuilder(client()).username("user" + userNumber.getAndIncrement())
                                .password(password, getFastStoredHashAlgoForTests())
                                .roles(randomAlphaOfLengthBetween(1, 16))
                        );
                    }

                    barrier.await(10L, TimeUnit.SECONDS);

                    for (PutUserRequestBuilder request : requests) {
                        futures.add(request.execute());
                    }
                }
            }, "create_users_thread" + i);
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        assertThat(exceptions, Matchers.empty());
        assertEquals(futures.size(), numRequests * numThreads);
        for (ActionFuture<PutUserResponse> future : futures) {
            // In rare cases, the user could be updated instead of created. For the purpose of
            // this test, either created or updated is sufficient to prove that the security
            // index is created. So we don't need to assert the value.
            future.actionGet().created();
        }
    }

    @SuppressWarnings("unchecked")
    public void testOnIndexAvailableForSearchIndexCompletesWithinTimeout() throws Exception {
        final SecurityIndexManager securityIndexManager = internalCluster().getInstances(NativePrivilegeStore.class)
            .iterator()
            .next()
            .getSecurityIndexManager();
        final ActionFuture<Void> future = new PlainActionFuture<>();
        // pick longer wait than in the assertBusy that waits for below to ensure index has had enough time to initialize
        securityIndexManager.getProject(Metadata.DEFAULT_PROJECT_ID)
            .onIndexAvailableForSearch((ActionListener<Void>) future, TimeValue.timeValueSeconds(40));

        // check listener added
        assertThat(
            securityIndexManager.getStateChangeListeners(),
            hasItem(instanceOf(SecurityIndexManager.StateConsumerWithCancellable.class))
        );

        createSecurityIndexWithWaitForActiveShards();

        assertBusy(
            () -> assertThat(
                securityIndexManager.getProject(Metadata.DEFAULT_PROJECT_ID).isAvailable(SecurityIndexManager.Availability.SEARCH_SHARDS),
                is(true)
            ),
            30,
            TimeUnit.SECONDS
        );

        // security index creation is complete and index is available for search; therefore whenIndexAvailableForSearch should report
        // success in time
        future.actionGet();

        // check no remaining listeners
        assertThat(
            securityIndexManager.getStateChangeListeners(),
            not(hasItem(instanceOf(SecurityIndexManager.StateConsumerWithCancellable.class)))
        );
    }

    @SuppressWarnings("unchecked")
    public void testOnIndexAvailableForSearchIndexAlreadyAvailable() throws Exception {
        createSecurityIndexWithWaitForActiveShards();

        final SecurityIndexManager securityIndexManager = internalCluster().getInstances(NativePrivilegeStore.class)
            .iterator()
            .next()
            .getSecurityIndexManager();

        // ensure security index manager state is fully in the expected precondition state for this test (ready for search)
        assertBusy(
            () -> assertThat(
                securityIndexManager.getProject(Metadata.DEFAULT_PROJECT_ID).isAvailable(SecurityIndexManager.Availability.SEARCH_SHARDS),
                is(true)
            ),
            30,
            TimeUnit.SECONDS
        );

        // With 0 timeout
        {
            final ActionFuture<Void> future = new PlainActionFuture<>();
            securityIndexManager.getProject(Metadata.DEFAULT_PROJECT_ID)
                .onIndexAvailableForSearch((ActionListener<Void>) future, TimeValue.timeValueSeconds(0));
            future.actionGet();
        }

        // With non-0 timeout
        {
            final ActionFuture<Void> future = new PlainActionFuture<>();
            securityIndexManager.getProject(Metadata.DEFAULT_PROJECT_ID)
                .onIndexAvailableForSearch((ActionListener<Void>) future, TimeValue.timeValueSeconds(10));
            future.actionGet();
        }

        // check no remaining listeners
        assertThat(
            securityIndexManager.getStateChangeListeners(),
            not(hasItem(instanceOf(SecurityIndexManager.StateConsumerWithCancellable.class)))
        );
    }

    @SuppressWarnings("unchecked")
    public void testOnIndexAvailableForSearchIndexUnderConcurrentLoad() throws Exception {
        final SecurityIndexManager securityIndexManager = internalCluster().getInstances(NativePrivilegeStore.class)
            .iterator()
            .next()
            .getSecurityIndexManager();
        // Long time out calls should all succeed
        final List<Future<Void>> futures = new ArrayList<>();
        for (int i = 0; i < concurrentCallsToOnAvailable / 2; i++) {
            final Future<Void> future = executor.submit(() -> {
                try {
                    final ActionFuture<Void> f = new PlainActionFuture<>();
                    securityIndexManager.getProject(Metadata.DEFAULT_PROJECT_ID)
                        .onIndexAvailableForSearch((ActionListener<Void>) f, TimeValue.timeValueSeconds(40));
                    f.actionGet();
                } catch (Exception ex) {
                    fail(ex, "should not have encountered exception");
                }
                return null;
            });
            futures.add(future);
        }

        // short time-out tasks should all time out
        for (int i = 0; i < concurrentCallsToOnAvailable / 2; i++) {
            final Future<Void> future = executor.submit(() -> {
                expectThrows(ElasticsearchTimeoutException.class, () -> {
                    final ActionFuture<Void> f = new PlainActionFuture<>();
                    securityIndexManager.getProject(Metadata.DEFAULT_PROJECT_ID)
                        .onIndexAvailableForSearch((ActionListener<Void>) f, TimeValue.timeValueMillis(10));
                    f.actionGet();
                });
                return null;
            });
            futures.add(future);
        }

        // Sleep a second for short-running calls to timeout
        Thread.sleep(1000);

        createSecurityIndexWithWaitForActiveShards();
        // ensure security index manager state is fully in the expected precondition state for this test (ready for search)
        assertBusy(
            () -> assertThat(
                securityIndexManager.getProject(Metadata.DEFAULT_PROJECT_ID).isAvailable(SecurityIndexManager.Availability.SEARCH_SHARDS),
                is(true)
            ),
            30,
            TimeUnit.SECONDS
        );

        for (var future : futures) {
            future.get(10, TimeUnit.SECONDS);
        }

        // check no remaining listeners
        assertThat(
            securityIndexManager.getStateChangeListeners(),
            not(hasItem(instanceOf(SecurityIndexManager.StateConsumerWithCancellable.class)))
        );
    }

    @SuppressWarnings("unchecked")
    public void testOnIndexAvailableForSearchIndexWaitTimeOut() {
        // security index does not exist and nothing is triggering creation, so whenIndexAvailableForSearch will time out

        final SecurityIndexManager securityIndexManager = internalCluster().getInstances(NativePrivilegeStore.class)
            .iterator()
            .next()
            .getSecurityIndexManager();

        {
            final ActionFuture<Void> future = new PlainActionFuture<>();
            securityIndexManager.getProject(Metadata.DEFAULT_PROJECT_ID)
                .onIndexAvailableForSearch((ActionListener<Void>) future, TimeValue.timeValueMillis(100));
            expectThrows(ElasticsearchTimeoutException.class, future::actionGet);
        }

        // Also works with 0 timeout
        {
            final ActionFuture<Void> future = new PlainActionFuture<>();
            securityIndexManager.getProject(Metadata.DEFAULT_PROJECT_ID)
                .onIndexAvailableForSearch((ActionListener<Void>) future, TimeValue.timeValueMillis(0));
            expectThrows(ElasticsearchTimeoutException.class, future::actionGet);
        }

        // check no remaining listeners
        assertThat(
            securityIndexManager.getStateChangeListeners(),
            not(hasItem(instanceOf(SecurityIndexManager.StateConsumerWithCancellable.class)))
        );
    }

    public void testSecurityIndexSettingsCannotBeChanged() throws Exception {
        // make sure the security index is not auto-created
        GetIndexRequest getIndexRequest = new GetIndexRequest(TEST_REQUEST_TIMEOUT);
        getIndexRequest.indices(SECURITY_MAIN_ALIAS);
        getIndexRequest.indicesOptions(IndicesOptions.lenientExpandOpen());
        GetIndexResponse getIndexResponse = client().admin().indices().getIndex(getIndexRequest).actionGet();
        assertThat(getIndexResponse.getIndices().length, is(0));
        // use a variety of expressions that should match the main security index
        List<String> securityIndexNames = List.of(
            SecuritySystemIndices.SECURITY_MAIN_ALIAS + "*",
            SecuritySystemIndices.SECURITY_MAIN_ALIAS,
            ".security-7",
            ".security-7*",
            "*",
            ".*"
        );
        // create an old-style template
        assertAcked(
            indicesAdmin().preparePutTemplate("template-covering-the-main-security-index")
                .setPatterns(securityIndexNames)
                .setSettings(
                    Settings.builder()
                        .put("index.refresh_interval", "1234s")
                        .put("index.priority", "9876")
                        .put("index.number_of_replicas", "8")
                        .build()
                )
        );
        // create an new-style template
        ComposableIndexTemplate cit = ComposableIndexTemplate.builder()
            .indexPatterns(securityIndexNames)
            .template(
                new Template(
                    Settings.builder()
                        .put("index.refresh_interval", "1234s")
                        .put("index.priority", "9876")
                        .put("index.number_of_replicas", "8")
                        .build(),
                    null,
                    null
                )
            )
            .priority(4L)
            .version(5L)
            .build();
        assertAcked(
            client().execute(
                TransportPutComposableIndexTemplateAction.TYPE,
                new TransportPutComposableIndexTemplateAction.Request("composable-template-covering-the-main-security-index").indexTemplate(
                    cit
                )
            )
        );
        // trigger index auto-creation
        final PutUserResponse putUserResponse = new PutUserRequestBuilder(client()).username("user")
            .password(new SecureString("test-user-password".toCharArray()), getFastStoredHashAlgoForTests())
            .roles(randomAlphaOfLengthBetween(1, 16))
            .get();
        assertTrue(putUserResponse.created());
        getIndexResponse = client().admin().indices().getIndex(getIndexRequest).actionGet();
        assertThat(getIndexResponse.getIndices().length, is(1));
        assertThat(getIndexResponse.getIndices(), arrayContaining(".security-7"));
        // assert the settings from the templates don't show up in the newly created security index
        for (Settings settings : getIndexResponse.getSettings().values()) {
            assertThat(settings.get("index.refresh_interval"), nullValue());
            assertThat(settings.get("index.priority"), notNullValue());
            assertThat(settings.get("index.priority"), not("9876"));
            assertThat(settings.get("index.number_of_replicas"), not("8"));
        }
        // also assert that settings cannot be explicitly changed for the security index
        Settings someSettings = Settings.builder()
            .put("index.refresh_interval", "2345s")
            .put("index.priority", "8765")
            .put("index.number_of_replicas", "4")
            .build();
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(SECURITY_MAIN_ALIAS);
        updateSettingsRequest.settings(someSettings);
        expectThrows(IllegalStateException.class, () -> client().admin().indices().updateSettings(updateSettingsRequest).actionGet());
        UpdateSettingsRequest updateSettingsRequest2 = new UpdateSettingsRequest(".security-7");
        updateSettingsRequest2.settings(someSettings);
        expectThrows(IllegalStateException.class, () -> client().admin().indices().updateSettings(updateSettingsRequest2).actionGet());
    }

    @Before
    public void cleanupSecurityIndex() {
        super.deleteSecurityIndex();
    }
}
