/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.support;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xpack.core.security.action.user.PutUserRequestBuilder;
import org.elasticsearch.xpack.core.security.action.user.PutUserResponse;
import org.hamcrest.Matchers;
import org.junit.After;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class SecurityIndexManagerIntegTests extends SecurityIntegTestCase {

    public void testConcurrentOperationsTryingToCreateSecurityIndexAndAlias() throws Exception {
        assertSecurityIndexActive();
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

    public void testTemplateSettingsDontAffectSecurityIndex() throws Exception {
        List<String> securityIndexNames = List.of(
            SecuritySystemIndices.SECURITY_MAIN_ALIAS + "*",
            SecuritySystemIndices.SECURITY_MAIN_ALIAS,
            ".security-7",
            ".security-7*",
            "*",
            ".*"
        );
        assertAcked(
            indicesAdmin().preparePutTemplate("template-covering-the-main-security-index")
                .setPatterns(securityIndexNames)
                .setSettings(Settings.builder().put("index.refresh_interval", "1234s").put("index.priority", "9876").build())
                .get()
        );
        ComposableIndexTemplate cit = new ComposableIndexTemplate(
            securityIndexNames,
            new Template(Settings.builder().put("index.refresh_interval", "1234s").put("index.priority", "9876").build(), null, null),
            null,
            4L,
            5L,
            null
        );
        assertAcked(
            client().execute(
                PutComposableIndexTemplateAction.INSTANCE,
                new PutComposableIndexTemplateAction.Request("composable-template-covering-the-main-security-index").indexTemplate(cit)
            ).get()
        );
        final PutUserResponse putUserResponse = new PutUserRequestBuilder(client()).username("user")
            .password(new SecureString("test-user-password".toCharArray()), getFastStoredHashAlgoForTests())
            .roles(randomAlphaOfLengthBetween(1, 16))
            .get();
        assertTrue(putUserResponse.created());
        GetIndexRequest getIndexRequest = new GetIndexRequest();
        getIndexRequest.indices(SECURITY_MAIN_ALIAS);
        getIndexRequest.indicesOptions(IndicesOptions.lenientExpandOpen());
        GetIndexResponse getIndexResponse = client().admin().indices().getIndex(getIndexRequest).actionGet();
        assertThat(getIndexResponse.getIndices().length, is(1));
        for (Settings settings : getIndexResponse.getSettings().values()) {
            assertThat(settings.get("index.refresh_interval"), nullValue());
            assertThat(settings.get("index.priority"), notNullValue());
            assertThat(settings.get("index.priority"), not("9876"));
        }
    }

    @After
    public void cleanupSecurityIndex() throws Exception {
        super.deleteSecurityIndex();
    }
}
