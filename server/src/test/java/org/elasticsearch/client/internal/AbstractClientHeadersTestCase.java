/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.client.internal;

import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.TransportClusterRerouteAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.TransportCreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.stats.TransportClusterStatsAction;
import org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.TransportDeleteStoredScriptAction;
import org.elasticsearch.action.admin.indices.cache.clear.TransportClearIndicesCacheAction;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.flush.FlushAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.DefaultBuiltInExecutorBuilders;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public abstract class AbstractClientHeadersTestCase extends ESTestCase {

    protected static final Settings HEADER_SETTINGS = Settings.builder()
        .put(ThreadContext.PREFIX + ".key1", "val1")
        .put(ThreadContext.PREFIX + ".key2", "val 2")
        .build();

    private static final ActionType<?>[] ACTIONS = new ActionType<?>[] {
        // client actions
        TransportGetAction.TYPE,
        TransportSearchAction.TYPE,
        TransportDeleteAction.TYPE,
        TransportDeleteStoredScriptAction.TYPE,
        TransportIndexAction.TYPE,

        // cluster admin actions
        TransportClusterStatsAction.TYPE,
        TransportCreateSnapshotAction.TYPE,
        TransportClusterRerouteAction.TYPE,

        // indices admin actions
        TransportCreateIndexAction.TYPE,
        IndicesStatsAction.INSTANCE,
        TransportClearIndicesCacheAction.TYPE,
        FlushAction.INSTANCE };

    protected ThreadPool threadPool;
    private Client client;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Settings settings = Settings.builder()
            .put(HEADER_SETTINGS)
            .put("path.home", createTempDir().toString())
            .put("node.name", "test-" + getTestName())
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        threadPool = new ThreadPool(settings, MeterRegistry.NOOP, new DefaultBuiltInExecutorBuilders());
        client = buildClient(settings, ACTIONS);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        terminate(threadPool);
    }

    protected abstract Client buildClient(Settings headersSettings, ActionType<?>[] testedActions);

    public void testActions() {

        // TODO this is a really shitty way to test it, we need to figure out a way to test all the client methods
        // without specifying each one (reflection doesn't as each action needs its own special settings, without
        // them, request validation will fail before the test is executed. (one option is to enable disabling the
        // validation in the settings??? - ugly and conceptually wrong)

        // choosing arbitrary top level actions to test
        client.prepareGet("idx", "id").execute(new AssertingActionListener<>(TransportGetAction.TYPE.name(), client.threadPool()));
        client.prepareSearch().execute(new AssertingActionListener<>(TransportSearchAction.TYPE.name(), client.threadPool()));
        client.prepareDelete("idx", "id").execute(new AssertingActionListener<>(TransportDeleteAction.NAME, client.threadPool()));
        client.execute(
            TransportDeleteStoredScriptAction.TYPE,
            new DeleteStoredScriptRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "id"),
            new AssertingActionListener<>(TransportDeleteStoredScriptAction.TYPE.name(), client.threadPool())
        );
        client.prepareIndex("idx")
            .setId("id")
            .setSource("source", XContentType.JSON)
            .execute(new AssertingActionListener<>(TransportIndexAction.NAME, client.threadPool()));

        // choosing arbitrary cluster admin actions to test
        client.admin()
            .cluster()
            .prepareClusterStats()
            .execute(new AssertingActionListener<>(TransportClusterStatsAction.TYPE.name(), client.threadPool()));
        client.admin()
            .cluster()
            .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, "repo", "bck")
            .execute(new AssertingActionListener<>(TransportCreateSnapshotAction.TYPE.name(), client.threadPool()));
        client.execute(
            TransportClusterRerouteAction.TYPE,
            new ClusterRerouteRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT),
            new AssertingActionListener<>(TransportClusterRerouteAction.TYPE.name(), client.threadPool())
        );

        // choosing arbitrary indices admin actions to test
        client.admin()
            .indices()
            .prepareCreate("idx")
            .execute(new AssertingActionListener<>(TransportCreateIndexAction.TYPE.name(), client.threadPool()));
        client.admin().indices().prepareStats().execute(new AssertingActionListener<>(IndicesStatsAction.NAME, client.threadPool()));
        client.admin()
            .indices()
            .prepareClearCache("idx1", "idx2")
            .execute(new AssertingActionListener<>(TransportClearIndicesCacheAction.TYPE.name(), client.threadPool()));
        client.admin().indices().prepareFlush().execute(new AssertingActionListener<>(FlushAction.NAME, client.threadPool()));
    }

    public void testOverrideHeader() throws Exception {
        String key1Val = randomAlphaOfLength(5);
        Map<String, String> expected = new HashMap<>();
        expected.put("key1", key1Val);
        expected.put("key2", "val 2");
        client.threadPool().getThreadContext().putHeader("key1", key1Val);
        client.prepareGet("idx", "id")
            .execute(new AssertingActionListener<>(TransportGetAction.TYPE.name(), expected, client.threadPool()));

        client.admin()
            .cluster()
            .prepareClusterStats()
            .execute(new AssertingActionListener<>(TransportClusterStatsAction.TYPE.name(), expected, client.threadPool()));

        client.admin()
            .indices()
            .prepareCreate("idx")
            .execute(new AssertingActionListener<>(TransportCreateIndexAction.TYPE.name(), expected, client.threadPool()));
    }

    protected static void assertHeaders(Map<String, String> headers, Map<String, String> expected) {
        assertNotNull(headers);
        headers = new HashMap<>(headers);
        headers.remove("transport_client"); // default header on TPC
        assertEquals(expected.size(), headers.size());
        for (Map.Entry<String, String> expectedEntry : expected.entrySet()) {
            assertEquals(headers.get(expectedEntry.getKey()), expectedEntry.getValue());
        }
    }

    protected static void assertHeaders(ThreadPool pool) {
        Settings asSettings = HEADER_SETTINGS.getAsSettings(ThreadContext.PREFIX);
        assertHeaders(
            pool.getThreadContext().getHeaders(),
            asSettings.keySet().stream().collect(Collectors.toMap(Function.identity(), k -> asSettings.get(k)))
        );
    }

    public static class InternalException extends Exception {

        private final String action;

        public InternalException(String action) {
            this.action = action;
        }
    }

    protected static class AssertingActionListener<T> implements ActionListener<T> {

        private final String action;
        private final Map<String, String> expectedHeaders;
        private final ThreadPool pool;
        private static final Settings THREAD_HEADER_SETTINGS = HEADER_SETTINGS.getAsSettings(ThreadContext.PREFIX);

        public AssertingActionListener(String action, ThreadPool pool) {
            this(
                action,
                THREAD_HEADER_SETTINGS.keySet().stream().collect(Collectors.toMap(Function.identity(), k -> THREAD_HEADER_SETTINGS.get(k))),
                pool
            );
        }

        public AssertingActionListener(String action, Map<String, String> expectedHeaders, ThreadPool pool) {
            this.action = action;
            this.expectedHeaders = expectedHeaders;
            this.pool = pool;
        }

        @Override
        public void onResponse(T t) {
            fail("an internal exception was expected for action [" + action + "]");
        }

        @Override
        public void onFailure(Exception t) {
            Throwable e = unwrap(t, InternalException.class);
            assertThat("expected action [" + action + "] to throw an internal exception", e, notNullValue());
            assertThat(action, equalTo(((InternalException) e).action));
            Map<String, String> headers = pool.getThreadContext().getHeaders();
            assertHeaders(headers, expectedHeaders);
        }

        public Throwable unwrap(Throwable t, Class<? extends Throwable> exceptionType) {
            int counter = 0;
            Throwable result = t;
            while (exceptionType.isInstance(result) == false) {
                if (result.getCause() == null) {
                    return null;
                }
                if (result.getCause() == result) {
                    return null;
                }
                if (counter++ > 10) {
                    // dear god, if we got more than 10 levels down, WTF? just bail
                    fail("Exception cause unwrapping ran for 10 levels: " + ExceptionsHelper.stackTrace(t));
                    return null;
                }
                result = result.getCause();
            }
            return result;
        }
    }
}
