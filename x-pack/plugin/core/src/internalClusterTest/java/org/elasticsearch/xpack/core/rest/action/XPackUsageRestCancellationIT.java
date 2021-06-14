/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.rest.action;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.Netty4Plugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.action.XPackUsageAction;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

import static org.elasticsearch.action.support.ActionTestUtils.wrapAsRestResponseListener;
import static org.elasticsearch.test.TaskAssertions.assertAllCancellableTasksAreCancelled;
import static org.elasticsearch.test.TaskAssertions.assertAllTasksHaveFinished;
import static org.elasticsearch.test.TaskAssertions.awaitTaskWithPrefix;
import static org.hamcrest.core.IsEqual.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class XPackUsageRestCancellationIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(getTestTransportPlugin(), Netty4Plugin.class, BlockingUsageActionXPackPlugin.class);
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    public void testCancellation() throws Exception {
        internalCluster().startMasterOnlyNode();
        ensureStableCluster(1);
        final String actionName = XPackUsageAction.NAME;

        final Request request = new Request(HttpGet.METHOD_NAME, "/_xpack/usage");
        final PlainActionFuture<Response> future = new PlainActionFuture<>();
        final Cancellable cancellable = getRestClient().performRequestAsync(request, wrapAsRestResponseListener(future));

        assertThat(future.isDone(), equalTo(false));
        awaitTaskWithPrefix(actionName);

        BlockingXPackFeatureSet.waitUntilIsExecuted();
        cancellable.cancel();
        assertAllCancellableTasksAreCancelled(actionName);

        BlockingXPackFeatureSet.unblockExecution();
        expectThrows(CancellationException.class, future::actionGet);

        assertAllTasksHaveFinished(actionName);
    }

    public static class BlockingUsageActionXPackPlugin extends LocalStateCompositeXPackPlugin {
        public BlockingUsageActionXPackPlugin(Settings settings, Path configPath) {
            super(settings, configPath);
        }

        @Override
        public Collection<Module> createGuiceModules() {
            final List<Module> modules = new ArrayList<>(super.createGuiceModules());
            modules.add(b -> XPackPlugin.bindFeatureSet(b, BlockingXPackFeatureSet.class));
            modules.add(b -> XPackPlugin.bindFeatureSet(b, NonBlockingXPackFeatureSet.class));
            return modules;
        }
    }

    public static class BlockingXPackFeatureSet implements XPackFeatureSet {
        private static final CyclicBarrier barrier = new CyclicBarrier(2);
        private static final CountDownLatch blockActionLatch = new CountDownLatch(1);

        static void waitUntilIsExecuted() throws Exception {
            barrier.await();
        }

        static void unblockExecution() {
            blockActionLatch.countDown();
        }

        @Override
        public String name() {
            return "blocking";
        }

        @Override
        public boolean available() {
            return false;
        }

        @Override
        public boolean enabled() {
            return false;
        }

        @Override
        public Map<String, Object> nativeCodeInfo() {
            return Collections.emptyMap();
        }

        @Override
        public void usage(ActionListener<Usage> listener) {
            try {
                barrier.await();
                blockActionLatch.await();
            } catch (Exception e) {
                throw new AssertionError(e);
            }
            listener.onResponse(new XPackFeatureSet.Usage("test", false, false) {
                @Override
                public Version getMinimalSupportedVersion() {
                    return Version.CURRENT;
                }
            });
        }
    }

    public static class NonBlockingXPackFeatureSet implements XPackFeatureSet {
        @Override
        public String name() {
            return "non_blocking";
        }

        @Override
        public boolean available() {
            return false;
        }

        @Override
        public boolean enabled() {
            return false;
        }

        @Override
        public Map<String, Object> nativeCodeInfo() {
            return null;
        }

        @Override
        public void usage(ActionListener<Usage> listener) {
            assert false : "Unexpected execution";
        }
    }
}
