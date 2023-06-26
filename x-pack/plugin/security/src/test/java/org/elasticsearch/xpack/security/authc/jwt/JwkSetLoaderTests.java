/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JwkSetLoaderTests extends ESTestCase {

    public void testConcurrentReloadWillBeQueuedAndShareTheResults() throws IOException, InterruptedException {
        final Path tempDir = createTempDir();
        final Path path = tempDir.resolve("jwkset.json");
        Files.write(path, List.of("{\"keys\":[]}"), StandardCharsets.UTF_8);
        final RealmConfig realmConfig = mock(RealmConfig.class);
        when(realmConfig.getSetting(JwtRealmSettings.PKC_JWKSET_PATH)).thenReturn("jwkset.json");
        final Environment env = mock(Environment.class);
        when(env.configFile()).thenReturn(tempDir);
        when(realmConfig.env()).thenReturn(env);

        final JwkSetLoader jwkSetLoader = spy(new JwkSetLoader(realmConfig, List.of(), null));

        final int nThreads = randomIntBetween(2, 8);
        final var futures = IntStream.range(0, nThreads)
            .mapToObj(i -> new PlainActionFuture<Tuple<Boolean, JwkSetLoader.JwksAlgs>>())
            .toList();

        // Start the first thread for reloading
        // Ensure it is inside the actual loading method and make it wait there to simulate slow processing
        final var loadingLatch = new CountDownLatch(1);
        final var readyLatch = new CountDownLatch(1);
        doAnswer(invocation -> {
            loadingLatch.countDown();
            assertThat(readyLatch.await(10, TimeUnit.SECONDS), is(true));
            invocation.callRealMethod();
            return null;
        }).when(jwkSetLoader).loadInternal(anyActionListener());

        new Thread(() -> jwkSetLoader.reload(futures.get(0))).start();
        assertThat(loadingLatch.await(10, TimeUnit.SECONDS), is(true));

        // Start rest of the threads for racing and ensure they are all through the concurrency controlling area
        Mockito.reset(jwkSetLoader);
        final var threadsCountDown = new CountDownLatch(nThreads - 1);
        doAnswer(invocation -> {
            final Object result = invocation.callRealMethod();
            threadsCountDown.countDown();
            return result;
        }).when(jwkSetLoader).getFuture();
        IntStream.range(1, nThreads).forEach(i -> new Thread(() -> jwkSetLoader.reload(futures.get(i))).start());
        assertThat(threadsCountDown.await(10, TimeUnit.SECONDS), is(true));

        // Notify the first thread to finish the loading work
        readyLatch.countDown();

        // All concurrent reloading calls will get the same result from the first thread and skip the actual loading work
        final Tuple<Boolean, JwkSetLoader.JwksAlgs> tuple = futures.get(0).actionGet();
        futures.subList(1, nThreads).forEach(future -> assertThat(future.actionGet(), sameInstance(tuple)));
        verify(jwkSetLoader, never()).loadInternal(anyActionListener());
    }
}
