/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JwkSetLoaderTests extends ESTestCase {

    public void testConcurrentReloadWillBeQueuedAndShareTheResults() throws IOException, InterruptedException {
        final Path tempDir = createTempDir();
        final Path path = tempDir.resolve("jwkset.json");
        Files.write(path, List.of("{\"keys\":[]}"), StandardCharsets.UTF_8);
        final RealmConfig realmConfig = mock(RealmConfig.class);
        when(realmConfig.getSetting(JwtRealmSettings.PKC_JWKSET_PATH)).thenReturn("jwkset.json");
        when(realmConfig.getSetting(JwtRealmSettings.PKC_JWKSET_RELOAD_ENABLED)).thenReturn(false);
        final Environment env = mock(Environment.class);
        when(env.configDir()).thenReturn(tempDir);
        when(realmConfig.env()).thenReturn(env);

        final JwkSetLoader jwkSetLoader = spy(new JwkSetLoader(realmConfig, List.of(), null, null, () -> {}));

        final int nThreads = randomIntBetween(2, 8);
        final var futures = IntStream.range(0, nThreads).mapToObj(i -> new PlainActionFuture<Void>()).toList();

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
        reset(jwkSetLoader);
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

        // All concurrent reloading calls will see the same result as the first thread and skip the actual loading work
        futures.get(0).actionGet();
        final JwkSetLoader.JwksAlgs algs = jwkSetLoader.getContentAndJwksAlgs().jwksAlgs();
        futures.subList(1, nThreads).forEach(future -> {
            future.actionGet();
            assertSame(algs, jwkSetLoader.getContentAndJwksAlgs().jwksAlgs());
        });
        verify(jwkSetLoader, never()).loadInternal(anyActionListener());
    }

    public void testCalculateNextUrlReload() {
        final TimeValue min = TimeValue.timeValueMinutes(5);
        final TimeValue max = TimeValue.timeValueMinutes(60);
        assertThat(calculateNextUrlReload(min, max, null), is(min));
        assertThat(calculateNextUrlReload(min, max, Instant.now().minusSeconds(100)), is(min));
        assertThat(calculateNextUrlReload(min, max, Instant.now()), is(min));
        assertThat(calculateNextUrlReload(min, max, Instant.now().plusSeconds(min.seconds() - 10)), is(min));
        assertThat(calculateNextUrlReload(min, max, Instant.now().plusSeconds(max.seconds() + 1000)), is(max));
        assertThat(
            calculateNextUrlReload(min, max, Instant.now().plusSeconds(min.seconds() + 10)).seconds(),
            both(greaterThan(305L)).and(lessThan(315L))
        ); // 5s10s +/- 5s
    }

    private static TimeValue calculateNextUrlReload(TimeValue min, TimeValue max, Instant lastModifiedTime) {
        return JwkSetLoader.calculateNextUrlReload(min, max, lastModifiedTime, 0);
    }

    public void testCalculateNextUrlReloadWithJitter() {
        for (int i = 0; i < 100; i++) {
            assertThat(
                JwkSetLoader.calculateNextUrlReload(TimeValue.timeValueSeconds(100), TimeValue.timeValueSeconds(100), null, 0.1).seconds(),
                both(greaterThanOrEqualTo(90L)).and(lessThanOrEqualTo(110L))
            ); // 100s +/- 10s
        }
    }

    public void testJitterSeconds() {
        for (int i = 0; i < 100; i++) {
            long jitter = JwkSetLoader.jitterSeconds(Duration.ofSeconds(100), 0.1).toSeconds();
            assertThat(jitter, both(greaterThanOrEqualTo(-10L)).and(lessThanOrEqualTo(10L)));
        }
    }

    public void testFileChangeWatcher() throws IOException {
        Path path = createTempFile();
        var now = Instant.now();
        int ticks = 0;
        var watcher = new JwkSetLoader.FileChangeWatcher(path);
        assertThat(watcher.changedSinceLastCall(), is(true));
        Files.setLastModifiedTime(path, FileTime.from(now.plusSeconds(++ticks)));
        assertThat(watcher.changedSinceLastCall(), is(true));
        assertThat(watcher.changedSinceLastCall(), is(false));
        Files.setLastModifiedTime(path, FileTime.from(now.plusSeconds(++ticks)));
        assertThat(watcher.changedSinceLastCall(), is(true));
        assertThat(watcher.changedSinceLastCall(), is(false));
    }

    public void testFilePkcJwkSetLoader() throws IOException {
        Path tempDir = createTempDir();
        String file = "tmp.txt";
        Path path = tempDir.resolve(file);
        byte[] hello = "hello world".getBytes(StandardCharsets.UTF_8);
        Files.write(path, hello);
        RealmConfig realmConfig = mock(RealmConfig.class);
        Environment env = mock(Environment.class);
        when(env.configDir()).thenReturn(tempDir);
        when(realmConfig.env()).thenReturn(env);
        when(realmConfig.getSetting(JwtRealmSettings.PKC_JWKSET_PATH)).thenReturn(file);
        when(realmConfig.getSetting(JwtRealmSettings.PKC_JWKSET_RELOAD_FILE_INTERVAL)).thenReturn(TimeValue.timeValueMinutes(0)); // do now
        ThreadPool threadPool = mock(ThreadPool.class);
        CountingCallback callback = new CountingCallback();

        var loader = new JwkSetLoader.FilePkcJwkSetLoader(realmConfig, threadPool, path.toString(), callback);
        loader.start(); // schedules task

        ArgumentCaptor<Runnable> taskCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(threadPool, times(1)).scheduleWithFixedDelay(taskCaptor.capture(), any(TimeValue.class), isNull());

        taskCaptor.getValue().run(); // run first time
        assertThat(callback.content, is(equalTo(hello)));
        assertThat(callback.count, is(1));

        taskCaptor.getValue().run(); // run second time, no file change, ignored
        assertThat(callback.count, is(1)); // file didn't change
        byte[] goodbye = "goodbye world".getBytes(StandardCharsets.UTF_8);
        Files.write(path, goodbye);
        Files.setLastModifiedTime(path, FileTime.from(Instant.now().plusSeconds(1))); // ensure mod time changes

        taskCaptor.getValue().run(); // run third time, change detected
        assertThat(callback.content, is(equalTo(goodbye)));
        assertThat(callback.count, is(2));

        loader.stop();

        Files.writeString(path, "too late");
        Files.setLastModifiedTime(path, FileTime.from(Instant.now().plusSeconds(1))); // ensure mod time changes

        taskCaptor.getValue().run(); // run fourth time, callback not invoked due to closure
        assertThat(callback.content, is(equalTo(goodbye)));
        assertThat(callback.count, is(2));
    }

    public void testUrlPkcJwkSetLoader() throws IOException {
        var url = "https://localhost/jwkset.json";
        var uri = URI.create(url);
        RealmConfig realmConfig = makeRealmConfigWithReloadUrl(url);
        ThreadPool threadPool = mock(ThreadPool.class);
        CloseableHttpAsyncClient httpClient = mock(CloseableHttpAsyncClient.class);

        CountingCallback callback = new CountingCallback();

        var loader = new JwkSetLoader.UrlPkcJwkSetLoader(realmConfig, threadPool, uri, httpClient, callback);
        loader.start(); // schedules task

        int iterations = randomIntBetween(5, 10);
        for (int i = 0; i < iterations; i++) {
            verifySchedulingIteration(callback, threadPool, httpClient, i + 1);
        }

        loader.stop();
        verify(httpClient, times(1)).close();
        verifySchedulingIteration(callback, threadPool, httpClient, iterations + 1); // last schedule call, no subsequent reschedule
        verify(threadPool, never()).schedule(any(Runnable.class), any(TimeValue.class), isNull());
    }

    public void testUrlPkcJwkSetLoaderListenerException() throws IOException {
        var url = "https://localhost/jwkset.json";
        var uri = URI.create(url);
        RealmConfig realmConfig = makeRealmConfigWithReloadUrl(url);
        ThreadPool threadPool = mock(ThreadPool.class);
        CloseableHttpAsyncClient httpClient = mock(CloseableHttpAsyncClient.class);

        Consumer<byte[]> callback = bytes -> { throw new RuntimeException(); };

        new JwkSetLoader.UrlPkcJwkSetLoader(realmConfig, threadPool, uri, httpClient, callback).start(); // schedules task

        int iterations = randomIntBetween(5, 10);
        for (int i = 0; i < iterations; i++) {
            verifySchedulingIterationWithListenerException(threadPool, httpClient);
        }
    }

    private static RealmConfig makeRealmConfigWithReloadUrl(String url) {
        RealmConfig realmConfig = mock(RealmConfig.class);
        when(realmConfig.getSetting(JwtRealmSettings.PKC_JWKSET_PATH)).thenReturn(url);
        when(realmConfig.getSetting(JwtRealmSettings.PKC_JWKSET_RELOAD_URL_INTERVAL_MIN)).thenReturn(TimeValue.timeValueMinutes(1));
        when(realmConfig.getSetting(JwtRealmSettings.PKC_JWKSET_RELOAD_URL_INTERVAL_MAX)).thenReturn(TimeValue.timeValueMinutes(60));
        return realmConfig;
    }

    private void verifySchedulingIteration(
        CountingCallback callback,
        ThreadPool threadPool,
        CloseableHttpAsyncClient httpClient,
        int iteration
    ) throws IOException {
        // capture scheduled task and delay
        ArgumentCaptor<Runnable> taskCaptor = ArgumentCaptor.forClass(Runnable.class);
        ArgumentCaptor<TimeValue> timeCaptor = ArgumentCaptor.forClass(TimeValue.class);
        verify(threadPool, times(1)).schedule(taskCaptor.capture(), timeCaptor.capture(), isNull());

        verifyScheduleTime(iteration == 1, timeCaptor);

        // run the scheduled task, which triggers HTTP call
        taskCaptor.getValue().run();
        @SuppressWarnings("unchecked")
        ArgumentCaptor<FutureCallback<HttpResponse>> responseFn = ArgumentCaptor.forClass(FutureCallback.class);
        verify(httpClient, times(1)).execute(any(HttpGet.class), responseFn.capture());
        byte[] bytes = "x".repeat(iteration).getBytes(StandardCharsets.UTF_8);
        HttpResponse response = makeHttpResponse(bytes, randomBoolean());

        reset(threadPool, httpClient);

        // invoke response handler
        responseFn.getValue().completed(response);
        assertThat(callback.content, is(equalTo(bytes)));
        assertThat(callback.count, is(iteration));
    }

    private void verifySchedulingIterationWithListenerException(ThreadPool threadPool, CloseableHttpAsyncClient httpClient)
        throws IOException {
        // capture scheduled task and delay
        ArgumentCaptor<Runnable> taskCaptor = ArgumentCaptor.forClass(Runnable.class);
        ArgumentCaptor<TimeValue> timeCaptor = ArgumentCaptor.forClass(TimeValue.class);
        verify(threadPool, times(1)).schedule(taskCaptor.capture(), timeCaptor.capture(), isNull());

        TimeValue delay = timeCaptor.getValue();
        assertThat(delay, is(TimeValue.timeValueMinutes(1))); // 1 minute always when exception occurs

        // run the scheduled task, which triggers HTTP call
        taskCaptor.getValue().run();
        @SuppressWarnings("unchecked")
        ArgumentCaptor<FutureCallback<HttpResponse>> responseFn = ArgumentCaptor.forClass(FutureCallback.class);
        verify(httpClient, times(1)).execute(any(HttpGet.class), responseFn.capture());
        HttpResponse response = makeHttpResponse(new byte[0], randomBoolean());

        reset(threadPool, httpClient);

        // invoke response handler
        responseFn.getValue().completed(response);
    }

    private static void verifyScheduleTime(boolean firstIteration, ArgumentCaptor<TimeValue> timeCaptor) {
        TimeValue delay = timeCaptor.getValue();
        if (firstIteration) {
            assertThat(delay, is(TimeValue.timeValueMinutes(1))); // first delay is always minimum (1 minute configured above)
        } else {
            TimeValue lower = TimeValue.timeValueMinutes(9);
            TimeValue upper = TimeValue.timeValueMinutes(11);
            assertThat(delay, both(greaterThanOrEqualTo(lower)).and(lessThanOrEqualTo(upper))); // 10 minutes +/-1 minute (jitter)
        }
    }

    private static HttpResponse makeHttpResponse(byte[] bytes, boolean expiresHeader) throws IOException {
        HttpEntity entity = mock(HttpEntity.class);
        StatusLine statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(200);
        when(entity.getContent()).thenReturn(new ByteArrayInputStream(bytes));
        HttpResponse response = mock(HttpResponse.class);
        when(response.getStatusLine()).thenReturn(statusLine);
        when(response.getEntity()).thenReturn(entity);
        Header eh = expiresHeader ? expiresHeader(10) : null;
        Header cc = expiresHeader ? null : cacheControlHeader(10);
        when(response.getFirstHeader(eq("Expires"))).thenReturn(eh);
        when(response.getFirstHeader(eq("Cache-Control"))).thenReturn(cc);
        return response;
    }

    private static Header expiresHeader(int minutes) {
        Header header = mock(Header.class);
        when(header.getValue()).thenReturn(expiresHeaderValue(minutes));
        return header;
    }

    private static Header cacheControlHeader(int minutes) {
        Header header = mock(Header.class);
        when(header.getValue()).thenReturn("max-age=" + (minutes * 60));
        return header;
    }

    private static String expiresHeaderValue(int plusMinutes) {
        ZonedDateTime nowUtc = ZonedDateTime.now(ZoneId.of("UTC"));
        ZonedDateTime zdt = nowUtc.plusMinutes(plusMinutes);
        return zdt.format(DateTimeFormatter.RFC_1123_DATE_TIME);
    }

    public void testUrlPkcJwkSetLoaderInvalidIntervals() {
        RealmConfig realmConfig = mock(RealmConfig.class);
        when(realmConfig.getSetting(JwtRealmSettings.PKC_JWKSET_RELOAD_URL_INTERVAL_MIN)).thenReturn(TimeValue.timeValueMinutes(10));
        when(realmConfig.getSetting(JwtRealmSettings.PKC_JWKSET_RELOAD_URL_INTERVAL_MAX)).thenReturn(TimeValue.timeValueMinutes(1));
        expectThrows(SettingsException.class, () -> new JwkSetLoader.UrlPkcJwkSetLoader(realmConfig, null, null, null, null));
    }

    static class CountingCallback implements Consumer<byte[]> {
        byte[] content;
        int count;

        @Override
        public void accept(byte[] bytes) {
            content = bytes;
            count++;
        }
    }
}
