/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.workloadidentity;

import org.apache.logging.log4j.Level;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.After;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class WorkloadIdentitySslConfigTests extends ESTestCase {

    private ThreadPool threadPool;
    private ResourceWatcherService resourceWatcher;
    private Path workDir;
    private Path caFile;

    @Before
    public void setupCollaborators() throws Exception {
        this.workDir = createTempDir();
        // Stage a writable copy of the reusable test CA under the temp work dir so the watcher
        // sees real on-disk mutations rather than read-only classpath resources.
        this.caFile = workDir.resolve("ca.crt");
        Files.copy(getDataPath("ca.crt"), caFile, StandardCopyOption.REPLACE_EXISTING);
        this.threadPool = new TestThreadPool(getTestName());
        // ENABLED=false suppresses the background scheduler; tests call notifyNow(HIGH) to
        // simulate a polling tick at deterministic points.
        this.resourceWatcher = new ResourceWatcherService(
            Settings.builder().put(ResourceWatcherService.ENABLED.getKey(), false).build(),
            threadPool
        );
    }

    @After
    public void shutdown() {
        resourceWatcher.close();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    /**
     * On a successful change to a watched file, the reload listener must fire exactly once per
     * watcher tick, and a subsequent {@link WorkloadIdentitySslConfig#getStrategy()} call must
     * still return a usable strategy (over the freshly-loaded context).
     */
    public void testReloadFiresListenersWhenFileChanges() throws Exception {
        final AtomicInteger reloadCount = new AtomicInteger();
        final WorkloadIdentitySslConfig sslConfig = newSslConfig(caFile, reloadCount::incrementAndGet);
        // The initial publish during start() fired the listener exactly once.
        assertThat(reloadCount.get(), equalTo(1));

        // Touch the file with the same content (idempotent rewrite); the FileWatcher compares
        // size + lastModifiedTime + (mtime tiebreaker) bytes, so to make the change observable we
        // append a benign blank line. The PEM parser tolerates trailing whitespace.
        Files.writeString(caFile, Files.readString(caFile) + "\n", StandardCharsets.US_ASCII);

        resourceWatcher.notifyNow(ResourceWatcherService.Frequency.HIGH);

        // One more invocation for the single watcher-driven reload: initial publish + one tick = 2.
        assertThat("listener must fire once more after a watched file changes", reloadCount.get(), equalTo(2));
        assertThat(sslConfig.getStrategy(), notNullValue());
    }

    /**
     * If reload fails to parse the new file content, the previously-published context must remain
     * usable, the listener must NOT fire, and the failure must surface as a WARN log naming the
     * cause rather than as an exception on the watcher thread. The watcher will fire again on
     * the next change, so the operator can recover by overwriting with valid material.
     */
    public void testReloadKeepsPreviousContextOnFailure() throws Exception {
        final AtomicInteger reloadCount = new AtomicInteger();
        final WorkloadIdentitySslConfig sslConfig = newSslConfig(caFile, reloadCount::incrementAndGet);
        // The initial publish during start() fired the listener exactly once.
        assertThat(reloadCount.get(), equalTo(1));

        // Sanity: the original strategy is non-null before any reload attempt.
        assertThat(sslConfig.getStrategy(), notNullValue());

        // Replace the CA with garbage; the watcher tick below then drives the failed reload,
        // which must catch the parse failure, log WARN, and return without propagating.
        Files.writeString(caFile, "this is not a PEM certificate\n", StandardCharsets.US_ASCII);
        MockLog.assertThatLogger(
            () -> resourceWatcher.notifyNow(ResourceWatcherService.Frequency.HIGH),
            WorkloadIdentitySslConfig.class,
            new MockLog.SeenEventExpectation(
                "failed reload warns and retains previous context",
                WorkloadIdentitySslConfig.class.getCanonicalName(),
                Level.WARN,
                "*failed to reload workload-identity SSL context*"
            )
        );

        // Still 1: the failed reload must not fire the listener, leaving only the initial publish.
        assertThat("failed reload must not fire the listener", reloadCount.get(), equalTo(1));
        // The previously-loaded context is still serving; getStrategy() must still succeed.
        assertThat(sslConfig.getStrategy(), notNullValue());
    }

    /**
     * A listener that throws must not prevent other listeners from observing the swap, must not
     * propagate the exception out to the resource watcher thread, and must surface as a WARN log
     * naming the listener failure so the operator has a trail to follow.
     */
    public void testListenerExceptionIsIsolated() throws Exception {
        final AtomicInteger secondCount = new AtomicInteger();
        // The throwing listener also fires (and is isolated) during the initial publish, which the
        // peer counter observes once.
        final WorkloadIdentitySslConfig sslConfig = newSslConfig(
            caFile,
            () -> { throw new RuntimeException("listener boom"); },
            secondCount::incrementAndGet
        );
        assertThat(secondCount.get(), equalTo(1));

        Files.writeString(caFile, Files.readString(caFile) + "\n", StandardCharsets.US_ASCII);
        // notifyNow must not propagate the listener's RuntimeException to the test thread, and
        // the failure must be logged so an operator can correlate the symptom with the cause.
        MockLog.assertThatLogger(
            () -> resourceWatcher.notifyNow(ResourceWatcherService.Frequency.HIGH),
            WorkloadIdentitySslConfig.class,
            new MockLog.SeenEventExpectation(
                "throwing listener is logged and isolated",
                WorkloadIdentitySslConfig.class.getCanonicalName(),
                Level.WARN,
                "*workload-identity SSL reload listener threw*"
            )
        );

        // Ran again for the watcher-driven reload despite the peer throwing: initial + one tick = 2.
        assertThat("subsequent listeners must still run after a peer throws", secondCount.get(), equalTo(2));
    }

    /**
     * A listener registered before {@link WorkloadIdentitySslConfig#start()} must observe the
     * initial publish exactly once. That edge is what populates the
     * {@link WorkloadIdentityHttpClientManager}'s SSL strategy in production.
     */
    public void testListenerRegisteredBeforeStartObservesInitialPublish() {
        final Settings settings = settingsWithCaFile(caFile);
        final Environment environment = TestEnvironment.newEnvironment(settings);
        final WorkloadIdentitySslConfig sslConfig = new WorkloadIdentitySslConfig(settings, environment, resourceWatcher);
        final AtomicInteger publishCount = new AtomicInteger();
        sslConfig.addReloadListener(publishCount::incrementAndGet);

        sslConfig.start();

        assertThat("listener registered before start() must observe the initial publish", publishCount.get(), equalTo(1));
        assertThat(sslConfig.getStrategy(), notNullValue());
    }

    /**
     * Reload listeners are fixed before {@link WorkloadIdentitySslConfig#start()}: registering one
     * afterwards is a programming error (the listener would silently miss the initial publish), so
     * it must throw rather than be quietly accepted.
     */
    public void testAddReloadListenerAfterStartThrows() {
        final Settings settings = settingsWithCaFile(caFile);
        final Environment environment = TestEnvironment.newEnvironment(settings);
        final WorkloadIdentitySslConfig sslConfig = new WorkloadIdentitySslConfig(settings, environment, resourceWatcher);

        sslConfig.start();

        final IllegalStateException ex = expectThrows(IllegalStateException.class, () -> sslConfig.addReloadListener(() -> {}));
        assertThat(ex.getMessage(), containsString("cannot add a reload listener"));
    }

    /**
     * {@link WorkloadIdentitySslConfig#start()} is not idempotent: a second call surfaces the
     * programming error rather than silently re-registering watchers or re-publishing.
     */
    public void testStartTwiceThrows() {
        final Settings settings = settingsWithCaFile(caFile);
        final Environment environment = TestEnvironment.newEnvironment(settings);
        final WorkloadIdentitySslConfig sslConfig = new WorkloadIdentitySslConfig(settings, environment, resourceWatcher);

        sslConfig.start();

        final IllegalStateException ex = expectThrows(IllegalStateException.class, sslConfig::start);
        assertThat(ex.getMessage(), containsString("cannot start workload-identity SSL config in state [STARTED]"));
        assertThat(sslConfig.getStrategy(), notNullValue());
    }

    /**
     * {@link WorkloadIdentitySslConfig#getStrategy()} must fail loudly if called before
     * {@link WorkloadIdentitySslConfig#start()} rather than dereferencing a null context.
     */
    public void testGetStrategyBeforeStartThrows() {
        final Settings settings = settingsWithCaFile(caFile);
        final Environment environment = TestEnvironment.newEnvironment(settings);
        final WorkloadIdentitySslConfig sslConfig = new WorkloadIdentitySslConfig(settings, environment, resourceWatcher);
        final IllegalStateException ex = expectThrows(IllegalStateException.class, sslConfig::getStrategy);
        assertThat(ex.getMessage(), containsString("not been started"));
    }

    /**
     * {@link WorkloadIdentitySslConfig#close()} must unregister the file watchers: a subsequent
     * file change and watcher tick must no longer drive a reload. This is the leak/teardown
     * guarantee that lets a caller create and discard instances without watchers accumulating.
     */
    public void testCloseStopsWatchers() throws Exception {
        final AtomicInteger reloadCount = new AtomicInteger();
        final WorkloadIdentitySslConfig sslConfig = newSslConfig(caFile, reloadCount::incrementAndGet);
        // The initial publish during start() fired the listener exactly once.
        assertThat(reloadCount.get(), equalTo(1));

        sslConfig.close();

        // The watcher is unregistered, so a real file change plus a tick must not reach the listener.
        Files.writeString(caFile, Files.readString(caFile) + "\n", StandardCharsets.US_ASCII);
        resourceWatcher.notifyNow(ResourceWatcherService.Frequency.HIGH);

        // Still 1: no watcher-driven reload occurred after close().
        assertThat("listener must not fire after close() unregisters the watchers", reloadCount.get(), equalTo(1));
    }

    /**
     * A reload invoked after {@link WorkloadIdentitySslConfig#close()} must not republish or
     * notify listeners, matching {@link WorkloadIdentityHttpClientManager#reload()}.
     */
    public void testReloadAfterCloseIsNoOp() {
        final AtomicInteger reloadCount = new AtomicInteger();
        final WorkloadIdentitySslConfig sslConfig = newSslConfig(caFile, reloadCount::incrementAndGet);
        assertThat(reloadCount.get(), equalTo(1));

        sslConfig.close();
        sslConfig.reloadNow();

        assertThat("reload after close() must not notify listeners", reloadCount.get(), equalTo(1));
    }

    /**
     * {@link WorkloadIdentitySslConfig#close()} is idempotent: a repeat call is a no-op rather
     * than throwing or double-stopping watchers.
     */
    public void testCloseIsIdempotent() {
        final WorkloadIdentitySslConfig sslConfig = newSslConfig(caFile);

        sslConfig.close();
        sslConfig.close();
    }

    /**
     * Mirroring {@link WorkloadIdentityHttpClientManager}, only the {@code STARTED → CLOSED}
     * transition does work: {@code close()} on a never-started instance is a no-op that leaves it
     * in {@code INIT}, so it can still be started afterwards (and then closed for real).
     */
    public void testCloseBeforeStartIsNoOp() {
        final Settings settings = settingsWithCaFile(caFile);
        final Environment environment = TestEnvironment.newEnvironment(settings);
        final WorkloadIdentitySslConfig sslConfig = new WorkloadIdentitySslConfig(settings, environment, resourceWatcher);

        sslConfig.close();

        // close() from INIT did not transition to CLOSED, so a subsequent start() still succeeds.
        sslConfig.start();
        assertThat(sslConfig.getStrategy(), notNullValue());
    }

    /**
     * {@code start()} from {@code CLOSED} must throw rather than silently flip the state or try
     * to re-register watchers.
     */
    public void testStartAfterCloseThrows() {
        final WorkloadIdentitySslConfig sslConfig = newSslConfig(caFile);

        sslConfig.close();
        final IllegalStateException ex = expectThrows(IllegalStateException.class, sslConfig::start);
        assertThat(ex.getMessage(), containsString("[CLOSED]"));
    }

    /**
     * {@code close()} only stops future reloads; a strategy can still be obtained over the
     * last-published context so any in-flight TLS work is unaffected.
     */
    public void testGetStrategyUsableAfterClose() {
        final WorkloadIdentitySslConfig sslConfig = newSslConfig(caFile);

        sslConfig.close();

        assertThat(sslConfig.getStrategy(), notNullValue());
    }

    private WorkloadIdentitySslConfig newSslConfig(Path caCertPath, Runnable... reloadListeners) {
        final Settings settings = settingsWithCaFile(caCertPath);
        final Environment environment = TestEnvironment.newEnvironment(settings);
        final WorkloadIdentitySslConfig config = new WorkloadIdentitySslConfig(settings, environment, resourceWatcher);
        // Reload listeners are fixed before start(); register them here so the initial publish
        // during start() reaches them. That initial publish fires each listener once, so callers
        // that count only subsequent reloads reset their counters after this returns.
        for (Runnable listener : reloadListeners) {
            config.addReloadListener(listener);
        }
        config.start();
        return config;
    }

    private static Settings settingsWithCaFile(Path caCertPath) {
        return Settings.builder()
            .put("path.home", caCertPath.getParent())
            .putList(WorkloadIdentitySslConfig.SETTING_PREFIX + "certificate_authorities", caCertPath.toString())
            .build();
    }
}
