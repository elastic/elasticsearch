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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
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
        final WorkloadIdentitySslConfig sslConfig = newSslConfig(caFile);
        final AtomicInteger reloadCount = new AtomicInteger();
        sslConfig.addReloadListener(reloadCount::incrementAndGet);

        // Sanity-check: no reload has been triggered yet.
        assertThat(reloadCount.get(), equalTo(0));

        // Touch the file with the same content (idempotent rewrite); the FileWatcher compares
        // size + lastModifiedTime + (mtime tiebreaker) bytes, so to make the change observable we
        // append a benign blank line. The PEM parser tolerates trailing whitespace.
        Files.writeString(caFile, Files.readString(caFile) + "\n", StandardCharsets.US_ASCII);

        resourceWatcher.notifyNow(ResourceWatcherService.Frequency.HIGH);

        assertThat("listener must fire after a watched file changes", reloadCount.get(), greaterThan(0));
        assertThat(sslConfig.getStrategy(), notNullValue());
    }

    /**
     * If reload fails to parse the new file content, the previously-published context must remain
     * usable, the listener must NOT fire, and the failure must surface as a WARN log naming the
     * cause rather than as an exception on the watcher thread. The watcher will fire again on
     * the next change, so the operator can recover by overwriting with valid material.
     */
    public void testReloadKeepsPreviousContextOnFailure() throws Exception {
        final WorkloadIdentitySslConfig sslConfig = newSslConfig(caFile);
        final AtomicInteger reloadCount = new AtomicInteger();
        sslConfig.addReloadListener(reloadCount::incrementAndGet);

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

        assertThat("listener must NOT fire when reload fails", reloadCount.get(), equalTo(0));
        // The previously-loaded context is still serving; getStrategy() must still succeed.
        assertThat(sslConfig.getStrategy(), notNullValue());
    }

    /**
     * A listener that throws must not prevent other listeners from observing the swap, must not
     * propagate the exception out to the resource watcher thread, and must surface as a WARN log
     * naming the listener failure so the operator has a trail to follow.
     */
    public void testListenerExceptionIsIsolated() throws Exception {
        final WorkloadIdentitySslConfig sslConfig = newSslConfig(caFile);
        final AtomicInteger secondCount = new AtomicInteger();
        sslConfig.addReloadListener(() -> { throw new RuntimeException("listener boom"); });
        sslConfig.addReloadListener(secondCount::incrementAndGet);

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

        assertThat("subsequent listeners must still run after a peer throws", secondCount.get(), greaterThan(0));
    }

    private WorkloadIdentitySslConfig newSslConfig(Path caCertPath) {
        final Settings settings = settingsWithCaFile(caCertPath);
        final Environment environment = TestEnvironment.newEnvironment(settings);
        return new WorkloadIdentitySslConfig(settings, environment, resourceWatcher);
    }

    private static Settings settingsWithCaFile(Path caCertPath) {
        return Settings.builder()
            .put("path.home", caCertPath.getParent())
            .putList(WorkloadIdentitySslConfig.SETTING_PREFIX + "certificate_authorities", caCertPath.toString())
            .build();
    }
}
