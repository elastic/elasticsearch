/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.elasticsearch.ingest.geoip.GeoIpTestUtils.copyDatabase;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.matchesRegex;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Pins the structural invariants introduced when retire-and-delete was moved off the cluster state applier
 * thread:
 * <ul>
 *     <li>each {@link DatabaseReaderLazyLoader} owns a unique on-disk path
 *         ({@code <basename>.<6-hex-token>.mmdb}), so retiring one loader cannot delete another's file
 *         (eliminates the install-vs-retire race that going-async would have introduced and the lookup-vs-install
 *         race that preceded this change);</li>
 *     <li>every retire site routes through a single offload helper that enqueues
 *         {@link DatabaseReaderLazyLoader#shutdown(boolean) shutdown(true)} onto the generic pool — the applier
 *         thread never blocks on {@code Reader.close()} or {@code Files.delete(...)}.</li>
 * </ul>
 * Tests use a recording executor so we can drive the applier-side mutations and the queued I/O independently.
 *
 * <p>The class-level {@code @SuppressWarnings("resource")} silences IDE inspections that flag every
 * {@link #installDatabase} call as a leaked {@link AutoCloseable}: {@link DatabaseReaderLazyLoader} implements
 * {@link IpDatabase} which extends {@link AutoCloseable}, but {@code close()} on this type is a lookup-ref
 * release paired with {@code preLookup()} — it is <em>not</em> a "free this resource" operation. The free-this-
 * resource operation is {@code shutdown()}, which is invoked for every still-registered loader by
 * {@code databaseNodeService.shutdown()} in {@link #tearDown()}. Wrapping {@code installDatabase(...)} in a
 * try-with-resources would corrupt the in-flight-lookup counter and is actively wrong on this type.
 */
@LuceneTestCase.SuppressFileSystems(value = "ExtrasFS")
@SuppressWarnings("resource")
public class DatabaseLifecycleConcurrencyTests extends ESTestCase {

    private Path geoIpConfigDir;
    private Path geoIpTmpDir;
    private ProjectId projectId;
    private ProjectResolver projectResolver;
    private RecordingExecutor recordingExecutor;
    private DatabaseNodeService databaseNodeService;

    @Before
    public void setup() throws IOException {
        boolean multiProject = randomBoolean();
        projectId = multiProject ? randomProjectIdOrDefault() : ProjectId.DEFAULT;
        projectResolver = multiProject ? TestProjectResolvers.singleProject(projectId) : TestProjectResolvers.DEFAULT_PROJECT_ONLY;

        geoIpConfigDir = createTempDir().resolve("ingest-geoip");
        Files.createDirectories(geoIpConfigDir);
        geoIpTmpDir = createTempDir();

        recordingExecutor = new RecordingExecutor();
        databaseNodeService = newService(recordingExecutor);
    }

    @Override
    public void tearDown() throws Exception {
        if (databaseNodeService != null) {
            databaseNodeService.shutdown();
        }
        super.tearDown();
    }

    private DatabaseNodeService newService(Consumer<Runnable> executor) throws IOException {
        GeoIpCache cache = new GeoIpCache(1000);
        ConfigDatabases configDatabases = new ConfigDatabases(geoIpConfigDir, cache);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(
            ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(ProjectMetadata.builder(projectId).build()).build()
        );
        DatabaseNodeService service = new DatabaseNodeService(
            geoIpTmpDir,
            mock(Client.class),
            cache,
            configDatabases,
            executor,
            clusterService,
            projectResolver
        );
        service.initialize("nodeId", mock(ResourceWatcherService.class));
        return service;
    }

    /**
     * Pins the BWC contract of {@link DatabaseNodeService#stripInstallTokenInfix}: per-loader install paths
     * collapse back to {@code <basename>.mmdb}, while every other on-disk artifact (canonical names, side
     * files, intermediates, lookalikes) passes through unchanged. {@code _geoip/stats.files_in_temp} relies
     * on this exact set of cases — operators / monitoring tooling match against the logical names, so the
     * install-token infix must never leak out.
     */
    public void testStripInstallTokenInfix() {
        // per-loader paths -> collapsed to canonical
        assertEquals("GeoLite2-City.mmdb", DatabaseNodeService.stripInstallTokenInfix("GeoLite2-City.abcdef.mmdb"));
        assertEquals("GeoLite2-City.mmdb", DatabaseNodeService.stripInstallTokenInfix("GeoLite2-City.000000.mmdb"));
        assertEquals("MyCustomGeoLite2-City.mmdb", DatabaseNodeService.stripInstallTokenInfix("MyCustomGeoLite2-City.123abc.mmdb"));

        // canonical / non-mmdb / side files / intermediates -> unchanged
        assertEquals("GeoLite2-City.mmdb", DatabaseNodeService.stripInstallTokenInfix("GeoLite2-City.mmdb"));
        assertEquals("GeoLite2-City.mmdb_LICENSE.txt", DatabaseNodeService.stripInstallTokenInfix("GeoLite2-City.mmdb_LICENSE.txt"));
        assertEquals("GeoLite2-City.mmdb_COPYRIGHT.txt", DatabaseNodeService.stripInstallTokenInfix("GeoLite2-City.mmdb_COPYRIGHT.txt"));
        assertEquals("GeoLite2-City.mmdb.tmp", DatabaseNodeService.stripInstallTokenInfix("GeoLite2-City.mmdb.tmp"));
        assertEquals("GeoLite2-City.mmdb.tmp.retrieved", DatabaseNodeService.stripInstallTokenInfix("GeoLite2-City.mmdb.tmp.retrieved"));

        // wrong-length tokens / non-hex / uppercase hex -> unchanged (the install path emits exactly 6 lowercase hex chars)
        assertEquals("GeoLite2-City.abcde.mmdb", DatabaseNodeService.stripInstallTokenInfix("GeoLite2-City.abcde.mmdb"));
        assertEquals("GeoLite2-City.abcdef0.mmdb", DatabaseNodeService.stripInstallTokenInfix("GeoLite2-City.abcdef0.mmdb"));
        assertEquals("GeoLite2-City.ABCDEF.mmdb", DatabaseNodeService.stripInstallTokenInfix("GeoLite2-City.ABCDEF.mmdb"));
        assertEquals("GeoLite2-City.abcdeg.mmdb", DatabaseNodeService.stripInstallTokenInfix("GeoLite2-City.abcdeg.mmdb"));

        // anchored at the trailing .mmdb; an embedded ".[6-hex].mmdb" without trailing context does not strip
        assertEquals("GeoLite2-City.abcdef.mmdb.bak", DatabaseNodeService.stripInstallTokenInfix("GeoLite2-City.abcdef.mmdb.bak"));
    }

    /**
     * Each call to {@link DatabaseNodeService#updateDatabase} (via a real install through
     * {@link #installDatabase}) lands the file at a fresh path. Verifies the per-loader-path invariant that
     * makes async retire safe.
     */
    public void testPerLoaderPathsAreUnique() {
        DatabaseReaderLazyLoader l1 = installDatabase("GeoLite2-City.mmdb", "md5_v1");
        DatabaseReaderLazyLoader l2 = installDatabase("GeoLite2-City.mmdb", "md5_v2");
        assertNotEquals("the two installs must own distinct on-disk paths", l2.getDatabasePath(), l1.getDatabasePath());
        String n1 = l1.getDatabasePath().getFileName().toString();
        String n2 = l2.getDatabasePath().getFileName().toString();
        assertThat(n1, matchesRegex("^GeoLite2-City\\.[0-9a-f]{6}\\.mmdb$"));
        assertThat(n2, matchesRegex("^GeoLite2-City\\.[0-9a-f]{6}\\.mmdb$"));
    }

    /**
     * Reinstalling a database with the same content (same md5) still produces a distinct on-disk path. Pins
     * the deliberate decision in {@code DatabaseNodeService#computeLoaderPath} not to derive the install
     * token from md5: an md5-derived suffix would defeat the per-loader ownership invariant whenever a
     * project is dropped and re-added with the same content (consumer pruning + re-acquisition, brief role
     * flip, etc.).
     */
    public void testReinstallSameMd5_mintsDistinctPath() {
        DatabaseReaderLazyLoader l1 = installDatabase("GeoLite2-City.mmdb", "same-md5");
        DatabaseReaderLazyLoader l2 = installDatabase("GeoLite2-City.mmdb", "same-md5");
        assertNotEquals(l2.getDatabasePath(), l1.getDatabasePath());
    }

    /**
     * {@link DatabaseNodeService#updateDatabase} retires the previous loader by routing through
     * {@code shutdownAsync}: the applier-side put completes synchronously, and the {@code Reader.close() +
     * Files.delete(...)} for the retired loader is enqueued on the generic pool.
     */
    public void testUpdateDatabase_offloadsRetirementToExecutor() {
        DatabaseReaderLazyLoader l1 = installDatabase("GeoLite2-City.mmdb", "md5_v1");
        Path l1Path = l1.getDatabasePath();
        recordingExecutor.clear();

        DatabaseReaderLazyLoader l2 = installDatabase("GeoLite2-City.mmdb", "md5_v2");
        Path l2Path = l2.getDatabasePath();

        assertThat("retirement of the previous loader must be enqueued, not run inline", recordingExecutor.captured(), hasSize(1));
        assertTrue("freshly-installed loader's file must still be on disk before the queued retire runs", Files.exists(l2Path));
        assertTrue("previous loader's file is still there until its delete runs", Files.exists(l1Path));

        recordingExecutor.runAll();
        assertFalse("queued retire deletes only the previous loader's file", Files.exists(l1Path));
        assertTrue("queued retire must not clobber the freshly-installed file", Files.exists(l2Path));
    }

    /**
     * {@link DatabaseNodeService#checkDatabases} drops a project's loaders by routing through
     * {@code shutdownAsync}; the applier never touches the disk.
     */
    public void testDropProjectAndShutdown_offloadsToExecutor() {
        DatabaseReaderLazyLoader l1 = installDatabase("GeoLite2-City.mmdb", "md5_v1");
        DatabaseReaderLazyLoader l2 = installDatabase("GeoLite2-Country.mmdb", "md5_v1");
        Path l1Path = l1.getDatabasePath();
        Path l2Path = l2.getDatabasePath();
        recordingExecutor.clear();

        databaseNodeService.checkDatabases(stateWithoutRelevantConsumer());

        assertThat("both loaders' shutdowns must be enqueued by the applier", recordingExecutor.captured(), hasSize(2));
        assertTrue(Files.exists(l1Path));
        assertTrue(Files.exists(l2Path));

        recordingExecutor.runAll();
        assertFalse(Files.exists(l1Path));
        assertFalse(Files.exists(l2Path));
        assertNull(databaseNodeService.databases.get(projectId));
    }

    /**
     * Race A regression — content-change variant. With the previous canonical-path scheme, queueing the
     * retire while a fresh install is racing to land at the same path would let the queued
     * {@code Files.delete(...)} clobber the freshly-installed file. Per-loader paths make this impossible.
     */
    public void testRetireDoesNotClobberFreshInstall_contentChange() {
        DatabaseReaderLazyLoader l1 = installDatabase("GeoLite2-City.mmdb", "md5_abc");
        Path l1Path = l1.getDatabasePath();
        recordingExecutor.clear();

        DatabaseReaderLazyLoader l2 = installDatabase("GeoLite2-City.mmdb", "md5_def");
        Path l2Path = l2.getDatabasePath();

        recordingExecutor.runAll();

        assertFalse(Files.exists(l1Path));
        assertTrue("L2's file must survive L1's queued retire-delete", Files.exists(l2Path));
    }

    /**
     * Race A regression — same-md5 variant. The case an md5-keyed install path would <em>not</em> have
     * covered: project drop + re-add for identical content. Per-loader install tokens are independent of
     * content, so even back-to-back installs with the same md5 land at distinct paths.
     */
    public void testRetireDoesNotClobberFreshInstall_sameMd5_dropAndReinstall() {
        DatabaseReaderLazyLoader l1 = installDatabase("GeoLite2-City.mmdb", "md5_abc");
        Path l1Path = l1.getDatabasePath();
        recordingExecutor.clear();

        // applier-side drop: shutdownAsync(L1) is enqueued but not yet run
        databaseNodeService.checkDatabases(stateWithoutRelevantConsumer());
        assertThat(recordingExecutor.captured(), hasSize(1));

        // re-add the same database with the same content while the queued retire is still pending
        DatabaseReaderLazyLoader l3 = installDatabase("GeoLite2-City.mmdb", "md5_abc");
        Path l3Path = l3.getDatabasePath();
        assertNotEquals("re-install must not reuse the still-pending retire's path", l1Path, l3Path);
        assertTrue("freshly-installed file must already exist", Files.exists(l3Path));

        recordingExecutor.runAll();
        assertFalse(Files.exists(l1Path));
        assertTrue("L3's file must survive L1's queued retire-delete (same-md5 case)", Files.exists(l3Path));
    }

    /**
     * Race B regression — lookup vs. install. With a shared canonical path, an old loader whose
     * {@code Reader} hadn't yet been lazy-opened could open the new loader's content. Per-loader paths make
     * each loader read from its own file, period.
     */
    public void testLookupReadsFromOwnFile_notLatestInstall() throws Exception {
        DatabaseReaderLazyLoader l1 = installDatabase("GeoLite2-City.mmdb", "md5_v1");
        installDatabase("GeoLite2-City.mmdb", "md5_v2");

        // lazy-open l1's Reader: it must read its own (still-on-disk) file, not the latest install's
        assertEquals("GeoLite2-City", l1.getDatabaseType());
        assertEquals("md5_v1", l1.getMd5());
    }

    /**
     * Public {@link DatabaseNodeService#getFilesInTemp} must not leak the install-token infix: monitoring
     * tooling matches against the logical {@code <basename>.mmdb} names.
     */
    public void testGetFilesInTemp_stripsInstallTokenInfix() throws IOException {
        installDatabase("GeoLite2-City.mmdb", "md5_v1");
        installDatabase("GeoLite2-Country.mmdb", "md5_v1");

        Files.createFile(perProjectDir(projectId).resolve("GeoLite2-City.mmdb_LICENSE.txt"));

        Set<String> files = databaseNodeService.getFilesInTemp(projectId);
        assertThat(files, containsInAnyOrder("GeoLite2-City.mmdb", "GeoLite2-Country.mmdb", "GeoLite2-City.mmdb_LICENSE.txt"));
    }

    /**
     * Plugin shutdown is deliberately synchronous and inline (no enqueue): the generic pool may already be
     * draining at this point, and we want all readers closed before the JVM exits to avoid leaked mmap
     * segments. See {@link DatabaseNodeService#shutdown()}.
     */
    public void testPluginShutdownIsInlineAndDoesNotEnqueue() throws IOException {
        DatabaseReaderLazyLoader l1 = installDatabase("GeoLite2-City.mmdb", "md5_v1");
        DatabaseReaderLazyLoader l2 = installDatabase("GeoLite2-Country.mmdb", "md5_v1");
        recordingExecutor.clear();

        databaseNodeService.shutdown();

        assertThat("plugin shutdown must not route any work through the generic pool", recordingExecutor.captured(), empty());
        assertThat(databaseNodeService.databases.entrySet(), empty());
        // post-shutdown: files remain on disk (no-arg shutdown does not delete; next-startup sweep cleans them)
        assertTrue(Files.exists(l1.getDatabasePath()));
        assertTrue(Files.exists(l2.getDatabasePath()));
        databaseNodeService = null; // avoid double-shutdown in tearDown
    }

    /**
     * In-flight lookup defers the file delete until the last {@code close()} drives the usage counter to
     * the terminal value; shutdown enqueued by {@code shutdownAsync} runs immediately on the generic pool
     * but the final {@code doShutdown()} (which performs {@code Reader.close()} + {@code Files.delete(...)})
     * waits for the in-flight lookup to release its claim.
     */
    public void testInFlightLookupDefersDeleteUntilClose() throws IOException {
        DatabaseReaderLazyLoader l1 = installDatabase("GeoLite2-City.mmdb", "md5_v1");
        Path l1Path = l1.getDatabasePath();
        assertTrue(l1.preLookup());
        recordingExecutor.clear();

        DatabaseReaderLazyLoader l2 = installDatabase("GeoLite2-City.mmdb", "md5_v2");
        assertThat(recordingExecutor.captured(), hasSize(1));

        recordingExecutor.runAll();
        assertTrue(
            "in-flight lookup must defer the on-disk delete until close() drives the counter to the terminal value",
            Files.exists(l1Path)
        );

        l1.close();
        assertFalse("after close(), the deferred doShutdown() must delete the retired loader's file", Files.exists(l1Path));
        assertTrue(Files.exists(l2.getDatabasePath()));
    }

    /**
     * Multi-project isolation: per-project subdirectories keep loaders for different projects on disjoint
     * paths even when they carry the same database name and identical content. Combined with the per-loader
     * install token, this makes it structurally impossible for one project's retire to touch another
     * project's file.
     */
    public void testMultiProjectIsolation() {
        assumeTrue(
            "multi-project subdirs are only present when the project resolver supports multiple projects",
            projectResolver.supportsMultipleProjects()
        );

        ProjectId other = randomValueOtherThan(projectId, ESTestCase::randomProjectIdOrDefault);
        DatabaseReaderLazyLoader l1 = installDatabase(projectId, "GeoLite2-City.mmdb", "md5_same");
        DatabaseReaderLazyLoader l2 = installDatabase(other, "GeoLite2-City.mmdb", "md5_same");

        assertNotEquals(l2.getDatabasePath(), l1.getDatabasePath());
        assertNotEquals("project subdirectories must be siblings", l2.getDatabasePath().getParent(), l1.getDatabasePath().getParent());
        assertTrue(Files.exists(l1.getDatabasePath()));
        assertTrue(Files.exists(l2.getDatabasePath()));
    }

    private DatabaseReaderLazyLoader installDatabase(String databaseName, String md5) {
        return installDatabase(projectId, databaseName, md5);
    }

    /**
     * Mimics the production install path: mints a fresh per-loader path through
     * {@link DatabaseNodeService#computeLoaderPath} (the same call site
     * {@link DatabaseNodeService#retrieveAndUpdateDatabase} uses), copies a real GeoLite2 fixture there, and
     * funnels through {@link DatabaseNodeService#updateDatabase}. Reusing the production path-minting helper
     * means any future change to the on-disk shape (basename strip, install-token format, per-project subdir
     * resolution) automatically applies here, so test and production cannot drift.
     */
    private DatabaseReaderLazyLoader installDatabase(ProjectId pid, String databaseName, String md5) {
        Path file = databaseNodeService.computeLoaderPath(pid, databaseName);
        copyDatabase("GeoLite2-City-Test.mmdb", file);
        databaseNodeService.updateDatabase(pid, databaseName, md5, file);
        DatabaseReaderLazyLoader loader = databaseNodeService.get(pid, databaseName);
        assertNotNull("loader for [" + databaseName + "] must have been registered", loader);
        assertEquals(file, loader.getDatabasePath());
        return loader;
    }

    private Path perProjectDir(ProjectId pid) {
        Path nodeDir = geoIpTmpDir.resolve("geoip-databases").resolve("nodeId");
        return projectResolver.supportsMultipleProjects() ? nodeDir.resolve(pid.toString()) : nodeDir;
    }

    private ClusterState stateWithoutRelevantConsumer() {
        return stateWithoutRelevantConsumerFor(projectId);
    }

    private ClusterState stateWithoutRelevantConsumerFor(ProjectId pid) {
        // Empty consumer custom -> hasRelevantConsumer() is false -> dropProjectAndShutdown path
        IpLocationDownloadConsumers empty = IpLocationDownloadConsumers.EMPTY;
        PersistentTasksCustomMetadata noTasks = DatabaseNodeServiceTests.geoIpDownloaderTask(
            pid,
            Map.of(),
            projectResolver.supportsMultipleProjects()
        );
        return DatabaseNodeServiceTests.createClusterState(pid, noTasks, empty);
    }

    /**
     * Captures {@code Runnable}s instead of running them. Tests explicitly call {@link #runAll()} to advance the
     * queued I/O. {@link DatabaseNodeService} accepts a {@code Consumer<Runnable>} executor; this implements
     * that interface.
     */
    @SuppressWarnings("NewClassNamingConvention")
    private static final class RecordingExecutor implements Consumer<Runnable> {
        private final Deque<Runnable> captured = new ArrayDeque<>();

        @Override
        public void accept(Runnable runnable) {
            captured.addLast(runnable);
        }

        List<Runnable> captured() {
            return new ArrayList<>(captured);
        }

        void clear() {
            captured.clear();
        }

        void runAll() {
            while (captured.isEmpty() == false) {
                captured.removeFirst().run();
            }
        }
    }
}
