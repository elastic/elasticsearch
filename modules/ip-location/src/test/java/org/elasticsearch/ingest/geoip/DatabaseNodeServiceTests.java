/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.maxmind.db.InvalidDatabaseException;

import org.apache.lucene.search.TotalHits;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ProjectClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.iplocation.api.IpLocationConsumer;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.elasticsearch.ingest.geoip.GeoIpDownloaderTaskExecutor.getTaskId;
import static org.elasticsearch.ingest.geoip.GeoIpTestUtils.DEFAULT_DATABASES;
import static org.elasticsearch.ingest.geoip.GeoIpTestUtils.copyDefaultDatabases;
import static org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import static org.elasticsearch.persistent.PersistentTasksCustomMetadata.TYPE;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@LuceneTestCase.SuppressFileSystems(value = "ExtrasFS") // Don't randomly add 'extra' files to directory.
public class DatabaseNodeServiceTests extends ESTestCase {

    /**
     * Each test runs twice: once as a single-project setup (legacy behaviour) and once as a multi-project setup.
     */
    @ParametersFactory(shuffle = false)
    public static Iterable<Object[]> parameters() {
        return List.of(new Object[] { false }, new Object[] { true });
    }

    /**
     * Default number of chunks each test database is split into. Tests that use this constant configure
     * {@link GeoIpTaskState.Metadata} with {@code firstChunk}/{@code lastChunk} such that
     * {@code lastChunk - firstChunk + 1 == CHUNKS_PER_DATABASE}, and {@link #mockSearches} is invoked
     * with the same range. One {@code search} call is issued per chunk, so this is also the expected
     * number of {@code search} invocations when a database is fully fetched.
     */
    private static final int CHUNKS_PER_DATABASE = 10;

    private static final Settings WATCHER_SETTINGS = Settings.builder()
        .put("resource.reload.interval.high", TimeValue.timeValueMillis(100))
        .build();

    /**
     * Shared across all tests and both parameter values: constructing a {@link TestThreadPool} is the dominant
     * cost of this suite's fixture. {@link ResourceWatcherService#close()} only cancels its own scheduled
     * futures, so each test still gets a fresh watcher service on top of this shared pool.
     */
    private static ThreadPool threadPool;

    /**
     * Class-scoped read-only directory pre-populated once with the default GeoLite2 mmdbs. None of the
     * tests in this suite mutate this directory: {@code geoIpConfigDir} is a strictly local variable
     * inside {@link #setup()} consumed only by {@link ConfigDatabases}, and per-test mutations target
     * {@link #geoIpTmpDir} (the {@link DatabaseNodeService} working dir) instead.
     */
    private static Path sharedGeoIpConfigDir;

    @BeforeClass
    public static void setupThreadPool() {
        threadPool = new TestThreadPool(DatabaseNodeServiceTests.class.getSimpleName());
    }

    @BeforeClass
    public static void setupSharedGeoIpConfigDir() throws IOException {
        sharedGeoIpConfigDir = createTempDir().resolve("ingest-geoip");
        Files.createDirectories(sharedGeoIpConfigDir);
        copyDefaultDatabases(sharedGeoIpConfigDir);
    }

    @AfterClass
    public static void shutdownThreadPool() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    private final boolean multiProject;

    private Client client;
    private ProjectClient projectClient;
    private Path geoIpTmpDir;
    private DatabaseNodeService databaseNodeService;
    private ResourceWatcherService resourceWatcherService;
    private ClusterService clusterService;
    private ProjectId projectId;
    private ProjectResolver projectResolver;
    // Backs the eager-download supplier handed to DatabaseNodeService; toggled per-test. Defaults to false so
    // existing tests keep exercising the consumer-gated retrieval path.
    private volatile boolean eagerDownload = false;

    private final Collection<Releasable> toRelease = new CopyOnWriteArrayList<>();

    public DatabaseNodeServiceTests(@Name("multiProject") boolean multiProject) {
        this.multiProject = multiProject;
    }

    @Before
    public void setup() throws IOException {
        projectId = multiProject ? randomProjectIdOrDefault() : ProjectId.DEFAULT;
        projectResolver = multiProject ? TestProjectResolvers.singleProject(projectId) : TestProjectResolvers.DEFAULT_PROJECT_ONLY;
        GeoIpCache cache = new GeoIpCache(1000);
        ConfigDatabases configDatabases = new ConfigDatabases(sharedGeoIpConfigDir, cache);
        for (String name : DEFAULT_DATABASES) {
            configDatabases.updateDatabase(sharedGeoIpConfigDir.resolve(name), true);
        }

        resourceWatcherService = new ResourceWatcherService(WATCHER_SETTINGS, threadPool);

        projectClient = mock(ProjectClient.class);
        client = mock(Client.class);
        when(client.projectClient(any())).thenReturn(projectClient);
        clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(
            ClusterState.builder(new ClusterName("test")).putProjectMetadata(ProjectMetadata.builder(projectId).build()).build()
        );
        geoIpTmpDir = createTempDir();
        databaseNodeService = new DatabaseNodeService(
            geoIpTmpDir,
            client,
            cache,
            configDatabases,
            Runnable::run,
            clusterService,
            projectResolver,
            () -> eagerDownload
        );
        databaseNodeService.initialize("nodeId", resourceWatcherService);
    }

    @After
    public void cleanup() {
        resourceWatcherService.close();
        Releasables.close(toRelease);
        toRelease.clear();
        verify(client, Mockito.atLeast(0)).projectClient(any());
        verifyNoMoreInteractions(client);
    }

    public void testCheckDatabases() throws Exception {
        String md5 = mockSearches("GeoIP2-City.mmdb", 5, 14);
        PersistentTasksCustomMetadata tasksCustomMetadata = geoIpDownloaderTask(
            projectId,
            "GeoIP2-City.mmdb",
            new GeoIpTaskState.Metadata(10, 5, 14, md5, 10)
        );

        IpLocationDownloadConsumers consumers = IpLocationDownloadConsumers.EMPTY.withConsumer(IpLocationConsumer.INGEST);
        ClusterState state = createClusterState(projectId, tasksCustomMetadata, consumers);

        List<String> notifiedDatabases = new ArrayList<>();
        databaseNodeService.addDatabaseAvailabilityListener((pid, dbFile) -> notifiedDatabases.add(dbFile));

        assertThat(databaseNodeService.getDatabase(projectId, "GeoIP2-City.mmdb"), nullValue());
        // Nothing should be downloaded, since the database is no longer valid (older than 30 days)
        databaseNodeService.checkDatabases(state);
        DatabaseReaderLazyLoader database = databaseNodeService.getDatabaseReaderLazyLoader(projectId, "GeoIP2-City.mmdb");
        assertThat(database, nullValue());
        verify(projectClient, times(0)).search(any());
        assertThat(notifiedDatabases, empty());
        try (Stream<Path> files = Files.list(geoIpTmpDir.resolve("geoip-databases").resolve("nodeId"))) {
            assertEquals(0, files.count());
        }

        tasksCustomMetadata = geoIpDownloaderTask(
            projectId,
            "GeoIP2-City.mmdb",
            new GeoIpTaskState.Metadata(10, 5, 14, md5, System.currentTimeMillis())
        );

        state = createClusterState(projectId, tasksCustomMetadata, consumers);

        // Database should be downloaded
        databaseNodeService.checkDatabases(state);
        database = databaseNodeService.getDatabaseReaderLazyLoader(projectId, "GeoIP2-City.mmdb");
        assertThat(database, notNullValue());
        verify(projectClient, times(CHUNKS_PER_DATABASE)).search(any());
        try (Stream<Path> files = Files.list(geoIpTmpDir.resolve("geoip-databases").resolve("nodeId"))) {
            assertThat(files.count(), greaterThanOrEqualTo(1L));
        }
        // First time GeoIP2-City.mmdb is downloaded, so listener should be notified:
        assertThat(notifiedDatabases, equalTo(List.of("GeoIP2-City.mmdb")));
        // 30 days check passed but we mocked mmdb data so parsing will fail
        expectThrows(InvalidDatabaseException.class, database::get);
    }

    public void testCheckDatabases_dontCheckDatabaseWhenConsumersCustomAbsent() throws Exception {
        String md5 = mockSearches("GeoIP2-City.mmdb", 0, 9);
        PersistentTasksCustomMetadata tasksCustomMetadata = geoIpDownloaderTask(
            projectId,
            "GeoIP2-City.mmdb",
            new GeoIpTaskState.Metadata(0L, 0, 9, md5, 10)
        );

        ClusterState state = createClusterState(projectId, tasksCustomMetadata);

        // No requestDownloads() called, so checkDatabases should skip downloading
        databaseNodeService.checkDatabases(state);
        assertThat(databaseNodeService.getDatabase(projectId, "GeoIP2-City.mmdb"), nullValue());
        verify(projectClient, never()).search(any());
        try (Stream<Path> files = Files.list(geoIpTmpDir.resolve("geoip-databases").resolve("nodeId"))) {
            assertThat(files.toList(), empty());
        }
    }

    /**
     * With {@code ingest.geoip.downloader.eager.download} enabled, an ingest-capable node must retrieve and load
     * downloaded databases locally even when no {@link IpLocationConsumer} is registered (i.e. before any geoip
     * pipeline exists).
     */
    public void testCheckDatabases_eagerDownloadRetrievesOnIngestNodeWithoutConsumer() throws Exception {
        String databaseName = "GeoIP2-City.mmdb";
        String md5 = mockSearches(databaseName, 5, 14);
        PersistentTasksCustomMetadata tasksCustomMetadata = geoIpDownloaderTask(
            projectId,
            databaseName,
            new GeoIpTaskState.Metadata(10, 5, 14, md5, System.currentTimeMillis())
        );

        // No IpLocationDownloadConsumers custom at all: without eager downloading nothing would be retrieved.
        ClusterState state = createClusterState(projectId, tasksCustomMetadata);

        // Precondition: eager off + no consumer -> the node does not retrieve the database.
        databaseNodeService.checkDatabases(state);
        assertThat(databaseNodeService.getDatabaseReaderLazyLoader(projectId, databaseName), nullValue());
        verify(projectClient, never()).search(any());

        // Eager download makes the ingest-capable node retrieve and load the database even without a registered consumer.
        eagerDownload = true;
        databaseNodeService.checkDatabases(state);
        assertThat(databaseNodeService.getDatabaseReaderLazyLoader(projectId, databaseName), notNullValue());
        verify(projectClient, times(CHUNKS_PER_DATABASE)).search(any());
    }

    /**
     * Eager downloading only warms ingest-capable nodes. A master-only node must not retrieve databases even
     * with {@code eager.download} enabled, since it never runs ingest pipelines.
     */
    public void testCheckDatabases_eagerDownloadDoesNotRetrieveOnMasterOnlyNode() throws Exception {
        String databaseName = "GeoIP2-City.mmdb";
        String md5 = mockSearches(databaseName, 5, 14);
        PersistentTasksCustomMetadata tasksCustomMetadata = geoIpDownloaderTask(
            projectId,
            databaseName,
            new GeoIpTaskState.Metadata(10, 5, 14, md5, System.currentTimeMillis())
        );

        DiscoveryNodes masterOnlyNodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.builder("_id1").roles(Set.of(DiscoveryNodeRole.MASTER_ROLE)).build())
            .localNodeId("_id1")
            .build();
        ClusterState state = createClusterState(projectId, tasksCustomMetadata, masterOnlyNodes, null);

        eagerDownload = true;
        databaseNodeService.checkDatabases(state);
        assertThat(databaseNodeService.getDatabaseReaderLazyLoader(projectId, databaseName), nullValue());
        verify(projectClient, never()).search(any());
    }

    public void testCheckDatabases_dontCheckDatabaseWhenNoDatabasesIndex() throws Exception {
        String md5 = mockSearches("GeoIP2-City.mmdb", 0, 9);
        PersistentTasksCustomMetadata tasksCustomMetadata = geoIpDownloaderTask(
            projectId,
            "GeoIP2-City.mmdb",
            new GeoIpTaskState.Metadata(0L, 0, 9, md5, 10)
        );

        IpLocationDownloadConsumers consumers = IpLocationDownloadConsumers.EMPTY.withConsumer(IpLocationConsumer.INGEST);
        ClusterState state = ClusterState.builder(new ClusterName("name"))
            .putProjectMetadata(
                ProjectMetadata.builder(projectId)
                    .putCustom(TYPE, tasksCustomMetadata)
                    .putCustom(IpLocationDownloadConsumers.TYPE, consumers)
            )
            .nodes(new DiscoveryNodes.Builder().add(DiscoveryNodeUtils.create("_id1")).localNodeId("_id1"))
            .build();

        databaseNodeService.checkDatabases(state);
        assertThat(databaseNodeService.getDatabase(projectId, "GeoIP2-City.mmdb"), nullValue());
        verify(projectClient, never()).search(any());
        try (Stream<Path> files = Files.list(geoIpTmpDir.resolve("geoip-databases").resolve("nodeId"))) {
            assertThat(files.toList(), empty());
        }
    }

    public void testCheckDatabases_dontCheckDatabaseWhenNoGeoIpDownloadTask() throws Exception {
        PersistentTasksCustomMetadata tasksCustomMetadata = new PersistentTasksCustomMetadata(0L, Map.of());

        IpLocationDownloadConsumers consumers = IpLocationDownloadConsumers.EMPTY.withConsumer(IpLocationConsumer.INGEST);
        ClusterState state = createClusterState(projectId, tasksCustomMetadata, consumers);

        mockSearches("GeoIP2-City.mmdb", 0, 9);

        databaseNodeService.checkDatabases(state);
        assertThat(databaseNodeService.getDatabase(projectId, "GeoIP2-City.mmdb"), nullValue());
        verify(projectClient, never()).search(any());
        try (Stream<Path> files = Files.list(geoIpTmpDir.resolve("geoip-databases").resolve("nodeId"))) {
            assertThat(files.toList(), empty());
        }
    }

    public void testCheckDatabases_ingestConsumerDoesNotCopyToDataOnlyNode() throws Exception {
        String md5 = mockSearches("GeoIP2-City.mmdb", 5, 14);
        PersistentTasksCustomMetadata tasksCustomMetadata = geoIpDownloaderTask(
            projectId,
            "GeoIP2-City.mmdb",
            new GeoIpTaskState.Metadata(10, 5, 14, md5, System.currentTimeMillis())
        );

        DiscoveryNodes nodesWithoutIngestRole = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.builder("_id1").roles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE)).build())
            .localNodeId("_id1")
            .build();
        IpLocationDownloadConsumers consumers = IpLocationDownloadConsumers.EMPTY.withConsumer(IpLocationConsumer.INGEST);
        ClusterState state = createClusterState(projectId, tasksCustomMetadata, nodesWithoutIngestRole, consumers);

        databaseNodeService.checkDatabases(state);

        verify(projectClient, never().description("INGEST consumer should not trigger database copy on a node without the ingest role"))
            .search(any());
        assertThat(
            "database should not be available on a node without the ingest role when only INGEST consumer is active",
            databaseNodeService.getDatabase(projectId, "GeoIP2-City.mmdb"),
            nullValue()
        );
    }

    public void testCheckDatabases_ingestConsumerDoesNotCopyToMasterOnlyNode() throws Exception {
        String md5 = mockSearches("GeoIP2-City.mmdb", 5, 14);
        PersistentTasksCustomMetadata tasksCustomMetadata = geoIpDownloaderTask(
            projectId,
            "GeoIP2-City.mmdb",
            new GeoIpTaskState.Metadata(10, 5, 14, md5, System.currentTimeMillis())
        );

        DiscoveryNodes masterOnlyNodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.builder("_id1").roles(Set.of(DiscoveryNodeRole.MASTER_ROLE)).build())
            .localNodeId("_id1")
            .build();
        IpLocationDownloadConsumers consumers = IpLocationDownloadConsumers.EMPTY.withConsumer(IpLocationConsumer.INGEST);
        ClusterState state = createClusterState(projectId, tasksCustomMetadata, masterOnlyNodes, consumers);

        databaseNodeService.checkDatabases(state);

        verify(projectClient, never().description("INGEST consumer should not trigger database copy on a master-only node")).search(any());
        assertThat(
            "database should not be available on a master-only node when only INGEST consumer is active",
            databaseNodeService.getDatabase(projectId, "GeoIP2-City.mmdb"),
            nullValue()
        );
    }

    public void testCheckDatabases_esqlConsumerTriggersDownloadOnDataCapableNode() throws Exception {
        String md5 = mockSearches("GeoIP2-City.mmdb", 5, 14);
        PersistentTasksCustomMetadata tasksCustomMetadata = geoIpDownloaderTask(
            projectId,
            "GeoIP2-City.mmdb",
            new GeoIpTaskState.Metadata(10, 5, 14, md5, System.currentTimeMillis())
        );

        DiscoveryNodes fullRoleNodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.builder("_id1").roles(Set.of(DiscoveryNodeRole.INGEST_ROLE, DiscoveryNodeRole.DATA_ROLE)).build())
            .localNodeId("_id1")
            .build();
        IpLocationDownloadConsumers consumers = IpLocationDownloadConsumers.EMPTY.withConsumer(IpLocationConsumer.ESQL);
        ClusterState state = createClusterState(projectId, tasksCustomMetadata, fullRoleNodes, consumers);

        databaseNodeService.checkDatabases(state);

        verify(
            projectClient,
            times(CHUNKS_PER_DATABASE).description("ESQL consumer alone should trigger database download on a data-capable node")
        ).search(any());
        assertThat(
            "database should be available when ESQL consumer is registered and the local node is data-capable",
            databaseNodeService.getDatabaseReaderLazyLoader(projectId, "GeoIP2-City.mmdb"),
            notNullValue()
        );
    }

    /**
     * Rolling-upgrade safety: when the {@link IpLocationDownloadConsumers} custom is absent from a project's
     * metadata (e.g. on an upgraded 9.5 ingest node while the master is still 9.4 or has just been upgraded
     * and hasn't written the custom yet), previously loaded databases must be preserved. Treating absent as
     * "no consumers" would delete the local database files and silently degrade ingest until the master
     * re-populates the custom.
     */
    public void testCheckDatabases_preservesLoadersWhenCustomAbsent() throws Exception {
        String databaseName = "GeoIP2-City.mmdb";
        String md5 = mockSearches(databaseName, 5, 14);
        PersistentTasksCustomMetadata tasksCustomMetadata = geoIpDownloaderTask(
            projectId,
            databaseName,
            new GeoIpTaskState.Metadata(10, 5, 14, md5, System.currentTimeMillis())
        );

        IpLocationDownloadConsumers consumers = IpLocationDownloadConsumers.EMPTY.withConsumer(IpLocationConsumer.INGEST);
        ClusterState stateWithConsumer = createClusterState(projectId, tasksCustomMetadata, consumers);
        databaseNodeService.checkDatabases(stateWithConsumer);
        assertThat(
            "precondition: database should load when INGEST consumer is registered",
            databaseNodeService.getDatabaseReaderLazyLoader(projectId, databaseName),
            notNullValue()
        );

        // Same state, but IpLocationDownloadConsumers custom removed entirely.
        ClusterState stateWithoutCustom = createClusterState(projectId, tasksCustomMetadata);
        databaseNodeService.checkDatabases(stateWithoutCustom);

        assertThat(
            "loader must be preserved when IpLocationDownloadConsumers custom is absent (rolling-upgrade safety)",
            databaseNodeService.getDatabaseReaderLazyLoader(projectId, databaseName),
            notNullValue()
        );
    }

    /**
     * Once the {@link IpLocationDownloadConsumers} custom exists (even if empty), it authoritatively describes
     * the consumer set: an empty custom means "no consumers want IP databases", so previously loaded databases
     * must be pruned. This is the complement of {@link #testCheckDatabases_preservesLoadersWhenCustomAbsent}
     * and pins the distinction between "absent" and "present but empty".
     */
    public void testCheckDatabases_prunesLoadersWhenCustomPresentButEmpty() throws Exception {
        String databaseName = "GeoIP2-City.mmdb";
        String md5 = mockSearches(databaseName, 5, 14);
        PersistentTasksCustomMetadata tasksCustomMetadata = geoIpDownloaderTask(
            projectId,
            databaseName,
            new GeoIpTaskState.Metadata(10, 5, 14, md5, System.currentTimeMillis())
        );

        IpLocationDownloadConsumers consumers = IpLocationDownloadConsumers.EMPTY.withConsumer(IpLocationConsumer.INGEST);
        ClusterState stateWithConsumer = createClusterState(projectId, tasksCustomMetadata, consumers);
        databaseNodeService.checkDatabases(stateWithConsumer);
        assertThat(
            "precondition: database should load when INGEST consumer is registered",
            databaseNodeService.getDatabaseReaderLazyLoader(projectId, databaseName),
            notNullValue()
        );

        // Custom remains in cluster state but all consumers have been unregistered.
        ClusterState stateWithEmptyCustom = createClusterState(projectId, tasksCustomMetadata, IpLocationDownloadConsumers.EMPTY);
        databaseNodeService.checkDatabases(stateWithEmptyCustom);

        assertThat(
            "loader must be pruned when IpLocationDownloadConsumers is present but empty",
            databaseNodeService.getDatabaseReaderLazyLoader(projectId, databaseName),
            nullValue()
        );
    }

    /**
     * When the {@link IpLocationDownloadConsumers} custom is present and non-empty but its consumers require
     * roles this node does not have (e.g. only INGEST registered while this is a master-only node), previously
     * loaded databases must be pruned. Sibling to {@link #testCheckDatabases_ingestConsumerDoesNotCopyToMasterOnlyNode}
     * which covers the no-loader case; this one pins the prune path.
     */
    public void testCheckDatabases_prunesLoadersWhenConsumerRoleNotRelevant() throws Exception {
        String databaseName = "GeoIP2-City.mmdb";
        String md5 = mockSearches(databaseName, 5, 14);
        PersistentTasksCustomMetadata tasksCustomMetadata = geoIpDownloaderTask(
            projectId,
            databaseName,
            new GeoIpTaskState.Metadata(10, 5, 14, md5, System.currentTimeMillis())
        );

        // Node carries both ingest and master roles so the INGEST-phase loader can be installed.
        DiscoveryNodes ingestAndMasterNodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.builder("_id1").roles(Set.of(DiscoveryNodeRole.INGEST_ROLE, DiscoveryNodeRole.MASTER_ROLE)).build())
            .localNodeId("_id1")
            .build();
        IpLocationDownloadConsumers ingestConsumers = IpLocationDownloadConsumers.EMPTY.withConsumer(IpLocationConsumer.INGEST);
        ClusterState stateIngest = createClusterState(projectId, tasksCustomMetadata, ingestAndMasterNodes, ingestConsumers);
        databaseNodeService.checkDatabases(stateIngest);
        assertThat(
            "precondition: database should load when INGEST consumer matches the local node's ingest role",
            databaseNodeService.getDatabaseReaderLazyLoader(projectId, databaseName),
            notNullValue()
        );

        // Now switch to a master-only node with only INGEST registered — INGEST needs an ingest-capable
        // node, so none of the registered consumers applies to this node.
        DiscoveryNodes masterOnlyNodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.builder("_id1").roles(Set.of(DiscoveryNodeRole.MASTER_ROLE)).build())
            .localNodeId("_id1")
            .build();
        ClusterState stateMasterOnly = createClusterState(projectId, tasksCustomMetadata, masterOnlyNodes, ingestConsumers);
        databaseNodeService.checkDatabases(stateMasterOnly);

        assertThat(
            "loader must be pruned when no registered consumer matches the local node's roles",
            databaseNodeService.getDatabaseReaderLazyLoader(projectId, databaseName),
            nullValue()
        );
    }

    /**
     * Verifies that when a project is removed from the cluster state, {@link DatabaseNodeService} releases the
     * per-project database loaders it previously created for that project while leaving loaders for still-present
     * projects intact. This catches the stale-project leak that arises because {@code checkDatabases(ClusterState)}
     * iterates via {@code state.forEachProject(...)} and therefore never visits removed projects.
     */
    public void testCheckDatabases_removesStaleProjectDatabases() throws Exception {
        assumeTrue("stale-project cleanup is only meaningful in multi-project mode", multiProject);

        ProjectId projectA = randomUniqueProjectId();
        ProjectId projectB = randomValueOtherThan(projectA, ESTestCase::randomUniqueProjectId);
        String databaseName = "GeoIP2-City.mmdb";

        String md5 = mockSearches(databaseName, 5, 14);
        GeoIpTaskState.Metadata validMetadata = new GeoIpTaskState.Metadata(10, 5, 14, md5, System.currentTimeMillis());
        IpLocationDownloadConsumers consumers = IpLocationDownloadConsumers.EMPTY.withConsumer(IpLocationConsumer.INGEST);

        // Build two self-contained single-project states and compose them into a two-project state. This avoids
        // duplicating the index / routing-table setup that createClusterState() already handles.
        ClusterState stateA = createClusterState(projectA, geoIpDownloaderTask(projectA, databaseName, validMetadata), consumers);
        ClusterState stateB = createClusterState(projectB, geoIpDownloaderTask(projectB, databaseName, validMetadata), consumers);
        ClusterState stateBoth = ClusterState.builder(stateA)
            .putProjectMetadata(stateB.metadata().getProject(projectB))
            .putRoutingTable(projectB, stateB.routingTable(projectB))
            .build();

        databaseNodeService.checkDatabases(stateBoth);
        assertThat(
            "project A database should be loaded after checkDatabases with both projects present",
            databaseNodeService.getDatabaseReaderLazyLoader(projectA, databaseName),
            notNullValue()
        );
        assertThat(
            "project B database should be loaded after checkDatabases with both projects present",
            databaseNodeService.getDatabaseReaderLazyLoader(projectB, databaseName),
            notNullValue()
        );

        // Remove project B; project A stays.
        ClusterState stateOnlyA = ClusterState.builder(stateBoth)
            .metadata(Metadata.builder(stateBoth.metadata()).removeProject(projectB))
            .routingTable(GlobalRoutingTable.builder(stateBoth.globalRoutingTable()).removeProject(projectB).build())
            .build();

        databaseNodeService.checkDatabases(stateOnlyA);
        assertThat(
            "project A database should remain loaded after project B is removed from the cluster state",
            databaseNodeService.getDatabaseReaderLazyLoader(projectA, databaseName),
            notNullValue()
        );
        assertThat(
            "project B database should be unloaded after project B is removed from the cluster state",
            databaseNodeService.getDatabaseReaderLazyLoader(projectB, databaseName),
            nullValue()
        );
    }

    public void testRequestDownloadsBuildsRegisterRequest() {
        ProjectId requestedProject = randomProjectIdOrDefault();
        ClusterState state = ClusterState.builder(new ClusterName("name"))
            .putProjectMetadata(ProjectMetadata.builder(requestedProject).build())
            .build();
        when(clusterService.state()).thenReturn(state);

        databaseNodeService.requestDownloads(requestedProject.id(), IpLocationConsumer.ESQL);

        ArgumentCaptor<RequestIpLocationDownloadsAction.Request> requestCaptor = ArgumentCaptor.forClass(
            RequestIpLocationDownloadsAction.Request.class
        );
        verify(client).execute(Mockito.eq(RequestIpLocationDownloadsAction.INSTANCE), requestCaptor.capture(), any());
        RequestIpLocationDownloadsAction.Request request = requestCaptor.getValue();
        assertEquals(requestedProject, request.getProjectId());
        assertEquals(IpLocationConsumer.ESQL, request.getConsumer());
        assertEquals(RequestIpLocationDownloadsAction.Request.Operation.REGISTER, request.operation());
    }

    public void testCancelDownloadRequestBuildsUnregisterRequest() {
        ProjectId requestedProject = randomProjectIdOrDefault();
        IpLocationDownloadConsumers consumers = IpLocationDownloadConsumers.EMPTY.withConsumer(IpLocationConsumer.INGEST);
        ClusterState state = ClusterState.builder(new ClusterName("name"))
            .putProjectMetadata(ProjectMetadata.builder(requestedProject).putCustom(IpLocationDownloadConsumers.TYPE, consumers).build())
            .build();
        when(clusterService.state()).thenReturn(state);

        databaseNodeService.cancelDownloadRequest(requestedProject.id(), IpLocationConsumer.INGEST);

        ArgumentCaptor<RequestIpLocationDownloadsAction.Request> requestCaptor = ArgumentCaptor.forClass(
            RequestIpLocationDownloadsAction.Request.class
        );
        verify(client).execute(Mockito.eq(RequestIpLocationDownloadsAction.INSTANCE), requestCaptor.capture(), any());
        RequestIpLocationDownloadsAction.Request request = requestCaptor.getValue();
        assertEquals(requestedProject, request.getProjectId());
        assertEquals(IpLocationConsumer.INGEST, request.getConsumer());
        assertEquals(RequestIpLocationDownloadsAction.Request.Operation.UNREGISTER, request.operation());
    }

    public void testRequestAndCancelDoNothingWhenProjectMissing() {
        ProjectId existingProject = randomProjectIdOrDefault();
        // {@link ClusterState.Builder} starts from {@link Metadata#EMPTY_METADATA}, which auto-creates the
        // default project; {@code putProjectMetadata} adds to that rather than replacing it. Exclude both the
        // explicitly added project and the default project to guarantee the "missing" project is truly absent.
        ProjectId missingProject = randomValueOtherThanMany(
            pid -> pid.equals(existingProject) || pid.equals(ProjectId.DEFAULT),
            ESTestCase::randomProjectIdOrDefault
        );
        ClusterState state = ClusterState.builder(new ClusterName("name"))
            .putProjectMetadata(ProjectMetadata.builder(existingProject).build())
            .build();
        when(clusterService.state()).thenReturn(state);

        databaseNodeService.requestDownloads(missingProject.id(), IpLocationConsumer.INGEST);
        databaseNodeService.cancelDownloadRequest(missingProject.id(), IpLocationConsumer.INGEST);

        verify(client, never()).execute(Mockito.eq(RequestIpLocationDownloadsAction.INSTANCE), any(), any());
    }

    public void testRetrieveDatabase() throws Exception {
        String md5 = mockSearches("_name", 0, 29);
        GeoIpTaskState.Metadata metadata = new GeoIpTaskState.Metadata(-1, 0, 29, md5, 10);

        @SuppressWarnings("unchecked")
        CheckedConsumer<byte[], IOException> chunkConsumer = mock(CheckedConsumer.class);
        @SuppressWarnings("unchecked")
        CheckedRunnable<Exception> completedHandler = mock(CheckedRunnable.class);
        @SuppressWarnings("unchecked")
        Consumer<Exception> failureHandler = mock(Consumer.class);
        databaseNodeService.retrieveDatabase(projectId, "_name", md5, metadata, chunkConsumer, completedHandler, failureHandler);
        verify(failureHandler, never()).accept(any());
        verify(chunkConsumer, times(30)).accept(any());
        verify(completedHandler, times(1)).run();
        verify(projectClient, times(30)).search(any());
    }

    public void testRetrieveDatabaseCorruption() throws Exception {
        String md5 = mockSearches("_name", 0, 9);
        String incorrectMd5 = "different";
        GeoIpTaskState.Metadata metadata = new GeoIpTaskState.Metadata(-1, 0, 9, incorrectMd5, 10);

        @SuppressWarnings("unchecked")
        CheckedConsumer<byte[], IOException> chunkConsumer = mock(CheckedConsumer.class);
        @SuppressWarnings("unchecked")
        CheckedRunnable<Exception> completedHandler = mock(CheckedRunnable.class);
        @SuppressWarnings("unchecked")
        Consumer<Exception> failureHandler = mock(Consumer.class);
        databaseNodeService.retrieveDatabase(projectId, "_name", incorrectMd5, metadata, chunkConsumer, completedHandler, failureHandler);
        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(failureHandler, times(1)).accept(exceptionCaptor.capture());
        assertThat(exceptionCaptor.getAllValues().size(), equalTo(1));
        assertThat(
            exceptionCaptor.getAllValues().getFirst().getMessage(),
            equalTo("expected md5 hash [different], " + "but got md5 hash [" + md5 + "]")
        );
        verify(chunkConsumer, times(CHUNKS_PER_DATABASE)).accept(any());
        verify(completedHandler, times(0)).run();
        verify(projectClient, times(CHUNKS_PER_DATABASE)).search(any());
    }

    public void testUpdateDatabase() throws Exception {
        List<String> notifiedDatabases = new ArrayList<>();
        databaseNodeService.addDatabaseAvailabilityListener((pid, dbFile) -> notifiedDatabases.add(dbFile));

        databaseNodeService.updateDatabase(projectId, "_name", "_md5", geoIpTmpDir.resolve("some-file"));

        // Updating the first time should notify listeners.
        verify(clusterService, times(1)).addListener(any());
        assertThat(notifiedDatabases, equalTo(List.of("_name")));
        verifyNoMoreInteractions(clusterService);
        reset(clusterService);

        // Subsequent updates shouldn't notify again.
        notifiedDatabases.clear();
        databaseNodeService.updateDatabase(projectId, "_name", "_md5", geoIpTmpDir.resolve("some-file"));
        verifyNoMoreInteractions(clusterService);
        assertThat(notifiedDatabases, empty());
    }

    /**
     * Guards against a race between {@link DatabaseNodeService#updateDatabase} and
     * {@link DatabaseNodeService#checkDatabases} that, before the fix, would orphan a freshly-loaded loader:
     * {@code updateDatabase} mutated the {@code databases} map in two non-atomic steps
     * ({@code computeIfAbsent} then {@code put}), so a concurrent {@code checkDatabases} could detach the
     * inner map between the two steps; the listener still fired for the put (which then returned null on the
     * detached inner), yielding a "ghost-available" database that no lookup could ever find.
     * <p>
     * The fix funnels every mutation of {@code databases} through a single
     * {@code databases.compute(projectId, ...)}, so the bin lock held by {@code updateDatabase} prevents any
     * concurrent {@code checkDatabases} from detaching the same project's inner map until the put has
     * completed. This test pins that invariant: while the updater is mid-{@code put} (deterministically
     * parked via an instrumented inner map), a concurrent cleanup must not be able to detach the inner
     * from the outer map.
     */
    public void testUpdateDatabase_doesNotOrphanLoaderUnderConcurrentCleanup() throws Exception {
        String dbName = "GeoIP2-City.mmdb";

        CountDownLatch innerPutEntered = new CountDownLatch(1);
        CountDownLatch resumeUpdater = new CountDownLatch(1);

        // creating the instrumented inner map
        @SuppressWarnings("serial")
        ConcurrentMap<String, DatabaseReaderLazyLoader> instrumentedInner = new ConcurrentHashMap<>() {
            @Override
            public DatabaseReaderLazyLoader put(String key, DatabaseReaderLazyLoader value) {
                // signal that put entered, then wait for a signal to resume and only then do the actual put
                innerPutEntered.countDown();
                safeAwait(resumeUpdater);
                // without the fix, this would now have put on a detached inner map
                return super.put(key, value);
            }
        };
        // adding the inner map to the outer map
        databaseNodeService.databases.put(projectId, instrumentedInner);

        // INGEST consumer registered, but the local node only has the master role: checkDatabases hits the
        // "no relevant consumer for this node" branch and tries to drop the project entry.
        DiscoveryNodes masterOnlyNodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.builder("_id1").roles(Set.of(DiscoveryNodeRole.MASTER_ROLE)).build())
            .localNodeId("_id1")
            .build();
        IpLocationDownloadConsumers ingestOnly = IpLocationDownloadConsumers.EMPTY.withConsumer(IpLocationConsumer.INGEST);
        PersistentTasksCustomMetadata tasks = geoIpDownloaderTask(
            projectId,
            dbName,
            new GeoIpTaskState.Metadata(10, 5, 14, "md5", System.currentTimeMillis())
        );
        ClusterState state = createClusterState(projectId, tasks, masterOnlyNodes, ingestOnly);

        List<String> notified = new CopyOnWriteArrayList<>();
        databaseNodeService.addDatabaseAvailabilityListener((pid, db) -> notified.add(db));

        Thread updater = new Thread(
            // the put that is called through this will block until the cleaner thread will signal it to resume and actually put
            () -> databaseNodeService.updateDatabase(projectId, dbName, "md5", geoIpTmpDir.resolve(dbName)),
            "test-updater"
        );
        updater.setDaemon(true);
        updater.start();
        // wait until put entered
        safeAwait(innerPutEntered);

        // Cleanup runs concurrently. With the fix, it blocks on the bin lock that the updater (still inside
        // databases.compute(projectId, ...) -> inner.put) is holding. Without the fix, it proceeds and
        // detaches the inner map from `databases`.
        Thread cleaner = new Thread(() -> databaseNodeService.checkDatabases(state), "test-cleaner");
        cleaner.setDaemon(true);
        cleaner.start();

        try {
            // Generous window: a non-blocked cleaner removes the project entry in microseconds, so anything
            // longer than a few ms reliably distinguishes "blocked on bin lock" (fix) from "completed" (bug).
            Thread.sleep(500);

            assertSame(
                "the project's inner map must not be detached from the outer map while updateDatabase is in progress",
                instrumentedInner,
                databaseNodeService.databases.get(projectId)
            );
        } finally {
            resumeUpdater.countDown();
        }

        updater.join(TimeUnit.SECONDS.toMillis(10));
        cleaner.join(TimeUnit.SECONDS.toMillis(10));
        assertFalse("updater thread did not complete in time", updater.isAlive());
        assertFalse("cleaner thread did not complete in time", cleaner.isAlive());

        // After both threads finish: listener fired exactly once for the freshly-loaded database, and the
        // cleaner has now properly removed the project entry (collecting and shutting down the loader).
        assertThat(notified, equalTo(List.of(dbName)));
        assertThat(databaseNodeService.databases.get(projectId), nullValue());
    }

    /**
     * Builds a {@link PersistentTasksCustomMetadata} that contains a single {@link GeoIpDownloader} task for the given
     * project, advertising one database download in its {@link GeoIpTaskState}. The task id is derived through
     * {@link GeoIpDownloaderTaskExecutor#getTaskId} so that it matches what {@link DatabaseNodeService} looks up at
     * runtime under either single- or multi-project mode.
     */
    private PersistentTasksCustomMetadata geoIpDownloaderTask(ProjectId projectId, String databaseName, GeoIpTaskState.Metadata metadata) {
        return geoIpDownloaderTask(projectId, Map.of(databaseName, metadata), projectResolver.supportsMultipleProjects());
    }

    /**
     * Static counterpart of {@link #geoIpDownloaderTask(ProjectId, String, GeoIpTaskState.Metadata)} that other unit
     * tests in this package can use to drive {@link DatabaseNodeService#checkDatabases(ClusterState)} with a fully
     * constructed task state. Pass an empty map to produce a state that purges all entries for {@code projectId}.
     */
    static PersistentTasksCustomMetadata geoIpDownloaderTask(
        ProjectId projectId,
        Map<String, GeoIpTaskState.Metadata> databases,
        boolean supportsMultipleProjects
    ) {
        String taskId = getTaskId(projectId, supportsMultipleProjects);
        PersistentTask<?> task = new PersistentTask<>(taskId, GeoIpDownloader.GEOIP_DOWNLOADER, new GeoIpTaskParams(), 1, null);
        task = new PersistentTask<>(task, new GeoIpTaskState(databases));
        return new PersistentTasksCustomMetadata(1L, Map.of(taskId, task));
    }

    private String mockSearches(String databaseName, int firstChunk, int lastChunk) throws IOException {
        String dummyContent = "test: " + databaseName;
        List<byte[]> data;
        // We want to make sure we handle gzip files or plain mmdb files equally well:
        if (randomBoolean()) {
            data = gzip(databaseName, dummyContent, lastChunk - firstChunk + 1);
            assertThat(gunzip(data), equalTo(dummyContent));
        } else {
            data = chunkBytes(dummyContent, lastChunk - firstChunk + 1);
            assertThat(unchunkBytes(data), equalTo(dummyContent));
        }

        Map<String, ActionFuture<SearchResponse>> requestMap = new HashMap<>();
        for (int i = firstChunk; i <= lastChunk; i++) {
            byte[] chunk;
            if (i - firstChunk < data.size()) {
                chunk = data.get(i - firstChunk);
            } else {
                chunk = new byte[0]; // We had so little data that the chunk(s) at the end will be empty
            }
            final byte[] sourceBytes;
            try (XContentBuilder builder = XContentBuilder.builder(XContentType.SMILE.xContent())) {
                builder.map(Map.of("data", chunk));
                builder.flush();
                sourceBytes = ((ByteArrayOutputStream) builder.getOutputStream()).toByteArray();
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }

            // Produce a fresh SearchHit/SearchResponse per actionGet() call: SearchHit#getSourceAsMap asserts it is
            // only invoked once per hit instance, so sharing a single hit across multiple consumers (e.g. two
            // projects downloading the same database) trips that assertion. The underlying source bytes are
            // immutable and safely reusable.
            //
            // Pooled (ref-counted) SearchHits/SearchHit semantics (since #147167): the response built from
            // freshHits incref's its argument, so we drop our local ref via {@code freshHits.decRef()} to leave
            // the response as the sole owner. The production consumer (see DatabaseNodeService#retrieveDatabase)
            // then decRef's the response, which cascades through SearchHits.deallocate() and decRef's the inner
            // SearchHit. The response must therefore not be added to {@link #toRelease}: decRef-ing it again from
            // @After would fail with "already closed".
            final int chunkIdx = i;
            @SuppressWarnings("unchecked")
            ActionFuture<SearchResponse> actionFuture = mock(ActionFuture.class);
            when(actionFuture.actionGet()).thenAnswer((Answer<SearchResponse>) invocation -> {
                SearchHit freshHit = new SearchHit(chunkIdx);
                freshHit.sourceRef(new BytesArray(sourceBytes));
                SearchHits freshHits = new SearchHits(new SearchHit[] { freshHit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1f);
                try {
                    return SearchResponseUtils.successfulResponse(freshHits);
                } finally {
                    freshHits.decRef();
                }
            });
            requestMap.put(databaseName + "_" + i, actionFuture);
        }
        when(projectClient.search(any())).thenAnswer(invocationOnMock -> {
            SearchRequest req = (SearchRequest) invocationOnMock.getArguments()[0];
            TermQueryBuilder term = (TermQueryBuilder) req.source().query();
            String id = (String) Objects.requireNonNull(term).value();
            return requestMap.get(id.substring(0, id.lastIndexOf('_')));
        });

        MessageDigest md = MessageDigests.md5();
        data.forEach(md::update);
        return MessageDigests.toHexString(md.digest());
    }

    static ClusterState createClusterState(ProjectId projectId, PersistentTasksCustomMetadata tasksCustomMetadata) {
        return createClusterState(projectId, tasksCustomMetadata, false, null, null);
    }

    static ClusterState createClusterState(
        ProjectId projectId,
        PersistentTasksCustomMetadata tasksCustomMetadata,
        IpLocationDownloadConsumers consumers
    ) {
        return createClusterState(projectId, tasksCustomMetadata, false, null, consumers);
    }

    static ClusterState createClusterState(
        ProjectId projectId,
        PersistentTasksCustomMetadata tasksCustomMetadata,
        boolean noStartedShards
    ) {
        return createClusterState(projectId, tasksCustomMetadata, noStartedShards, null, null);
    }

    static ClusterState createClusterState(ProjectId projectId, PersistentTasksCustomMetadata tasksCustomMetadata, DiscoveryNodes nodes) {
        return createClusterState(projectId, tasksCustomMetadata, false, nodes, null);
    }

    static ClusterState createClusterState(
        ProjectId projectId,
        PersistentTasksCustomMetadata tasksCustomMetadata,
        DiscoveryNodes nodes,
        IpLocationDownloadConsumers consumers
    ) {
        return createClusterState(projectId, tasksCustomMetadata, false, nodes, consumers);
    }

    private static ClusterState createClusterState(
        ProjectId projectId,
        PersistentTasksCustomMetadata tasksCustomMetadata,
        boolean noStartedShards,
        DiscoveryNodes nodes,
        IpLocationDownloadConsumers consumers
    ) {
        if (nodes == null) {
            nodes = DiscoveryNodes.builder().add(DiscoveryNodeUtils.create("_id1")).localNodeId("_id1").build();
        }
        boolean aliasGeoipDatabase = randomBoolean();
        String indexName = aliasGeoipDatabase
            ? GeoIpDownloader.DATABASES_INDEX + "-" + randomAlphaOfLength(5)
            : GeoIpDownloader.DATABASES_INDEX;
        Index index = new Index(indexName, UUID.randomUUID().toString());
        IndexMetadata.Builder idxMeta = IndexMetadata.builder(index.getName())
            .settings(indexSettings(IndexVersion.current(), 1, 0).put("index.uuid", index.getUUID()));
        if (aliasGeoipDatabase) {
            idxMeta.putAlias(AliasMetadata.builder(GeoIpDownloader.DATABASES_INDEX));
        }
        ShardRouting shardRouting = ShardRouting.newUnassigned(
            new ShardId(index, 0),
            true,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""),
            ShardRouting.Role.DEFAULT
        );
        String nodeId = ESTestCase.randomAlphaOfLength(8);
        shardRouting = shardRouting.initialize(nodeId, null, shardRouting.getExpectedShardSize());
        if (noStartedShards == false) {
            shardRouting = shardRouting.moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
        }
        ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(projectId).put(idxMeta).putCustom(TYPE, tasksCustomMetadata);
        if (consumers != null) {
            projectBuilder.putCustom(IpLocationDownloadConsumers.TYPE, consumers);
        }
        return ClusterState.builder(new ClusterName("name"))
            .putProjectMetadata(projectBuilder)
            .nodes(nodes)
            .putRoutingTable(
                projectId,
                RoutingTable.builder()
                    .add(
                        IndexRoutingTable.builder(index)
                            .addIndexShard(IndexShardRoutingTable.builder(new ShardId(index, 0)).addShard(shardRouting))
                    )
                    .build()
            )
            .build();
    }

    private static List<byte[]> chunkBytes(String content, int chunks) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
        byteArrayOutputStream.write(contentBytes);
        byteArrayOutputStream.close();

        byte[] all = byteArrayOutputStream.toByteArray();
        int chunkSize = Math.max(1, all.length / chunks);
        List<byte[]> data = new ArrayList<>();

        for (int from = 0; from < all.length;) {
            int to = from + chunkSize;
            if (to > all.length) {
                to = all.length;
            }
            data.add(Arrays.copyOfRange(all, from, to));
            from = to;
        }

        while (data.size() > chunks) {
            byte[] last = data.removeLast();
            byte[] secondLast = data.removeLast();
            byte[] merged = new byte[secondLast.length + last.length];
            System.arraycopy(secondLast, 0, merged, 0, secondLast.length);
            System.arraycopy(last, 0, merged, secondLast.length, last.length);
            data.add(merged);
        }

        assert data.size() == Math.min(chunks, content.length());
        return data;
    }

    private static List<byte[]> gzip(String name, String content, int chunks) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(bytes);
        byte[] header = new byte[512];
        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
        byte[] sizeBytes = Strings.format("%1$012o", contentBytes.length).getBytes(StandardCharsets.UTF_8);
        System.arraycopy(nameBytes, 0, header, 0, nameBytes.length);
        System.arraycopy(sizeBytes, 0, header, 124, 12);
        gzipOutputStream.write(header);
        gzipOutputStream.write(contentBytes);
        gzipOutputStream.write(512 - contentBytes.length);
        gzipOutputStream.write(new byte[512]);
        gzipOutputStream.write(new byte[512]);
        gzipOutputStream.close();

        byte[] all = bytes.toByteArray();
        int chunkSize = all.length / chunks;
        List<byte[]> data = new ArrayList<>();

        for (int from = 0; from < all.length;) {
            int to = from + chunkSize;
            if (to > all.length) {
                to = all.length;
            }
            data.add(Arrays.copyOfRange(all, from, to));
            from = to;
        }

        while (data.size() > chunks) {
            byte[] last = data.removeLast();
            byte[] secondLast = data.removeLast();
            byte[] merged = new byte[secondLast.length + last.length];
            System.arraycopy(secondLast, 0, merged, 0, secondLast.length);
            System.arraycopy(last, 0, merged, secondLast.length, last.length);
            data.add(merged);
        }

        assert data.size() == chunks;
        return data;
    }

    private static byte[] unchunkBytesToByteArray(List<byte[]> chunks) throws IOException {
        byte[] allBytes = new byte[chunks.stream().mapToInt(value -> value.length).sum()];
        int written = 0;
        for (byte[] chunk : chunks) {
            System.arraycopy(chunk, 0, allBytes, written, chunk.length);
            written += chunk.length;
        }
        return allBytes;
    }

    private static String unchunkBytes(List<byte[]> chunks) throws IOException {
        byte[] allBytes = unchunkBytesToByteArray(chunks);
        return new String(allBytes, StandardCharsets.UTF_8);
    }

    private static String gunzip(List<byte[]> chunks) throws IOException {
        byte[] gzippedContent = unchunkBytesToByteArray(chunks);
        TarInputStream gzipInputStream = new TarInputStream(new GZIPInputStream(new ByteArrayInputStream(gzippedContent)));
        gzipInputStream.getNextEntry();
        return Streams.readFully(gzipInputStream).utf8ToString();
    }

}
