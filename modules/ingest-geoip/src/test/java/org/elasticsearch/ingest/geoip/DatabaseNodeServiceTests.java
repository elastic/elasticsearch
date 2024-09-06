/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import com.maxmind.db.InvalidDatabaseException;

import org.apache.lucene.search.TotalHits;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
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
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.elasticsearch.ingest.geoip.GeoIpTestUtils.copyDefaultDatabases;
import static org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import static org.elasticsearch.persistent.PersistentTasksCustomMetadata.TYPE;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@LuceneTestCase.SuppressFileSystems(value = "ExtrasFS") // Don't randomly add 'extra' files to directory.
public class DatabaseNodeServiceTests extends ESTestCase {

    private Client client;
    private Path geoIpTmpDir;
    private ThreadPool threadPool;
    private DatabaseNodeService databaseNodeService;
    private ResourceWatcherService resourceWatcherService;
    private IngestService ingestService;
    private ClusterService clusterService;

    private final Collection<Releasable> toRelease = new CopyOnWriteArrayList<>();

    @Before
    public void setup() throws IOException {
        final Path geoIpConfigDir = createTempDir();
        GeoIpCache cache = new GeoIpCache(1000);
        ConfigDatabases configDatabases = new ConfigDatabases(geoIpConfigDir, cache);
        copyDefaultDatabases(geoIpConfigDir, configDatabases);

        threadPool = new TestThreadPool(ConfigDatabases.class.getSimpleName());
        Settings settings = Settings.builder().put("resource.reload.interval.high", TimeValue.timeValueMillis(100)).build();
        resourceWatcherService = new ResourceWatcherService(settings, threadPool);

        client = mock(Client.class);
        ingestService = mock(IngestService.class);
        clusterService = mock(ClusterService.class);
        geoIpTmpDir = createTempDir();
        databaseNodeService = new DatabaseNodeService(geoIpTmpDir, client, cache, configDatabases, Runnable::run, clusterService);
        databaseNodeService.initialize("nodeId", resourceWatcherService, ingestService);
    }

    @After
    public void cleanup() {
        resourceWatcherService.close();
        threadPool.shutdownNow();
        Releasables.close(toRelease);
        toRelease.clear();
    }

    public void testCheckDatabases() throws Exception {
        String md5 = mockSearches("GeoIP2-City.mmdb", 5, 14);
        String taskId = GeoIpDownloader.GEOIP_DOWNLOADER;
        PersistentTask<?> task = new PersistentTask<>(taskId, GeoIpDownloader.GEOIP_DOWNLOADER, new GeoIpTaskParams(), 1, null);
        task = new PersistentTask<>(task, new GeoIpTaskState(Map.of("GeoIP2-City.mmdb", new GeoIpTaskState.Metadata(10, 5, 14, md5, 10))));
        PersistentTasksCustomMetadata tasksCustomMetadata = new PersistentTasksCustomMetadata(1L, Map.of(taskId, task));

        ClusterState state = createClusterState(tasksCustomMetadata);

        int numPipelinesToBeReloaded = randomInt(4);
        List<String> pipelineIds = IntStream.range(0, numPipelinesToBeReloaded).mapToObj(String::valueOf).toList();
        when(ingestService.getPipelineWithProcessorType(any(), any())).thenReturn(pipelineIds);

        assertThat(databaseNodeService.getDatabase("GeoIP2-City.mmdb"), nullValue());
        // Nothing should be downloaded, since the database is no longer valid (older than 30 days)
        databaseNodeService.checkDatabases(state);
        DatabaseReaderLazyLoader database = databaseNodeService.getDatabaseReaderLazyLoader("GeoIP2-City.mmdb");
        assertThat(database, nullValue());
        verify(client, times(0)).search(any());
        verify(ingestService, times(0)).reloadPipeline(anyString());
        try (Stream<Path> files = Files.list(geoIpTmpDir.resolve("geoip-databases").resolve("nodeId"))) {
            assertEquals(0, files.count());
        }

        task = new PersistentTask<>(
            task,
            new GeoIpTaskState(Map.of("GeoIP2-City.mmdb", new GeoIpTaskState.Metadata(10, 5, 14, md5, System.currentTimeMillis())))
        );
        tasksCustomMetadata = new PersistentTasksCustomMetadata(1L, Map.of(taskId, task));

        state = createClusterState(tasksCustomMetadata);

        // Database should be downloaded
        databaseNodeService.checkDatabases(state);
        database = databaseNodeService.getDatabaseReaderLazyLoader("GeoIP2-City.mmdb");
        assertThat(database, notNullValue());
        verify(client, times(10)).search(any());
        try (Stream<Path> files = Files.list(geoIpTmpDir.resolve("geoip-databases").resolve("nodeId"))) {
            assertThat(files.count(), greaterThanOrEqualTo(1L));
        }
        // First time GeoIP2-City.mmdb is downloaded, so a pipeline reload can happen:
        verify(ingestService, times(numPipelinesToBeReloaded)).reloadPipeline(anyString());
        // 30 days check passed but we mocked mmdb data so parsing will fail
        expectThrows(InvalidDatabaseException.class, database::get);
    }

    public void testCheckDatabases_dontCheckDatabaseOnNonIngestNode() throws Exception {
        String md5 = mockSearches("GeoIP2-City.mmdb", 0, 9);
        String taskId = GeoIpDownloader.GEOIP_DOWNLOADER;
        PersistentTask<?> task = new PersistentTask<>(taskId, GeoIpDownloader.GEOIP_DOWNLOADER, new GeoIpTaskParams(), 1, null);
        task = new PersistentTask<>(task, new GeoIpTaskState(Map.of("GeoIP2-City.mmdb", new GeoIpTaskState.Metadata(0L, 0, 9, md5, 10))));
        PersistentTasksCustomMetadata tasksCustomMetadata = new PersistentTasksCustomMetadata(1L, Map.of(taskId, task));

        ClusterState state = ClusterState.builder(createClusterState(tasksCustomMetadata))
            .nodes(
                new DiscoveryNodes.Builder().add(
                    DiscoveryNodeUtils.builder("_id1").name("_name1").roles(Set.of(DiscoveryNodeRole.MASTER_ROLE)).build()
                ).localNodeId("_id1")
            )
            .build();

        databaseNodeService.checkDatabases(state);
        assertThat(databaseNodeService.getDatabase("GeoIP2-City.mmdb"), nullValue());
        verify(client, never()).search(any());
        try (Stream<Path> files = Files.list(geoIpTmpDir.resolve("geoip-databases").resolve("nodeId"))) {
            assertThat(files.toList(), empty());
        }
    }

    public void testCheckDatabases_dontCheckDatabaseWhenNoDatabasesIndex() throws Exception {
        String md5 = mockSearches("GeoIP2-City.mmdb", 0, 9);
        String taskId = GeoIpDownloader.GEOIP_DOWNLOADER;
        PersistentTask<?> task = new PersistentTask<>(taskId, GeoIpDownloader.GEOIP_DOWNLOADER, new GeoIpTaskParams(), 1, null);
        task = new PersistentTask<>(task, new GeoIpTaskState(Map.of("GeoIP2-City.mmdb", new GeoIpTaskState.Metadata(0L, 0, 9, md5, 10))));
        PersistentTasksCustomMetadata tasksCustomMetadata = new PersistentTasksCustomMetadata(1L, Map.of(taskId, task));

        ClusterState state = ClusterState.builder(new ClusterName("name"))
            .metadata(Metadata.builder().putCustom(TYPE, tasksCustomMetadata).build())
            .nodes(new DiscoveryNodes.Builder().add(DiscoveryNodeUtils.create("_id1")).localNodeId("_id1"))
            .build();

        databaseNodeService.checkDatabases(state);
        assertThat(databaseNodeService.getDatabase("GeoIP2-City.mmdb"), nullValue());
        verify(client, never()).search(any());
        try (Stream<Path> files = Files.list(geoIpTmpDir.resolve("geoip-databases").resolve("nodeId"))) {
            assertThat(files.toList(), empty());
        }
    }

    public void testCheckDatabases_dontCheckDatabaseWhenGeoIpDownloadTask() throws Exception {
        PersistentTasksCustomMetadata tasksCustomMetadata = new PersistentTasksCustomMetadata(0L, Map.of());

        ClusterState state = createClusterState(tasksCustomMetadata);

        mockSearches("GeoIP2-City.mmdb", 0, 9);

        databaseNodeService.checkDatabases(state);
        assertThat(databaseNodeService.getDatabase("GeoIP2-City.mmdb"), nullValue());
        verify(client, never()).search(any());
        try (Stream<Path> files = Files.list(geoIpTmpDir.resolve("geoip-databases").resolve("nodeId"))) {
            assertThat(files.toList(), empty());
        }
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
        databaseNodeService.retrieveDatabase("_name", md5, metadata, chunkConsumer, completedHandler, failureHandler);
        verify(failureHandler, never()).accept(any());
        verify(chunkConsumer, times(30)).accept(any());
        verify(completedHandler, times(1)).run();
        verify(client, times(30)).search(any());
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
        databaseNodeService.retrieveDatabase("_name", incorrectMd5, metadata, chunkConsumer, completedHandler, failureHandler);
        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(failureHandler, times(1)).accept(exceptionCaptor.capture());
        assertThat(exceptionCaptor.getAllValues().size(), equalTo(1));
        assertThat(
            exceptionCaptor.getAllValues().get(0).getMessage(),
            equalTo("expected md5 hash [different], " + "but got md5 hash [" + md5 + "]")
        );
        verify(chunkConsumer, times(10)).accept(any());
        verify(completedHandler, times(0)).run();
        verify(client, times(10)).search(any());
    }

    public void testUpdateDatabase() throws Exception {
        int numPipelinesToBeReloaded = randomInt(4);
        List<String> pipelineIds = IntStream.range(0, numPipelinesToBeReloaded).mapToObj(String::valueOf).toList();
        when(ingestService.getPipelineWithProcessorType(any(), any())).thenReturn(pipelineIds);

        databaseNodeService.updateDatabase("_name", "_md5", geoIpTmpDir.resolve("some-file"));

        // Updating the first time may trigger a reload.
        verify(clusterService, times(1)).addListener(any());
        verify(ingestService, times(1)).getPipelineWithProcessorType(any(), any());
        verify(ingestService, times(numPipelinesToBeReloaded)).reloadPipeline(anyString());
        verifyNoMoreInteractions(clusterService);
        verifyNoMoreInteractions(ingestService);
        reset(clusterService);
        reset(ingestService);

        // Subsequent updates shouldn't trigger a reload.
        databaseNodeService.updateDatabase("_name", "_md5", geoIpTmpDir.resolve("some-file"));
        verifyNoMoreInteractions(clusterService);
        verifyNoMoreInteractions(ingestService);
    }

    private String mockSearches(String databaseName, int firstChunk, int lastChunk) throws IOException {
        String dummyContent = "test: " + databaseName;
        List<byte[]> data = gzip(databaseName, dummyContent, lastChunk - firstChunk + 1);
        assertThat(gunzip(data), equalTo(dummyContent));

        Map<String, ActionFuture<SearchResponse>> requestMap = new HashMap<>();
        for (int i = firstChunk; i <= lastChunk; i++) {
            byte[] chunk = data.get(i - firstChunk);
            SearchHit hit = SearchHit.unpooled(i);
            try (XContentBuilder builder = XContentBuilder.builder(XContentType.SMILE.xContent())) {
                builder.map(Map.of("data", chunk));
                builder.flush();
                ByteArrayOutputStream outputStream = (ByteArrayOutputStream) builder.getOutputStream();
                hit.sourceRef(new BytesArray(outputStream.toByteArray()));
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }

            SearchHits hits = SearchHits.unpooled(new SearchHit[] { hit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1f);
            SearchResponse searchResponse = new SearchResponse(hits, null, null, false, null, null, 0, null, 1, 1, 0, 1L, null, null);
            toRelease.add(searchResponse::decRef);
            @SuppressWarnings("unchecked")
            ActionFuture<SearchResponse> actionFuture = mock(ActionFuture.class);
            when(actionFuture.actionGet()).thenAnswer((Answer<SearchResponse>) invocation -> {
                searchResponse.incRef();
                return searchResponse;
            });
            requestMap.put(databaseName + "_" + i, actionFuture);
        }
        when(client.search(any())).thenAnswer(invocationOnMock -> {
            SearchRequest req = (SearchRequest) invocationOnMock.getArguments()[0];
            TermQueryBuilder term = (TermQueryBuilder) req.source().query();
            String id = (String) term.value();
            return requestMap.get(id.substring(0, id.lastIndexOf('_')));
        });

        MessageDigest md = MessageDigests.md5();
        data.forEach(md::update);
        return MessageDigests.toHexString(md.digest());
    }

    static ClusterState createClusterState(PersistentTasksCustomMetadata tasksCustomMetadata) {
        return createClusterState(tasksCustomMetadata, false);
    }

    static ClusterState createClusterState(PersistentTasksCustomMetadata tasksCustomMetadata, boolean noStartedShards) {
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
        return ClusterState.builder(new ClusterName("name"))
            .metadata(Metadata.builder().putCustom(TYPE, tasksCustomMetadata).put(idxMeta))
            .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create("_id1")).localNodeId("_id1"))
            .routingTable(
                RoutingTable.builder()
                    .add(
                        IndexRoutingTable.builder(index)
                            .addIndexShard(IndexShardRoutingTable.builder(new ShardId(index, 0)).addShard(shardRouting))
                    )
            )
            .build();
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
            byte[] last = data.remove(data.size() - 1);
            byte[] secondLast = data.remove(data.size() - 1);
            byte[] merged = new byte[secondLast.length + last.length];
            System.arraycopy(secondLast, 0, merged, 0, secondLast.length);
            System.arraycopy(last, 0, merged, secondLast.length, last.length);
            data.add(merged);
        }

        assert data.size() == chunks;
        return data;
    }

    private static String gunzip(List<byte[]> chunks) throws IOException {
        byte[] gzippedContent = new byte[chunks.stream().mapToInt(value -> value.length).sum()];
        int written = 0;
        for (byte[] chunk : chunks) {
            System.arraycopy(chunk, 0, gzippedContent, written, chunk.length);
            written += chunk.length;
        }
        TarInputStream gzipInputStream = new TarInputStream(new GZIPInputStream(new ByteArrayInputStream(gzippedContent)));
        gzipInputStream.getNextEntry();
        return Streams.readFully(gzipInputStream).utf8ToString();
    }

}
