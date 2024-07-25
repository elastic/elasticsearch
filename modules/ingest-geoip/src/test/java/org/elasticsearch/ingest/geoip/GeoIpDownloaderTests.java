/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.flush.FlushAction;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.ingest.geoip.stats.GeoIpDownloaderStats;
import org.elasticsearch.node.Node;
import org.elasticsearch.persistent.PersistentTaskResponse;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.persistent.UpdatePersistentTaskStatusAction;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static org.elasticsearch.ingest.geoip.DatabaseNodeServiceTests.createClusterState;
import static org.elasticsearch.ingest.geoip.GeoIpDownloader.ENDPOINT_SETTING;
import static org.elasticsearch.ingest.geoip.GeoIpDownloader.MAX_CHUNK_SIZE;
import static org.elasticsearch.tasks.TaskId.EMPTY_TASK_ID;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class GeoIpDownloaderTests extends ESTestCase {

    private HttpClient httpClient;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private MockClient client;
    private GeoIpDownloader geoIpDownloader;

    @Before
    public void setup() throws IOException {
        httpClient = mock(HttpClient.class);
        when(httpClient.getBytes(anyString())).thenReturn("[]".getBytes(StandardCharsets.UTF_8));
        clusterService = mock(ClusterService.class);
        threadPool = new ThreadPool(Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "test").build(), MeterRegistry.NOOP);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(
                Settings.EMPTY,
                Set.of(
                    GeoIpDownloaderTaskExecutor.EAGER_DOWNLOAD_SETTING,
                    GeoIpDownloader.ENDPOINT_SETTING,
                    GeoIpDownloaderTaskExecutor.POLL_INTERVAL_SETTING,
                    GeoIpDownloaderTaskExecutor.ENABLED_SETTING
                )
            )
        );
        ClusterState state = createClusterState(new PersistentTasksCustomMetadata(1L, Map.of()));
        when(clusterService.state()).thenReturn(state);
        client = new MockClient(threadPool);
        geoIpDownloader = new GeoIpDownloader(
            client,
            httpClient,
            clusterService,
            threadPool,
            Settings.EMPTY,
            1,
            "",
            "",
            "",
            EMPTY_TASK_ID,
            Map.of(),
            () -> GeoIpDownloaderTaskExecutor.POLL_INTERVAL_SETTING.getDefault(Settings.EMPTY),
            () -> GeoIpDownloaderTaskExecutor.EAGER_DOWNLOAD_SETTING.getDefault(Settings.EMPTY),
            () -> true
        ) {
            {
                GeoIpTaskParams geoIpTaskParams = mock(GeoIpTaskParams.class);
                when(geoIpTaskParams.getWriteableName()).thenReturn(GeoIpDownloader.GEOIP_DOWNLOADER);
                init(new PersistentTasksService(clusterService, threadPool, client), null, null, 0);
            }
        };
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testGetChunkEndOfStream() throws IOException {
        byte[] chunk = GeoIpDownloader.getChunk(new InputStream() {
            @Override
            public int read() {
                return -1;
            }
        });
        assertArrayEquals(new byte[0], chunk);
        chunk = GeoIpDownloader.getChunk(new ByteArrayInputStream(new byte[0]));
        assertArrayEquals(new byte[0], chunk);
    }

    public void testGetChunkLessThanChunkSize() throws IOException {
        ByteArrayInputStream is = new ByteArrayInputStream(new byte[] { 1, 2, 3, 4 });
        byte[] chunk = GeoIpDownloader.getChunk(is);
        assertArrayEquals(new byte[] { 1, 2, 3, 4 }, chunk);
        chunk = GeoIpDownloader.getChunk(is);
        assertArrayEquals(new byte[0], chunk);

    }

    public void testGetChunkExactlyChunkSize() throws IOException {
        byte[] bigArray = new byte[MAX_CHUNK_SIZE];
        for (int i = 0; i < MAX_CHUNK_SIZE; i++) {
            bigArray[i] = (byte) i;
        }
        ByteArrayInputStream is = new ByteArrayInputStream(bigArray);
        byte[] chunk = GeoIpDownloader.getChunk(is);
        assertArrayEquals(bigArray, chunk);
        chunk = GeoIpDownloader.getChunk(is);
        assertArrayEquals(new byte[0], chunk);
    }

    public void testGetChunkMoreThanChunkSize() throws IOException {
        byte[] bigArray = new byte[MAX_CHUNK_SIZE * 2];
        for (int i = 0; i < MAX_CHUNK_SIZE * 2; i++) {
            bigArray[i] = (byte) i;
        }
        byte[] smallArray = new byte[MAX_CHUNK_SIZE];
        System.arraycopy(bigArray, 0, smallArray, 0, MAX_CHUNK_SIZE);
        ByteArrayInputStream is = new ByteArrayInputStream(bigArray);
        byte[] chunk = GeoIpDownloader.getChunk(is);
        assertArrayEquals(smallArray, chunk);
        System.arraycopy(bigArray, MAX_CHUNK_SIZE, smallArray, 0, MAX_CHUNK_SIZE);
        chunk = GeoIpDownloader.getChunk(is);
        assertArrayEquals(smallArray, chunk);
        chunk = GeoIpDownloader.getChunk(is);
        assertArrayEquals(new byte[0], chunk);
    }

    public void testGetChunkRethrowsIOException() {
        expectThrows(IOException.class, () -> GeoIpDownloader.getChunk(new InputStream() {
            @Override
            public int read() throws IOException {
                throw new IOException();
            }
        }));
    }

    public void testIndexChunksNoData() throws IOException {
        client.addHandler(FlushAction.INSTANCE, (FlushRequest request, ActionListener<BroadcastResponse> flushResponseActionListener) -> {
            assertArrayEquals(new String[] { GeoIpDownloader.DATABASES_INDEX }, request.indices());
            flushResponseActionListener.onResponse(mock(BroadcastResponse.class));
        });
        client.addHandler(
            RefreshAction.INSTANCE,
            (RefreshRequest request, ActionListener<BroadcastResponse> flushResponseActionListener) -> {
                assertArrayEquals(new String[] { GeoIpDownloader.DATABASES_INDEX }, request.indices());
                flushResponseActionListener.onResponse(mock(BroadcastResponse.class));
            }
        );

        InputStream empty = new ByteArrayInputStream(new byte[0]);
        assertEquals(0, geoIpDownloader.indexChunks("test", empty, 0, "d41d8cd98f00b204e9800998ecf8427e", 0));
    }

    public void testIndexChunksMd5Mismatch() {
        client.addHandler(FlushAction.INSTANCE, (FlushRequest request, ActionListener<BroadcastResponse> flushResponseActionListener) -> {
            assertArrayEquals(new String[] { GeoIpDownloader.DATABASES_INDEX }, request.indices());
            flushResponseActionListener.onResponse(mock(BroadcastResponse.class));
        });
        client.addHandler(
            RefreshAction.INSTANCE,
            (RefreshRequest request, ActionListener<BroadcastResponse> flushResponseActionListener) -> {
                assertArrayEquals(new String[] { GeoIpDownloader.DATABASES_INDEX }, request.indices());
                flushResponseActionListener.onResponse(mock(BroadcastResponse.class));
            }
        );

        IOException exception = expectThrows(
            IOException.class,
            () -> geoIpDownloader.indexChunks("test", new ByteArrayInputStream(new byte[0]), 0, "123123", 0)
        );
        assertEquals("md5 checksum mismatch, expected [123123], actual [d41d8cd98f00b204e9800998ecf8427e]", exception.getMessage());
    }

    public void testIndexChunks() throws IOException {
        byte[] bigArray = new byte[MAX_CHUNK_SIZE + 20];
        for (int i = 0; i < MAX_CHUNK_SIZE + 20; i++) {
            bigArray[i] = (byte) i;
        }
        byte[][] chunksData = new byte[2][];
        chunksData[0] = new byte[MAX_CHUNK_SIZE];
        System.arraycopy(bigArray, 0, chunksData[0], 0, MAX_CHUNK_SIZE);
        chunksData[1] = new byte[20];
        System.arraycopy(bigArray, MAX_CHUNK_SIZE, chunksData[1], 0, 20);

        AtomicInteger chunkIndex = new AtomicInteger();

        client.addHandler(TransportIndexAction.TYPE, (IndexRequest request, ActionListener<DocWriteResponse> listener) -> {
            int chunk = chunkIndex.getAndIncrement();
            assertEquals(OpType.CREATE, request.opType());
            assertThat(request.id(), Matchers.startsWith("test_" + (chunk + 15) + "_"));
            assertEquals(XContentType.SMILE, request.getContentType());
            Map<String, Object> source = request.sourceAsMap();
            assertEquals("test", source.get("name"));
            assertArrayEquals(chunksData[chunk], (byte[]) source.get("data"));
            assertEquals(chunk + 15, source.get("chunk"));
            listener.onResponse(mock(IndexResponse.class));
        });
        client.addHandler(FlushAction.INSTANCE, (FlushRequest request, ActionListener<BroadcastResponse> flushResponseActionListener) -> {
            assertArrayEquals(new String[] { GeoIpDownloader.DATABASES_INDEX }, request.indices());
            flushResponseActionListener.onResponse(mock(BroadcastResponse.class));
        });
        client.addHandler(
            RefreshAction.INSTANCE,
            (RefreshRequest request, ActionListener<BroadcastResponse> flushResponseActionListener) -> {
                assertArrayEquals(new String[] { GeoIpDownloader.DATABASES_INDEX }, request.indices());
                flushResponseActionListener.onResponse(mock(BroadcastResponse.class));
            }
        );

        InputStream big = new ByteArrayInputStream(bigArray);
        assertEquals(17, geoIpDownloader.indexChunks("test", big, 15, "a67563dfa8f3cba8b8cff61eb989a749", 0));

        assertEquals(2, chunkIndex.get());
    }

    public void testProcessDatabaseNew() throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(new byte[0]);
        when(httpClient.get("http://a.b/t1")).thenReturn(bais);

        geoIpDownloader = new GeoIpDownloader(
            client,
            httpClient,
            clusterService,
            threadPool,
            Settings.EMPTY,
            1,
            "",
            "",
            "",
            EMPTY_TASK_ID,
            Map.of(),
            () -> GeoIpDownloaderTaskExecutor.POLL_INTERVAL_SETTING.getDefault(Settings.EMPTY),
            () -> GeoIpDownloaderTaskExecutor.EAGER_DOWNLOAD_SETTING.getDefault(Settings.EMPTY),
            () -> true
        ) {
            @Override
            protected void updateTimestamp(String name, GeoIpTaskState.Metadata metadata) {
                fail();
            }

            @Override
            int indexChunks(String name, InputStream is, int chunk, String expectedMd5, long start) {
                assertSame(bais, is);
                assertEquals(0, chunk);
                return 11;
            }

            @Override
            void updateTaskState() {
                assertEquals(0, state.getDatabases().get("test.mmdb").firstChunk());
                assertEquals(10, state.getDatabases().get("test.mmdb").lastChunk());
            }

            @Override
            void deleteOldChunks(String name, int firstChunk) {
                assertEquals("test.mmdb", name);
                assertEquals(0, firstChunk);
            }
        };

        geoIpDownloader.setState(GeoIpTaskState.EMPTY);
        geoIpDownloader.processDatabase(Map.of("name", "test.tgz", "url", "http://a.b/t1", "md5_hash", "1"));
        GeoIpDownloaderStats stats = geoIpDownloader.getStatus();
        assertEquals(0, stats.getFailedDownloads());
    }

    public void testProcessDatabaseUpdate() throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(new byte[0]);
        when(httpClient.get("http://a.b/t1")).thenReturn(bais);

        geoIpDownloader = new GeoIpDownloader(
            client,
            httpClient,
            clusterService,
            threadPool,
            Settings.EMPTY,
            1,
            "",
            "",
            "",
            EMPTY_TASK_ID,
            Map.of(),
            () -> GeoIpDownloaderTaskExecutor.POLL_INTERVAL_SETTING.getDefault(Settings.EMPTY),
            () -> GeoIpDownloaderTaskExecutor.EAGER_DOWNLOAD_SETTING.getDefault(Settings.EMPTY),
            () -> true
        ) {
            @Override
            protected void updateTimestamp(String name, GeoIpTaskState.Metadata metadata) {
                fail();
            }

            @Override
            int indexChunks(String name, InputStream is, int chunk, String expectedMd5, long start) {
                assertSame(bais, is);
                assertEquals(9, chunk);
                return 11;
            }

            @Override
            void updateTaskState() {
                assertEquals(9, state.getDatabases().get("test.mmdb").firstChunk());
                assertEquals(10, state.getDatabases().get("test.mmdb").lastChunk());
            }

            @Override
            void deleteOldChunks(String name, int firstChunk) {
                assertEquals("test.mmdb", name);
                assertEquals(9, firstChunk);
            }
        };

        geoIpDownloader.setState(GeoIpTaskState.EMPTY.put("test.mmdb", new GeoIpTaskState.Metadata(0, 5, 8, "0", 0)));
        geoIpDownloader.processDatabase(Map.of("name", "test.tgz", "url", "http://a.b/t1", "md5_hash", "1"));
        GeoIpDownloaderStats stats = geoIpDownloader.getStatus();
        assertEquals(0, stats.getFailedDownloads());
    }

    public void testProcessDatabaseSame() throws IOException {
        GeoIpTaskState.Metadata metadata = new GeoIpTaskState.Metadata(0, 4, 10, "1", 0);
        GeoIpTaskState taskState = GeoIpTaskState.EMPTY.put("test.mmdb", metadata);
        ByteArrayInputStream bais = new ByteArrayInputStream(new byte[0]);
        when(httpClient.get("a.b/t1")).thenReturn(bais);

        geoIpDownloader = new GeoIpDownloader(
            client,
            httpClient,
            clusterService,
            threadPool,
            Settings.EMPTY,
            1,
            "",
            "",
            "",
            EMPTY_TASK_ID,
            Map.of(),
            () -> GeoIpDownloaderTaskExecutor.POLL_INTERVAL_SETTING.getDefault(Settings.EMPTY),
            () -> GeoIpDownloaderTaskExecutor.EAGER_DOWNLOAD_SETTING.getDefault(Settings.EMPTY),
            () -> true
        ) {
            @Override
            protected void updateTimestamp(String name, GeoIpTaskState.Metadata newMetadata) {
                assertEquals(metadata, newMetadata);
                assertEquals("test.mmdb", name);
            }

            @Override
            int indexChunks(String name, InputStream is, int chunk, String expectedMd5, long start) {
                fail();
                return 0;
            }

            @Override
            void updateTaskState() {
                fail();
            }

            @Override
            void deleteOldChunks(String name, int firstChunk) {
                fail();
            }
        };
        geoIpDownloader.setState(taskState);
        geoIpDownloader.processDatabase(Map.of("name", "test.tgz", "url", "http://a.b/t1", "md5_hash", "1"));
        GeoIpDownloaderStats stats = geoIpDownloader.getStatus();
        assertEquals(0, stats.getFailedDownloads());
    }

    public void testCleanDatabases() throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(new byte[0]);
        when(httpClient.get("http://a.b/t1")).thenReturn(bais);

        final AtomicInteger count = new AtomicInteger(0);

        geoIpDownloader = new GeoIpDownloader(
            client,
            httpClient,
            clusterService,
            threadPool,
            Settings.EMPTY,
            1,
            "",
            "",
            "",
            EMPTY_TASK_ID,
            Map.of(),
            () -> GeoIpDownloaderTaskExecutor.POLL_INTERVAL_SETTING.getDefault(Settings.EMPTY),
            () -> GeoIpDownloaderTaskExecutor.EAGER_DOWNLOAD_SETTING.getDefault(Settings.EMPTY),
            () -> true
        ) {
            @Override
            void updateDatabases() throws IOException {
                // noop
            }

            @Override
            void deleteOldChunks(String name, int firstChunk) {
                count.incrementAndGet();
                assertEquals("test.mmdb", name);
                assertEquals(21, firstChunk);
            }

            @Override
            void updateTaskState() {
                // noop
            }
        };

        geoIpDownloader.setState(GeoIpTaskState.EMPTY.put("test.mmdb", new GeoIpTaskState.Metadata(10, 10, 20, "md5", 20)));
        geoIpDownloader.runDownloader();
        geoIpDownloader.runDownloader();
        GeoIpDownloaderStats stats = geoIpDownloader.getStatus();
        assertEquals(1, stats.getExpiredDatabases());
        assertEquals(2, count.get()); // somewhat surprising, not necessarily wrong
        assertEquals(18, geoIpDownloader.state.getDatabases().get("test.mmdb").lastCheck()); // highly surprising, seems wrong
    }

    @SuppressWarnings("unchecked")
    public void testUpdateTaskState() {
        geoIpDownloader = new GeoIpDownloader(
            client,
            httpClient,
            clusterService,
            threadPool,
            Settings.EMPTY,
            1,
            "",
            "",
            "",
            EMPTY_TASK_ID,
            Map.of(),
            () -> GeoIpDownloaderTaskExecutor.POLL_INTERVAL_SETTING.getDefault(Settings.EMPTY),
            () -> GeoIpDownloaderTaskExecutor.EAGER_DOWNLOAD_SETTING.getDefault(Settings.EMPTY),
            () -> true
        ) {
            @Override
            public void updatePersistentTaskState(PersistentTaskState state, ActionListener<PersistentTask<?>> listener) {
                assertSame(GeoIpTaskState.EMPTY, state);
                PersistentTask<?> task = mock(PersistentTask.class);
                when(task.getState()).thenReturn(GeoIpTaskState.EMPTY);
                listener.onResponse(task);
            }
        };
        geoIpDownloader.setState(GeoIpTaskState.EMPTY);
        geoIpDownloader.updateTaskState();
    }

    @SuppressWarnings("unchecked")
    public void testUpdateTaskStateError() {
        geoIpDownloader = new GeoIpDownloader(
            client,
            httpClient,
            clusterService,
            threadPool,
            Settings.EMPTY,
            1,
            "",
            "",
            "",
            EMPTY_TASK_ID,
            Map.of(),
            () -> GeoIpDownloaderTaskExecutor.POLL_INTERVAL_SETTING.getDefault(Settings.EMPTY),
            () -> GeoIpDownloaderTaskExecutor.EAGER_DOWNLOAD_SETTING.getDefault(Settings.EMPTY),
            () -> true
        ) {
            @Override
            public void updatePersistentTaskState(PersistentTaskState state, ActionListener<PersistentTask<?>> listener) {
                assertSame(GeoIpTaskState.EMPTY, state);
                PersistentTask<?> task = mock(PersistentTask.class);
                when(task.getState()).thenReturn(GeoIpTaskState.EMPTY);
                listener.onFailure(new IllegalStateException("test failure"));
            }
        };
        geoIpDownloader.setState(GeoIpTaskState.EMPTY);
        IllegalStateException exception = expectThrows(IllegalStateException.class, geoIpDownloader::updateTaskState);
        assertEquals("test failure", exception.getMessage());
    }

    public void testUpdateDatabases() throws IOException {
        List<Map<String, Object>> maps = List.of(Map.of("a", 1, "name", "a.tgz"), Map.of("a", 2, "name", "a.tgz"));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        XContentBuilder builder = new XContentBuilder(XContentType.JSON.xContent(), baos);
        builder.startArray();
        builder.map(Map.of("a", 1, "name", "a.tgz"));
        builder.map(Map.of("a", 2, "name", "a.tgz"));
        builder.endArray();
        builder.close();
        when(httpClient.getBytes("a.b?elastic_geoip_service_tos=agree")).thenReturn(baos.toByteArray());
        Iterator<Map<String, Object>> it = maps.iterator();
        final AtomicBoolean atLeastOneGeoipProcessor = new AtomicBoolean(false);
        geoIpDownloader = new GeoIpDownloader(
            client,
            httpClient,
            clusterService,
            threadPool,
            Settings.builder().put(ENDPOINT_SETTING.getKey(), "a.b").build(),
            1,
            "",
            "",
            "",
            EMPTY_TASK_ID,
            Map.of(),
            () -> GeoIpDownloaderTaskExecutor.POLL_INTERVAL_SETTING.getDefault(Settings.EMPTY),
            () -> GeoIpDownloaderTaskExecutor.EAGER_DOWNLOAD_SETTING.getDefault(Settings.EMPTY),
            atLeastOneGeoipProcessor::get
        ) {
            @Override
            void processDatabase(Map<String, Object> databaseInfo) {
                assertEquals(it.next(), databaseInfo);
            }
        };
        geoIpDownloader.updateDatabases();
        assertTrue(it.hasNext());
        atLeastOneGeoipProcessor.set(true);
        geoIpDownloader.updateDatabases();
        assertFalse(it.hasNext());
    }

    public void testUpdateDatabasesWriteBlock() {
        ClusterState state = createClusterState(new PersistentTasksCustomMetadata(1L, Map.of()));
        var geoIpIndex = state.getMetadata().getIndicesLookup().get(GeoIpDownloader.DATABASES_INDEX).getWriteIndex().getName();
        state = ClusterState.builder(state)
            .blocks(new ClusterBlocks.Builder().addIndexBlock(geoIpIndex, IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK))
            .build();
        when(clusterService.state()).thenReturn(state);
        var e = expectThrows(ClusterBlockException.class, () -> geoIpDownloader.updateDatabases());
        assertThat(
            e.getMessage(),
            equalTo(
                "index ["
                    + geoIpIndex
                    + "] blocked by: [TOO_MANY_REQUESTS/12/disk usage exceeded flood-stage watermark, "
                    + "index has read-only-allow-delete block];"
            )
        );
        verifyNoInteractions(httpClient);
    }

    public void testUpdateDatabasesIndexNotReady() {
        ClusterState state = createClusterState(new PersistentTasksCustomMetadata(1L, Map.of()), true);
        var geoIpIndex = state.getMetadata().getIndicesLookup().get(GeoIpDownloader.DATABASES_INDEX).getWriteIndex().getName();
        state = ClusterState.builder(state)
            .blocks(new ClusterBlocks.Builder().addIndexBlock(geoIpIndex, IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK))
            .build();
        when(clusterService.state()).thenReturn(state);
        var e = expectThrows(ElasticsearchException.class, () -> geoIpDownloader.updateDatabases());
        assertThat(e.getMessage(), equalTo("not all primary shards of [.geoip_databases] index are active"));
        verifyNoInteractions(httpClient);
    }

    public void testThatRunDownloaderDeletesExpiredDatabases() {
        /*
         * This test puts some expired databases and some non-expired ones into the GeoIpTaskState, and then calls runDownloader(), making
         * sure that the expired databases have been deleted.
         */
        AtomicInteger updatePersistentTaskStateCount = new AtomicInteger(0);
        AtomicInteger deleteCount = new AtomicInteger(0);
        int expiredDatabasesCount = randomIntBetween(1, 100);
        int unexpiredDatabasesCount = randomIntBetween(0, 100);
        Map<String, GeoIpTaskState.Metadata> databases = new HashMap<>();
        for (int i = 0; i < expiredDatabasesCount; i++) {
            databases.put("expiredDatabase" + i, newGeoIpTaskStateMetadata(true));
        }
        for (int i = 0; i < unexpiredDatabasesCount; i++) {
            databases.put("unexpiredDatabase" + i, newGeoIpTaskStateMetadata(false));
        }
        GeoIpTaskState geoIpTaskState = new GeoIpTaskState(databases);
        geoIpDownloader.setState(geoIpTaskState);
        client.addHandler(
            UpdatePersistentTaskStatusAction.INSTANCE,
            (UpdatePersistentTaskStatusAction.Request request, ActionListener<PersistentTaskResponse> taskResponseListener) -> {
                PersistentTasksCustomMetadata.Assignment assignment = mock(PersistentTasksCustomMetadata.Assignment.class);
                PersistentTasksCustomMetadata.PersistentTask<?> persistentTask = new PersistentTasksCustomMetadata.PersistentTask<>(
                    GeoIpDownloader.GEOIP_DOWNLOADER,
                    GeoIpDownloader.GEOIP_DOWNLOADER,
                    new GeoIpTaskParams(),
                    request.getAllocationId(),
                    assignment
                );
                updatePersistentTaskStateCount.incrementAndGet();
                taskResponseListener.onResponse(new PersistentTaskResponse(new PersistentTask<>(persistentTask, request.getState())));
            }
        );
        client.addHandler(
            DeleteByQueryAction.INSTANCE,
            (DeleteByQueryRequest request, ActionListener<BulkByScrollResponse> flushResponseActionListener) -> {
                deleteCount.incrementAndGet();
            }
        );
        geoIpDownloader.runDownloader();
        assertThat(geoIpDownloader.getStatus().getExpiredDatabases(), equalTo(expiredDatabasesCount));
        for (int i = 0; i < expiredDatabasesCount; i++) {
            // This currently fails because we subtract one millisecond from the lastChecked time
            // assertThat(geoIpDownloader.state.getDatabases().get("expiredDatabase" + i).lastCheck(), equalTo(-1L));
        }
        for (int i = 0; i < unexpiredDatabasesCount; i++) {
            assertThat(
                geoIpDownloader.state.getDatabases().get("unexpiredDatabase" + i).lastCheck(),
                greaterThanOrEqualTo(Instant.now().minus(30, ChronoUnit.DAYS).toEpochMilli())
            );
        }
        assertThat(deleteCount.get(), equalTo(expiredDatabasesCount));
        assertThat(updatePersistentTaskStateCount.get(), equalTo(expiredDatabasesCount));
        geoIpDownloader.runDownloader();
        /*
         * The following two lines assert current behavior that might not be desirable -- we continue to delete expired databases every
         * time that runDownloader runs. This seems unnecessary.
         */
        assertThat(deleteCount.get(), equalTo(expiredDatabasesCount * 2));
        assertThat(updatePersistentTaskStateCount.get(), equalTo(expiredDatabasesCount * 2));
    }

    private GeoIpTaskState.Metadata newGeoIpTaskStateMetadata(boolean expired) {
        Instant lastChecked;
        if (expired) {
            lastChecked = Instant.now().minus(randomIntBetween(31, 100), ChronoUnit.DAYS);
        } else {
            lastChecked = Instant.now().minus(randomIntBetween(0, 29), ChronoUnit.DAYS);
        }
        return new GeoIpTaskState.Metadata(0, 0, 0, randomAlphaOfLength(20), lastChecked.toEpochMilli());
    }

    private static class MockClient extends NoOpClient {

        private final Map<ActionType<?>, BiConsumer<? extends ActionRequest, ? extends ActionListener<?>>> handlers = new HashMap<>();

        private MockClient(ThreadPool threadPool) {
            super(threadPool);
        }

        public <Response extends ActionResponse, Request extends ActionRequest> void addHandler(
            ActionType<Response> action,
            BiConsumer<Request, ActionListener<Response>> listener
        ) {
            handlers.put(action, listener);
        }

        @SuppressWarnings("unchecked")
        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if (handlers.containsKey(action)) {
                BiConsumer<ActionRequest, ActionListener<?>> biConsumer = (BiConsumer<ActionRequest, ActionListener<?>>) handlers.get(
                    action
                );
                biConsumer.accept(request, listener);
            } else {
                throw new IllegalStateException("unexpected action called [" + action.name() + "]");
            }
        }
    }
}
