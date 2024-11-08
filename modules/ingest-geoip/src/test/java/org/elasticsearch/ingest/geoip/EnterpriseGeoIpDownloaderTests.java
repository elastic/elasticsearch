/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

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
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.ingest.EnterpriseGeoIpTask;
import org.elasticsearch.ingest.geoip.EnterpriseGeoIpDownloader.Checksum;
import org.elasticsearch.ingest.geoip.direct.DatabaseConfiguration;
import org.elasticsearch.node.Node;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.DefaultBuiltInExecutorBuilders;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static org.elasticsearch.ingest.geoip.DatabaseNodeServiceTests.createClusterState;
import static org.elasticsearch.ingest.geoip.EnterpriseGeoIpDownloader.MAX_CHUNK_SIZE;
import static org.elasticsearch.tasks.TaskId.EMPTY_TASK_ID;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class EnterpriseGeoIpDownloaderTests extends ESTestCase {

    private HttpClient httpClient;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private MockClient client;
    private EnterpriseGeoIpDownloader geoIpDownloader;

    @Before
    public void setup() throws IOException {
        httpClient = mock(HttpClient.class);
        when(httpClient.getBytes(any(), anyString())).thenReturn(
            "e4a3411cdd7b21eaf18675da5a7f9f360d33c6882363b2c19c38715834c9e836  GeoIP2-City_20240709.tar.gz".getBytes(StandardCharsets.UTF_8)
        );
        clusterService = mock(ClusterService.class);
        threadPool = new ThreadPool(
            Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "test").build(),
            MeterRegistry.NOOP,
            new DefaultBuiltInExecutorBuilders()
        );
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(Settings.EMPTY, Set.of(GeoIpDownloaderTaskExecutor.POLL_INTERVAL_SETTING))
        );
        ClusterState state = createClusterState(new PersistentTasksCustomMetadata(1L, Map.of()));
        when(clusterService.state()).thenReturn(state);
        client = new MockClient(threadPool);
        geoIpDownloader = new EnterpriseGeoIpDownloader(
            client,
            httpClient,
            clusterService,
            threadPool,
            1,
            "",
            "",
            "",
            EMPTY_TASK_ID,
            Map.of(),
            () -> GeoIpDownloaderTaskExecutor.POLL_INTERVAL_SETTING.getDefault(Settings.EMPTY),
            (type) -> "password".toCharArray()
        ) {
            {
                EnterpriseGeoIpTask.EnterpriseGeoIpTaskParams geoIpTaskParams = mock(EnterpriseGeoIpTask.EnterpriseGeoIpTaskParams.class);
                when(geoIpTaskParams.getWriteableName()).thenReturn(EnterpriseGeoIpTask.ENTERPRISE_GEOIP_DOWNLOADER);
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
        byte[] chunk = EnterpriseGeoIpDownloader.getChunk(new InputStream() {
            @Override
            public int read() {
                return -1;
            }
        });
        assertArrayEquals(new byte[0], chunk);
        chunk = EnterpriseGeoIpDownloader.getChunk(new ByteArrayInputStream(new byte[0]));
        assertArrayEquals(new byte[0], chunk);
    }

    public void testGetChunkLessThanChunkSize() throws IOException {
        ByteArrayInputStream is = new ByteArrayInputStream(new byte[] { 1, 2, 3, 4 });
        byte[] chunk = EnterpriseGeoIpDownloader.getChunk(is);
        assertArrayEquals(new byte[] { 1, 2, 3, 4 }, chunk);
        chunk = EnterpriseGeoIpDownloader.getChunk(is);
        assertArrayEquals(new byte[0], chunk);

    }

    public void testGetChunkExactlyChunkSize() throws IOException {
        byte[] bigArray = new byte[MAX_CHUNK_SIZE];
        for (int i = 0; i < MAX_CHUNK_SIZE; i++) {
            bigArray[i] = (byte) i;
        }
        ByteArrayInputStream is = new ByteArrayInputStream(bigArray);
        byte[] chunk = EnterpriseGeoIpDownloader.getChunk(is);
        assertArrayEquals(bigArray, chunk);
        chunk = EnterpriseGeoIpDownloader.getChunk(is);
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
        byte[] chunk = EnterpriseGeoIpDownloader.getChunk(is);
        assertArrayEquals(smallArray, chunk);
        System.arraycopy(bigArray, MAX_CHUNK_SIZE, smallArray, 0, MAX_CHUNK_SIZE);
        chunk = EnterpriseGeoIpDownloader.getChunk(is);
        assertArrayEquals(smallArray, chunk);
        chunk = EnterpriseGeoIpDownloader.getChunk(is);
        assertArrayEquals(new byte[0], chunk);
    }

    public void testGetChunkRethrowsIOException() {
        expectThrows(IOException.class, () -> EnterpriseGeoIpDownloader.getChunk(new InputStream() {
            @Override
            public int read() throws IOException {
                throw new IOException();
            }
        }));
    }

    public void testIndexChunksNoData() throws IOException {
        client.addHandler(FlushAction.INSTANCE, (FlushRequest request, ActionListener<BroadcastResponse> flushResponseActionListener) -> {
            assertArrayEquals(new String[] { EnterpriseGeoIpDownloader.DATABASES_INDEX }, request.indices());
            flushResponseActionListener.onResponse(mock(BroadcastResponse.class));
        });
        client.addHandler(
            RefreshAction.INSTANCE,
            (RefreshRequest request, ActionListener<BroadcastResponse> flushResponseActionListener) -> {
                assertArrayEquals(new String[] { EnterpriseGeoIpDownloader.DATABASES_INDEX }, request.indices());
                flushResponseActionListener.onResponse(mock(BroadcastResponse.class));
            }
        );

        InputStream empty = new ByteArrayInputStream(new byte[0]);
        assertEquals(
            Tuple.tuple(0, "d41d8cd98f00b204e9800998ecf8427e"),
            geoIpDownloader.indexChunks(
                "test",
                empty,
                0,
                Checksum.sha256("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
                0
            )
        );
    }

    public void testIndexChunksMd5Mismatch() {
        client.addHandler(FlushAction.INSTANCE, (FlushRequest request, ActionListener<BroadcastResponse> flushResponseActionListener) -> {
            assertArrayEquals(new String[] { EnterpriseGeoIpDownloader.DATABASES_INDEX }, request.indices());
            flushResponseActionListener.onResponse(mock(BroadcastResponse.class));
        });
        client.addHandler(
            RefreshAction.INSTANCE,
            (RefreshRequest request, ActionListener<BroadcastResponse> flushResponseActionListener) -> {
                assertArrayEquals(new String[] { EnterpriseGeoIpDownloader.DATABASES_INDEX }, request.indices());
                flushResponseActionListener.onResponse(mock(BroadcastResponse.class));
            }
        );

        IOException exception = expectThrows(
            IOException.class,
            () -> geoIpDownloader.indexChunks("test", new ByteArrayInputStream(new byte[0]), 0, Checksum.sha256("123123"), 0)
        );
        assertEquals(
            "checksum mismatch, expected [123123], actual [e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855]",
            exception.getMessage()
        );
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
            assertArrayEquals(new String[] { EnterpriseGeoIpDownloader.DATABASES_INDEX }, request.indices());
            flushResponseActionListener.onResponse(mock(BroadcastResponse.class));
        });
        client.addHandler(
            RefreshAction.INSTANCE,
            (RefreshRequest request, ActionListener<BroadcastResponse> flushResponseActionListener) -> {
                assertArrayEquals(new String[] { EnterpriseGeoIpDownloader.DATABASES_INDEX }, request.indices());
                flushResponseActionListener.onResponse(mock(BroadcastResponse.class));
            }
        );

        InputStream big = new ByteArrayInputStream(bigArray);
        assertEquals(
            Tuple.tuple(17, "a67563dfa8f3cba8b8cff61eb989a749"),
            geoIpDownloader.indexChunks(
                "test",
                big,
                15,
                Checksum.sha256("f2304545f224ff9ffcc585cb0a993723f911e03beb552cc03937dd443e931eab"),
                0
            )
        );

        assertEquals(2, chunkIndex.get());
    }

    public void testProcessDatabaseNew() throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(new byte[0]);
        when(httpClient.get(any(), any())).thenReturn(bais);
        AtomicBoolean indexedChunks = new AtomicBoolean(false);
        geoIpDownloader = new EnterpriseGeoIpDownloader(
            client,
            httpClient,
            clusterService,
            threadPool,
            1,
            "",
            "",
            "",
            EMPTY_TASK_ID,
            Map.of(),
            () -> GeoIpDownloaderTaskExecutor.POLL_INTERVAL_SETTING.getDefault(Settings.EMPTY),
            (type) -> "password".toCharArray()
        ) {
            @Override
            protected void updateTimestamp(String name, GeoIpTaskState.Metadata metadata) {
                fail();
            }

            @Override
            Tuple<Integer, String> indexChunks(String name, InputStream is, int chunk, Checksum checksum, long start) {
                assertSame(bais, is);
                assertEquals(0, chunk);
                indexedChunks.set(true);
                return Tuple.tuple(11, checksum.checksum());
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

        geoIpDownloader.setState(EnterpriseGeoIpTaskState.EMPTY);
        String id = randomIdentifier();
        DatabaseConfiguration databaseConfiguration = new DatabaseConfiguration(id, "test", new DatabaseConfiguration.Maxmind("name"));
        geoIpDownloader.processDatabase(id, databaseConfiguration);
        assertThat(indexedChunks.get(), equalTo(true));
    }

    public void testProcessDatabaseUpdate() throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(new byte[0]);
        when(httpClient.get(any(), any())).thenReturn(bais);
        AtomicBoolean indexedChunks = new AtomicBoolean(false);
        geoIpDownloader = new EnterpriseGeoIpDownloader(
            client,
            httpClient,
            clusterService,
            threadPool,
            1,
            "",
            "",
            "",
            EMPTY_TASK_ID,
            Map.of(),
            () -> GeoIpDownloaderTaskExecutor.POLL_INTERVAL_SETTING.getDefault(Settings.EMPTY),
            (type) -> "password".toCharArray()
        ) {
            @Override
            protected void updateTimestamp(String name, GeoIpTaskState.Metadata metadata) {
                fail();
            }

            @Override
            Tuple<Integer, String> indexChunks(String name, InputStream is, int chunk, Checksum checksum, long start) {
                assertSame(bais, is);
                assertEquals(9, chunk);
                indexedChunks.set(true);
                return Tuple.tuple(1, checksum.checksum());
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

        geoIpDownloader.setState(EnterpriseGeoIpTaskState.EMPTY.put("test.mmdb", new GeoIpTaskState.Metadata(0, 5, 8, "0", 0)));
        String id = randomIdentifier();
        DatabaseConfiguration databaseConfiguration = new DatabaseConfiguration(id, "test", new DatabaseConfiguration.Maxmind("name"));
        geoIpDownloader.processDatabase(id, databaseConfiguration);
        assertThat(indexedChunks.get(), equalTo(true));
    }

    public void testProcessDatabaseSame() throws IOException {
        GeoIpTaskState.Metadata metadata = new GeoIpTaskState.Metadata(
            0,
            4,
            10,
            "1",
            0,
            "e4a3411cdd7b21eaf18675da5a7f9f360d33c6882363b2c19c38715834c9e836"
        );
        EnterpriseGeoIpTaskState taskState = EnterpriseGeoIpTaskState.EMPTY.put("test.mmdb", metadata);
        ByteArrayInputStream bais = new ByteArrayInputStream(new byte[0]);
        when(httpClient.get(any(), any())).thenReturn(bais);

        geoIpDownloader = new EnterpriseGeoIpDownloader(
            client,
            httpClient,
            clusterService,
            threadPool,
            1,
            "",
            "",
            "",
            EMPTY_TASK_ID,
            Map.of(),
            () -> GeoIpDownloaderTaskExecutor.POLL_INTERVAL_SETTING.getDefault(Settings.EMPTY),
            (type) -> "password".toCharArray()
        ) {
            @Override
            protected void updateTimestamp(String name, GeoIpTaskState.Metadata newMetadata) {
                assertEquals(metadata, newMetadata);
                assertEquals("test.mmdb", name);
            }

            @Override
            Tuple<Integer, String> indexChunks(String name, InputStream is, int chunk, Checksum checksum, long start) {
                fail();
                return Tuple.tuple(0, checksum.checksum());
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
        String id = randomIdentifier();
        DatabaseConfiguration databaseConfiguration = new DatabaseConfiguration(id, "test", new DatabaseConfiguration.Maxmind("name"));
        geoIpDownloader.processDatabase(id, databaseConfiguration);
    }

    public void testUpdateDatabasesWriteBlock() {
        ClusterState state = createClusterState(new PersistentTasksCustomMetadata(1L, Map.of()));
        var geoIpIndex = state.getMetadata()
            .getProject()
            .getIndicesLookup()
            .get(EnterpriseGeoIpDownloader.DATABASES_INDEX)
            .getWriteIndex()
            .getName();
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
                    + "index has read-only-allow-delete block; for more information, see "
                    + ReferenceDocs.FLOOD_STAGE_WATERMARK
                    + "];"
            )
        );
        verifyNoInteractions(httpClient);
    }

    public void testUpdateDatabasesIndexNotReady() throws IOException {
        ClusterState state = createClusterState(new PersistentTasksCustomMetadata(1L, Map.of()), true);
        var geoIpIndex = state.getMetadata()
            .getProject()
            .getIndicesLookup()
            .get(EnterpriseGeoIpDownloader.DATABASES_INDEX)
            .getWriteIndex()
            .getName();
        state = ClusterState.builder(state)
            .blocks(new ClusterBlocks.Builder().addIndexBlock(geoIpIndex, IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK))
            .build();
        when(clusterService.state()).thenReturn(state);
        geoIpDownloader.updateDatabases();
        verifyNoInteractions(httpClient);
    }

    public void testMaxmindUrls() {
        // non-static classes have fun syntax, but it's nice to be able to test this behavior by itself
        final EnterpriseGeoIpDownloader.MaxmindDownload download = geoIpDownloader.new MaxmindDownload(
            "GeoLite2-City", new DatabaseConfiguration.Maxmind("account_id")
        );

        {
            String url = "https://download.maxmind.com/geoip/databases/GeoLite2-City/download?suffix=tar.gz";
            assertThat(download.url("tar.gz"), equalTo(url));
        }
        {
            String url = "https://download.maxmind.com/geoip/databases/GeoLite2-City/download?suffix=tar.gz.sha256";
            assertThat(download.url("tar.gz.sha256"), equalTo(url));
        }
    }

    public void testIpinfoUrls() {
        // a 'free' database like 'asn' has 'free/' in the url (automatically)
        final EnterpriseGeoIpDownloader.IpinfoDownload download = geoIpDownloader.new IpinfoDownload(
            "asn", new DatabaseConfiguration.Ipinfo()
        );

        {
            String url = "https://ipinfo.io/data/free/asn.mmdb";
            assertThat(download.url("mmdb"), equalTo(url));
        }
        {
            String url = "https://ipinfo.io/data/free/asn.mmdb/checksums";
            assertThat(download.url("mmdb/checksums"), equalTo(url));
        }

        // but a non-'free' database like 'standard_asn' does not
        final EnterpriseGeoIpDownloader.IpinfoDownload download2 = geoIpDownloader.new IpinfoDownload(
            "standard_asn", new DatabaseConfiguration.Ipinfo()
        );

        {
            String url = "https://ipinfo.io/data/standard_asn.mmdb";
            assertThat(download2.url("mmdb"), equalTo(url));
        }
        {
            String url = "https://ipinfo.io/data/standard_asn.mmdb/checksums";
            assertThat(download2.url("mmdb/checksums"), equalTo(url));
        }
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
