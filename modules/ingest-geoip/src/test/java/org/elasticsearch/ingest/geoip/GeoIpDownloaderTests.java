/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.admin.indices.flush.FlushAction;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.node.Node;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static org.elasticsearch.ingest.geoip.GeoIpDownloader.ENDPOINT_SETTING;
import static org.elasticsearch.ingest.geoip.GeoIpDownloader.MAX_CHUNK_SIZE;
import static org.elasticsearch.tasks.TaskId.EMPTY_TASK_ID;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GeoIpDownloaderTests extends ESTestCase {

    private HttpClient httpClient;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private MockClient client;
    private GeoIpDownloader geoIpDownloader;

    @Before
    public void setup() {
        httpClient = mock(HttpClient.class);
        clusterService = mock(ClusterService.class);
        threadPool = new ThreadPool(Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "test").build());
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(Settings.EMPTY,
            Set.of(GeoIpDownloader.ENDPOINT_SETTING, GeoIpDownloader.POLL_INTERVAL_SETTING, GeoIpDownloaderTaskExecutor.ENABLED_SETTING)));
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();
        when(clusterService.state()).thenReturn(state);
        client = new MockClient(threadPool);
        geoIpDownloader = new GeoIpDownloader(client, httpClient, clusterService, threadPool, Settings.EMPTY,
            1, "", "", "", EMPTY_TASK_ID, Collections.emptyMap());
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testGetChunkEndOfStream() throws IOException {
        byte[] chunk = geoIpDownloader.getChunk(new InputStream() {
            @Override
            public int read() {
                return -1;
            }
        });
        assertArrayEquals(new byte[0], chunk);
        chunk = geoIpDownloader.getChunk(new ByteArrayInputStream(new byte[0]));
        assertArrayEquals(new byte[0], chunk);
    }

    public void testGetChunkLessThanChunkSize() throws IOException {
        ByteArrayInputStream is = new ByteArrayInputStream(new byte[]{1, 2, 3, 4});
        byte[] chunk = geoIpDownloader.getChunk(is);
        assertArrayEquals(new byte[]{1, 2, 3, 4}, chunk);
        chunk = geoIpDownloader.getChunk(is);
        assertArrayEquals(new byte[0], chunk);

    }

    public void testGetChunkExactlyChunkSize() throws IOException {
        byte[] bigArray = new byte[MAX_CHUNK_SIZE];
        for (int i = 0; i < MAX_CHUNK_SIZE; i++) {
            bigArray[i] = (byte) i;
        }
        ByteArrayInputStream is = new ByteArrayInputStream(bigArray);
        byte[] chunk = geoIpDownloader.getChunk(is);
        assertArrayEquals(bigArray, chunk);
        chunk = geoIpDownloader.getChunk(is);
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
        byte[] chunk = geoIpDownloader.getChunk(is);
        assertArrayEquals(smallArray, chunk);
        System.arraycopy(bigArray, MAX_CHUNK_SIZE, smallArray, 0, MAX_CHUNK_SIZE);
        chunk = geoIpDownloader.getChunk(is);
        assertArrayEquals(smallArray, chunk);
        chunk = geoIpDownloader.getChunk(is);
        assertArrayEquals(new byte[0], chunk);
    }

    public void testGetChunkRethrowsIOException() {
        expectThrows(IOException.class, () -> geoIpDownloader.getChunk(new InputStream() {
            @Override
            public int read() throws IOException {
                throw new IOException();
            }
        }));
    }

    public void testIndexChunksNoData() throws IOException {
        client.addHandler(FlushAction.INSTANCE, (FlushRequest request, ActionListener<FlushResponse> flushResponseActionListener) -> {
            assertArrayEquals(new String[] {GeoIpDownloader.DATABASES_INDEX}, request.indices());
            flushResponseActionListener.onResponse(mock(FlushResponse.class));
        });
        client.addHandler(RefreshAction.INSTANCE, (RefreshRequest request, ActionListener<RefreshResponse> flushResponseActionListener) -> {
            assertArrayEquals(new String[] {GeoIpDownloader.DATABASES_INDEX}, request.indices());
            flushResponseActionListener.onResponse(mock(RefreshResponse.class));
        });

        InputStream empty = new ByteArrayInputStream(new byte[0]);
        assertEquals(0, geoIpDownloader.indexChunks("test", empty, 0, "d41d8cd98f00b204e9800998ecf8427e", 0));
    }

    public void testIndexChunksMd5Mismatch() {
        client.addHandler(FlushAction.INSTANCE, (FlushRequest request, ActionListener<FlushResponse> flushResponseActionListener) -> {
            assertArrayEquals(new String[] {GeoIpDownloader.DATABASES_INDEX}, request.indices());
            flushResponseActionListener.onResponse(mock(FlushResponse.class));
        });
        client.addHandler(RefreshAction.INSTANCE, (RefreshRequest request, ActionListener<RefreshResponse> flushResponseActionListener) -> {
            assertArrayEquals(new String[] {GeoIpDownloader.DATABASES_INDEX}, request.indices());
            flushResponseActionListener.onResponse(mock(RefreshResponse.class));
        });

        IOException exception = expectThrows(IOException.class, () -> geoIpDownloader.indexChunks("test",
            new ByteArrayInputStream(new byte[0]), 0, "123123", 0));
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

        client.addHandler(IndexAction.INSTANCE, (IndexRequest request, ActionListener<IndexResponse> listener) -> {
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
        client.addHandler(FlushAction.INSTANCE, (FlushRequest request, ActionListener<FlushResponse> flushResponseActionListener) -> {
            assertArrayEquals(new String[] {GeoIpDownloader.DATABASES_INDEX}, request.indices());
            flushResponseActionListener.onResponse(mock(FlushResponse.class));
        });
        client.addHandler(RefreshAction.INSTANCE, (RefreshRequest request, ActionListener<RefreshResponse> flushResponseActionListener) -> {
            assertArrayEquals(new String[] {GeoIpDownloader.DATABASES_INDEX}, request.indices());
            flushResponseActionListener.onResponse(mock(RefreshResponse.class));
        });

        InputStream big = new ByteArrayInputStream(bigArray);
        assertEquals(17, geoIpDownloader.indexChunks("test", big, 15, "a67563dfa8f3cba8b8cff61eb989a749", 0));

        assertEquals(2, chunkIndex.get());
    }

    public void testProcessDatabaseNew() throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(new byte[0]);
        when(httpClient.get("http://a.b/t1")).thenReturn(bais);

        geoIpDownloader = new GeoIpDownloader(client, httpClient, clusterService, threadPool, Settings.EMPTY,
            1, "", "", "", EMPTY_TASK_ID, Collections.emptyMap()) {
            @Override
            void updateTaskState() {
                assertEquals(0, state.get("test").getFirstChunk());
                assertEquals(10, state.get("test").getLastChunk());
            }

            @Override
            int indexChunks(String name, InputStream is, int chunk, String expectedMd5, long start) {
                assertSame(bais, is);
                assertEquals(0, chunk);
                return 11;
            }

            @Override
            protected void updateTimestamp(String name, GeoIpTaskState.Metadata metadata) {
                fail();
            }

            @Override
            void deleteOldChunks(String name, int firstChunk) {
                assertEquals("test", name);
                assertEquals(0, firstChunk);
            }
        };

        geoIpDownloader.setState(GeoIpTaskState.EMPTY);
        geoIpDownloader.processDatabase(Map.of("name", "test.tgz", "url", "http://a.b/t1", "md5_hash", "1"));
    }

    public void testProcessDatabaseUpdate() throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(new byte[0]);
        when(httpClient.get("http://a.b/t1")).thenReturn(bais);

        geoIpDownloader = new GeoIpDownloader(client, httpClient, clusterService, threadPool, Settings.EMPTY,
            1, "", "", "", EMPTY_TASK_ID, Collections.emptyMap()) {
            @Override
            void updateTaskState() {
                assertEquals(9, state.get("test.mmdb").getFirstChunk());
                assertEquals(10, state.get("test.mmdb").getLastChunk());
            }

            @Override
            int indexChunks(String name, InputStream is, int chunk, String expectedMd5, long start) {
                assertSame(bais, is);
                assertEquals(9, chunk);
                return 11;
            }

            @Override
            protected void updateTimestamp(String name, GeoIpTaskState.Metadata metadata) {
                fail();
            }

            @Override
            void deleteOldChunks(String name, int firstChunk) {
                assertEquals("test.mmdb", name);
                assertEquals(9, firstChunk);
            }
        };

        geoIpDownloader.setState(GeoIpTaskState.EMPTY.put("test.mmdb", new GeoIpTaskState.Metadata(0, 5, 8, "0", 0)));
        geoIpDownloader.processDatabase(Map.of("name", "test.tgz", "url", "http://a.b/t1", "md5_hash", "1"));
    }


    public void testProcessDatabaseSame() throws IOException {
        GeoIpTaskState.Metadata metadata = new GeoIpTaskState.Metadata(0, 4, 10, "1", 0);
        GeoIpTaskState taskState = GeoIpTaskState.EMPTY.put("test.mmdb", metadata);
        ByteArrayInputStream bais = new ByteArrayInputStream(new byte[0]);
        when(httpClient.get("a.b/t1")).thenReturn(bais);

        geoIpDownloader = new GeoIpDownloader(client, httpClient, clusterService, threadPool, Settings.EMPTY,
            1, "", "", "", EMPTY_TASK_ID, Collections.emptyMap()) {
            @Override
            void updateTaskState() {
                fail();
            }

            @Override
            int indexChunks(String name, InputStream is, int chunk, String expectedMd5, long start) {
                fail();
                return 0;
            }

            @Override
            protected void updateTimestamp(String name, GeoIpTaskState.Metadata newMetadata) {
                assertEquals(metadata, newMetadata);
                assertEquals("test.mmdb", name);
            }

            @Override
            void deleteOldChunks(String name, int firstChunk) {
                fail();
            }
        };
        geoIpDownloader.setState(taskState);
        geoIpDownloader.processDatabase(Map.of("name", "test.tgz", "url", "http://a.b/t1", "md5_hash", "1"));
    }

    @SuppressWarnings("unchecked")
    public void testUpdateTaskState() {
        geoIpDownloader = new GeoIpDownloader(client, httpClient, clusterService, threadPool, Settings.EMPTY,
            1, "", "", "", EMPTY_TASK_ID, Collections.emptyMap()) {
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
        geoIpDownloader = new GeoIpDownloader(client, httpClient, clusterService, threadPool, Settings.EMPTY,
            1, "", "", "", EMPTY_TASK_ID, Collections.emptyMap()) {
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
        when(httpClient.getBytes("a.b?elastic_geoip_service_tos=agree"))
            .thenReturn(baos.toByteArray());
        Iterator<Map<String, Object>> it = maps.iterator();
        geoIpDownloader = new GeoIpDownloader(client, httpClient, clusterService, threadPool,
            Settings.builder().put(ENDPOINT_SETTING.getKey(), "a.b").build(),
            1, "", "", "", EMPTY_TASK_ID, Collections.emptyMap()) {
            @Override
            void processDatabase(Map<String, Object> databaseInfo) {
                assertEquals(it.next(), databaseInfo);
            }
        };
        geoIpDownloader.updateDatabases();
        assertFalse(it.hasNext());
    }

    private static class MockClient extends NoOpClient {

        private final Map<ActionType<?>, BiConsumer<? extends ActionRequest, ? extends ActionListener<?>>> handlers = new HashMap<>();

        private MockClient(ThreadPool threadPool) {
            super(threadPool);
        }

        public <Response extends ActionResponse, Request extends ActionRequest> void addHandler(ActionType<Response> action,
                                                                                                BiConsumer<Request,
                                                                                                    ActionListener<Response>> listener) {
            handlers.put(action, listener);
        }

        @SuppressWarnings("unchecked")
        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(ActionType<Response> action,
                                                                                                  Request request,
                                                                                                  ActionListener<Response> listener) {
            if (handlers.containsKey(action)) {
                BiConsumer<ActionRequest, ActionListener<?>> biConsumer =
                    (BiConsumer<ActionRequest, ActionListener<?>>) handlers.get(action);
                biConsumer.accept(request, listener);
            } else {
                throw new IllegalStateException("unexpected action called [" + action.name() + "]");
            }
        }
    }
}
