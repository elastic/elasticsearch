/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest;

import org.apache.http.ConnectionClosedException;
import org.apache.http.HttpResponse;
import org.apache.http.MalformedChunkCodingException;
import org.apache.http.nio.ContentDecoder;
import org.apache.http.nio.IOControl;
import org.apache.http.nio.protocol.HttpAsyncResponseConsumer;
import org.apache.http.protocol.HttpContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThrottledIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.elasticsearch.rest.ChunkedZipResponse.ZIP_CONTENT_TYPE;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;

@ESIntegTestCase.ClusterScope(numDataNodes = 1)
public class ChunkedZipResponseIT extends ESIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopyNoNullElements(super.nodePlugins(), RandomZipResponsePlugin.class);
    }

    public static class RandomZipResponsePlugin extends Plugin implements ActionPlugin {

        public static final String ROUTE = "/_random_zip_response";
        public static final String RESPONSE_FILENAME = "test-response";

        public static final String INFINITE_ROUTE = "/_infinite_zip_response";
        public static final String GET_NEXT_PART_COUNT_DOWN_PARAM = "getNextPartCountDown";

        public final AtomicReference<Response> responseRef = new AtomicReference<>();

        public record EntryPart(List<BytesReference> chunks) {
            public EntryPart {
                Objects.requireNonNull(chunks);
            }
        }

        public record EntryBody(List<EntryPart> parts) {
            public EntryBody {
                Objects.requireNonNull(parts);
            }
        }

        public record Response(Map<String, EntryBody> entries, CountDownLatch completedLatch) {}

        @Override
        public Collection<RestHandler> getRestHandlers(
            Settings settings,
            NamedWriteableRegistry namedWriteableRegistry,
            RestController restController,
            ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings,
            SettingsFilter settingsFilter,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster,
            Predicate<NodeFeature> clusterSupportsFeature
        ) {
            return List.of(new RestHandler() {
                @Override
                public List<Route> routes() {
                    return List.of(new Route(RestRequest.Method.GET, ROUTE));
                }

                @Override
                public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) {
                    final var response = new Response(new HashMap<>(), new CountDownLatch(1));
                    final var maxSize = between(1, ByteSizeUnit.MB.toIntBytes(1));
                    final var entryCount = between(0, ByteSizeUnit.MB.toIntBytes(10) / maxSize); // limit total size to 10MiB
                    for (int i = 0; i < entryCount; i++) {
                        response.entries().put(randomIdentifier(), randomContent(between(1, 10), maxSize));
                    }
                    assertTrue(responseRef.compareAndSet(null, response));
                    handleZipRestRequest(
                        channel,
                        client.threadPool(),
                        response.completedLatch(),
                        () -> {},
                        response.entries().entrySet().iterator()
                    );
                }
            }, new RestHandler() {

                @Override
                public List<Route> routes() {
                    return List.of(new Route(RestRequest.Method.GET, INFINITE_ROUTE));
                }

                @Override
                public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) {
                    final var response = new Response(null, new CountDownLatch(1));
                    assertTrue(responseRef.compareAndSet(null, response));
                    final var getNextPartCountDown = request.paramAsInt(GET_NEXT_PART_COUNT_DOWN_PARAM, -1);
                    final Runnable onGetNextPart;
                    final Supplier<EntryBody> entryBodySupplier;
                    if (getNextPartCountDown <= 0) {
                        onGetNextPart = () -> {};
                        entryBodySupplier = () -> randomContent(between(1, 10), ByteSizeUnit.MB.toIntBytes(1));
                    } else {
                        final AtomicInteger remaining = new AtomicInteger(getNextPartCountDown);
                        entryBodySupplier = () -> randomContent(between(2, 10), ByteSizeUnit.KB.toIntBytes(1));
                        if (randomBoolean()) {
                            onGetNextPart = () -> {
                                final var newRemaining = remaining.decrementAndGet();
                                assertThat(newRemaining, greaterThanOrEqualTo(0));
                                if (newRemaining <= 0) {
                                    throw new ElasticsearchException("simulated failure");
                                }
                            };
                        } else {
                            onGetNextPart = () -> {
                                if (remaining.decrementAndGet() == 0) {
                                    request.getHttpChannel().close();
                                }
                            };
                        }
                    }
                    handleZipRestRequest(channel, client.threadPool(), response.completedLatch(), onGetNextPart, new Iterator<>() {

                        private long id;

                        // carry on yielding content even after the channel closes
                        private final Semaphore trailingContentPermits = new Semaphore(between(0, 20));

                        @Override
                        public boolean hasNext() {
                            return request.getHttpChannel().isOpen() || trailingContentPermits.tryAcquire();
                        }

                        @Override
                        public Map.Entry<String, EntryBody> next() {
                            return new Map.Entry<>() {
                                private final String key = Long.toString(id++);
                                private final EntryBody content = entryBodySupplier.get();

                                @Override
                                public String getKey() {
                                    return key;
                                }

                                @Override
                                public EntryBody getValue() {
                                    return content;
                                }

                                @Override
                                public EntryBody setValue(EntryBody value) {
                                    return fail(null, "must not setValue");
                                }
                            };
                        }
                    });
                }
            });
        }

        private static EntryBody randomContent(int partCount, int maxSize) {
            if (randomBoolean()) {
                return null;
            }

            final var maxPartSize = maxSize / partCount;
            return new EntryBody(randomList(partCount, partCount, () -> {
                final var chunkCount = between(1, 10);
                return randomEntryPart(chunkCount, maxPartSize / chunkCount);
            }));
        }

        private static EntryPart randomEntryPart(int chunkCount, int maxChunkSize) {
            final var chunks = randomList(chunkCount, chunkCount, () -> randomBytesReference(between(0, maxChunkSize)));
            Collections.shuffle(chunks, random());
            return new EntryPart(chunks);
        }

        private static void handleZipRestRequest(
            RestChannel channel,
            ThreadPool threadPool,
            CountDownLatch completionLatch,
            Runnable onGetNextPart,
            Iterator<Map.Entry<String, EntryBody>> entryIterator
        ) {
            try (var refs = new RefCountingRunnable(completionLatch::countDown)) {
                final var chunkedZipResponse = new ChunkedZipResponse(RESPONSE_FILENAME, channel, refs.acquire());
                ThrottledIterator.run(
                    entryIterator,
                    (ref, entry) -> randomFrom(EsExecutors.DIRECT_EXECUTOR_SERVICE, threadPool.generic()).execute(
                        ActionRunnable.supply(
                            chunkedZipResponse.newEntryListener(entry.getKey(), Releasables.wrap(ref, refs.acquire())),
                            () -> entry.getValue() == null && randomBoolean() // randomBoolean() to allow some null entries to fail with NPE
                                ? null
                                : new TestBytesReferenceBodyPart(
                                    entry.getKey(),
                                    threadPool,
                                    entry.getValue().parts().iterator(),
                                    refs,
                                    onGetNextPart
                                )
                        )
                    ),
                    between(1, 10),
                    Releasables.wrap(refs.acquire(), chunkedZipResponse)::close
                );
            }
        }
    }

    private static class TestBytesReferenceBodyPart implements ChunkedRestResponseBodyPart {

        private final String name;
        private final ThreadPool threadPool;
        private final Iterator<BytesReference> chunksIterator;
        private final Iterator<RandomZipResponsePlugin.EntryPart> partsIterator;
        private final RefCountingRunnable refs;
        private final Runnable onGetNextPart;

        TestBytesReferenceBodyPart(
            String name,
            ThreadPool threadPool,
            Iterator<RandomZipResponsePlugin.EntryPart> partsIterator,
            RefCountingRunnable refs,
            Runnable onGetNextPart
        ) {
            this.onGetNextPart = onGetNextPart;
            assert partsIterator.hasNext();
            this.name = name;
            this.threadPool = threadPool;
            this.partsIterator = partsIterator;
            this.chunksIterator = partsIterator.next().chunks().iterator();
            this.refs = refs;
        }

        @Override
        public boolean isPartComplete() {
            return chunksIterator.hasNext() == false;
        }

        @Override
        public boolean isLastPart() {
            return partsIterator.hasNext() == false;
        }

        @Override
        public void getNextPart(ActionListener<ChunkedRestResponseBodyPart> listener) {
            threadPool.generic().execute(ActionRunnable.supply(listener, () -> {
                onGetNextPart.run();
                return new TestBytesReferenceBodyPart(name, threadPool, partsIterator, refs, onGetNextPart);
            }));
        }

        @Override
        public ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) {
            assert chunksIterator.hasNext();
            return new ReleasableBytesReference(chunksIterator.next(), refs.acquire());
        }

        @Override
        public String getResponseContentTypeString() {
            return "application/binary";
        }
    }

    public void testRandomZipResponse() throws IOException {
        final var request = new Request("GET", RandomZipResponsePlugin.ROUTE);
        if (randomBoolean()) {
            request.setOptions(
                RequestOptions.DEFAULT.toBuilder()
                    .addHeader("accept-encoding", String.join(", ", randomSubsetOf(List.of("deflate", "gzip", "zstd", "br"))))
            );
        }
        final var response = getRestClient().performRequest(request);
        assertEquals(ZIP_CONTENT_TYPE, response.getHeader("Content-Type"));
        assertNull(response.getHeader("content-encoding")); // zip file is already compressed
        assertEquals(
            "attachment; filename=\"" + RandomZipResponsePlugin.RESPONSE_FILENAME + ".zip\"",
            response.getHeader("Content-Disposition")
        );
        final var pathPrefix = RandomZipResponsePlugin.RESPONSE_FILENAME + "/";

        final var actualEntries = new HashMap<String, BytesReference>();
        final var copyBuffer = new byte[PageCacheRecycler.BYTE_PAGE_SIZE];

        try (var zipStream = new ZipInputStream(response.getEntity().getContent())) {
            ZipEntry zipEntry;
            while ((zipEntry = zipStream.getNextEntry()) != null) {
                assertThat(zipEntry.getName(), startsWith(pathPrefix));
                final var name = zipEntry.getName().substring(pathPrefix.length());
                try (var bytesStream = new BytesStreamOutput()) {
                    while (true) {
                        final var readLength = zipStream.read(copyBuffer, 0, copyBuffer.length);
                        if (readLength < 0) {
                            break;
                        }
                        bytesStream.write(copyBuffer, 0, readLength);
                    }
                    actualEntries.put(name, bytesStream.bytes());
                }
            }
        } finally {
            assertEquals(getExpectedEntries(), actualEntries);
        }
    }

    public void testAbort() throws IOException {
        final var request = new Request("GET", RandomZipResponsePlugin.INFINITE_ROUTE);
        final var responseStarted = new CountDownLatch(1);
        final var bodyConsumed = new CountDownLatch(1);
        request.setOptions(RequestOptions.DEFAULT.toBuilder().setHttpAsyncResponseConsumerFactory(() -> new HttpAsyncResponseConsumer<>() {

            final ByteBuffer readBuffer = ByteBuffer.allocate(ByteSizeUnit.KB.toIntBytes(4));
            int bytesToConsume = ByteSizeUnit.MB.toIntBytes(1);

            @Override
            public void responseReceived(HttpResponse response) {
                assertEquals("application/zip", response.getHeaders("Content-Type")[0].getValue());
                final var contentDispositionHeader = response.getHeaders("Content-Disposition")[0].getElements()[0];
                assertEquals("attachment", contentDispositionHeader.getName());
                assertEquals(
                    RandomZipResponsePlugin.RESPONSE_FILENAME + ".zip",
                    contentDispositionHeader.getParameterByName("filename").getValue()
                );
                responseStarted.countDown();
            }

            @Override
            public void consumeContent(ContentDecoder decoder, IOControl ioControl) throws IOException {
                readBuffer.clear();
                final var bytesRead = decoder.read(readBuffer);
                if (bytesRead > 0) {
                    bytesToConsume -= bytesRead;
                }

                if (bytesToConsume <= 0) {
                    bodyConsumed.countDown();
                    ioControl.shutdown();
                }
            }

            @Override
            public void responseCompleted(HttpContext context) {}

            @Override
            public void failed(Exception ex) {}

            @Override
            public Exception getException() {
                return null;
            }

            @Override
            public HttpResponse getResult() {
                return null;
            }

            @Override
            public boolean isDone() {
                return false;
            }

            @Override
            public void close() {}

            @Override
            public boolean cancel() {
                return false;
            }
        }));

        try {
            try (var restClient = createRestClient(internalCluster().getRandomNodeName())) {
                // one-node REST client to avoid retries
                expectThrows(ConnectionClosedException.class, () -> restClient.performRequest(request));
            }
            safeAwait(responseStarted);
            safeAwait(bodyConsumed);
        } finally {
            assertNull(getExpectedEntries()); // mainly just checking that all refs are released
        }
    }

    public void testGetNextPartFailure() throws IOException {
        final var request = new Request("GET", RandomZipResponsePlugin.INFINITE_ROUTE);
        request.addParameter(RandomZipResponsePlugin.GET_NEXT_PART_COUNT_DOWN_PARAM, Integer.toString(between(1, 100)));

        try (var restClient = createRestClient(internalCluster().getRandomNodeName())) {
            // one-node REST client to avoid retries
            assertThat(
                safeAwaitFailure(
                    Response.class,
                    l -> restClient.performRequestAsync(request, ActionTestUtils.wrapAsRestResponseListener(l))
                ),
                anyOf(instanceOf(ConnectionClosedException.class), instanceOf(MalformedChunkCodingException.class))
            );
        } finally {
            assertNull(getExpectedEntries()); // mainly just checking that all refs are released
        }
    }

    private static Map<String, BytesReference> getExpectedEntries() {
        final List<Map<String, BytesReference>> nodeResponses = StreamSupport
            // concatenate all the chunks in all the entries
            .stream(internalCluster().getInstances(PluginsService.class).spliterator(), false)
            .flatMap(p -> p.filterPlugins(RandomZipResponsePlugin.class))
            .flatMap(p -> {
                final var maybeResponse = p.responseRef.getAndSet(null);
                if (maybeResponse == null) {
                    return Stream.of();
                } else {
                    safeAwait(maybeResponse.completedLatch()); // ensures that all refs have been released
                    if (maybeResponse.entries() == null) {
                        return Stream.of((Map<String, BytesReference>) null);
                    } else {
                        final var expectedEntries = Maps.<String, BytesReference>newMapWithExpectedSize(maybeResponse.entries().size());
                        maybeResponse.entries().forEach((entryName, entryBody) -> {
                            if (entryBody != null) {
                                try (var bytesStreamOutput = new BytesStreamOutput()) {
                                    for (final var part : entryBody.parts()) {
                                        for (final var chunk : part.chunks()) {
                                            chunk.writeTo(bytesStreamOutput);
                                        }
                                    }
                                    expectedEntries.put(entryName, bytesStreamOutput.bytes());
                                } catch (IOException e) {
                                    throw new AssertionError(e);
                                }
                            }
                        });
                        return Stream.of(expectedEntries);
                    }
                }
            })
            .toList();
        assertThat(nodeResponses, hasSize(1));
        return nodeResponses.get(0);
    }
}
