/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.rest;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.core.Streams;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

/**
 * The body of a rest response that uses chunked HTTP encoding. Implementations are used to avoid materializing full responses on heap and
 * instead serialize only as much of the response as can be flushed to the network right away.
 */
public interface ChunkedRestResponseBody extends Releasable {

    Logger logger = LogManager.getLogger(ChunkedRestResponseBody.class);

    /**
     * @return true once this response has been written fully.
     */
    boolean isDone();

    /**
     * Serializes approximately as many bytes of the response as request by {@code sizeHint} to a {@link ReleasableBytesReference} that
     * is created from buffers backed by the given {@code recycler}.
     *
     * @param sizeHint how many bytes to approximately serialize for the given chunk
     * @param recycler recycler used to acquire buffers
     * @return serialized chunk
     * @throws IOException on serialization failure
     */
    ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) throws IOException;

    /**
     * @return the response Content-Type header value for this response body
     */
    String getResponseContentTypeString();

    /**
     * Create a chunked response body to be written to a specific {@link RestChannel} from a {@link ChunkedToXContent}.
     *
     * @param chunkedToXContent chunked x-content instance to serialize
     * @param params parameters to use for serialization
     * @param channel channel the response will be written to
     * @param releasable resource to release when the response is fully sent, or {@code null} if nothing to release
     * @return chunked rest response body
     */
    static ChunkedRestResponseBody fromXContent(
        ChunkedToXContent chunkedToXContent,
        ToXContent.Params params,
        RestChannel channel,
        @Nullable Releasable releasable
    ) throws IOException {

        return new ChunkedRestResponseBody() {

            private final OutputStream out = new OutputStream() {
                @Override
                public void write(int b) throws IOException {
                    target.write(b);
                }

                @Override
                public void write(byte[] b, int off, int len) throws IOException {
                    target.write(b, off, len);
                }
            };

            private final XContentBuilder builder = channel.newBuilder(
                channel.request().getXContentType(),
                null,
                true,
                Streams.noCloseStream(out)
            );

            private final Iterator<? extends ToXContent> serialization = builder.getRestApiVersion() == RestApiVersion.V_7
                ? chunkedToXContent.toXContentChunkedV7(params)
                : chunkedToXContent.toXContentChunked(params);

            private BytesStream target;

            @Override
            public boolean isDone() {
                return serialization.hasNext() == false;
            }

            @Override
            public ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) throws IOException {
                try {
                    final RecyclerBytesStreamOutput chunkStream = new RecyclerBytesStreamOutput(recycler);
                    assert target == null;
                    target = chunkStream;
                    while (serialization.hasNext()) {
                        serialization.next().toXContent(builder, params);
                        if (chunkStream.size() >= sizeHint) {
                            break;
                        }
                    }
                    if (serialization.hasNext() == false) {
                        builder.close();
                    }
                    final var result = new ReleasableBytesReference(
                        chunkStream.bytes(),
                        () -> Releasables.closeExpectNoException(chunkStream)
                    );
                    target = null;
                    return result;
                } catch (Exception e) {
                    logger.error("failure encoding chunk", e);
                    throw e;
                } finally {
                    if (target != null) {
                        assert false : "failure encoding chunk";
                        IOUtils.closeWhileHandlingException(target);
                        target = null;
                    }
                }
            }

            @Override
            public String getResponseContentTypeString() {
                return builder.getResponseContentTypeString();
            }

            @Override
            public void close() {
                Releasables.closeExpectNoException(releasable);
            }
        };
    }

    /**
     * Create a chunked response body to be written to a specific {@link RestChannel} from a stream of text chunks, each represented as a
     * consumer of a {@link Writer}. The last chunk that the iterator yields must write at least one byte.
     */
    static ChunkedRestResponseBody fromTextChunks(
        String contentType,
        Iterator<CheckedConsumer<Writer, IOException>> chunkIterator,
        @Nullable Releasable releasable
    ) {
        return new ChunkedRestResponseBody() {
            private RecyclerBytesStreamOutput currentOutput;
            private final Writer writer = new OutputStreamWriter(new OutputStream() {
                @Override
                public void write(int b) throws IOException {
                    assert currentOutput != null;
                    currentOutput.write(b);
                }

                @Override
                public void write(byte[] b, int off, int len) throws IOException {
                    assert currentOutput != null;
                    currentOutput.write(b, off, len);
                }

                @Override
                public void flush() {
                    assert currentOutput != null;
                    currentOutput.flush();
                }

                @Override
                public void close() {
                    assert currentOutput != null;
                    currentOutput.flush();
                }
            }, StandardCharsets.UTF_8);

            @Override
            public boolean isDone() {
                return chunkIterator.hasNext() == false;
            }

            @Override
            public ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) throws IOException {
                try {
                    assert currentOutput == null;
                    currentOutput = new RecyclerBytesStreamOutput(recycler);

                    while (chunkIterator.hasNext() && currentOutput.size() < sizeHint) {
                        chunkIterator.next().accept(writer);
                    }

                    if (chunkIterator.hasNext()) {
                        writer.flush();
                    } else {
                        writer.close();
                    }

                    final var chunkOutput = currentOutput;
                    final var result = new ReleasableBytesReference(
                        chunkOutput.bytes(),
                        () -> Releasables.closeExpectNoException(chunkOutput)
                    );
                    currentOutput = null;
                    return result;
                } catch (Exception e) {
                    logger.error("failure encoding text chunk", e);
                    throw e;
                } finally {
                    if (currentOutput != null) {
                        assert false : "failure encoding text chunk";
                        Releasables.closeExpectNoException(currentOutput);
                        currentOutput = null;
                    }
                }
            }

            @Override
            public String getResponseContentTypeString() {
                return contentType;
            }

            @Override
            public void close() {
                Releasables.closeExpectNoException(releasable);
            }
        };
    }
}
