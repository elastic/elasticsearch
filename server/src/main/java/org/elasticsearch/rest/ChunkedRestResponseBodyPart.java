/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.rest;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Streams;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

/**
 * <p>A body (or a part thereof) of an HTTP response that uses the {@code chunked} transfer-encoding. This allows Elasticsearch to avoid
 * materializing the full response into on-heap buffers up front, instead serializing only as much of the response as can be flushed to the
 * network right away.</p>
 *
 * <p>Each {@link ChunkedRestResponseBodyPart} represents a sequence of chunks that are ready for <i>immediate</i> transmission: if
 * {@link #isPartComplete} returns {@code false} then {@link #encodeChunk} can be called at any time and must synchronously return the next
 * chunk to be sent. Many HTTP responses will be a single part, but if an implementation's {@link #isLastPart} returns {@code false} at the
 * end of the part then the transmission is paused and {@link #getNextPart} is called to compute the next sequence of chunks
 * asynchronously.</p>
 */
public interface ChunkedRestResponseBodyPart {

    Logger logger = LogManager.getLogger(ChunkedRestResponseBodyPart.class);

    /**
     * @return {@code true} if this body part contains no more chunks and the REST layer should check for a possible continuation by calling
     * {@link #isLastPart}, or {@code false} if the REST layer should request another chunk from this body using {@link #encodeChunk}.
     */
    boolean isPartComplete();

    /**
     * @return {@code true} if this is the last chunked body part in the response, or {@code false} if the REST layer should request further
     * chunked bodies by calling {@link #getNextPart}.
     */
    boolean isLastPart();

    /**
     * <p>Asynchronously retrieves the next part of the response body. Called if {@link #isLastPart} returns {@code false}.</p>
     *
     * <p>Note that this is called on a transport thread: implementations must take care to dispatch any nontrivial work elsewhere.</p>

     * <p>Note that the {@link Task} corresponding to any invocation of {@link Client#execute} completes as soon as the client action
     * returns its response, so it no longer exists when this method is called and cannot be used to receive cancellation notifications.
     * Instead, if the HTTP channel is closed while sending a response then the REST layer will invoke {@link RestResponse#close}. If the
     * HTTP channel is closed while the REST layer is waiting for a continuation then the {@link RestResponse} will not be closed until the
     * continuation listener is completed. Implementations will typically explicitly create a {@link CancellableTask} to represent the
     * computation and transmission of the entire {@link RestResponse}, and will cancel this task if the {@link RestResponse} is closed
     * prematurely.</p>
     *
     * @param listener Listener to complete with the next part of the body. By the point this is called we have already started to send
     *                 the body of the response, so there's no good ways to handle an exception here. Completing the listener exceptionally
     *                 will log an error, abort sending the response, and close the HTTP connection.
     */
    void getNextPart(ActionListener<ChunkedRestResponseBodyPart> listener);

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
     * Create a one-part chunked response body to be written to a specific {@link RestChannel} from a {@link ChunkedToXContent}.
     *
     * @param chunkedToXContent chunked x-content instance to serialize
     * @param params parameters to use for serialization
     * @param channel channel the response will be written to
     * @return chunked rest response body
     */
    static ChunkedRestResponseBodyPart fromXContent(ChunkedToXContent chunkedToXContent, ToXContent.Params params, RestChannel channel)
        throws IOException {

        return new ChunkedRestResponseBodyPart() {

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

            private final Iterator<? extends ToXContent> serialization = chunkedToXContent.toXContentChunked(
                builder.getRestApiVersion(),
                params
            );

            private BytesStream target;

            @Override
            public boolean isPartComplete() {
                return serialization.hasNext() == false;
            }

            @Override
            public boolean isLastPart() {
                return true;
            }

            @Override
            public void getNextPart(ActionListener<ChunkedRestResponseBodyPart> listener) {
                assert false : "no continuations";
                listener.onFailure(new IllegalStateException("no continuations available"));
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
                    final var result = chunkStream.moveToBytesReference();
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
        };
    }

    /**
     * Create a one-part chunked response body to be written to a specific {@link RestChannel} from a stream of UTF-8-encoded text chunks,
     * each represented as a consumer of a {@link Writer}.
     */
    static ChunkedRestResponseBodyPart fromTextChunks(String contentType, Iterator<CheckedConsumer<Writer, IOException>> chunkIterator) {
        return new ChunkedRestResponseBodyPart() {
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
            public boolean isPartComplete() {
                return chunkIterator.hasNext() == false;
            }

            @Override
            public boolean isLastPart() {
                return true;
            }

            @Override
            public void getNextPart(ActionListener<ChunkedRestResponseBodyPart> listener) {
                assert false : "no continuations";
                listener.onFailure(new IllegalStateException("no continuations available"));
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
        };
    }
}
