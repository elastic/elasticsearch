/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Streams;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.plugin.StreamingProvider;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.util.Iterator;
import java.util.List;

/**
 * Serializes one chunk of a streaming ES|QL response as NDJSON.
 * Streams XContent directly to the network buffer without intermediate byte[] copies.
 */
public class EsqlStreamResponseBodyPart implements ChunkedRestResponseBodyPart {

    private static final Logger logger = LogManager.getLogger(EsqlStreamResponseBodyPart.class);
    private static final byte[] NEWLINE = new byte[] { '\n' };

    private final StreamingProvider provider;
    private final HttpChannel httpChannel;
    private final List<Page> pagesToRelease;
    private final Iterator<? extends ToXContent> serialization;
    private final XContentBuilder builder;
    private BytesStream target;
    private boolean lastPart = false;
    private boolean completionClaimed = false;
    private boolean pagesReleased = false;
    private byte[] completionBytes = null;

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

    public EsqlStreamResponseBodyPart(StreamingProvider.StreamingChunk chunk, StreamingProvider provider, HttpChannel httpChannel) {
        this.provider = provider;
        this.httpChannel = httpChannel;
        this.pagesToRelease = chunk.pagesToRelease();
        this.serialization = buildIterator(chunk);
        try {
            this.builder = new XContentBuilder(XContentType.JSON.xContent(), Streams.noCloseStream(out));
        } catch (IOException e) {
            throw new RuntimeException("failed to create XContentBuilder", e);
        }
    }

    @SuppressWarnings("unchecked")
    private static Iterator<? extends ToXContent> buildIterator(StreamingProvider.StreamingChunk chunk) {
        Iterator<? extends ToXContent> result = null;
        if (chunk.isFirstChunk() && chunk.columns() != null) {
            result = Iterators.concat(
                ChunkedToXContentHelper.startObject(),
                ResponseXContentUtils.allColumns(chunk.columns(), "columns"),
                ChunkedToXContentHelper.endObject(),
                newlineChunk()
            );
        }
        if (chunk.pages().isEmpty() == false && chunk.rowCount() > 0) {
            Iterator<? extends ToXContent> values = Iterators.concat(
                ChunkedToXContentHelper.startObject(),
                ChunkedToXContentHelper.array(
                    "values",
                    ResponseXContentUtils.columnValues(
                        chunk.columns(),
                        chunk.pages(),
                        false,
                        null,
                        ZoneOffset.UTC,
                        chunk.offset(),
                        chunk.rowCount()
                    )
                ),
                ChunkedToXContentHelper.endObject(),
                newlineChunk()
            );
            result = result != null ? Iterators.concat(result, values) : values;
        }
        return result != null ? result : Iterators.concat();
    }

    private static Iterator<ToXContent> newlineChunk() {
        return List.<ToXContent>of((builder, params) -> {
            builder.flush();
            builder.getOutputStream().write(NEWLINE);
            return builder;
        }).iterator();
    }

    @Override
    public boolean isPartComplete() {
        if (completionBytes != null) {
            return false;
        }
        if (serialization.hasNext()) {
            return false;
        }
        releasePages();
        if (completionClaimed) {
            return true;
        }
        StreamingProvider.CompletionInfo completion = provider.tryClaimCompletion();
        if (completion != null) {
            completionBytes = buildCompletionNdjson(completion);
            completionClaimed = true;
            lastPart = true;
            return false;
        }
        return true;
    }

    private void releasePages() {
        if (pagesReleased) {
            return;
        }
        pagesReleased = true;
        for (Page page : pagesToRelease) {
            page.releaseBlocks();
        }
    }

    @Override
    public boolean isLastPart() {
        return lastPart;
    }

    private static byte[] buildCompletionNdjson(StreamingProvider.CompletionInfo info) {
        String status = info.isPartial() ? "partial" : "ok";
        return ("{\"completion\":{\"status\":\""
            + status
            + "\",\"total_rows\":"
            + info.totalRows()
            + ",\"took\":"
            + info.tookMillis()
            + ",\"is_partial\":"
            + info.isPartial()
            + "}}\n").getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void getNextPart(ActionListener<ChunkedRestResponseBodyPart> listener) {
        provider.requestNextPart(listener);
    }

    @Override
    public ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) throws IOException {
        if (httpChannel.isOpen() == false) {
            throw new IOException("HTTP channel closed, aborting streaming response");
        }
        long t0 = System.nanoTime();
        try {
            final RecyclerBytesStreamOutput chunkStream = new RecyclerBytesStreamOutput(recycler);
            assert target == null;
            target = chunkStream;
            while (serialization.hasNext()) {
                serialization.next().toXContent(builder, ToXContent.EMPTY_PARAMS);
                if (chunkStream.size() >= sizeHint) {
                    break;
                }
            }
            if (serialization.hasNext() == false) {
                builder.close();
                if (completionBytes != null) {
                    chunkStream.write(completionBytes);
                    completionBytes = null;
                }
            }
            final var result = chunkStream.moveToBytesReference();
            target = null;
            long t1 = System.nanoTime();
            logger.info(
                "[TIMING] encodeChunk: size={}bytes, time={}ms, thread={}",
                result.length(),
                (t1 - t0) / 1_000_000,
                Thread.currentThread().getName()
            );
            return result;
        } catch (Exception e) {
            logger.error("failure encoding streaming chunk", e);
            throw e;
        } finally {
            if (target != null) {
                assert false : "failure encoding streaming chunk";
                IOUtils.closeWhileHandlingException(target);
                target = null;
            }
        }
    }

    @Override
    public String getResponseContentTypeString() {
        return "application/x-ndjson";
    }
}
