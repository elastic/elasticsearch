/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Nullable;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Simple {@link StreamStorage} that keeps all streams on the heap.
 * <p>
 * Expired streams are not evicted. They stay until {@link #delete} is called.
 * Stored pages are owned by the store and released on delete.
 * Pages returned from {@link #getPages} are shallow copies that the caller must close.
 * <p>
 * Offsets are row positions in the overall result. {@code expirationMillis} is an absolute epoch time.
 */
public class InMemoryStreamStore implements StreamStorage {

    private final Map<String, Stream> streams = new ConcurrentHashMap<>();

    @Override
    public void create(String streamId, long expirationMillis, ActionListener<Void> listener) {
        ActionListener.completeWith(listener, () -> {
            if (streams.putIfAbsent(streamId, new Stream(streamId, Instant.ofEpochMilli(expirationMillis))) != null) {
                throw new IllegalStateException("stream [" + streamId + "] already exists");
            }
            return null;
        });
    }

    @Override
    public void extendTtl(String streamId, long expirationMillis, ActionListener<Void> listener) {
        ActionListener.completeWith(listener, () -> {
            get(streamId).extendTtl(Instant.ofEpochMilli(expirationMillis));
            return null;
        });
    }

    @Override
    public void appendPage(String streamingId, int offset, Page page, ActionListener<Void> listener) {
        ActionListener.completeWith(listener, () -> {
            try {
                get(streamingId).append(offset, page);
            } catch (RuntimeException e) {
                // The store takes ownership of the page even when the append is rejected.
                page.releaseBlocks();
                throw e;
            }
            return null;
        });
    }

    @Override
    public void markComplete(String streamId, int totalRows, int totalPages, ActionListener<Void> listener) {
        ActionListener.completeWith(listener, () -> {
            get(streamId).markComplete(totalRows, totalPages);
            return null;
        });
    }

    @Override
    public void markFailed(String streamId, ActionListener<Void> listener) {
        ActionListener.completeWith(listener, () -> {
            get(streamId).markFailed();
            return null;
        });
    }

    @Override
    public void getMetadata(String streamId, ActionListener<StreamMetadata> listener) {
        ActionListener.completeWith(listener, () -> {
            Stream stream = streams.get(streamId);
            return stream != null ? stream.metadata() : null;
        });
    }

    /**
     * Returns pages covering rows {@code [offset, offset + rows)} of the result, trimmed at both ends to that range.
     * Returns fewer rows if the stream does not have them (yet).
     */
    @Override
    public void getPages(String streamId, int offset, int rows, ActionListener<List<Page>> listener) {
        ActionListener.completeWith(listener, () -> get(streamId).getPages(offset, rows));
    }

    @Override
    public void delete(String streamId, ActionListener<Void> listener) {
        ActionListener.completeWith(listener, () -> {
            Stream stream = streams.remove(streamId);
            if (stream != null) {
                stream.close();
            }
            return null;
        });
    }

    private Stream get(String streamId) {
        Stream stream = streams.get(streamId);
        if (stream == null) {
            throw notFound(streamId);
        }
        return stream;
    }

    private static ResourceNotFoundException notFound(String streamId) {
        return new ResourceNotFoundException("stream [" + streamId + "] not found");
    }

    /** A page together with the position of its first row in the overall result. */
    private record Entry(int offset, Page page) {
        int size() {
            return page.getPositionCount();
        }
    }

    /**
     * Metadata and pages of a single stream. All state is guarded by the lock on the stream itself,
     * so operations on different streams do not block each other.
     */
    private static class Stream {
        private final String streamId;
        private final List<Entry> pages = new ArrayList<>();
        private Instant ttl;
        private StreamMetadata.Status status = StreamMetadata.Status.RUNNING;
        /** Rows appended so far. */
        private int rows;
        /** Set once the stream is deleted. A writer or reader that looked the stream up before the delete must not touch its pages. */
        private boolean closed;

        Stream(String streamId, Instant ttl) {
            this.streamId = streamId;
            this.ttl = ttl;
        }

        synchronized void extendTtl(Instant ttl) {
            ensureOpen();
            this.ttl = ttl;
        }

        synchronized void append(int offset, Page page) {
            ensureOpen();
            if (status != StreamMetadata.Status.RUNNING) {
                throw new IllegalStateException("stream [" + streamId + "] is already " + status);
            }
            // Pages must arrive in order, a gap or a repeat means a bug in the writer.
            if (offset != rows) {
                throw new IllegalArgumentException(
                    "unexpected offset [" + offset + "] for stream [" + streamId + "], expected [" + rows + "]"
                );
            }
            Entry entry = new Entry(offset, page);
            pages.add(entry);
            rows += entry.size();
        }

        synchronized void markComplete(int totalRows, int totalPages) {
            ensureOpen();
            if (pages.size() != totalPages || rows != totalRows) {
                throw new IllegalArgumentException(
                    "stream ["
                        + streamId
                        + "] has ["
                        + rows
                        + "] rows in ["
                        + pages.size()
                        + "] pages, but completed with ["
                        + totalRows
                        + "] rows in ["
                        + totalPages
                        + "] pages"
                );
            }
            status = StreamMetadata.Status.COMPLETE;
        }

        synchronized void markFailed() {
            ensureOpen();
            status = StreamMetadata.Status.FAILED;
        }

        @Nullable
        synchronized StreamMetadata metadata() {
            if (closed) {
                return null;
            }
            // Totals are only known once the writer completes the stream.
            boolean complete = status == StreamMetadata.Status.COMPLETE;
            return new StreamMetadata(streamId, ttl, status, complete ? rows : null, complete ? pages.size() : null);
        }

        synchronized List<Page> getPages(int offset, int rows) {
            ensureOpen();
            long end = (long) offset + rows;
            List<Page> result = new ArrayList<>();
            try {
                for (Entry entry : pages) {
                    int pageStart = entry.offset();
                    int pageEnd = pageStart + entry.size();
                    if (pageEnd <= offset) {
                        continue;
                    }
                    if (pageStart >= end) {
                        break;
                    }
                    int from = Math.max(offset, pageStart) - pageStart;
                    int to = (int) Math.min(end, pageEnd) - pageStart;
                    if (from == 0 && to == entry.size()) {
                        result.add(entry.page().shallowCopy());
                    } else {
                        result.add(entry.page().slice(from, to));
                    }
                }
            } catch (RuntimeException e) {
                for (Page page : result) {
                    page.releaseBlocks();
                }
                throw e;
            }
            return result;
        }

        synchronized void close() {
            if (closed) {
                return;
            }
            closed = true;
            for (Entry entry : pages) {
                entry.page().releaseBlocks();
            }
            pages.clear();
        }

        private void ensureOpen() {
            if (closed) {
                throw notFound(streamId);
            }
        }
    }
}
