/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.spill.disk;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.compute.spill.spi.SpillLimitExceededException;
import org.elasticsearch.compute.spill.spi.SpillObject;
import org.elasticsearch.compute.spill.spi.SpillWriter;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.tasks.TaskCancelledException;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * One file, written sequentially with one append outstanding. The chunk is admitted against the cap before the I/O is
 * scheduled, the free-space margin is checked on the pool right before the write, the chunk is written page by page
 * from the engine's buffers without a copy, and its bytes stay counted until the file is gone, whether the write
 * succeeded or failed part way through the chunk; the backend's reference to the chunk goes before the callback. A
 * failure poisons the writer; abort deletes the file and returns everything, and when it finds an append in flight it
 * leaves the file to that append's completion, which removes it, so that no byte stays counted for a file nobody
 * tracks and no byte on disk goes uncounted.
 */
final class DiskSpillWriter implements SpillWriter {

    private enum State {
        OPEN,
        FINISHING,
        FINISHED,
        ABORTED,
        FAILED
    }

    private final DiskSpillScope scope;
    private final Path path;
    private final String nameHint;
    private final FileChannel channel;
    private State state = State.OPEN;
    private boolean outstanding;
    private long written;

    DiskSpillWriter(DiskSpillScope scope, Path path, String nameHint, FileChannel channel) {
        this.scope = scope;
        this.path = path;
        this.nameHint = nameHint;
        this.channel = channel;
    }

    Path path() {
        return path;
    }

    @Override
    public void append(ReleasableBytesReference chunk, ActionListener<Void> listener) {
        final int length = chunk.length();
        Exception refused = null;
        synchronized (this) {
            if (state != State.OPEN) {
                refused = new IllegalStateException("writer [" + nameHint + "] is " + state);
            } else if (outstanding) {
                state = State.FAILED;
                refused = new IllegalStateException("writer [" + nameHint + "] has an append outstanding");
            } else if (chunk.hasReferences() == false) {
                state = State.FAILED;
                refused = new IllegalStateException("chunk appended without a live reference");
            } else {
                try {
                    scope.store().reserve(length);
                    outstanding = true;
                } catch (SpillLimitExceededException e) {
                    state = State.FAILED;
                    refused = e;
                }
            }
        }
        if (refused != null) {
            listener.onFailure(refused);
            return;
        }
        final ReleasableBytesReference mine = chunk.retain();
        scope.io(ActionListener.runBefore(new ActionListener<Void>() {
            @Override
            public void onResponse(Void unused) {
                final boolean aborted;
                final long size;
                synchronized (DiskSpillWriter.this) {
                    outstanding = false;
                    aborted = state == State.ABORTED;
                    if (aborted) {
                        size = written + length;
                        written = 0;
                    } else {
                        written += length;
                        size = 0;
                    }
                }
                scope.addBytes(length);
                if (aborted) {
                    // the abort found this append in flight and left the file to it; this runs on the I/O pool
                    removeFile(size);
                    listener.onFailure(new IllegalStateException("writer [" + nameHint + "] aborted"));
                    return;
                }
                listener.onResponse(null);
            }

            @Override
            public void onFailure(Exception e) {
                // the write may have failed part way, so the chunk's bytes stay counted as the file's, in full rather than
                // by what the channel reports, until the file is gone: an over-count of at most one chunk, for a file that
                // the writer's failure has the framed writer remove right away
                final boolean aborted;
                final long size;
                synchronized (DiskSpillWriter.this) {
                    outstanding = false;
                    written += length;
                    aborted = state == State.ABORTED;
                    if (aborted) {
                        size = written;
                        written = 0;
                    } else {
                        size = 0;
                        if (state == State.OPEN) {
                            state = State.FAILED;
                        }
                    }
                }
                scope.addBytes(length);
                if (aborted) {
                    // the abort found this append in flight and left the file to it, with whatever the failed write left
                    // in it. This runs on the I/O pool, except after a rejection, when the cleanup is the one small thing left
                    removeFile(size);
                }
                listener.onFailure(e);
            }
        }, mine::close), () -> {
            scope.store().checkFreeSpace(length);
            BytesRefIterator pages = mine.iterator();
            BytesRef page;
            while ((page = pages.next()) != null) {
                if (scope.cancelled()) {
                    throw new TaskCancelledException("spill scope [" + scope.queryId() + "] cancelled");
                }
                Channels.writeToChannel(page.bytes, page.offset, page.length, channel);
            }
            return null;
        });
    }

    @Override
    public void finish(ActionListener<SpillObject> listener) {
        final long size;
        Exception refused = null;
        synchronized (this) {
            if (state != State.OPEN) {
                refused = new IllegalStateException("writer [" + nameHint + "] is " + state);
            } else if (outstanding) {
                state = State.FAILED;
                refused = new IllegalStateException("finish while an append is outstanding");
            } else {
                state = State.FINISHING;
            }
            size = written;
        }
        if (refused != null) {
            listener.onFailure(refused);
            return;
        }
        scope.io(new ActionListener<>() {
            @Override
            public void onResponse(DiskSpillObject object) {
                synchronized (DiskSpillWriter.this) {
                    state = State.FINISHED;
                }
                scope.writerDone(DiskSpillWriter.this);
                scope.objectPublished(object);
                listener.onResponse(object);
            }

            @Override
            public void onFailure(Exception e) {
                synchronized (DiskSpillWriter.this) {
                    state = State.FAILED;
                }
                listener.onFailure(e);
            }
        }, () -> {
            channel.close();
            return new DiskSpillObject(scope, path, size);
        });
    }

    @Override
    public void abort(ActionListener<Void> listener) {
        scope.ioUncancellable(listener, () -> {
            discard();
            return null;
        });
    }

    /**
     * Closes the channel and deletes the file, returning its bytes; a no-op once finished or aborted. With an append in
     * flight it only marks the writer, and that append's completion removes the file. Runs on the I/O pool.
     */
    void discard() {
        final long size;
        synchronized (this) {
            if (state == State.FINISHED || state == State.ABORTED || state == State.FINISHING) {
                return;
            }
            state = State.ABORTED;
            if (outstanding) {
                return;
            }
            size = written;
            written = 0;
        }
        removeFile(size);
    }

    private void removeFile(long size) {
        IOUtils.closeWhileHandlingException(channel);
        IOException failure = null;
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            failure = e;
        }
        scope.addBytes(-size);
        scope.store().deleted(path, size, failure);
        scope.writerDone(this);
    }
}
