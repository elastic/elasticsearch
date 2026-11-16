/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.spill.disk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.spill.spi.SpillObject;
import org.elasticsearch.compute.spill.spi.SpillReader;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * A finished file. Readers open channels of their own; a delete removes the file and returns its bytes once it is gone,
 * and fails every read issued afterwards.
 */
final class DiskSpillObject implements SpillObject {

    private final DiskSpillScope scope;
    private final Path path;
    private final long size;
    private volatile boolean deleted;

    DiskSpillObject(DiskSpillScope scope, Path path, long size) {
        this.scope = scope;
        this.path = path;
        this.size = size;
    }

    Path path() {
        return path;
    }

    DiskSpillScope scope() {
        return scope;
    }

    boolean deleted() {
        return deleted;
    }

    @Override
    public long sizeInBytes() {
        return size;
    }

    @Override
    public void openRange(long offset, long length, ActionListener<SpillReader> listener) {
        if (offset < 0 || length < 0 || offset > size || length > size - offset) {
            listener.onFailure(
                new IllegalArgumentException("range [" + offset + ", +" + length + ") outside object of " + size + " bytes")
            );
            return;
        }
        if (deleted) {
            listener.onFailure(new IllegalStateException("object is deleted"));
            return;
        }
        scope.io(listener, () -> {
            if (deleted) {
                throw new IllegalStateException("object is deleted");
            }
            FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
            return new DiskSpillReader(this, channel, offset, offset + length, scope.breaker());
        });
    }

    @Override
    public void delete(ActionListener<Void> listener) {
        scope.ioUncancellable(listener, () -> {
            discard();
            return null;
        });
    }

    /**
     * Deletes the file once; runs on the I/O pool.
     */
    void discard() {
        boolean first;
        synchronized (this) {
            first = deleted == false;
            deleted = true;
        }
        if (first == false) {
            return;
        }
        IOException failure = null;
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            failure = e;
        }
        scope.addBytes(-size);
        scope.store().deleted(path, size, failure);
        scope.objectGone(this);
    }
}
