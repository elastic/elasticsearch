/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.seektracker;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.IndexModule;

import java.io.IOException;

public class SeekTrackingDirectoryWrapper implements IndexModule.DirectoryWrapper {

    private final IndexSeekTracker seekTracker;

    public SeekTrackingDirectoryWrapper(IndexSeekTracker seekTracker) {
        this.seekTracker = seekTracker;
    }

    @Override
    public Directory wrap(Directory directory, ShardRouting shardRouting) {
        seekTracker.track(shardRouting.shardId().toString());
        return new FilterDirectory(directory) {
            @Override
            public IndexInput openInput(String name, IOContext context) throws IOException {
                return wrapIndexInput(shardRouting.shardId().toString(), name, super.openInput(name, context));
            }
        };
    }

    private IndexInput wrapIndexInput(String directory, String name, IndexInput in) {
        return new IndexInput(in.toString()) {
            @Override
            public void close() throws IOException {
                in.close();
            }

            @Override
            public long getFilePointer() {
                return in.getFilePointer();
            }

            @Override
            public void seek(long pos) throws IOException {
                in.seek(pos);
                seekTracker.increment(directory, name);
            }

            @Override
            public long length() {
                return in.length();
            }

            @Override
            public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
                return wrapIndexInput(directory, name, in.slice(sliceDescription, offset, length));
            }

            @Override
            public byte readByte() throws IOException {
                return in.readByte();
            }

            @Override
            public void readBytes(byte[] b, int offset, int len) throws IOException {
                in.readBytes(b, offset, len);
            }
        };
    }
}
