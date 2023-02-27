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
import org.apache.lucene.store.RandomAccessInput;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.IndexModule;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

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
                IndexInput input = super.openInput(name, context);
                if (input instanceof RandomAccessInput) {
                    return new RandomAccessSeekCountingIndexInput(input, shardRouting.shardId().toString(), name);
                }
                return wrapIndexInput(shardRouting.shardId().toString(), name, input);
            }
        };
    }

    private IndexInput wrapIndexInput(String directory, String name, IndexInput in) {
        return new SeekCountingIndexInput(in, directory, name);
    }

    class RandomAccessSeekCountingIndexInput extends SeekCountingIndexInput implements RandomAccessInput {

        private final RandomAccessInput randomAccessInput;

        RandomAccessSeekCountingIndexInput(IndexInput in, String directory, String name) {
            super(in, directory, name);
            randomAccessInput = (RandomAccessInput) unwrap(in);
        }

        @Override
        public IndexInput clone() {
            return new RandomAccessSeekCountingIndexInput(super.clone(), directory, name);
        }

        @Override
        public byte readByte(long pos) throws IOException {
            return randomAccessInput.readByte(pos);
        }

        @Override
        public short readShort(long pos) throws IOException {
            return randomAccessInput.readShort(pos);
        }

        @Override
        public int readInt(long pos) throws IOException {
            return randomAccessInput.readInt(pos);
        }

        @Override
        public long readLong(long pos) throws IOException {
            return randomAccessInput.readLong(pos);
        }
    }

    class SeekCountingIndexInput extends IndexInput {

        public static IndexInput unwrap(IndexInput input) {
            while (input instanceof SeekCountingIndexInput) {
                input = ((SeekCountingIndexInput) input).in;
            }
            return input;
        }

        final IndexInput in;
        final String directory;
        final String name;

        SeekCountingIndexInput(IndexInput in, String directory, String name) {
            super(unwrap(in).toString() + "[seek_tracked]");
            this.in = unwrap(in);
            this.directory = directory;
            this.name = name;
        }

        @Override
        public IndexInput clone() {
            return new SeekCountingIndexInput(in.clone(), directory, name);
        }

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
            return wrapIndexInput(directory, name, in.slice(sliceDescription + "[seek_tracked]", offset, length));
        }

        @Override
        public RandomAccessInput randomAccessSlice(long offset, long length) throws IOException {
            final IndexInput innerSlice = in.slice("randomaccess", offset, length);
            if (innerSlice instanceof RandomAccessInput) {
                // slice() already supports random access
                return new RandomAccessSeekCountingIndexInput(innerSlice, directory, name);
            } else {
                IndexInput slice = wrapIndexInput(directory, name, innerSlice);
                // return default impl
                return new RandomAccessInput() {
                    @Override
                    public byte readByte(long pos) throws IOException {
                        slice.seek(pos);
                        return slice.readByte();
                    }

                    @Override
                    public short readShort(long pos) throws IOException {
                        slice.seek(pos);
                        return slice.readShort();
                    }

                    @Override
                    public int readInt(long pos) throws IOException {
                        slice.seek(pos);
                        return slice.readInt();
                    }

                    @Override
                    public long readLong(long pos) throws IOException {
                        slice.seek(pos);
                        return slice.readLong();
                    }

                    @Override
                    public String toString() {
                        return "RandomAccessInput(" + slice + ")";
                    }
                };
            }
        }

        @Override
        public byte readByte() throws IOException {
            return in.readByte();
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            in.readBytes(b, offset, len);
        }

        @Override
        public void readBytes(byte[] b, int offset, int len, boolean useBuffer) throws IOException {
            in.readBytes(b, offset, len, useBuffer);
        }

        @Override
        public short readShort() throws IOException {
            return in.readShort();
        }

        @Override
        public int readInt() throws IOException {
            return in.readInt();
        }

        @Override
        public int readVInt() throws IOException {
            return in.readVInt();
        }

        @Override
        public int readZInt() throws IOException {
            return in.readZInt();
        }

        @Override
        public long readLong() throws IOException {
            return in.readLong();
        }

        @Override
        public long readVLong() throws IOException {
            return in.readVLong();
        }

        @Override
        public long readZLong() throws IOException {
            return in.readZLong();
        }

        @Override
        public String readString() throws IOException {
            return in.readString();
        }

        @Override
        public Map<String, String> readMapOfStrings() throws IOException {
            return in.readMapOfStrings();
        }

        @Override
        public Set<String> readSetOfStrings() throws IOException {
            return in.readSetOfStrings();
        }

        @Override
        public void skipBytes(long numBytes) throws IOException {
            in.skipBytes(numBytes);
        }

        @Override
        public void readFloats(float[] floats, int offset, int len) throws IOException {
            in.readFloats(floats, offset, len);
        }

        @Override
        public void readLongs(long[] dst, int offset, int length) throws IOException {
            in.readLongs(dst, offset, length);
        }

        @Override
        public void readInts(int[] dst, int offset, int length) throws IOException {
            in.readInts(dst, offset, length);
        }

    }
}
