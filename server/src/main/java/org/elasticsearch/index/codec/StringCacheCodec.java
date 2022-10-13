/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompoundDirectory;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class StringCacheCodec extends FilterCodec {

    public StringCacheCodec(Codec delegate) {
        // this codec doesn't actually change any of the storage structure from the delegate,
        // it just provides canonical String objects from various read methods to save on memory
        super(delegate.getName(), delegate);
    }

    @Override
    public FieldInfosFormat fieldInfosFormat() {
        return new StringCacheFieldInfosFormat(super.fieldInfosFormat());
    }

    @Override
    public CompoundFormat compoundFormat() {
        return new StringCacheCompoundFormat(super.compoundFormat());
    }

    private static class StringCacheFieldInfosFormat extends FieldInfosFormat {
        private final FieldInfosFormat delegate;

        private StringCacheFieldInfosFormat(FieldInfosFormat delegate) {
            this.delegate = delegate;
        }

        @Override
        public FieldInfos read(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, IOContext iocontext) throws IOException {
            return delegate.read(new StringCacheDirectoryWrapper(directory), segmentInfo, segmentSuffix, iocontext);
        }

        @Override
        public void write(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, FieldInfos infos, IOContext context)
            throws IOException {
            delegate.write(directory, segmentInfo, segmentSuffix, infos, context);
        }
    }

    private static class StringCacheCompoundFormat extends CompoundFormat {
        private final CompoundFormat delegate;

        private StringCacheCompoundFormat(CompoundFormat delegate) {
            this.delegate = delegate;
        }

        @Override
        public CompoundDirectory getCompoundReader(Directory dir, SegmentInfo si, IOContext context) throws IOException {
            return new StringCacheCompoundDirectoryWrapper(delegate.getCompoundReader(dir, si, context));
        }

        @Override
        public void write(Directory dir, SegmentInfo si, IOContext context) throws IOException {
            delegate.write(dir, si, context);
        }
    }

    private static class StringCacheCompoundDirectoryWrapper extends CompoundDirectory {
        private final CompoundDirectory delegate;

        private StringCacheCompoundDirectoryWrapper(CompoundDirectory delegate) {
            this.delegate = delegate;
        }

        @Override
        public void checkIntegrity() throws IOException {
            delegate.checkIntegrity();
        }

        @Override
        public String[] listAll() throws IOException {
            return delegate.listAll();
        }

        @Override
        public long fileLength(String name) throws IOException {
            return delegate.fileLength(name);
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            return new IndexInputWrapper(delegate.openInput(name, context));
        }

        @Override
        public Set<String> getPendingDeletions() throws IOException {
            return delegate.getPendingDeletions();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    private static class StringCacheDirectoryWrapper extends FilterDirectory {
        private StringCacheDirectoryWrapper(Directory delegate) {
            super(delegate);
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            return new IndexInputWrapper(super.openInput(name, context));
        }

        @Override
        public ChecksumIndexInput openChecksumInput(String name, IOContext context) throws IOException {
            return new ChecksumIndexInputWrapper(super.openChecksumInput(name, context));
        }
    }

    private static class IndexInputWrapper extends IndexInput {
        private final IndexInput delegate;
        private final Map<String, String> canonicalStrings;

        private IndexInputWrapper(IndexInput delegate) {
            this(delegate, new HashMap<>());
        }

        private IndexInputWrapper(IndexInput delegate, Map<String, String> canonicalStrings) {
            super(delegate.toString());
            this.delegate = delegate;
            this.canonicalStrings = canonicalStrings;
        }

        private String getCanonicalString(String s) {
            return canonicalStrings.computeIfAbsent(s, Function.identity());
        }

        @Override
        public long getFilePointer() {
            return delegate.getFilePointer();
        }

        @Override
        public long length() {
            return delegate.length();
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            return new IndexInputWrapper(delegate.slice(sliceDescription, offset, length), canonicalStrings);
        }

        @Override
        public byte readByte() throws IOException {
            return delegate.readByte();
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            delegate.readBytes(b, offset, len);
        }

        @Override
        public void seek(long pos) throws IOException {
            delegate.seek(pos);
        }

        @Override
        public void skipBytes(long numBytes) throws IOException {
            delegate.skipBytes(numBytes);
        }

        @Override
        public RandomAccessInput randomAccessSlice(long offset, long length) throws IOException {
            return delegate.randomAccessSlice(offset, length);
        }

        @Override
        public void readBytes(byte[] b, int offset, int len, boolean useBuffer) throws IOException {
            delegate.readBytes(b, offset, len, useBuffer);
        }

        @Override
        public short readShort() throws IOException {
            return delegate.readShort();
        }

        @Override
        public int readInt() throws IOException {
            return delegate.readInt();
        }

        @Override
        public int readVInt() throws IOException {
            return delegate.readVInt();
        }

        @Override
        public int readZInt() throws IOException {
            return delegate.readZInt();
        }

        @Override
        public long readLong() throws IOException {
            return delegate.readLong();
        }

        @Override
        public void readLongs(long[] dst, int offset, int length) throws IOException {
            delegate.readLongs(dst, offset, length);
        }

        @Override
        public void readInts(int[] dst, int offset, int length) throws IOException {
            delegate.readInts(dst, offset, length);
        }

        @Override
        public void readFloats(float[] floats, int offset, int len) throws IOException {
            delegate.readFloats(floats, offset, len);
        }

        @Override
        public long readVLong() throws IOException {
            return delegate.readVLong();
        }

        @Override
        public long readZLong() throws IOException {
            return delegate.readZLong();
        }

        @Override
        public String readString() throws IOException {
            // readMapOfStrings and readSetOfStrings delegates to this method
            return getCanonicalString(delegate.readString());
        }

        @Override
        public String toString() {
            return delegate.toString();
        }

        @Override
        public IndexInput clone() {
            return new IndexInputWrapper(delegate.clone());
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    private static class ChecksumIndexInputWrapper extends ChecksumIndexInput {
        private final ChecksumIndexInput delegate;
        private final Map<String, String> canonicalStrings;

        private ChecksumIndexInputWrapper(ChecksumIndexInput delegate) {
            this(delegate, new HashMap<>());
        }

        private ChecksumIndexInputWrapper(ChecksumIndexInput delegate, Map<String, String> canonicalStrings) {
            super(delegate.toString());
            this.delegate = delegate;
            this.canonicalStrings = canonicalStrings;
        }

        private String getCanonicalString(String s) {
            return canonicalStrings.computeIfAbsent(s, Function.identity());
        }

        @Override
        public long getChecksum() throws IOException {
            return delegate.getChecksum();
        }

        @Override
        public long getFilePointer() {
            return delegate.getFilePointer();
        }

        @Override
        public long length() {
            return delegate.length();
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            return new IndexInputWrapper(delegate.slice(sliceDescription, offset, length), canonicalStrings);
        }

        @Override
        public byte readByte() throws IOException {
            return delegate.readByte();
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            delegate.readBytes(b, offset, len);
        }

        @Override
        public void seek(long pos) throws IOException {
            delegate.seek(pos);
        }

        @Override
        public void skipBytes(long numBytes) throws IOException {
            delegate.skipBytes(numBytes);
        }

        @Override
        public RandomAccessInput randomAccessSlice(long offset, long length) throws IOException {
            return delegate.randomAccessSlice(offset, length);
        }

        @Override
        public void readBytes(byte[] b, int offset, int len, boolean useBuffer) throws IOException {
            delegate.readBytes(b, offset, len, useBuffer);
        }

        @Override
        public short readShort() throws IOException {
            return delegate.readShort();
        }

        @Override
        public int readInt() throws IOException {
            return delegate.readInt();
        }

        @Override
        public int readVInt() throws IOException {
            return delegate.readVInt();
        }

        @Override
        public int readZInt() throws IOException {
            return delegate.readZInt();
        }

        @Override
        public long readLong() throws IOException {
            return delegate.readLong();
        }

        @Override
        public void readLongs(long[] dst, int offset, int length) throws IOException {
            delegate.readLongs(dst, offset, length);
        }

        @Override
        public void readInts(int[] dst, int offset, int length) throws IOException {
            delegate.readInts(dst, offset, length);
        }

        @Override
        public void readFloats(float[] floats, int offset, int len) throws IOException {
            delegate.readFloats(floats, offset, len);
        }

        @Override
        public long readVLong() throws IOException {
            return delegate.readVLong();
        }

        @Override
        public long readZLong() throws IOException {
            return delegate.readZLong();
        }

        @Override
        public String readString() throws IOException {
            // readMapOfStrings and readSetOfStrings delegates to this method
            return getCanonicalString(delegate.readString());
        }

        @Override
        public String toString() {
            return delegate.toString();
        }

        @Override
        public IndexInput clone() {
            return new ChecksumIndexInputWrapper((ChecksumIndexInput) delegate.clone());
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
