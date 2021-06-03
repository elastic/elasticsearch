/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.util.offheap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.core.internal.io.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.MappedByteBuffer;
import java.nio.ShortBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class MappedTempDirectBuffer implements Closeable, Releasable {

    private static final Logger logger = LogManager.getLogger(MappedTempDirectBuffer.class);
    private final Path tmpFilePath;
    private final RandomAccessFile tmpFile;
    private final AtomicReference<MappedByteBuffer> bufferRef = new AtomicReference<>();
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    public MappedTempDirectBuffer(long initialFileLength, Path tempDirectory) throws IOException {
        this.tmpFilePath = generateRandomFilePath(tempDirectory);
        this.tmpFile = createTempFile(tmpFilePath, initialFileLength);
        this.bufferRef.set(initMappedByteBuffer(tmpFile));
    }

    public MappedTempDirectBuffer(long arraySize, int numBytesPerElement, Path tmpDirectory) throws IOException {
        this(arraySize * numBytesPerElement, tmpDirectory);
    }

    public static MappedTempDirectBuffer createDirectBuffer(long arraySize, int numBytesPerElement, Path tmpDirectory) throws IOException {
        return new MappedTempDirectBuffer(arraySize, numBytesPerElement, tmpDirectory);
    }


    @Override
    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            try {
                releaseBuffer(bufferRef.getAndSet(null));
            } catch (Exception e) {
                String errMsg = "unable to release temp direct buffer";
                logger.error(errMsg, e);
                throw new RuntimeException(errMsg, e);
            } finally {
                try {
                    IOUtils.close(tmpFile);
                } catch (IOException e) {
                    logger.error(e);
                }
                IOUtils.deleteFilesIgnoringExceptions(tmpFilePath);
            }
        }
    }

    public void resizeInPlace(long arraySize, int numBytesPerElement) throws IOException, ReflectiveOperationException {
        long length = arraySize * numBytesPerElement;
        tmpFile.setLength(length);
        MappedByteBuffer newBuffer = initMappedByteBuffer(tmpFile);
        MappedByteBuffer prev = bufferRef.getAndSet(newBuffer);
        releaseBuffer(prev);
    }

    public ByteBuffer duplicate() {
        validateOperation();
        return delegate().duplicate();
    }

    public ByteBuffer asByteBuffer() {
        return delegate();
    }


    public ByteBuffer asReadOnlyBuffer() {
        validateOperation();
        return delegate().asReadOnlyBuffer();
    }


    public byte get() {
        validateOperation();
        return delegate().get();
    }


    public ByteBuffer put(byte b) {
        validateOperation();
        return delegate().put(b);
    }


    public byte get(int index) {
        validateOperation();
        return delegate().get(index);
    }


    public ByteBuffer put(int index, byte b) {
        validateOperation();
        return delegate().put(index, b);
    }


    public ByteBuffer compact() {
        validateOperation();
        return delegate().compact();
    }


    public boolean isReadOnly() {
        validateOperation();
        return delegate().isReadOnly();
    }


    public boolean isDirect() {
        validateOperation();
        return delegate().isDirect();
    }


    public char getChar() {
        validateOperation();
        return delegate().getChar();
    }


    public ByteBuffer putChar(char value) {
        validateOperation();
        return delegate().putChar(value);
    }


    public char getChar(int index) {
        validateOperation();
        return delegate().getChar(index);
    }


    public ByteBuffer putChar(int index, char value) {
        validateOperation();
        return delegate().putChar(index, value);
    }


    public CharBuffer asCharBuffer() {
        validateOperation();
        return delegate().asCharBuffer();
    }


    public short getShort() {
        validateOperation();
        return delegate().getShort();
    }


    public ByteBuffer putShort(short value) {
        validateOperation();
        return delegate().putShort(value);
    }


    public short getShort(int index) {
        validateOperation();
        return delegate().getShort(index);
    }


    public ByteBuffer putShort(int index, short value) {
        validateOperation();
        return delegate().putShort(index, value);
    }


    public ShortBuffer asShortBuffer() {
        validateOperation();
        return delegate().asShortBuffer();
    }


    public int getInt() {
        validateOperation();
        return delegate().getInt();
    }


    public ByteBuffer putInt(int value) {
        validateOperation();
        return delegate().putInt(value);
    }


    public int getInt(int index) {
        validateOperation();
        return delegate().getInt(index);
    }


    public ByteBuffer putInt(int index, int value) {
        validateOperation();
        return delegate().putInt(index, value);
    }


    public IntBuffer asIntBuffer() {
        validateOperation();
        return delegate().asIntBuffer();
    }


    public long getLong() {
        validateOperation();
        return delegate().getLong();
    }


    public ByteBuffer putLong(long value) {
        validateOperation();
        return delegate().putLong(value);
    }


    public long getLong(int index) {
        validateOperation();
        return delegate().getLong(index);
    }


    public ByteBuffer putLong(int index, long value) {
        validateOperation();
        return delegate().putLong(index, value);
    }


    public LongBuffer asLongBuffer() {
        validateOperation();
        return delegate().asLongBuffer();
    }


    public float getFloat() {
        validateOperation();
        return delegate().getFloat();
    }


    public ByteBuffer putFloat(float value) {
        validateOperation();
        return delegate().putFloat(value);
    }


    public float getFloat(int index) {
        validateOperation();
        return delegate().getFloat(index);
    }


    public ByteBuffer putFloat(int index, float value) {
        validateOperation();
        return delegate().putFloat(index, value);
    }


    public FloatBuffer asFloatBuffer() {
        validateOperation();
        return delegate().asFloatBuffer();
    }


    public double getDouble() {
        validateOperation();
        return delegate().getDouble();
    }


    public ByteBuffer putDouble(double value) {
        validateOperation();
        return delegate().putDouble(value);
    }


    public double getDouble(int index) {
        validateOperation();
        return delegate().getDouble(index);
    }


    public ByteBuffer putDouble(int index, double value) {
        validateOperation();
        return delegate().putDouble(index, value);
    }


    public DoubleBuffer asDoubleBuffer() {
        validateOperation();
        return delegate().asDoubleBuffer();
    }


    private RandomAccessFile createTempFile(Path filePath, long fileLength) throws IOException {
        RandomAccessFile raFile = new RandomAccessFile(filePath.toFile(), "rw");
        raFile.setLength(fileLength);
        return raFile;
    }

    private MappedByteBuffer initMappedByteBuffer(RandomAccessFile raFile) throws IOException {
        FileChannel fc = raFile.getChannel();
        return fc.map(FileChannel.MapMode.READ_WRITE, 0, fc.size());
    }


    private Path generateRandomFilePath(Path parent) {
        return PathUtils.get(parent.toString(), "temp_arr_" + UUIDs.randomBase64UUID());
    }

    private void validateOperation() {
        if (isClosed.get()) {
            throw new UnsupportedOperationException("already closed");
        }
    }

    /**
     * release direct buffer by reflection
     */
    private void releaseBuffer(MappedByteBuffer buffer) throws IOException {
        CleanerUtil.getCleaner().freeBuffer(buffer);
    }

    private MappedByteBuffer delegate() {
        return bufferRef.get();
    }

}
