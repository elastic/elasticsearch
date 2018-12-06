/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.hamcrest.Matchers.containsString;

public class ChannelsTests extends ESTestCase {

    byte[] randomBytes;
    FileChannel fileChannel;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        Path tmpFile = createTempFile();
        FileChannel randomAccessFile = FileChannel.open(tmpFile, StandardOpenOption.READ, StandardOpenOption.WRITE);
        fileChannel = new MockFileChannel(randomAccessFile);
        randomBytes = randomUnicodeOfLength(scaledRandomIntBetween(10, 100000)).getBytes("UTF-8");
    }

    @Override
    @After
    public void tearDown() throws Exception {
        fileChannel.close();
        super.tearDown();
    }

    public void testReadWriteThoughArrays() throws Exception {
        Channels.writeToChannel(randomBytes, fileChannel);
        byte[] readBytes = Channels.readFromFileChannel(fileChannel, 0, randomBytes.length);
        assertThat("read bytes didn't match written bytes", randomBytes, Matchers.equalTo(readBytes));
    }


    public void testPartialReadWriteThroughArrays() throws Exception {
        int length = randomIntBetween(1, randomBytes.length / 2);
        int offset = randomIntBetween(0, randomBytes.length - length);
        Channels.writeToChannel(randomBytes, offset, length, fileChannel);

        int lengthToRead = randomIntBetween(1, length);
        int offsetToRead = randomIntBetween(0, length - lengthToRead);
        byte[] readBytes = new byte[randomBytes.length];
        Channels.readFromFileChannel(fileChannel, offsetToRead, readBytes, offset + offsetToRead, lengthToRead);

        BytesReference source = new BytesArray(randomBytes, offset + offsetToRead, lengthToRead);
        BytesReference read = new BytesArray(readBytes, offset + offsetToRead, lengthToRead);

        assertThat("read bytes didn't match written bytes", BytesReference.toBytes(source), Matchers.equalTo(BytesReference.toBytes(read)));
    }

    public void testBufferReadPastEOFWithException() throws Exception {
        int bytesToWrite = randomIntBetween(0, randomBytes.length - 1);
        Channels.writeToChannel(randomBytes, 0, bytesToWrite, fileChannel);
        try {
            Channels.readFromFileChannel(fileChannel, 0, bytesToWrite + 1 + randomInt(1000));
            fail("Expected an EOFException");
        } catch (EOFException e) {
            assertThat(e.getMessage(), containsString("read past EOF"));
        }
    }

    public void testBufferReadPastEOFWithoutException() throws Exception {
        int bytesToWrite = randomIntBetween(0, randomBytes.length - 1);
        Channels.writeToChannel(randomBytes, 0, bytesToWrite, fileChannel);
        byte[] bytes = new byte[bytesToWrite + 1 + randomInt(1000)];
        int read = Channels.readFromFileChannel(fileChannel, 0, bytes, 0, bytes.length);
        assertThat(read, Matchers.lessThan(0));
    }

    public void testReadWriteThroughBuffers() throws IOException {
        ByteBuffer source;
        if (randomBoolean()) {
            source = ByteBuffer.wrap(randomBytes);
        } else {
            source = ByteBuffer.allocateDirect(randomBytes.length);
            source.put(randomBytes);
            source.flip();
        }
        Channels.writeToChannel(source, fileChannel);
        ByteBuffer copy;
        if (randomBoolean()) {
            copy = ByteBuffer.allocate(randomBytes.length);
        } else {
            copy = ByteBuffer.allocateDirect(randomBytes.length);
        }
        int read = Channels.readFromFileChannel(fileChannel, 0, copy);
        assertThat(read, Matchers.equalTo(randomBytes.length));
        byte[] copyBytes = new byte[read];
        copy.flip();
        copy.get(copyBytes);
        assertThat("read bytes didn't match written bytes", randomBytes, Matchers.equalTo(copyBytes));
    }

    public void testPartialReadWriteThroughBuffers() throws IOException {
        int length = randomIntBetween(1, randomBytes.length / 2);
        int offset = randomIntBetween(0, randomBytes.length - length);
        ByteBuffer source;
        if (randomBoolean()) {
            source = ByteBuffer.wrap(randomBytes, offset, length);
        } else {
            source = ByteBuffer.allocateDirect(length);
            source.put(randomBytes, offset, length);
            source.flip();
        }
        Channels.writeToChannel(source, fileChannel);

        int lengthToRead = randomIntBetween(1, length);
        int offsetToRead = randomIntBetween(0, length - lengthToRead);
        ByteBuffer copy;
        if (randomBoolean()) {
            copy = ByteBuffer.allocate(lengthToRead);
        } else {
            copy = ByteBuffer.allocateDirect(lengthToRead);
        }
        int read = Channels.readFromFileChannel(fileChannel, offsetToRead, copy);
        assertThat(read, Matchers.equalTo(lengthToRead));
        copy.flip();

        BytesReference sourceRef = new BytesArray(randomBytes, offset + offsetToRead, lengthToRead);
        byte[] tmp = new byte[copy.remaining()];
        copy.duplicate().get(tmp);
        BytesReference copyRef = new BytesArray(tmp);

        assertTrue("read bytes didn't match written bytes", sourceRef.equals(copyRef));
    }

    class MockFileChannel extends FileChannel {

        FileChannel delegate;

        MockFileChannel(FileChannel delegate) {
            this.delegate = delegate;
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            // delay buffer read..
            int willActuallyRead = randomInt(dst.remaining());
            ByteBuffer mockDst = dst.duplicate();
            mockDst.limit(mockDst.position() + willActuallyRead);
            try {
                return delegate.read(mockDst);
            } finally {
                dst.position(mockDst.position());
            }
        }

        @Override
        public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
            return delegate.read(dsts, offset, length);
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            // delay buffer write..
            int willActuallyWrite = randomInt(src.remaining());
            ByteBuffer mockSrc = src.duplicate();
            mockSrc.limit(mockSrc.position() + willActuallyWrite);
            try {
                return delegate.write(mockSrc);
            } finally {
                src.position(mockSrc.position());
            }
        }

        @Override
        public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
            return delegate.write(srcs, offset, length);
        }

        @Override
        public long position() throws IOException {
            return delegate.position();
        }

        @Override
        public FileChannel position(long newPosition) throws IOException {
            return delegate.position(newPosition);
        }

        @Override
        public long size() throws IOException {
            return delegate.size();
        }

        @Override
        public FileChannel truncate(long size) throws IOException {
            return delegate.truncate(size);
        }

        @Override
        public void force(boolean metaData) throws IOException {
            delegate.force(metaData);
        }

        @Override
        public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
            return delegate.transferTo(position, count, target);
        }

        @Override
        public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
            return delegate.transferFrom(src, position, count);
        }

        @Override
        public int read(ByteBuffer dst, long position) throws IOException {
            return delegate.read(dst, position);
        }

        @Override
        public int write(ByteBuffer src, long position) throws IOException {
            return delegate.write(src, position);
        }

        @Override
        public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
            return delegate.map(mode, position, size);
        }

        @Override
        public FileLock lock(long position, long size, boolean shared) throws IOException {
            return delegate.lock(position, size, shared);
        }

        @Override
        public FileLock tryLock(long position, long size, boolean shared) throws IOException {
            return delegate.tryLock(position, size, shared);
        }

        @Override
        protected void implCloseChannel() throws IOException {
            delegate.close();
        }
    }
}
