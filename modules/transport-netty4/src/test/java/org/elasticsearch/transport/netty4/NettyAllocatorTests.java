/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.transport.netty4.TrashingByteBufAllocator.TrashingByteBuf;

public class NettyAllocatorTests extends ESTestCase {

    static void assertBufferTrashed(BytesReference bytesRef) throws IOException {
        var iter = bytesRef.iterator();
        BytesRef br;
        while ((br = iter.next()) != null) {
            for (var i = br.offset; i < br.offset + br.length; i++) {
                assertEquals("off=" + br.offset + " len=" + br.length + " i=" + i, 0, br.bytes[i]);
            }
        }
    }

    public void testTrashArrayByteBuf() {
        var arr = randomByteArrayOfLength(between(1024, 2048));
        var buf = Unpooled.wrappedBuffer(arr);
        var tBuf = new TrashingByteBuf(buf);
        tBuf.release();
        var emptyArr = new byte[arr.length];
        assertArrayEquals(emptyArr, arr);
    }

    public void testNioBufsTrashingByteBuf() {
        var arrCnt = between(1, 16);
        var byteArrs = new byte[arrCnt][];
        var byteBufs = new ByteBuffer[arrCnt];
        for (var i = 0; i < arrCnt; i++) {
            byteArrs[i] = randomByteArrayOfLength(between(1024, 2048));
            byteBufs[i] = ByteBuffer.wrap(byteArrs[i]);
        }
        var buf = Unpooled.wrappedBuffer(byteBufs);
        var tBuf = new TrashingByteBuf(buf);
        tBuf.release();
        for (int i = 0; i < arrCnt; i++) {
            for (int j = 0; j < byteArrs[i].length; j++) {
                assertEquals(0, byteArrs[i][j]);
            }
        }
    }

    public void testNioBufOffsetTrashingByteBuf() {
        var arr = randomByteArrayOfLength(1024);
        var off = 1;
        var len = arr.length - 2;
        arr[0] = 1;
        arr[arr.length - 1] = 1;
        var buf = Unpooled.wrappedBuffer(arr, off, len);
        var tBuf = new TrashingByteBuf(buf);
        tBuf.release();
        assertEquals(1, arr[0]);
        assertEquals(1, arr[arr.length - 1]);
        for (int i = 1; i < arr.length - 1; i++) {
            assertEquals("at index " + i, 0, arr[i]);
        }
    }

    public void testTrashingByteBufAllocator() throws IOException {
        var alloc = new TrashingByteBufAllocator(ByteBufAllocator.DEFAULT);
        var size = between(1024 * 1024, 10 * 1024 * 1024);

        // use 3 different heap allocation methods
        for (var buf : List.of(alloc.heapBuffer(), alloc.heapBuffer(1024), alloc.heapBuffer(1024, size))) {
            buf.writeBytes(randomByteArrayOfLength(size));
            var bytesRef = Netty4Utils.toBytesReference(buf);
            buf.release();
            assertBufferTrashed(bytesRef);
        }
    }

    public void testRetainedSliceTrashedOnlyAfterRootAndSliceReleased() throws IOException {
        var alloc = new TrashingByteBufAllocator(ByteBufAllocator.DEFAULT);
        var size = between(1024, 4096);
        var content = randomByteArrayOfLength(size);
        var root = alloc.heapBuffer(size, size);
        root.writeBytes(content);

        var off = between(0, size - 2);
        var len = between(1, size - off - 1);
        var slice = root.retainedSlice(off, len);
        var sliceRef = Netty4Utils.toBytesReference(slice);
        var expected = Arrays.copyOfRange(content, off, off + len);

        assertEquals(2, root.refCnt());
        assertEquals(1, slice.refCnt());

        root.release();
        assertEquals("slice content must survive releasing the root, off=" + off + " len=" + len, 1, slice.refCnt());
        assertArrayEquals(
            "slice content must survive releasing the root, off=" + off + " len=" + len,
            expected,
            BytesReference.toBytes(sliceRef)
        );

        slice.release();
        assertEquals(0, slice.refCnt());
        assertBufferTrashed(sliceRef);
    }

    public void testRootContentSurvivesReleasingRetainedSlice() throws IOException {
        var alloc = new TrashingByteBufAllocator(ByteBufAllocator.DEFAULT);
        var size = between(1024, 4096);
        var content = randomByteArrayOfLength(size);
        var root = alloc.heapBuffer(size, size);
        root.writeBytes(content);

        var off = between(0, size - 2);
        var len = between(1, size - off - 1);
        var slice = root.retainedSlice(off, len);
        var rootRef = Netty4Utils.toBytesReference(root);

        slice.release();
        assertEquals(0, slice.refCnt());
        assertEquals(1, root.refCnt());
        assertArrayEquals(
            "root content must survive releasing the retained slice, off=" + off + " len=" + len,
            content,
            BytesReference.toBytes(rootRef)
        );

        root.release();
        assertBufferTrashed(rootRef);
    }

    public void testTrashingStaysInsideOwnPooledRegion() throws IOException {
        var alloc = new TrashingByteBufAllocator(ByteBufAllocator.DEFAULT);
        var size = between(64, 512);
        var before = alloc.heapBuffer(size, size);
        var victim = alloc.heapBuffer(size, size);
        var after = alloc.heapBuffer(size, size);

        var beforeContent = randomByteArrayOfLength(size);
        var afterContent = randomByteArrayOfLength(size);
        before.writeBytes(beforeContent);
        victim.writeBytes(randomByteArrayOfLength(size));
        after.writeBytes(afterContent);

        assertSame("expected pooled buffers to share a chunk array", victim.array(), before.array());
        assertSame("expected pooled buffers to share a chunk array", victim.array(), after.array());
        assertEquals(
            "expected three distinct slots in the chunk, offsets="
                + before.arrayOffset()
                + ","
                + victim.arrayOffset()
                + ","
                + after.arrayOffset(),
            3,
            Set.of(before.arrayOffset(), victim.arrayOffset(), after.arrayOffset()).size()
        );

        var beforeRef = Netty4Utils.toBytesReference(before);
        var afterRef = Netty4Utils.toBytesReference(after);

        victim.release();

        assertArrayEquals("preceding pooled buffer must not be trashed", beforeContent, BytesReference.toBytes(beforeRef));
        assertArrayEquals("following pooled buffer must not be trashed", afterContent, BytesReference.toBytes(afterRef));

        before.release();
        after.release();
    }

    public void testRetainedSliceShouldNotTrashContentOnRelease() {
        var alloc = new TrashingByteBufAllocator(ByteBufAllocator.DEFAULT);
        var buf = alloc.heapBuffer();
        var data = randomByteArrayOfLength(between(1024, 2048));
        buf.writeBytes(data);

        var slice = buf.retainedSlice();

        slice.release();

        var actual = new byte[data.length];
        buf.getBytes(buf.readerIndex(), actual);
        assertArrayEquals("Original buffer content should be intact after releasing a retained slice", data, actual);

        buf.release();
    }

    public void testFullyConsumedBufferIsTrashed() throws IOException {
        var alloc = new TrashingByteBufAllocator(ByteBufAllocator.DEFAULT);
        var size = between(1024, 4096);
        var root = alloc.heapBuffer(size, size);
        root.writeBytes(randomByteArrayOfLength(size));
        var ref = Netty4Utils.toBytesReference(root);

        root.skipBytes(size);
        assertEquals("reader index must have caught up with writer index", 0, root.readableBytes());

        root.release();
        assertBufferTrashed(ref);
    }

    public void testNestedRetainedSlicesTrashOnlyAfterLastRelease() throws IOException {
        var alloc = new TrashingByteBufAllocator(ByteBufAllocator.DEFAULT);
        var size = between(1024, 4096);
        var content = randomByteArrayOfLength(size);
        var root = alloc.heapBuffer(size, size);
        root.writeBytes(content);
        var rootRef = Netty4Utils.toBytesReference(root);

        var bufs = new ArrayList<ByteBuf>();
        bufs.add(root);
        for (var i = 0; i < between(1, 6); i++) {
            var parent = randomFrom(bufs);
            var len = parent.readableBytes();
            if (len < 2) {
                continue;
            }
            var off = between(0, len - 2);
            bufs.add(parent.retainedSlice(off, between(1, len - off - 1)));
        }

        Collections.shuffle(bufs, random());
        for (var i = 0; i < bufs.size(); i++) {
            assertArrayEquals(
                "content must be intact with " + (bufs.size() - i) + " of " + bufs.size() + " references outstanding",
                content,
                BytesReference.toBytes(rootRef)
            );
            bufs.get(i).release();
        }
        assertBufferTrashed(rootRef);
    }

    public void testTrashingCompositeByteBuf() throws IOException {
        var alloc = new TrashingByteBufAllocator(ByteBufAllocator.DEFAULT);
        var compBuf = alloc.compositeHeapBuffer();
        for (var i = 0; i < between(1, 10); i++) {
            var buf = alloc.heapBuffer().writeBytes(randomByteArrayOfLength(between(1024, 8192)));
            compBuf.addComponent(true, buf);
        }
        var bytesRef = Netty4Utils.toBytesReference(compBuf);
        compBuf.release();
        assertBufferTrashed(bytesRef);
    }

}
