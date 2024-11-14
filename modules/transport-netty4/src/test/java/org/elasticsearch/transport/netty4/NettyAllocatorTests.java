/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport.netty4;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class NettyAllocatorTests extends ESTestCase {

    public void testArrayThrashingByteBuf() {
        var arr = randomByteArrayOfLength(between(1024, 2048));
        var buf = Unpooled.wrappedBuffer(arr);
        var tBuf = new NettyAllocator.ThrashingByteBuf(buf);
        tBuf.release();
        var emptyArr = new byte[arr.length];
        assertArrayEquals(emptyArr, arr);
    }

    public void testNioBufsThrashingByteBuf() {
        var arrCnt = between(1, 16);
        var byteArrs = new byte[arrCnt][];
        var byteBufs = new ByteBuffer[arrCnt];
        for (var i = 0; i < arrCnt; i++) {
            byteArrs[i] = randomByteArrayOfLength(between(1024, 2048));
            byteBufs[i] = ByteBuffer.wrap(byteArrs[i]);
        }
        var buf = Unpooled.wrappedBuffer(byteBufs);
        var tBuf = new NettyAllocator.ThrashingByteBuf(buf);
        tBuf.release();
        for (int i = 0; i < arrCnt; i++) {
            for (int j = 0; j < byteArrs[i].length; j++) {
                assertEquals(0, byteArrs[i][j]);
            }
        }
    }

    public void testNioBufOffsetThrashingByteBuf() {
        var arr = randomByteArrayOfLength(1024);
        var off = 1;
        var len = arr.length - 2;
        arr[0] = 1;
        arr[arr.length - 1] = 1;
        var buf = Unpooled.wrappedBuffer(arr, off, len);
        var tBuf = new NettyAllocator.ThrashingByteBuf(buf);
        tBuf.release();
        assertEquals(1, arr[0]);
        assertEquals(1, arr[arr.length - 1]);
        for (int i = 1; i < arr.length - 1; i++) {
            assertEquals("at index " + i, 0, arr[i]);
        }
    }

    public void testThrashingByteBufAllocator() throws IOException {
        var alloc = new NettyAllocator.ThrashingByteBufAllocator(ByteBufAllocator.DEFAULT);
        var size = between(1024 * 1024, 10 * 1024 * 1024);

        // use 3 different heap allocation methods
        for (var buf : List.of(alloc.heapBuffer(), alloc.heapBuffer(1024), alloc.heapBuffer(1024, size))) {
            buf.writeBytes(randomByteArrayOfLength(size));
            var bytesRef = Netty4Utils.toBytesReference(buf);
            buf.release();

            var iter = bytesRef.iterator();
            BytesRef br;
            while ((br = iter.next()) != null) {
                for (var i = br.offset; i < br.offset + br.length; i++) {
                    assertEquals("off=" + br.offset + " len=" + br.length + " i=" + i, 0, br.bytes[i]);
                }
            }
        }
    }
}
