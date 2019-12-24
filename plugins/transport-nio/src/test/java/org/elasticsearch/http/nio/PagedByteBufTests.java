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

package org.elasticsearch.http.nio;

import io.netty.buffer.ByteBuf;
import org.elasticsearch.nio.Page;
import org.elasticsearch.test.ESTestCase;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class PagedByteBufTests extends ESTestCase {

    public void testReleasingPage() {
        AtomicInteger integer = new AtomicInteger(0);
        int pageCount = randomInt(10) + 1;
        ArrayList<Page> pages = new ArrayList<>();
        for (int i = 0; i < pageCount; ++i) {
            pages.add(new Page(ByteBuffer.allocate(10), integer::incrementAndGet));
        }

        ByteBuf byteBuf = PagedByteBuf.byteBufFromPages(pages.toArray(new Page[0]));

        assertEquals(0, integer.get());
        byteBuf.retain();
        byteBuf.release();
        assertEquals(0, integer.get());
        ByteBuf secondBuf = byteBuf.retainedSlice();
        byteBuf.release();
        assertEquals(0, integer.get());
        secondBuf.release();
        assertEquals(pageCount, integer.get());
    }

    public void testBytesAreUsed() {
        byte[] bytes1 = new byte[10];
        byte[] bytes2 = new byte[10];

        for (int i = 0; i < 10; ++i) {
            bytes1[i] = (byte) i;
        }

        for (int i = 10; i < 20; ++i) {
            bytes2[i - 10] = (byte) i;
        }

        Page[] pages = new Page[2];
        pages[0] = new Page(ByteBuffer.wrap(bytes1), () -> {});
        pages[1] = new Page(ByteBuffer.wrap(bytes2), () -> {});

        ByteBuf byteBuf = PagedByteBuf.byteBufFromPages(pages);
        assertEquals(20, byteBuf.readableBytes());

        for (int i = 0; i < 20; ++i) {
            assertEquals((byte) i, byteBuf.getByte(i));
        }

        Page[] pages2 = new Page[2];
        ByteBuffer firstBuffer = ByteBuffer.wrap(bytes1);
        firstBuffer.position(2);
        ByteBuffer secondBuffer = ByteBuffer.wrap(bytes2);
        secondBuffer.limit(8);
        pages2[0] = new Page(firstBuffer, () -> {});
        pages2[1] = new Page(secondBuffer, () -> {});

        ByteBuf byteBuf2 = PagedByteBuf.byteBufFromPages(pages2);
        assertEquals(16, byteBuf2.readableBytes());

        for (int i = 2; i < 18; ++i) {
            assertEquals((byte) i, byteBuf2.getByte(i - 2));
        }
    }
}
