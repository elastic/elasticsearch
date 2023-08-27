/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.test.ESTestCase;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class BigBytesStreamOutputTest extends ESTestCase {
    public void testEmpty() throws Exception {
        BytesStreamOutput out = new BigBytesStreamOutput();

        // test empty stream to array
        assertEquals(0, out.size());
        assertEquals(0, out.bytes().length());
        assertEquals((long) PageCacheRecycler.BYTE_PAGE_SIZE + 1, out.bytes.size());
        assertEquals(PageCacheRecycler.BYTE_PAGE_SIZE * 2, out.bytes.ramBytesUsed());
        out.close();
        out = new BigBytesStreamOutput(PageCacheRecycler.BYTE_PAGE_SIZE + 2);

        // test empty stream to array
        assertEquals(0, out.size());
        assertEquals(0, out.bytes().length());
        assertEquals((long) PageCacheRecycler.BYTE_PAGE_SIZE + 2, out.bytes.size());
        assertEquals(PageCacheRecycler.BYTE_PAGE_SIZE * 2, out.bytes.ramBytesUsed());
        out.close();
        out = new BigBytesStreamOutput(10);

        // test empty stream to array
        assertEquals(0, out.size());
        assertEquals(0, out.bytes().length());
        assertEquals((long) PageCacheRecycler.BYTE_PAGE_SIZE + 1, out.bytes.size());
        assertEquals(PageCacheRecycler.BYTE_PAGE_SIZE * 2, out.bytes.ramBytesUsed());
        out.close();
    }

    public void testFilled() throws Exception {
        BytesStreamOutput out = new BigBytesStreamOutput();
        byte b[] = new byte[PageCacheRecycler.BYTE_PAGE_SIZE + 1];
        out.write(b, 0, b.length);
        // test empty stream to array
        assertEquals(b.length, out.size());
        assertEquals(b.length, out.bytes().length());
        assertEquals((long) PageCacheRecycler.BYTE_PAGE_SIZE + 1, out.bytes.size());
        assertEquals(PageCacheRecycler.BYTE_PAGE_SIZE * 2, out.bytes.ramBytesUsed());
        out.close();
        out = new BigBytesStreamOutput();
        b = new byte[PageCacheRecycler.BYTE_PAGE_SIZE + 1];
        out.write(b, 0, b.length);
        out.write(b, 0, b.length);
        // test empty stream to array
        assertEquals(b.length * 2, out.size());
        assertEquals(b.length * 2, out.bytes().length());
        // This inconsistency is size is because of a code bug - Issue raised- https://github.com/elastic/elasticsearch/issues/98912
        // This test will start failing once this code bug is resolved.
        assertEquals((long) PageCacheRecycler.BYTE_PAGE_SIZE * 3, out.bytes.size());
        assertEquals(PageCacheRecycler.BYTE_PAGE_SIZE * 3, out.bytes.ramBytesUsed());
        out.close();
    }

    public void testInputStream() throws Exception {
        BigBytesStreamOutput out = new BigBytesStreamOutput();
        byte b[] = "Test String".getBytes(StandardCharsets.UTF_8);
        out.write(b, 0, b.length);
        InputStream in = out.resetAndGetInputStream();
        // test empty stream to array
        assertArrayEquals("Test String".getBytes(), in.readAllBytes());
        assertEquals(0, out.size());
        in = out.resetAndGetInputStream();
        assertEquals(0, in.available());
        out.close();
        out = new BigBytesStreamOutput();
        b = new byte[PageCacheRecycler.BYTE_PAGE_SIZE + 1];
        byte bt = 0x01;
        Arrays.fill(b, bt);
        out.write(b, 0, b.length);
        byte b2[] = new byte[PageCacheRecycler.BYTE_PAGE_SIZE + 1];
        bt = 0x02;
        Arrays.fill(b2, bt);
        out.write(b2, 0, b.length);
        in = out.resetAndGetInputStream();
        // test empty stream to array
        assertEquals(b.length + b2.length, in.available());
        assertArrayEquals(b, in.readNBytes(b.length));
        assertArrayEquals(b2, in.readNBytes(b.length));
        // test empty stream to array
        assertEquals(0, in.available());
        out.close();
    }

    protected byte[] randomizedByteArrayWithSize(int size) {
        byte[] data = new byte[size];
        random().nextBytes(data);
        return data;
    }

}
