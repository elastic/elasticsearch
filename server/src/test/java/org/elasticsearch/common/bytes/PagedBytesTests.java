/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.common.bytes;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

import static org.elasticsearch.common.util.PageCacheRecycler.BYTE_PAGE_SIZE;
import static org.hamcrest.Matchers.equalTo;

public class PagedBytesTests extends ESTestCase {
    public void testCompareTo() {
        for (int i = 0; i < 100; i++) {
            byte[] lhsFlat = randomByteArrayOfLength(randomIntBetween(0, BYTE_PAGE_SIZE * 3));
            byte[] rhsFlat = randomByteArrayOfLength(randomIntBetween(0, BYTE_PAGE_SIZE * 3));

            int expected = Integer.signum(Arrays.compareUnsigned(lhsFlat, rhsFlat));
            int actual = Integer.signum(newPagedBytes(lhsFlat).compareTo(newPagedBytes(rhsFlat)));
            assertThat(actual, equalTo(expected));
        }
    }

    public void testCompareToEqual() {
        byte[] flat = randomByteArrayOfLength(randomIntBetween(0, BYTE_PAGE_SIZE * 3));
        assertThat(newPagedBytes(flat).compareTo(newPagedBytes(flat)), equalTo(0));
    }

    public void testCompareToUnsigned() {
        // 0xFF unsigned is greater than 0x01
        assertThat(Integer.signum(newPagedBytes(new byte[] { (byte) 0xFF }).compareTo(newPagedBytes(new byte[] { 0x01 }))), equalTo(1));
    }

    public void testCompareToAcrossPageBoundary() {
        // lhs and rhs differ only in the last byte, which is on the second page
        byte[] lhsFlat = randomByteArrayOfLength(BYTE_PAGE_SIZE + 1);
        byte[] rhsFlat = Arrays.copyOf(lhsFlat, lhsFlat.length);
        lhsFlat[BYTE_PAGE_SIZE] = 0x01;
        rhsFlat[BYTE_PAGE_SIZE] = 0x02;
        assertThat(Integer.signum(newPagedBytes(lhsFlat).compareTo(newPagedBytes(rhsFlat))), equalTo(-1));
    }

    public void testEqualsAndHashCode() {
        for (int i = 0; i < 100; i++) {
            byte[] flat = randomByteArrayOfLength(randomIntBetween(0, BYTE_PAGE_SIZE * 3));
            PagedBytes a = newPagedBytes(flat);
            PagedBytes b = newPagedBytes(flat);
            assertEquals(a, b);
            assertThat(a.hashCode(), equalTo(b.hashCode()));
        }
    }

    public void testHashCodeMatchesBytesRef() {
        for (int i = 0; i < 100; i++) {
            byte[] flat = randomByteArrayOfLength(randomIntBetween(0, BYTE_PAGE_SIZE * 3));
            assertThat(newPagedBytes(flat).hashCode(), equalTo(new BytesRef(flat).hashCode()));
        }
    }

    public void testNotEqualsDifferentBytes() {
        byte[] flat = randomByteArrayOfLength(randomIntBetween(1, BYTE_PAGE_SIZE * 3));
        byte[] other = Arrays.copyOf(flat, flat.length);
        int pos = randomIntBetween(0, flat.length - 1);
        other[pos] = (byte) (flat[pos] + 1);
        assertNotEquals(newPagedBytes(flat), newPagedBytes(other));
    }

    public void testNotEqualsDifferentLength() {
        byte[] flat = randomByteArrayOfLength(randomIntBetween(1, BYTE_PAGE_SIZE * 3));
        assertNotEquals(newPagedBytes(flat), newPagedBytes(Arrays.copyOf(flat, flat.length - 1)));
    }

    public void testLimitedToStringEmpty() {
        assertThat(PagedBytes.limitedToString(new byte[0]), equalTo(""));
    }

    public void testLimitedToStringShort() {
        assertThat(PagedBytes.limitedToString(new byte[] { 0x0a, (byte) 0xff, 0x00 }), equalTo("0a ff 00"));
    }

    public void testLimitedToStringExactly100() {
        byte[] bytes = new byte[100];
        for (int i = 0; i < 100; i++) {
            bytes[i] = (byte) i;
        }
        String result = PagedBytes.limitedToString(bytes);
        assertFalse("should not truncate at exactly 100 bytes", result.endsWith("..."));
    }

    public void testLimitedToStringTruncates() {
        byte[] bytes = new byte[101];
        String result = PagedBytes.limitedToString(bytes);
        assertTrue("should truncate beyond 100 bytes", result.endsWith("..."));
        // should show exactly 100 tokens before the ellipsis
        String withoutEllipsis = result.substring(0, result.length() - 3);
        assertThat(withoutEllipsis.split(" ").length, equalTo(100));
    }

    /**
     * Builds a {@link PagedBytes} from a {@code byte[]}, splitting into pages of
     * {@link org.elasticsearch.common.util.PageCacheRecycler#BYTE_PAGE_SIZE}.
     */
    public static PagedBytes newPagedBytes(byte[] flat) {
        if (flat.length == 0) {
            return PagedBytes.EMPTY;
        }
        int pageCount = (flat.length + BYTE_PAGE_SIZE - 1) / BYTE_PAGE_SIZE;
        byte[][] pages = new byte[pageCount][];
        for (int p = 0; p < pageCount - 1; p++) {
            pages[p] = new byte[BYTE_PAGE_SIZE];
            System.arraycopy(flat, p * BYTE_PAGE_SIZE, pages[p], 0, BYTE_PAGE_SIZE);
        }
        int tailStart = (pageCount - 1) * BYTE_PAGE_SIZE;
        // Use a full BYTE_PAGE_SIZE page so bytes past the end are random garbage,
        // verifying that compareTo/equals respects length and ignores those bytes.
        if (randomBoolean()) {
            pages[pageCount - 1] = randomByteArrayOfLength(BYTE_PAGE_SIZE);
        } else {
            pages[pageCount - 1] = randomByteArrayOfLength(flat.length - tailStart);
        }
        System.arraycopy(flat, tailStart, pages[pageCount - 1], 0, flat.length - tailStart);
        return new PagedBytes(pages, flat.length, () -> {});
    }
}
