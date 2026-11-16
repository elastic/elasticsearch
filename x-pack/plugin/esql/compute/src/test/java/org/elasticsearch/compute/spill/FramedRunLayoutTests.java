/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * The layout's arithmetic and its header and footer codecs, including offsets past what an int can hold.
 */
public class FramedRunLayoutTests extends ESTestCase {

    public void testHeaderRoundTripAndCorruption() throws Exception {
        FramedRunLayout.Header header = new FramedRunLayout.Header(
            FramedRunLayout.FORMAT_VERSION,
            randomFrom(FramedRunLayout.KIND_PAGE, FramedRunLayout.KIND_VALUE, FramedRunLayout.KIND_SLICE),
            TransportVersion.current().id(),
            between(1, 1 << 24)
        );
        byte[] bytes = header.encode();
        assertThat(bytes.length, equalTo(FramedRunLayout.HEADER_BYTES));
        assertThat(FramedRunLayout.Header.decode(bytes, 0, "r"), equalTo(header));
        byte[] badMagic = bytes.clone();
        badMagic[3] ^= 1;
        assertThat(
            expectThrows(FramedRunCorruptionException.class, () -> FramedRunLayout.Header.decode(badMagic, 0, "r")).getMessage(),
            containsString("magic")
        );
        byte[] badVersion = bytes.clone();
        badVersion[8] = 7;
        assertThat(
            expectThrows(FramedRunCorruptionException.class, () -> FramedRunLayout.Header.decode(badVersion, 0, "r")).getMessage(),
            containsString("format version")
        );
        byte[] badChunk = bytes.clone();
        badChunk[20] = 0;
        badChunk[21] = 0;
        badChunk[22] = 0;
        badChunk[23] = 0;
        assertThat(
            expectThrows(FramedRunCorruptionException.class, () -> FramedRunLayout.Header.decode(badChunk, 0, "r")).getMessage(),
            containsString("chunk size")
        );
        assertThat(
            expectThrows(FramedRunCorruptionException.class, () -> FramedRunLayout.Header.decode(new byte[5], 0, "r")).getMessage(),
            containsString("expected")
        );
    }

    public void testFooterRoundTripAndCorruption() throws Exception {
        FramedRunLayout.Footer footer = new FramedRunLayout.Footer(
            randomLongBetween(0, 1L << 40),
            randomLongBetween(0, 1L << 40),
            randomLongBetween(32, 1L << 40),
            between(0, 1 << 16)
        );
        byte[] bytes = footer.encode();
        assertThat(bytes.length, equalTo(FramedRunLayout.FOOTER_BYTES));
        assertThat(FramedRunLayout.Footer.decode(bytes, 0, "r"), equalTo(footer));
        byte[] badMagic = bytes.clone();
        badMagic[35] ^= 1;
        assertThat(
            expectThrows(FramedRunCorruptionException.class, () -> FramedRunLayout.Footer.decode(badMagic, 0, "r")).getMessage(),
            containsString("truncated")
        );
        byte[] badCrc = bytes.clone();
        badCrc[between(0, 27)] ^= 0x10;
        assertThat(
            expectThrows(FramedRunCorruptionException.class, () -> FramedRunLayout.Footer.decode(badCrc, 0, "r")).getMessage(),
            containsString("checksum")
        );
    }

    public void testOffsetsBeyondTwoGigabytes() {
        int chunkSize = between(1, 1 << 20);
        long logical = randomLongBetween(1L << 31, 1L << 40);
        long chunk = logical / chunkSize;
        assertThat(FramedRunLayout.chunkOf(logical, chunkSize), equalTo(chunk));
        assertThat(
            FramedRunLayout.chunkStart(chunk, chunkSize),
            equalTo(FramedRunLayout.HEADER_BYTES + chunk * (chunkSize + FramedRunLayout.CHUNK_TRAILER_BYTES))
        );
        assertThat(
            FramedRunLayout.physicalOffset(logical, chunkSize),
            equalTo(FramedRunLayout.chunkStart(chunk, chunkSize) + logical % chunkSize)
        );
        // the physical offset of a logical byte never lands inside a trailer
        long physical = FramedRunLayout.physicalOffset(logical, chunkSize);
        long inChunk = (physical - FramedRunLayout.HEADER_BYTES) % (chunkSize + FramedRunLayout.CHUNK_TRAILER_BYTES);
        assertTrue(inChunk < chunkSize);
    }

    public void testPhysicalBodyLength() {
        int chunkSize = between(1, 4096);
        assertThat(FramedRunLayout.physicalBodyLength(0, chunkSize), equalTo(0L));
        assertThat(
            FramedRunLayout.physicalBodyLength(chunkSize, chunkSize),
            equalTo((long) chunkSize + FramedRunLayout.CHUNK_TRAILER_BYTES)
        );
        assertThat(
            FramedRunLayout.physicalBodyLength(chunkSize + 1L, chunkSize),
            equalTo(chunkSize + 1L + 2L * FramedRunLayout.CHUNK_TRAILER_BYTES)
        );
        long logical = randomLongBetween(1, 1L << 36);
        long chunks = (logical + chunkSize - 1) / chunkSize;
        assertThat(FramedRunLayout.physicalBodyLength(logical, chunkSize), equalTo(logical + chunks * FramedRunLayout.CHUNK_TRAILER_BYTES));
    }
}
