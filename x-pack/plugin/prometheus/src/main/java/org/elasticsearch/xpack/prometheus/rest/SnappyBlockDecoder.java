/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at:
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.rest.IndexingPressureAwareContentAggregator;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Decodes a Snappy block-compressed request body directly into recycled 16 KiB pages.
 * <p>
 * This is a fork of
 * <a href="https://github.com/netty/netty/blob/netty-4.1.130.Final/codec/src/main/java/io/netty/handler/codec/compression/Snappy.java">
 * {@code io.netty.handler.codec.compression.Snappy}</a> adapted to operate
 * on a {@link BytesReference} input (via {@link StreamInput}, zero-copy across chunks) and
 * recycled pages for output, following the same approach as
 * {@code org.elasticsearch.transport.Lz4TransportDecompressor}.
 * <p>
 * Neither the input nor the output requires a contiguous {@code byte[]} allocation.
 * Output pages are pre-allocated from the preamble-declared uncompressed length,
 * providing an additional bound that decoded output cannot exceed the declared size.
 *
 * @see <a href="https://github.com/google/snappy/blob/main/format_description.txt">Snappy format description</a>
 */
public final class SnappyBlockDecoder implements IndexingPressureAwareContentAggregator.BodyPostProcessor {

    private static final int LITERAL = 0;
    private static final int COPY_1_BYTE_OFFSET = 1;
    private static final int COPY_2_BYTE_OFFSET = 2;
    private static final int COPY_4_BYTE_OFFSET = 3;

    private final Recycler<BytesRef> recycler;

    public SnappyBlockDecoder(Recycler<BytesRef> recycler) {
        this.recycler = recycler;
    }

    @Override
    public ReleasableBytesReference process(ReleasableBytesReference body, long maxSize) throws IOException {
        try (body) {
            return decode(body.streamInput(), maxSize);
        }
    }

    private ReleasableBytesReference decode(StreamInput in, long maxSize) throws IOException {
        if (in.available() <= 0) {
            throw new IOException("empty snappy input");
        }

        int uncompressedLength = 0;
        int shift = 0;
        while (in.available() > 0) {
            int b = readUByte(in);
            uncompressedLength |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                break;
            }
            shift += 7;
            if (shift >= 32) {
                throw new IOException("snappy preamble is too large");
            }
        }

        if (uncompressedLength < 0) {
            throw new IOException("negative snappy uncompressed length: " + uncompressedLength);
        }
        if (uncompressedLength > maxSize) {
            throw new ElasticsearchStatusException(
                "snappy decompressed size [" + uncompressedLength + "] exceeds maximum [" + maxSize + "] bytes",
                RestStatus.REQUEST_ENTITY_TOO_LARGE
            );
        }
        if (uncompressedLength == 0) {
            return ReleasableBytesReference.empty();
        }

        var out = new PagedOutput(recycler, uncompressedLength);
        try {
            while (in.available() > 0 && out.written < uncompressedLength) {
                int tag = readUByte(in);
                switch (tag & 0x03) {
                    case LITERAL: {
                        int literalLen = readLiteralLength(tag, in);
                        out.writeLiteral(in, literalLen);
                        break;
                    }

                    case COPY_1_BYTE_OFFSET: {
                        int length = 4 + ((tag & 0x1C) >> 2);
                        int offset = ((tag & 0xE0) << 8 >> 5) | readUByte(in);
                        validateOffset(offset, out.written);
                        out.selfCopy(offset, length);
                        break;
                    }

                    case COPY_2_BYTE_OFFSET: {
                        int length = 1 + ((tag >> 2) & 0x3F);
                        int b0 = readUByte(in);
                        int b1 = readUByte(in);
                        int offset = b0 | (b1 << 8);
                        validateOffset(offset, out.written);
                        out.selfCopy(offset, length);
                        break;
                    }

                    case COPY_4_BYTE_OFFSET: {
                        int length = 1 + ((tag >> 2) & 0x3F);
                        int b0 = readUByte(in);
                        int b1 = readUByte(in);
                        int b2 = readUByte(in);
                        int b3 = readUByte(in);
                        int offset = b0 | (b1 << 8) | (b2 << 16) | (b3 << 24);
                        validateOffset(offset, out.written);
                        out.selfCopy(offset, length);
                        break;
                    }
                }
            }

            if (out.written != uncompressedLength) {
                throw new IOException("snappy: expected " + uncompressedLength + " bytes but decoded " + out.written);
            }

            return out.toBytesReference();
        } catch (Exception e) {
            out.close();
            throw e;
        }
    }

    private static int readUByte(StreamInput in) throws IOException {
        return in.readByte() & 0xFF;
    }

    /**
     * Reads the literal length from the tag and any following length bytes.
     */
    private static int readLiteralLength(int tag, StreamInput in) throws IOException {
        int val = (tag >> 2) & 0x3F;
        return switch (val) {
            case 60 -> readUByte(in) + 1;
            case 61 -> (readUByte(in) | (readUByte(in) << 8)) + 1;
            case 62 -> (readUByte(in) | (readUByte(in) << 8) | (readUByte(in) << 16)) + 1;
            case 63 -> (readUByte(in) | (readUByte(in) << 8) | (readUByte(in) << 16) | (readUByte(in) << 24)) + 1;
            default -> val + 1;
        };
    }

    private static void validateOffset(int offset, int writtenSoFar) throws IOException {
        if (offset <= 0) {
            throw new IOException("snappy: invalid copy offset " + offset);
        }
        if (offset > writtenSoFar) {
            throw new IOException("snappy: copy offset " + offset + " exceeds decoded bytes " + writtenSoFar);
        }
    }

    /**
     * Output buffer backed by recycled pages that supports both sequential writes and random
     * read access (needed for Snappy back-reference copies).
     */
    static final class PagedOutput {
        private final ArrayList<Recycler.V<BytesRef>> pages;
        private final int pageSize;
        private final int uncompressedLength;
        int written;

        PagedOutput(Recycler<BytesRef> recycler, int uncompressedLength) {
            this.pageSize = recycler.pageSize();
            this.uncompressedLength = uncompressedLength;
            int numPages = Math.ceilDiv(uncompressedLength, pageSize);
            this.pages = new ArrayList<>(numPages);
            try {
                for (int i = 0; i < numPages; i++) {
                    pages.add(recycler.obtain());
                }
            } catch (Exception e) {
                Releasables.close(pages);
                throw e;
            }
        }

        /**
         * Reads {@code length} bytes from the input and writes them directly into
         * output pages (literal copy, no intermediate buffer).
         */
        void writeLiteral(StreamInput in, int length) throws IOException {
            validateOutputLength(length);
            int remaining = length;
            while (remaining > 0) {
                int pageOff = written % pageSize;
                BytesRef page = pages.get(written / pageSize).v();
                int spaceInPage = pageSize - pageOff;
                int toCopy = Math.min(remaining, spaceInPage);

                // Copy from the input stream directly into the output page
                in.readBytes(page.bytes, page.offset + pageOff, toCopy);
                written += toCopy;
                remaining -= toCopy;
            }
        }

        /**
         * Copies {@code length} bytes from {@code backOffset} bytes behind the write position,
         * handling the overlap case where {@code backOffset < length} (run-length repetition).
         */
        void selfCopy(int backOffset, int length) throws IOException {
            validateOutputLength(length);
            int srcPos = written - backOffset;
            if (backOffset >= length) {
                bulkCopy(srcPos, length);
            } else {
                int remaining = length;
                // Each iteration doubles the copyable region: after copying backOffset bytes,
                // the destination now contains those bytes too, extending the available source.
                int copyable = backOffset;
                while (remaining > 0) {
                    int toCopy = Math.min(remaining, copyable);
                    bulkCopy(srcPos, toCopy);
                    remaining -= toCopy;
                    copyable += toCopy;
                }
            }
        }

        private void validateOutputLength(int length) throws IOException {
            // widen to long to avoid integer overflow
            if ((long) written + length > uncompressedLength) {
                throw new IOException(
                    "snappy: output of "
                        + ((long) written + length)
                        + " bytes would exceed declared uncompressed length of "
                        + uncompressedLength
                );
            }
        }

        /**
         * Bulk-copies {@code length} bytes from absolute position {@code srcPos} to the current
         * write position, handling page-boundary crossings on both source and destination.
         */
        private void bulkCopy(int srcPos, int length) {
            int remaining = length;
            int src = srcPos;
            while (remaining > 0) {
                int srcPageOff = src % pageSize;
                int dstPageOff = written % pageSize;

                BytesRef srcPage = pages.get(src / pageSize).v();
                BytesRef dstPage = pages.get(written / pageSize).v();

                int srcAvail = pageSize - srcPageOff;
                int dstAvail = pageSize - dstPageOff;
                int toCopy = Math.min(remaining, Math.min(srcAvail, dstAvail));

                System.arraycopy(srcPage.bytes, srcPage.offset + srcPageOff, dstPage.bytes, dstPage.offset + dstPageOff, toCopy);

                src += toCopy;
                written += toCopy;
                remaining -= toCopy;
            }
        }

        /** Assembles the written pages into a {@link ReleasableBytesReference}. */
        ReleasableBytesReference toBytesReference() {
            assert pages.isEmpty() == false
                : "toBytesReference() should only be called when uncompressedLength > 0, so at least one page must exist";
            if (pages.size() == 1) {
                Recycler.V<BytesRef> page = pages.getFirst();
                BytesRef ref = page.v();
                return new ReleasableBytesReference(new BytesArray(ref.bytes, ref.offset, written), page);
            }
            BytesReference[] refs = new BytesReference[pages.size()];
            int remaining = written;
            for (int i = 0; i < pages.size(); i++) {
                BytesRef ref = pages.get(i).v();
                int len = Math.min(remaining, pageSize);
                refs[i] = new BytesArray(ref.bytes, ref.offset, len);
                remaining -= len;
            }
            return new ReleasableBytesReference(CompositeBytesReference.of(refs), Releasables.wrap(pages));
        }

        void close() {
            Releasables.close(pages);
        }
    }
}
