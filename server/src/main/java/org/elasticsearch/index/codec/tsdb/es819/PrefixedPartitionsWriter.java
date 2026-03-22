/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es819;

import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Writes prefix-based partition metadata for the primary sort field during segment flush. Terms are grouped by their first
 * {@link #PARTITION_PREFIX_BITS} bits, and the starting document for each prefix group is recorded. This enables the query engine
 * to partition work by prefix without scanning all doc values.
 * <p>
 * Usage is two-pass: first call {@link #trackTerm} for each term during the terms dict write, then call {@link #prepareForTrackingDocs}
 * to compact the prefix-to-ordinal mapping, and finally call {@link #trackDoc} for each ordinal during the numeric field write.
 *
 * <pre>{@code
 * // Pass 1: track terms during terms dict write
 * PrefixedPartitionsWriter writer = new PrefixedPartitionsWriter();
 * for (BytesRef term = termsEnum.next(); term != null; term = termsEnum.next()) {
 *     writer.trackTerm(term, termsEnum.ord());
 * }
 *
 * // Transition: compact prefix-to-ordinal mapping
 * writer.prepareForTrackingDocs();
 *
 * // Pass 2: track start docs
 * for (int doc = values.nextDoc(); doc != NO_MORE_DOCS; doc = values.nextDoc()) {
 *     writer.trackDoc(doc, values.ordValue());
 * }
 *
 * writer.flush(data, meta);
 * }</pre>
 *
 * @see PrefixedPartitionsReader
 */
final class PrefixedPartitionsWriter {
    static final int PARTITION_PREFIX_BITS = 18; // the first byte is the metric type; the remaining 10 bits provide up to 1024 partitions
                                                 // per metric
    static final int PAGE_SHIFT = PARTITION_PREFIX_BITS - Byte.SIZE;
    static final int PAGE_MASK = (1 << PAGE_SHIFT) - 1;
    static final VarHandle BE_INT = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);

    private int currentOrd = -1;

    private final int[][] ordPages;

    private int numPrefixes;
    private int idx = 0;
    private int[] startDocs;

    // iteration state for nextOrd()
    private int pageIter = 0;
    private int offsetIter = -1;

    PrefixedPartitionsWriter() {
        ordPages = new int[1 << (PARTITION_PREFIX_BITS - PAGE_SHIFT)][];
    }

    private static int smallPrefix(BytesRef term) {
        int length = term.length;
        if (length == 0) {
            return 0;
        }
        int b0 = term.bytes[term.offset] & 0xFF;
        if (length == 1) {
            return b0 << PAGE_SHIFT;
        }
        int b1 = term.bytes[term.offset + 1] & 0xFF;
        if (length == 2) {
            return ((b0 << 8) | b1) << (PAGE_SHIFT - Byte.SIZE);
        }
        int b2 = term.bytes[term.offset + 2] & 0xFF;
        return ((b0 << 16) | (b1 << 8) | b2) >>> (2 * Byte.SIZE - PAGE_SHIFT);
    }

    private static int prefix(BytesRef term) {
        if (term.length < 4) {
            return smallPrefix(term);
        }
        return (int) BE_INT.get(term.bytes, term.offset) >>> (Integer.SIZE - PARTITION_PREFIX_BITS);
    }

    void trackTerm(BytesRef term, long ord) {
        final int prefix = prefix(term);
        final int pageIndex = prefix >>> PAGE_SHIFT;
        final int offsetInPage = prefix & PAGE_MASK;
        int[] page = ordPages[pageIndex];
        if (page == null) {
            page = ordPages[pageIndex] = new int[1 << PAGE_SHIFT];
            Arrays.fill(page, -1);
            page[offsetInPage] = (int) ord;
            numPrefixes++;
        } else if (page[offsetInPage] == -1) {
            page[offsetInPage] = (int) ord;
            numPrefixes++;
        }
    }

    private int nextOrd() {
        while (pageIter < ordPages.length) {
            int[] page = ordPages[pageIter];
            if (page != null) {
                while (++offsetIter < page.length) {
                    int ord = page[offsetIter];
                    if (ord != -1) {
                        return ord;
                    }
                }
            }
            ++pageIter;
            offsetIter = -1;
        }
        return Integer.MAX_VALUE;
    }

    void prepareForTrackingDocs() {
        startDocs = new int[numPrefixes];
        currentOrd = nextOrd();
    }

    void trackDoc(int docId, long ord) {
        while (currentOrd <= ord) {
            startDocs[idx++] = docId;
            currentOrd = nextOrd();
        }
    }

    void flush(IndexOutput data, IndexOutput meta) throws IOException {
        final long startPointer = data.getFilePointer();
        data.writeVInt(numPrefixes);
        int last = 0;
        for (int i = 0; i < ordPages.length; i++) {
            final int[] page = ordPages[i];
            if (page != null) {
                for (int j = 0; j < page.length; j++) {
                    int ord = page[j];
                    if (ord != -1) {
                        int prefix = i << PAGE_SHIFT | j;
                        data.writeVInt(prefix - last);
                        last = prefix;
                    }
                }
            }
        }
        last = 0;
        for (int startDoc : startDocs) {
            data.writeVInt(startDoc - last);
            last = startDoc;
        }
        final long length = data.getFilePointer() - startPointer;
        meta.writeByte((byte) PARTITION_PREFIX_BITS);
        meta.writeLong(startPointer);
        meta.writeVLong(length);
    }
}
