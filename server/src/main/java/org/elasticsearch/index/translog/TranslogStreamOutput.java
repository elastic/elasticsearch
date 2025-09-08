/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.translog;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.recycler.Recycler;

import java.io.IOException;
import java.util.Objects;
import java.util.zip.Checksum;

public class TranslogStreamOutput extends RecyclerBytesStreamOutput {

    public TranslogStreamOutput(Recycler<BytesRef> recycler) {
        super(recycler);
    }

    public void writeIndexHeader(Translog.Index indexOperation) {

    }

    public void write4Longs(long l1, long l2, long l3, long l4) throws IOException {
        final int currentPageOffset = this.currentPageOffset;
        if (32 > (pageSize - currentPageOffset)) {
            super.writeLong(l1);
            super.writeLong(l2);
            super.writeLong(l3);
            super.writeLong(l4);
        } else {
            int off = bytesRefOffset + currentPageOffset;
            VH_BE_LONG.set(bytesRefBytes, off, l1);
            VH_BE_LONG.set(bytesRefBytes, off + 8, l2);
            VH_BE_LONG.set(bytesRefBytes, off + 16, l3);
            VH_BE_LONG.set(bytesRefBytes, off + 24, l4);
            this.currentPageOffset = currentPageOffset + 32;
        }
    }

    public void calculateChecksum(Checksum checksum, int startPosition) {
        int position = (int) position();
        Objects.checkIndex(startPosition, position);

        int bytesToProcess = position - startPosition;
        if (bytesToProcess == 0) {
            return;
        }

        int startPageIndex = startPosition / pageSize;
        int startPageOffset = startPosition % pageSize;

        final int remainder = position % pageSize;
        final int bytesInLastPage = remainder != 0 ? remainder : pageSize;
        final int endPageIndex = (position - 1) / pageSize;

        if (startPageIndex == endPageIndex) {
            BytesRef page = pages.get(startPageIndex).v();
            checksum.update(page.bytes, page.offset + startPageOffset, bytesToProcess);
        } else {
            BytesRef firstPage = pages.get(startPageIndex).v();
            int firstPageBytes = pageSize - startPageOffset;
            checksum.update(firstPage.bytes, firstPage.offset + startPageOffset, firstPageBytes);

            for (int i = startPageIndex + 1; i < endPageIndex; i++) {
                BytesRef page = pages.get(i).v();
                checksum.update(page.bytes, page.offset, pageSize);
            }

            if (endPageIndex > startPageIndex) {
                BytesRef lastPage = pages.get(endPageIndex).v();
                checksum.update(lastPage.bytes, lastPage.offset, bytesInLastPage);
            }
        }
    }
}
