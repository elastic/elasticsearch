/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IOUtils;

import java.io.IOException;
import java.util.Arrays;

/**
 * Writes the region of {@link SortedSeparatorsSkip}. The directory and the payload are staged in temporary files and
 * appended to the skip-index file once all slots have arrived. Entries that have no value yet wait, since they take
 * the separator of the next value.
 */
final class SortedSeparatorsWriter implements PlainSkipKind.Writer {

    private final Directory directory;
    private final IndexOutput directoryOutput;
    private final IndexOutput payloadOutput;
    private final long numValues;
    private final BytesRefBuilder previous = new BytesRefBuilder();
    private long[] waitingSlots = new long[8];
    private int[] waitingDocs = new int[8];
    private int waiting;
    private boolean hasPrevious;
    private int direction;
    private boolean unordered;
    private int ownerChunk = -1;
    private long nextAddress;
    private int entriesWritten;
    private boolean outputsClosed;

    SortedSeparatorsWriter(Directory directory, IOContext context, String dataName, long numValues) throws IOException {
        this.directory = directory;
        this.numValues = numValues;
        this.directoryOutput = directory.createTempOutput(dataName, "columnar-plain-separators-directory", context);
        IndexOutput payload = null;
        try {
            payload = directory.createTempOutput(dataName, "columnar-plain-separators-payload", context);
        } finally {
            if (payload == null) {
                IOUtils.closeWhileHandlingException(directoryOutput);
                IOUtils.deleteFilesIgnoringExceptions(directory, directoryOutput.getName());
            }
        }
        this.payloadOutput = payload;
    }

    @Override
    public void add(long address, int doc, int chunk, BytesRef value, boolean repeat) throws IOException {
        if (address != nextAddress) {
            throw new IllegalStateException("slot [" + address + "] arrived when [" + nextAddress + "] was expected");
        }
        if (chunk < ownerChunk) {
            throw new IllegalStateException("slot [" + address + "] is owned by the chunk [" + chunk + "] after [" + ownerChunk + "]");
        }
        nextAddress++;
        if (unordered) {
            return;
        }
        if (chunk != ownerChunk) {
            startEntry(address, doc);
            ownerChunk = chunk;
        }
        if (value == null) {
            return;
        }
        if (repeat == false && inOrder(value) == false) {
            unordered = true;
            waiting = 0;
            return;
        }
        if (waiting > 0) {
            flushWaiting(writeSeparator(value));
        }
        if (repeat == false) {
            previous.copyBytes(value);
            hasPrevious = true;
        }
    }

    @Override
    public boolean ascending() {
        return unordered == false && direction >= 0;
    }

    @Override
    public StringColumnMetadata.Plain.Skipper finish(IndexOutput skipIndex) throws IOException {
        if (nextAddress != numValues) {
            throw new IllegalStateException("took [" + nextAddress + "] slots for [" + numValues + "] expected");
        }
        if (unordered || hasPrevious == false) {
            close();
            return null;
        }
        final long endOffset = writeSeparator(previous.get());
        flushWaiting(endOffset);
        assert entriesWritten > 0 : "a column with a value has an entry";
        final long payloadLength = payloadOutput.getFilePointer();
        closeOutputs();
        final long offset = skipIndex.getFilePointer();
        try {
            skipIndex.writeByte(direction < 0 ? SortedSeparatorsSkip.DESCENDING : SortedSeparatorsSkip.ASCENDING);
            skipIndex.writeLong(endOffset);
            append(directoryOutput.getName(), skipIndex);
            append(payloadOutput.getName(), skipIndex);
        } finally {
            close();
        }
        return new StringColumnMetadata.Plain.Skipper(SortedSeparatorsSkip.ID, offset, entriesWritten, payloadLength);
    }

    @Override
    public void close() throws IOException {
        try {
            closeOutputs();
        } finally {
            IOUtils.deleteFilesIgnoringExceptions(directory, directoryOutput.getName(), payloadOutput.getName());
        }
    }

    /** Whether {@code value} keeps the values seen so far in one order, which it fixes the first time they differ. */
    private boolean inOrder(BytesRef value) {
        if (hasPrevious == false) {
            return true;
        }
        final int comparison = value.compareTo(previous.get());
        if (comparison == 0) {
            return true;
        }
        final int step = comparison > 0 ? 1 : -1;
        if (direction == 0) {
            direction = step;
        }
        return direction == step;
    }

    private void startEntry(long slot, int doc) {
        if (waiting == waitingSlots.length) {
            waitingSlots = Arrays.copyOf(waitingSlots, waiting * 2);
            waitingDocs = Arrays.copyOf(waitingDocs, waiting * 2);
        }
        waitingSlots[waiting] = slot;
        waitingDocs[waiting] = doc;
        waiting++;
    }

    private long writeSeparator(BytesRef value) throws IOException {
        final long offset = payloadOutput.getFilePointer();
        payloadOutput.writeInt(value.length);
        payloadOutput.writeBytes(value.bytes, value.offset, value.length);
        return offset;
    }

    private void flushWaiting(long separatorOffset) throws IOException {
        for (int i = 0; i < waiting; i++) {
            directoryOutput.writeLong(waitingSlots[i]);
            directoryOutput.writeInt(waitingDocs[i]);
            directoryOutput.writeLong(separatorOffset);
            entriesWritten++;
        }
        waiting = 0;
    }

    private void append(String staged, IndexOutput skipIndex) throws IOException {
        try (IndexInput in = directory.openInput(staged, IOContext.READONCE)) {
            skipIndex.copyBytes(in, in.length());
        }
    }

    private void closeOutputs() throws IOException {
        if (outputsClosed == false) {
            outputsClosed = true;
            IOUtils.close(directoryOutput, payloadOutput);
        }
    }
}
