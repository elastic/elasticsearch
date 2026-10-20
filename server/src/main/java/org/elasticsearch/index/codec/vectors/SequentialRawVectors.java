/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.NoReuseHint;
import org.apache.lucene.util.IOSupplier;
import org.elasticsearch.core.Strings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.internal.FilterFloatVectorValues;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.stream.Stream;

/**
 * Raw vectors read sequentially for brute-force reads, see {@link SequentialVectorValues}. When searches read them at random
 * without reuse, they are opened again as a merge opens them; the reader is opened on first use and closed with its owner.
 */
public final class SequentialRawVectors implements Closeable {

    private static final Logger logger = LogManager.getLogger(SequentialRawVectors.class);

    private final FlatVectorsFormat format;
    private final SegmentReadState state;
    private FlatVectorsReader reader;
    private boolean unavailable;
    private boolean closed;

    /**
     * @param format the format of the raw vectors
     * @param state  the state the raw vectors were opened with for searches
     */
    public SequentialRawVectors(FlatVectorsFormat format, SegmentReadState state) {
        this.format = format;
        this.state = state;
        // only these get random advice, see FsDirectoryFactory#getReadAdviceFunc
        this.unavailable = (state.context.hints().contains(DataAccessHint.RANDOM)
            && state.context.hints().contains(NoReuseHint.INSTANCE)) == false;
    }

    /** The raw vectors of {@code field} read sequentially, or null to read them as searches do. */
    public FloatVectorValues getFloatVectorValues(String field) throws IOException {
        FlatVectorsReader sequentialReader = reader();
        return sequentialReader == null ? null : sequentialReader.getFloatVectorValues(field);
    }

    private synchronized FlatVectorsReader reader() throws IOException {
        if (closed) {
            throw new AlreadyClosedException("this reader is closed");
        }
        if (reader == null && unavailable == false) {
            IOContext.FileOpenHint[] hints = Stream.concat(
                state.context.hints().stream().filter(hint -> hint instanceof DataAccessHint == false),
                Stream.of(DataAccessHint.SEQUENTIAL)
            ).toArray(IOContext.FileOpenHint[]::new);
            try {
                reader = format.fieldsReader(new SegmentReadState(state, IOContext.merge().withHints(hints)));
            } catch (FileNotFoundException | NoSuchFileException e) {
                // an open reader outlives its files
                logger.debug(
                    () -> Strings.format("reading the vectors of segment [%s] at random: its files are gone", state.segmentInfo.name),
                    e
                );
                unavailable = true;
            }
        }
        return reader;
    }

    /** {@code values}, the raw vectors of {@code field} as searches read them, which can be read sequentially through this. */
    public FloatVectorValues forSearches(FloatVectorValues values, String field) {
        return values == null ? null : new RawFloatVectorValues(values, () -> getFloatVectorValues(field));
    }

    @Override
    public synchronized void close() throws IOException {
        closed = true;
        if (reader != null) {
            FlatVectorsReader toClose = reader;
            reader = null;
            toClose.close();
        }
    }

    /** Raw vectors as searches read them, read sequentially through the sequential reader. */
    private static final class RawFloatVectorValues extends FilterFloatVectorValues implements SequentialVectorValues {
        private final IOSupplier<FloatVectorValues> sequentialValues;

        RawFloatVectorValues(FloatVectorValues in, IOSupplier<FloatVectorValues> sequentialValues) {
            super(in);
            this.sequentialValues = sequentialValues;
        }

        @Override
        public FloatVectorValues copy() throws IOException {
            return new RawFloatVectorValues(in.copy(), sequentialValues);
        }

        @Override
        public FloatVectorValues sequential() throws IOException {
            FloatVectorValues sequential = sequentialValues.get();
            return sequential == null ? this : sequential;
        }
    }
}
