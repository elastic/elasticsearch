/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.dv;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetFormatReader;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetPositionalReader;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Serves the doc values of a stub segment from a Parquet file, through the ES|QL Parquet reader. The producer opens the file and reads
 * its footer once, when the first iterator asks for values, and each iterator gets its own positional reader over that footer, because a
 * reader is not thread-safe and Lucene hands out many iterators over one producer.
 *
 * <p>Neither the readers nor the footer hold resources that need to be released: the reader's {@code close()} methods are no-ops and the
 * file is opened through the {@link ParquetReadContext.Resolver} of the engine, which owns it. An iterator that is abandoned is just
 * garbage.
 */
public final class ParquetDocValuesProducer extends DocValuesProducer {

    static final int DEFAULT_WINDOW_SIZE = 4096;
    /** The most that the decoded pages of one segment use: the iterators on one column read pages close to each other. */
    static final long PAGE_CACHE_BYTES = 64L * 1024 * 1024;

    private final List<ColumnBinding> bindings;
    private final Map<String, ColumnBinding> byField = new HashMap<>();
    private final AtomicLong scansOpened = new AtomicLong();
    /** The decoded pages that the iterators of the segment share, see {@link DecodedPageCache}. */
    private final DecodedPageCache pageCache = new DecodedPageCache(PAGE_CACHE_BYTES);
    private final Map<String, Optional<ParquetDocValuesSkipper.Intervals>> skipperIntervals = new ConcurrentHashMap<>();
    private final ExternalFilePiece piece;
    @Nullable
    private final ParquetReadContext.Resolver resolver;
    private final int windowSize;
    /** Builds the blocks that the reader returns; the plugin's own factory unless a test gives another one to account for them. */
    private final BlockFactory blockFactory;
    private StorageObject storage;
    private ParquetMetadata footer;
    private ColumnWindow.FileLayout layout;
    /** Shared by every iterator of the segment, because its caches hold the parsed footer, which is large for a file with many columns. */
    private ParquetFormatReader formatReader;

    public ParquetDocValuesProducer(List<ColumnBinding> bindings, ExternalFilePiece piece, @Nullable ParquetReadContext.Resolver resolver) {
        this(bindings, piece, resolver, DEFAULT_WINDOW_SIZE);
    }

    ParquetDocValuesProducer(
        List<ColumnBinding> bindings,
        ExternalFilePiece piece,
        @Nullable ParquetReadContext.Resolver resolver,
        int windowSize
    ) {
        this(bindings, piece, resolver, windowSize, ParquetBlocks.factory());
    }

    ParquetDocValuesProducer(
        List<ColumnBinding> bindings,
        ExternalFilePiece piece,
        @Nullable ParquetReadContext.Resolver resolver,
        int windowSize,
        BlockFactory blockFactory
    ) {
        this.bindings = List.copyOf(bindings);
        for (ColumnBinding binding : bindings) {
            byField.put(binding.fieldName(), binding);
        }
        this.piece = piece;
        this.resolver = resolver;
        this.windowSize = windowSize;
        this.blockFactory = blockFactory;
    }

    /**
     * Resolves the file of the segment. The engine calls it when it opens the segment, because that is a thread on which a repository
     * may be asked for a blob container (it asserts the thread pool), and the later reads, which run on search and get threads,
     * only use the container. It makes no request: the length of the file is in the segment.
     */
    void resolveStorage() throws IOException {
        if (resolver != null) {
            synchronized (this) {
                storage = resolver.open(piece.location(), piece.length());
            }
        }
    }

    /** Counts a forward scan that one of the segment's iterators opened, for the tests and for diagnosing a thrashing access pattern. */
    long pageCacheHits() {
        return pageCache.hits();
    }

    void scanOpened() {
        scansOpened.incrementAndGet();
    }

    long scansOpened() {
        return scansOpened.get();
    }

    public int maxDoc() {
        return piece.rowCount();
    }

    List<ColumnBinding> bindings() {
        return bindings;
    }

    ExternalFilePiece piece() {
        return piece;
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
        ColumnBinding binding = binding(field, DocValuesType.NUMERIC);
        return new ParquetNumericDocValues(
            new NumericWindow(
                newReader(),
                layout(),
                binding.fieldName(),
                binding.parquetColumn(),
                piece.firstRow(),
                maxDoc(),
                windowSize,
                this,
                pageCache,
                blockFactory,
                binding.conversion()
            )
        );
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        ColumnBinding binding = binding(field, DocValuesType.BINARY);
        if (binding.isSynthesized()) {
            // the _id is the row number in the index: no file is read for it
            return new SyntheticIdDocValues(maxDoc(), piece.indexRowBase());
        }
        return new ParquetBinaryDocValues(
            new BytesWindow(
                newReader(),
                layout(),
                binding.fieldName(),
                binding.parquetColumn(),
                piece.firstRow(),
                maxDoc(),
                windowSize,
                this,
                pageCache,
                blockFactory
            )
        );
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) {
        throw new UnsupportedOperationException("Parquet doc values are NUMERIC or BINARY, not SORTED");
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) {
        throw new UnsupportedOperationException("Parquet doc values are NUMERIC or BINARY, not SORTED_NUMERIC");
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) {
        throw new UnsupportedOperationException("Parquet doc values are NUMERIC or BINARY, not SORTED_SET");
    }

    @Override
    public DocValuesSkipper getSkipper(FieldInfo field) {
        ColumnBinding binding = byField.get(field.name);
        if (binding == null || binding.isSynthesized() || binding.type() != DocValuesType.NUMERIC) {
            return null;
        }
        ParquetDocValuesSkipper.Intervals intervals = skipperIntervals.computeIfAbsent(
            binding.fieldName(),
            fieldName -> Optional.ofNullable(ParquetDocValuesSkipper.Intervals.of(piece, fieldName))
        ).orElse(null);
        // a skipper is stateful, so every caller gets its own over the shared intervals
        return intervals == null ? null : new ParquetDocValuesSkipper(intervals);
    }

    @Override
    public void checkIntegrity() {}

    @Override
    public void close() {
        ScanRegistry.INSTANCE.closeAllOf(this);
        pageCache.clear();
        // a file of an ES|QL data source holds a borrow of the storage provider, which has to be given back when the segment is done
        synchronized (this) {
            if (storage instanceof Closeable closeable) {
                IOUtils.closeWhileHandlingException(closeable);
            }
        }
    }

    /** The row group layout of the file; the file is opened by the first iterator. */
    private ColumnWindow.FileLayout layout() throws IOException {
        open();
        synchronized (this) {
            return layout;
        }
    }

    private ColumnBinding binding(FieldInfo field, DocValuesType expected) {
        ColumnBinding binding = byField.get(field.name);
        if (binding == null) {
            throw new IllegalStateException("field [" + field.name + "] is not bound to a Parquet column");
        }
        if (binding.type() != expected) {
            throw new IllegalStateException("field [" + field.name + "] has [" + binding.type() + "] doc values, not [" + expected + "]");
        }
        return binding;
    }

    private ParquetPositionalReader newReader() throws IOException {
        open();
        StorageObject file;
        ParquetMetadata parsed;
        ParquetFormatReader shared;
        synchronized (this) {
            file = storage;
            parsed = footer;
            shared = formatReader;
        }
        return ParquetPositionalReader.open(file, shared, parsed, null);
    }

    private void open() throws IOException {
        synchronized (this) {
            if (footer == null) {
                if (storage == null) {
                    throw new IllegalStateException("the segment was opened outside a Parquet engine, so it cannot find its file");
                }
                StorageObject opened = storage;
                ParquetMetadata read = ParquetPositionalReader.readFooter(opened, blockFactory, Settings.EMPTY);
                long rows = 0;
                for (BlockMetaData block : read.getBlocks()) {
                    rows += block.getRowCount();
                }
                if (piece.firstRow() + piece.rowCount() > rows) {
                    throw new IOException(
                        "file ["
                            + piece.location()
                            + "] has ["
                            + rows
                            + "] rows, fewer than the ["
                            + (piece.firstRow() + piece.rowCount())
                            + "] the segment needs"
                    );
                }
                storage = opened;
                footer = read;
                layout = new ColumnWindow.FileLayout(read);
                formatReader = ParquetPositionalReader.newFormatReader(Settings.EMPTY, blockFactory);
            }
        }
    }
}
