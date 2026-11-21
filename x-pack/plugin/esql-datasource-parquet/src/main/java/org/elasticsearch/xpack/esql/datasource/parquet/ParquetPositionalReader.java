/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.NodeByteBudgetService;
import org.elasticsearch.xpack.esql.datasources.cache.FooterByteCache;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.RangeReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

/**
 * Public entry point for reading Parquet columns by file-global row position, for plugins that extend this one.
 * <p>
 * The ES|QL reader only builds its positional {@link ColumnExtractor} from a running scan, and the implementation is
 * package-private. This facade builds one directly from a {@link StorageObject}, so that a consumer outside ES|QL
 * (for example a Lucene doc values format) can use the same decoding, coalesced I/O and type coercion without a
 * scan. It stays thin on purpose: every behavior comes from {@link ParquetColumnExtractor}.
 * <p>
 * Threading: like the extractor, an instance is not thread-safe. Use one instance per thread, and close it to
 * release the owned footer and the format reader.
 */
public final class ParquetPositionalReader implements Closeable {

    private final StorageObject storageObject;
    private final ParquetFormatReader formatReader;
    private final ParquetMetadata footer;
    private final ColumnExtractor extractor;
    @Nullable
    private final Consumer<String> warningSink;
    /** Whether {@link #close()} closes the format reader: only when this reader created it. */
    private final boolean ownsFormatReader;

    private ParquetPositionalReader(
        StorageObject storageObject,
        ParquetFormatReader formatReader,
        ParquetMetadata footer,
        ColumnExtractor extractor,
        @Nullable Consumer<String> warningSink,
        boolean ownsFormatReader
    ) {
        this.ownsFormatReader = ownsFormatReader;
        this.storageObject = storageObject;
        this.formatReader = formatReader;
        this.footer = footer;
        this.extractor = extractor;
        this.warningSink = warningSink;
    }

    /**
     * Reads and parses the footer of a file. Callers that open many readers over one file (one per doc values iterator, for example)
     * read the footer once here and pass it to {@link #open(StorageObject, BlockFactory, Settings, Consumer, ParquetMetadata)}.
     */
    public static ParquetMetadata readFooter(StorageObject storageObject, BlockFactory blockFactory, Settings settings) throws IOException {
        try (
            ParquetFileReader fileReader = ParquetFileReader.open(
                new ParquetStorageObjectAdapter(storageObject, FooterByteCache.fromSettings(settings), blockFactory.breaker()),
                PlainParquetReadOptions.builder(new PlainCompressionCodecFactory()).build()
            )
        ) {
            return fileReader.getFooter();
        }
    }

    /** Opens a positional reader over one Parquet file, reading the footer first. See the overload that takes a footer. */
    public static ParquetPositionalReader open(
        StorageObject storageObject,
        BlockFactory blockFactory,
        Settings settings,
        @Nullable Consumer<String> warningSink
    ) throws IOException {
        return open(storageObject, blockFactory, settings, warningSink, readFooter(storageObject, blockFactory, settings));
    }

    /**
     * Opens a positional reader over one Parquet file with a footer that the caller already read.
     *
     * @param storageObject the file; the caller keeps ownership and closes it after this reader
     * @param blockFactory  supplies the circuit breaker that charges the I/O buffers
     * @param settings      ES|QL external source settings, or {@link Settings#EMPTY}
     * @param warningSink   receives read-time warnings (for example a failed coercion), or {@code null} to emit them
     *                      as response headers
     * @param footer        the footer of exactly this file, from {@link #readFooter}; it is shared, never modified
     */
    public static ParquetPositionalReader open(
        StorageObject storageObject,
        BlockFactory blockFactory,
        Settings settings,
        @Nullable Consumer<String> warningSink,
        ParquetMetadata footer
    ) throws IOException {
        ParquetFormatReader formatReader = new ParquetFormatReader(settings, blockFactory);
        ParquetPositionalReader reader = open(storageObject, formatReader, footer, warningSink);
        return new ParquetPositionalReader(storageObject, formatReader, footer, reader.extractor, warningSink, true);
    }

    /**
     * Opens a positional reader that shares a {@link ParquetFormatReader} with other readers. The format reader holds the caches of
     * parsed footers and of footer bytes, which a large footer makes expensive to rebuild, and it is built to be shared by concurrent
     * readers. A caller that opens many readers over one file, such as a doc values producer, creates one format reader and passes it
     * to each, and the footer is then parsed once for all of them instead of once for each forward scan.
     *
     * @param formatReader a reader from {@link #newFormatReader}; this reader does not close it
     */
    public static ParquetPositionalReader open(
        StorageObject storageObject,
        ParquetFormatReader formatReader,
        ParquetMetadata footer,
        @Nullable Consumer<String> warningSink
    ) {
        // PERMISSIVE: a value that cannot be read becomes null with a warning, as the ES|QL default does.
        ColumnExtractor extractor = new ParquetColumnExtractor(storageObject, formatReader, footer, ErrorPolicy.PERMISSIVE, warningSink);
        return new ParquetPositionalReader(storageObject, formatReader, footer, extractor, warningSink, false);
    }

    /**
     * Creates the format reader that readers of one file share, see
     * {@link #open(StorageObject, ParquetFormatReader, ParquetMetadata, Consumer)}.
     *
     * <p>The reader gets a byte budget that never refuses. A forward scan asks the node's I/O byte budget for a ticket per row group
     * and keeps it until the scan is closed. The ES|QL driver copes with a full budget by parking and resuming when a ticket is free,
     * but a caller that reads synchronously, such as a Lucene iterator, can only block, and the threads that hold tickets are blocked
     * the same way, so a full budget is a deadlock. The price is that the scans of a caller are not limited by the budget, so the caller
     * must bound them itself and must not leave them open.
     */
    public static ParquetFormatReader newFormatReader(Settings settings, BlockFactory blockFactory) {
        return new ParquetFormatReader(settings, blockFactory, new NodeByteBudgetService(Long.MAX_VALUE / 4));
    }

    /** Number of rows in the file; valid positions are {@code 0 .. rowCount() - 1}. */
    public long rowCount() {
        return extractor.rowCount();
    }

    /** The file footer, for callers that need the schema or row group statistics. */
    public ParquetMetadata footer() {
        return footer;
    }

    /**
     * Reads the given columns at the given file-global row positions. Row {@code i} of every returned block holds the
     * value at {@code positions[i]}. Positions may be unsorted and may repeat. The caller owns the returned blocks and
     * must close them.
     *
     * @param targetTypes the ES|QL type to coerce each column to, or {@code null} to keep the file's own type
     */
    public Block[] extract(String[] columnNames, @Nullable DataType[] targetTypes, long[] positions, BlockFactory blockFactory)
        throws IOException {
        return extractor.extract(columnNames, targetTypes, positions, blockFactory);
    }

    /**
     * Scans one column of one row group forward, one page of at most {@code batchSize} rows at a time. This is the path for reading many
     * consecutive rows: it decodes each page once, where {@link #extract} pays about one pass over the column chunk for every call,
     * because a positional read cannot keep decoder state between calls. The pages hold the consecutive rows of the row group in file
     * order, so the first page starts at the first row of the group. The caller owns the iterator and the pages it returns.
     *
     * @param rowGroup the index of the row group in {@link #footer()}
     */
    public CloseableIterator<Page> scanRowGroup(String column, int rowGroup, int batchSize) throws IOException {
        return scanRowGroup(column, rowGroup, 0L, batchSize);
    }

    /**
     * Like {@link #scanRowGroup(String, int, int)}, but the scan starts at the row {@code startRow} of the group: the pages before it
     * are not decoded and not returned, and the pages that lie wholly before it are not even decompressed. The first page holds the row
     * {@code startRow} as its first row. A reader that serves a slice of a row group from a file without a column index uses this, so
     * that the start of the group costs a header for each page and not a decode.
     *
     * @param startRow the row of the group to start at, {@code 0} for the first row; a row beyond the group gives an empty scan
     */
    public CloseableIterator<Page> scanRowGroup(String column, int rowGroup, long startRow, int batchSize) throws IOException {
        BlockMetaData block = footer.getBlocks().get(rowGroup);
        // The reader reads the row groups whose midpoint is in the byte range, so the range of the group's own bytes selects exactly it.
        long start = block.getStartingPos();
        long end = start + block.getCompressedSize();
        return formatReader.readRange(
            storageObject,
            new RangeReadContext(List.of(column), batchSize, start, end, List.of(), ErrorPolicy.PERMISSIVE, warningSink),
            startRow
        );
    }

    @Override
    public void close() throws IOException {
        try {
            extractor.close();
        } finally {
            if (ownsFormatReader) {
                formatReader.close();
            }
        }
    }
}
