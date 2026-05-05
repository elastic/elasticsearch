/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.DictionaryPageReadStore;
import org.apache.parquet.column.values.bloomfilter.BlockSplitBloomFilter;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputDecompressor;
import org.apache.parquet.filter2.bloomfilterlevel.BloomFilterImpl;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.dictionarylevel.DictionaryFilter;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.filter2.predicate.SchemaCompatibilityValidator;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.filter2.statisticslevel.StatisticsFilter;
import org.apache.parquet.format.BloomFilterHeader;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.BloomFilterReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.MessageType;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Batches all dictionary-page and bloom-filter I/O for the row-group filtering phase into a
 * single coalesced read, then evaluates {@code STATISTICS}, {@code DICTIONARY}, and
 * {@code BLOOMFILTER} filter levels in-memory.
 *
 * <p>parquet-mr's {@link org.apache.parquet.filter2.compat.RowGroupFilter#filterRowGroups} drives
 * those reads <em>per row group</em> through the underlying {@link
 * org.apache.parquet.io.SeekableInputStream}. On remote storage (S3, GCS, Azure) each call lands
 * a synchronous range GET — typically 2-10 ms RTT plus a full TLS handshake. A file with 226
 * row groups and one predicate column yields ~226 sync requests for dictionary filtering alone,
 * and another ~226 if bloom filters are present. Profiling on S3 shows 79% of CPU spent in
 * AES-GCM/GHASH for those handshakes.
 *
 * <p>This class collapses that workload to one parallel batched fetch via
 * {@link CoalescedRangeReader#readCoalesced}, which already powers
 * {@link PreloadedRowGroupMetadata} and {@link ColumnChunkPrefetcher}. It serves the bytes
 * back to parquet-mr's filter visitors via in-memory {@link DictionaryPageReadStore} and
 * {@link BloomFilterReader} implementations — so the visitor logic itself is unchanged.
 *
 * <h2>Conditions for engaging the batch path</h2>
 * <ul>
 *   <li>A {@link StorageObject} is available (i.e., we can call {@code readBytesAsync}); local
 *       in-memory test fixtures fall back to the existing sequential path.</li>
 *   <li>The filter is a {@link FilterCompat.FilterPredicateCompat} (the only filter type that
 *       parquet-mr's three filter levels look at).</li>
 *   <li>At least one predicate column is present in the file (otherwise no I/O is needed).</li>
 * </ul>
 *
 * <h2>Encrypted columns</h2>
 *
 * The optimized reader rejects encrypted columns at decode time (see
 * {@link PrefetchedRowGroupBuilder}). This class detects encryption on predicate columns and
 * falls back to parquet-mr's sequential path so error handling stays in one place.
 */
final class BatchPrefetchedFilter {

    private static final Logger logger = LogManager.getLogger(BatchPrefetchedFilter.class);
    private static final ParquetMetadataConverter METADATA_CONVERTER = new ParquetMetadataConverter();

    private BatchPrefetchedFilter() {}

    /**
     * Evaluates {@code STATISTICS}, {@code DICTIONARY}, and {@code BLOOMFILTER} filter levels
     * for every row group in {@code blocks} and returns the per-row-group survival flag, or
     * {@code null} when the batch path does not apply (caller should fall back to parquet-mr's
     * {@link org.apache.parquet.filter2.compat.RowGroupFilter#filterRowGroups}).
     */
    static boolean[] computeSurvivingRowGroups(
        StorageObject storageObject,
        List<BlockMetaData> blocks,
        FilterCompat.Filter recordFilter,
        MessageType schema,
        CompressionCodecFactory codecFactory
    ) {
        if (storageObject == null || blocks.isEmpty()) {
            return null;
        }
        if ((recordFilter instanceof FilterCompat.FilterPredicateCompat) == false) {
            return null;
        }
        FilterPredicate predicate = ((FilterCompat.FilterPredicateCompat) recordFilter).getFilterPredicate();
        // Mirrors RowGroupFilter.visit(FilterPredicateCompat). FilterCompat.get() already runs
        // LogicalInverseRewriter, so we never see a Not at this point — but the schema check
        // can still reject a malformed predicate, which we surface to the caller as a fallback.
        try {
            SchemaCompatibilityValidator.validate(predicate, schema);
        } catch (RuntimeException e) {
            logger.debug("Schema compatibility check failed; falling back to parquet-mr filter path", e);
            return null;
        }

        Set<String> predicateColumns = collectPredicateColumns(predicate);
        if (predicateColumns.isEmpty()) {
            // Nothing for DICTIONARY/BLOOMFILTER to read; STATISTICS alone is free.
            boolean[] statsOnly = new boolean[blocks.size()];
            for (int i = 0; i < blocks.size(); i++) {
                statsOnly[i] = StatisticsFilter.canDrop(predicate, blocks.get(i).getColumns()) == false;
            }
            return statsOnly;
        }

        // Bail to the sequential path on any encrypted predicate column. Encryption is handled
        // by parquet-mr's reader (which decrypts inline) and we keep all encryption-related error
        // surfacing centralized in PrefetchedRowGroupBuilder.
        for (BlockMetaData block : blocks) {
            for (ColumnChunkMetaData col : block.getColumns()) {
                if (predicateColumns.contains(col.getPath().toDotString()) && col.isEncrypted()) {
                    logger.debug("Predicate column [{}] is encrypted; falling back", col.getPath());
                    return null;
                }
            }
        }

        // Run STATISTICS upfront so we can avoid fetching dictionary or bloom data for row groups
        // that min/max already eliminate. Statistics are read from the parsed footer in memory
        // and cost essentially nothing — the only real expense in this filter pass is the
        // dictionary/bloom I/O downstream. Skipping eliminated row groups here keeps the
        // coalesced fetch tight when a query predicate trivially excludes most of the file.
        boolean[] flags = new boolean[blocks.size()];
        for (int i = 0; i < blocks.size(); i++) {
            flags[i] = StatisticsFilter.canDrop(predicate, blocks.get(i).getColumns()) == false;
        }

        // Collect byte ranges. Each entry remembers (rowGroupOrdinal, ColumnChunk, kind) so the
        // fetched buffer can be routed back to its in-memory store after the coalesced read
        // completes.
        List<CoalescedRangeReader.ByteRange> ranges = new ArrayList<>();
        List<RangeMeta> rangeMetas = new ArrayList<>();

        for (int rg = 0; rg < blocks.size(); rg++) {
            if (flags[rg] == false) {
                continue;
            }
            BlockMetaData block = blocks.get(rg);
            for (ColumnChunkMetaData col : block.getColumns()) {
                String path = col.getPath().toDotString();
                if (predicateColumns.contains(path) == false) {
                    continue;
                }
                CoalescedRangeReader.ByteRange dict = dictionaryRange(col);
                if (dict != null) {
                    ranges.add(dict);
                    rangeMetas.add(new RangeMeta(dict, rg, col, RangeKind.DICTIONARY));
                }
                CoalescedRangeReader.ByteRange bloom = bloomFilterRange(col);
                if (bloom != null) {
                    ranges.add(bloom);
                    rangeMetas.add(new RangeMeta(bloom, rg, col, RangeKind.BLOOM));
                }
            }
        }

        if (ranges.isEmpty()) {
            // Either every row group was already dropped by STATISTICS, or no surviving row
            // group has dictionary/bloom data on a predicate column. Either way the in-memory
            // STATISTICS pass we already ran is the answer.
            return flags;
        }

        Map<CoalescedRangeReader.ByteRange, ByteBuffer> fetched;
        try {
            PlainActionFuture<Map<CoalescedRangeReader.ByteRange, ByteBuffer>> future = new PlainActionFuture<>();
            CoalescedRangeReader.readCoalesced(
                storageObject,
                ranges,
                CoalescedRangeReader.DEFAULT_MAX_COALESCE_GAP,
                Runnable::run,
                future
            );
            fetched = future.actionGet();
        } catch (Exception e) {
            // Coalesced fetch is opportunistic. parquet-mr's sequential path stays correct on its
            // own (it does its own seek+read against the same SeekableInputStream), so we degrade
            // rather than fail.
            logger.debug("Coalesced dictionary/bloom prefetch failed; falling back to sequential", e);
            return null;
        }

        // Index fetched bytes by (rowGroup, columnPath) per kind for cheap lookup during
        // per-row-group filter evaluation. Both maps are always mutable HashMaps so the
        // dispatch in the loop below stays uniform; the dictBytes/bloomBytes handles get
        // captured by the in-memory stores constructed per row group.
        Map<RowGroupColumnKey, ByteBuffer> dictBytes = new HashMap<>();
        Map<RowGroupColumnKey, ByteBuffer> bloomBytes = new HashMap<>();
        for (RangeMeta meta : rangeMetas) {
            ByteBuffer buf = fetched.get(meta.range());
            if (buf == null) {
                continue;
            }
            RowGroupColumnKey key = new RowGroupColumnKey(meta.rowGroupOrdinal(), meta.column().getPath());
            if (meta.kind() == RangeKind.DICTIONARY) {
                dictBytes.put(key, buf);
            } else {
                bloomBytes.put(key, buf);
            }
        }

        logger.debug(
            "Batch prefetch for row group filter: {} ranges over {} row groups, {} dictionary, {} bloom",
            ranges.size(),
            blocks.size(),
            dictBytes.size(),
            bloomBytes.size()
        );

        // Re-run dictionary then bloom for the row groups that survived statistics. STATISTICS
        // already ran in the first pass; flags carry that result forward.
        for (int rg = 0; rg < blocks.size(); rg++) {
            if (flags[rg] == false) {
                continue;
            }
            BlockMetaData block = blocks.get(rg);
            boolean drop = applyDictionaryFilter(predicate, block, rg, dictBytes, codecFactory);
            if (drop == false) {
                drop = applyBloomFilter(predicate, block, rg, bloomBytes);
            }
            if (drop) {
                flags[rg] = false;
            }
        }
        return flags;
    }

    /**
     * Mirrors {@link DictionaryFilter#canDrop} but driven from pre-fetched bytes. When a column
     * is missing from {@code dictBytes} (no dictionary page in the file or fetch failure for
     * that range), the in-memory store returns {@code null} from
     * {@link DictionaryPageReadStore#readDictionaryPage} — which {@code DictionaryFilter} treats
     * as "cannot eliminate" (BLOCK_MIGHT_MATCH).
     */
    private static boolean applyDictionaryFilter(
        FilterPredicate predicate,
        BlockMetaData block,
        int rowGroupOrdinal,
        Map<RowGroupColumnKey, ByteBuffer> dictBytes,
        CompressionCodecFactory codecFactory
    ) {
        DictionaryPageReadStore store = new InMemoryDictionaryPageReadStore(block, rowGroupOrdinal, dictBytes, codecFactory);
        return DictionaryFilter.canDrop(predicate, block.getColumns(), store);
    }

    private static boolean applyBloomFilter(
        FilterPredicate predicate,
        BlockMetaData block,
        int rowGroupOrdinal,
        Map<RowGroupColumnKey, ByteBuffer> bloomBytes
    ) {
        BloomFilterReader reader = new InMemoryBloomFilterReader(block, rowGroupOrdinal, bloomBytes);
        return BloomFilterImpl.canDrop(predicate, block.getColumns(), reader);
    }

    /**
     * The on-disk dictionary page for a column chunk lives in
     * {@code [getStartingPos(), getFirstDataPageOffset())} when present.
     * {@link ColumnChunkMetaData#getStartingPos()} resolves to the dictionary offset whenever a
     * usable dictionary page exists (it falls back to the first data page otherwise), so the
     * span is non-empty exactly when {@link ColumnChunkMetaData#hasDictionaryPage()} is true.
     */
    private static CoalescedRangeReader.ByteRange dictionaryRange(ColumnChunkMetaData col) {
        if (col.hasDictionaryPage() == false) {
            return null;
        }
        long start = col.getStartingPos();
        long firstDataOffset = col.getFirstDataPageOffset();
        long len = firstDataOffset - start;
        if (len <= 0) {
            return null;
        }
        return new CoalescedRangeReader.ByteRange(start, len);
    }

    /**
     * Bloom filter byte range. Pre-2.10 writers may not record the length, in which case we
     * skip the prefetch for that column (the length must be read from the on-disk header).
     * Falling back keeps correctness without adding a "header probe" round trip.
     */
    private static CoalescedRangeReader.ByteRange bloomFilterRange(ColumnChunkMetaData col) {
        long offset = col.getBloomFilterOffset();
        int length = col.getBloomFilterLength();
        if (offset < 0 || length <= 0) {
            return null;
        }
        return new CoalescedRangeReader.ByteRange(offset, length);
    }

    /**
     * Walks a {@link FilterPredicate} tree and collects the dotted paths of every column it
     * references. These are the only columns we need to fetch dictionary or bloom data for.
     */
    static Set<String> collectPredicateColumns(FilterPredicate predicate) {
        Set<String> out = new HashSet<>();
        predicate.accept(new ColumnCollector(out));
        return out;
    }

    private static final class ColumnCollector implements FilterPredicate.Visitor<Void> {
        private final Set<String> columns;

        ColumnCollector(Set<String> columns) {
            this.columns = columns;
        }

        private void add(Operators.Column<?> column) {
            columns.add(column.getColumnPath().toDotString());
        }

        @Override
        public <T extends Comparable<T>> Void visit(Operators.Eq<T> eq) {
            add(eq.getColumn());
            return null;
        }

        @Override
        public <T extends Comparable<T>> Void visit(Operators.NotEq<T> notEq) {
            add(notEq.getColumn());
            return null;
        }

        @Override
        public <T extends Comparable<T>> Void visit(Operators.Lt<T> lt) {
            add(lt.getColumn());
            return null;
        }

        @Override
        public <T extends Comparable<T>> Void visit(Operators.LtEq<T> ltEq) {
            add(ltEq.getColumn());
            return null;
        }

        @Override
        public <T extends Comparable<T>> Void visit(Operators.Gt<T> gt) {
            add(gt.getColumn());
            return null;
        }

        @Override
        public <T extends Comparable<T>> Void visit(Operators.GtEq<T> gtEq) {
            add(gtEq.getColumn());
            return null;
        }

        @Override
        public <T extends Comparable<T>> Void visit(Operators.In<T> in) {
            add(in.getColumn());
            return null;
        }

        @Override
        public <T extends Comparable<T>> Void visit(Operators.NotIn<T> notIn) {
            add(notIn.getColumn());
            return null;
        }

        @Override
        public <T extends Comparable<T>> Void visit(Operators.Contains<T> contains) {
            // Contains wraps an inner predicate that itself references columns; visit it.
            contains.filter(this, (l, r) -> null, (l, r) -> null, v -> null);
            return null;
        }

        @Override
        public Void visit(Operators.And and) {
            and.getLeft().accept(this);
            and.getRight().accept(this);
            return null;
        }

        @Override
        public Void visit(Operators.Or or) {
            or.getLeft().accept(this);
            or.getRight().accept(this);
            return null;
        }

        @Override
        public Void visit(Operators.Not not) {
            not.getPredicate().accept(this);
            return null;
        }

        @Override
        public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Void visit(Operators.UserDefined<T, U> udp) {
            add(udp.getColumn());
            return null;
        }

        @Override
        public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> Void visit(Operators.LogicalNotUserDefined<T, U> udp) {
            udp.getUserDefined().accept(this);
            return null;
        }
    }

    /**
     * In-memory {@link DictionaryPageReadStore}. parquet-mr's {@link DictionaryFilter} calls
     * {@link DictionaryPageReadStore#readDictionaryPage(ColumnDescriptor)} once per column chunk;
     * we serve the result from the pre-fetched coalesced buffer.
     *
     * <p>Returning {@code null} for a missing column path or a parse failure mirrors what
     * parquet-mr does (the dictionary may legitimately be absent, e.g., for non-dictionary
     * encoded chunks), so the upstream {@code DictionaryFilter} keeps the row group instead of
     * dropping it on a transient failure.
     */
    private static final class InMemoryDictionaryPageReadStore implements DictionaryPageReadStore {
        private final int rowGroupOrdinal;
        private final Map<RowGroupColumnKey, ByteBuffer> dictBytes;
        private final CompressionCodecFactory codecFactory;
        private final Map<ColumnPath, ColumnChunkMetaData> columnsByPath;

        InMemoryDictionaryPageReadStore(
            BlockMetaData block,
            int rowGroupOrdinal,
            Map<RowGroupColumnKey, ByteBuffer> dictBytes,
            CompressionCodecFactory codecFactory
        ) {
            this.rowGroupOrdinal = rowGroupOrdinal;
            this.dictBytes = dictBytes;
            this.codecFactory = codecFactory;
            this.columnsByPath = new HashMap<>();
            for (ColumnChunkMetaData col : block.getColumns()) {
                columnsByPath.put(col.getPath(), col);
            }
        }

        /**
         * Re-decodes on every call instead of caching the parsed {@link DictionaryPage}: the
         * {@link DictionaryFilter} consumes {@code page.getBytes()} via {@code toByteArray()},
         * which is repeatable for {@code ByteArrayBytesInput} but not guaranteed for the post-
         * decompression {@code BytesInput} variants. Reusing the same page across two calls
         * (e.g. {@code col == 1 OR col == 2}) can corrupt the second read for codecs whose
         * decompressed BytesInput is one-shot. Per call, we slice the in-memory buffer and feed
         * a fresh decompression — no I/O is incurred, only a cheap pass over a few KB.
         */
        @Override
        public DictionaryPage readDictionaryPage(ColumnDescriptor descriptor) {
            ColumnPath path = ColumnPath.get(descriptor.getPath());
            ColumnChunkMetaData col = columnsByPath.get(path);
            if (col == null) {
                return null;
            }
            ByteBuffer buf = dictBytes.get(new RowGroupColumnKey(rowGroupOrdinal, path));
            if (buf == null) {
                return null;
            }
            try {
                return decodeDictionaryPage(col, buf, codecFactory);
            } catch (IOException | RuntimeException e) {
                logger.debug(
                    "Failed to decode pre-fetched dictionary page for column [{}] in row group [{}]; "
                        + "treating as no-dictionary so the row group is kept",
                    col.getPath(),
                    rowGroupOrdinal,
                    e
                );
                return null;
            }
        }

        @Override
        public void close() {
            // Nothing to release: buffers are slices over heap arrays owned by the coalesced fetch.
        }
    }

    /**
     * In-memory {@link BloomFilterReader}. parquet-mr's class is non-final and exposes
     * {@code readBloomFilter(ColumnChunkMetaData)} as an overridable method, so we extend rather
     * than reimplement the bookkeeping. The two-arg super constructor is invoked with the host
     * block; it's only used to populate an internal column map we don't end up consulting (we
     * resolve via {@code meta} directly).
     *
     * <p>Passing {@code null} for the {@code ParquetFileReader} argument is safe because the
     * parent's only use of that reference is from its own (non-overridden) {@code readBloomFilter}
     * implementation, which we replace entirely. Should a future parquet-mr revision begin to
     * read from {@code reader} on a code path other than {@code readBloomFilter}, this assumption
     * needs to be re-validated — see {@link org.apache.parquet.hadoop.BloomFilterReader} in the
     * pinned parquet-hadoop-bundle 1.17.0 dependency for the current contract.
     */
    private static final class InMemoryBloomFilterReader extends BloomFilterReader {
        private final int rowGroupOrdinal;
        private final Map<RowGroupColumnKey, ByteBuffer> bloomBytes;
        private final Map<ColumnPath, BloomFilter> cache = new HashMap<>();

        InMemoryBloomFilterReader(BlockMetaData block, int rowGroupOrdinal, Map<RowGroupColumnKey, ByteBuffer> bloomBytes) {
            super(null, block);
            this.rowGroupOrdinal = rowGroupOrdinal;
            this.bloomBytes = bloomBytes;
        }

        @Override
        public BloomFilter readBloomFilter(ColumnChunkMetaData meta) {
            ColumnPath path = meta.getPath();
            if (cache.containsKey(path)) {
                return cache.get(path);
            }
            ByteBuffer buf = bloomBytes.get(new RowGroupColumnKey(rowGroupOrdinal, path));
            BloomFilter filter = null;
            if (buf != null) {
                try {
                    filter = decodeBloomFilter(buf);
                } catch (IOException | RuntimeException e) {
                    logger.debug(
                        "Failed to decode pre-fetched bloom filter for column [{}] in row group [{}]; "
                            + "treating as no-filter so the row group is kept",
                        path,
                        rowGroupOrdinal,
                        e
                    );
                }
            }
            cache.put(path, filter);
            return filter;
        }
    }

    /**
     * Parses a dictionary page from raw column-chunk-prefix bytes.
     * Layout: Thrift {@link PageHeader} followed by {@code compressed_page_size} bytes of
     * compressed dictionary data; mirrors {@link
     * org.apache.parquet.hadoop.ParquetFileReader#readDictionary} for the non-encrypted path.
     */
    private static DictionaryPage decodeDictionaryPage(
        ColumnChunkMetaData col,
        ByteBuffer buf,
        CompressionCodecFactory codecFactory
    ) throws IOException {
        ByteArrayInputStream in = bufferAsStream(buf);
        int before = in.available();
        PageHeader header = Util.readPageHeader(in);
        int headerLen = before - in.available();
        if (header.type != PageType.DICTIONARY_PAGE) {
            return null;
        }
        DictionaryPageHeader dph = header.dictionary_page_header;
        int compressedSize = header.getCompressed_page_size();
        ByteBuffer payload = sliceFromBuffer(buf, headerLen, compressedSize);
        BytesInput compressed = bytesInputFrom(payload);
        BytesInputDecompressor decompressor = codecFactory.getDecompressor(col.getCodec());
        BytesInput decompressed = decompressor.decompress(compressed, header.getUncompressed_page_size());
        Encoding encoding = METADATA_CONVERTER.getEncoding(dph.encoding);
        return new DictionaryPage(decompressed, header.getUncompressed_page_size(), dph.num_values, encoding);
    }

    /**
     * Parses a bloom filter from raw bytes. Layout: Thrift {@link BloomFilterHeader} followed by
     * {@code numBytes} bitset bytes. Only block-split + XXH64 + uncompressed is supported by
     * parquet-mr 1.15; other algorithms surface as {@code null} so the row group is kept (same
     * fallback as {@link org.apache.parquet.hadoop.ParquetFileReader#readBloomFilter}).
     */
    private static BloomFilter decodeBloomFilter(ByteBuffer buf) throws IOException {
        ByteArrayInputStream in = bufferAsStream(buf);
        BloomFilterHeader header = Util.readBloomFilterHeader(in);
        int numBytes = header.getNumBytes();
        if (numBytes <= 0 || numBytes > BlockSplitBloomFilter.UPPER_BOUND_BYTES) {
            return null;
        }
        if (header.getHash().isSetXXHASH() == false
            || header.getAlgorithm().isSetBLOCK() == false
            || header.getCompression().isSetUNCOMPRESSED() == false) {
            return null;
        }
        if (in.available() < numBytes) {
            return null;
        }
        byte[] bitset = new byte[numBytes];
        int read = in.read(bitset);
        if (read != numBytes) {
            return null;
        }
        return new BlockSplitBloomFilter(bitset);
    }

    private static ByteArrayInputStream bufferAsStream(ByteBuffer buffer) {
        // Same trick as PrefetchedRowGroupBuilder: Util.read* needs an InputStream; wrap the
        // backing array directly when possible to avoid a defensive copy.
        if (buffer.hasArray()) {
            return new ByteArrayInputStream(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
        }
        byte[] copy = new byte[buffer.remaining()];
        buffer.duplicate().get(copy);
        return new ByteArrayInputStream(copy);
    }

    private static ByteBuffer sliceFromBuffer(ByteBuffer source, int offsetFromPosition, int length) {
        ByteBuffer dup = source.duplicate();
        dup.position(source.position() + offsetFromPosition);
        dup.limit(dup.position() + length);
        return dup.slice();
    }

    private static BytesInput bytesInputFrom(ByteBuffer buffer) {
        if (buffer.hasArray()) {
            return BytesInput.from(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
        }
        byte[] copy = new byte[buffer.remaining()];
        buffer.duplicate().get(copy);
        return BytesInput.from(copy);
    }

    private enum RangeKind {
        DICTIONARY,
        BLOOM
    }

    private record RangeMeta(CoalescedRangeReader.ByteRange range, int rowGroupOrdinal, ColumnChunkMetaData column, RangeKind kind) {}

    private record RowGroupColumnKey(int rowGroupOrdinal, ColumnPath columnPath) {}
}
