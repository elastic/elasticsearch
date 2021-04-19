/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.engine;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.store.LuceneFilesExtensions;

import java.io.IOException;

public class SegmentsStats implements Writeable, ToXContentFragment {

    private long count;
    private long memoryInBytes;
    private long termsMemoryInBytes;
    private long storedFieldsMemoryInBytes;
    private long termVectorsMemoryInBytes;
    private long normsMemoryInBytes;
    private long pointsMemoryInBytes;
    private long docValuesMemoryInBytes;
    private long indexWriterMemoryInBytes;
    private long versionMapMemoryInBytes;
    private long maxUnsafeAutoIdTimestamp = Long.MIN_VALUE;
    private long bitsetMemoryInBytes;
    private ImmutableOpenMap<String, FileStats> files = ImmutableOpenMap.of();

    public SegmentsStats() {
    }

    public SegmentsStats(StreamInput in) throws IOException {
        count = in.readVLong();
        memoryInBytes = in.readLong();
        termsMemoryInBytes = in.readLong();
        storedFieldsMemoryInBytes = in.readLong();
        termVectorsMemoryInBytes = in.readLong();
        normsMemoryInBytes = in.readLong();
        pointsMemoryInBytes = in.readLong();
        docValuesMemoryInBytes = in.readLong();
        indexWriterMemoryInBytes = in.readLong();
        versionMapMemoryInBytes = in.readLong();
        bitsetMemoryInBytes = in.readLong();
        maxUnsafeAutoIdTimestamp = in.readLong();

        final int size = in.readVInt();
        final ImmutableOpenMap.Builder<String, FileStats> files = ImmutableOpenMap.builder(size);
        for (int i = 0; i < size; i++) {
            FileStats file = new FileStats(in);
            files.put(file.getExt(), file);
        }
        this.files = files.build();
    }

    public void add(long count, long memoryInBytes) {
        this.count += count;
        this.memoryInBytes += memoryInBytes;
    }

    public void addTermsMemoryInBytes(long termsMemoryInBytes) {
        this.termsMemoryInBytes += termsMemoryInBytes;
    }

    public void addStoredFieldsMemoryInBytes(long storedFieldsMemoryInBytes) {
        this.storedFieldsMemoryInBytes += storedFieldsMemoryInBytes;
    }

    public void addTermVectorsMemoryInBytes(long termVectorsMemoryInBytes) {
        this.termVectorsMemoryInBytes += termVectorsMemoryInBytes;
    }

    public void addNormsMemoryInBytes(long normsMemoryInBytes) {
        this.normsMemoryInBytes += normsMemoryInBytes;
    }

    public void addPointsMemoryInBytes(long pointsMemoryInBytes) {
        this.pointsMemoryInBytes += pointsMemoryInBytes;
    }

    public void addDocValuesMemoryInBytes(long docValuesMemoryInBytes) {
        this.docValuesMemoryInBytes += docValuesMemoryInBytes;
    }

    public void addIndexWriterMemoryInBytes(long indexWriterMemoryInBytes) {
        this.indexWriterMemoryInBytes += indexWriterMemoryInBytes;
    }

    public void addVersionMapMemoryInBytes(long versionMapMemoryInBytes) {
        this.versionMapMemoryInBytes += versionMapMemoryInBytes;
    }

    void updateMaxUnsafeAutoIdTimestamp(long maxUnsafeAutoIdTimestamp) {
        this.maxUnsafeAutoIdTimestamp = Math.max(maxUnsafeAutoIdTimestamp, this.maxUnsafeAutoIdTimestamp);
    }

    public void addBitsetMemoryInBytes(long bitsetMemoryInBytes) {
        this.bitsetMemoryInBytes += bitsetMemoryInBytes;
    }

    public void addFiles(ImmutableOpenMap<String, FileStats> files) {
        final ImmutableOpenMap.Builder<String, FileStats> map = ImmutableOpenMap.builder(this.files);
        for (ObjectObjectCursor<String, FileStats> entry : files) {
            final String extension = entry.key;
            if (map.containsKey(extension)) {
                FileStats previous = map.get(extension);
                map.put(extension, FileStats.merge(previous, entry.value));
            } else {
                map.put(extension, entry.value);
            }
        }
        this.files = map.build();
    }

    public void add(SegmentsStats mergeStats) {
        if (mergeStats == null) {
            return;
        }
        updateMaxUnsafeAutoIdTimestamp(mergeStats.maxUnsafeAutoIdTimestamp);
        add(mergeStats.count, mergeStats.memoryInBytes);
        addTermsMemoryInBytes(mergeStats.termsMemoryInBytes);
        addStoredFieldsMemoryInBytes(mergeStats.storedFieldsMemoryInBytes);
        addTermVectorsMemoryInBytes(mergeStats.termVectorsMemoryInBytes);
        addNormsMemoryInBytes(mergeStats.normsMemoryInBytes);
        addPointsMemoryInBytes(mergeStats.pointsMemoryInBytes);
        addDocValuesMemoryInBytes(mergeStats.docValuesMemoryInBytes);
        addIndexWriterMemoryInBytes(mergeStats.indexWriterMemoryInBytes);
        addVersionMapMemoryInBytes(mergeStats.versionMapMemoryInBytes);
        addBitsetMemoryInBytes(mergeStats.bitsetMemoryInBytes);
        addFiles(mergeStats.files);
    }

    /**
     * The number of segments.
     */
    public long getCount() {
        return this.count;
    }

    /**
     * Estimation of the memory usage used by a segment.
     */
    public long getMemoryInBytes() {
        return this.memoryInBytes;
    }

    public ByteSizeValue getMemory() {
        return new ByteSizeValue(memoryInBytes);
    }

    /**
     * Estimation of the terms dictionary memory usage by a segment.
     */
    public long getTermsMemoryInBytes() {
        return this.termsMemoryInBytes;
    }

    private ByteSizeValue getTermsMemory() {
        return new ByteSizeValue(termsMemoryInBytes);
    }

    /**
     * Estimation of the stored fields memory usage by a segment.
     */
    public long getStoredFieldsMemoryInBytes() {
        return this.storedFieldsMemoryInBytes;
    }

    private ByteSizeValue getStoredFieldsMemory() {
        return new ByteSizeValue(storedFieldsMemoryInBytes);
    }

    /**
     * Estimation of the term vectors memory usage by a segment.
     */
    public long getTermVectorsMemoryInBytes() {
        return this.termVectorsMemoryInBytes;
    }

    private ByteSizeValue getTermVectorsMemory() {
        return new ByteSizeValue(termVectorsMemoryInBytes);
    }

    /**
     * Estimation of the norms memory usage by a segment.
     */
    public long getNormsMemoryInBytes() {
        return this.normsMemoryInBytes;
    }

    private ByteSizeValue getNormsMemory() {
        return new ByteSizeValue(normsMemoryInBytes);
    }

    /**
     * Estimation of the points memory usage by a segment.
     */
    public long getPointsMemoryInBytes() {
        return this.pointsMemoryInBytes;
    }

    private ByteSizeValue getPointsMemory() {
        return new ByteSizeValue(pointsMemoryInBytes);
    }

    /**
     * Estimation of the doc values memory usage by a segment.
     */
    public long getDocValuesMemoryInBytes() {
        return this.docValuesMemoryInBytes;
    }

    private ByteSizeValue getDocValuesMemory() {
        return new ByteSizeValue(docValuesMemoryInBytes);
    }

    /**
     * Estimation of the memory usage by index writer
     */
    public long getIndexWriterMemoryInBytes() {
        return this.indexWriterMemoryInBytes;
    }

    public ByteSizeValue getIndexWriterMemory() {
        return new ByteSizeValue(indexWriterMemoryInBytes);
    }

    /**
     * Estimation of the memory usage by version map
     */
    public long getVersionMapMemoryInBytes() {
        return this.versionMapMemoryInBytes;
    }

    public ByteSizeValue getVersionMapMemory() {
        return new ByteSizeValue(versionMapMemoryInBytes);
    }

    /**
     * Estimation of how much the cached bit sets are taking. (which nested and p/c rely on)
     */
    public long getBitsetMemoryInBytes() {
        return bitsetMemoryInBytes;
    }

    public ByteSizeValue getBitsetMemory() {
        return new ByteSizeValue(bitsetMemoryInBytes);
    }

    public ImmutableOpenMap<String, FileStats> getFiles() {
        return files;
    }

    /**
     * Returns the max timestamp that is used to de-optimize documents with auto-generated IDs in the engine.
     * This is used to ensure we don't add duplicate documents when we assume an append only case based on auto-generated IDs
     */
    public long getMaxUnsafeAutoIdTimestamp() {
        return maxUnsafeAutoIdTimestamp;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.SEGMENTS);
        builder.field(Fields.COUNT, count);
        builder.humanReadableField(Fields.MEMORY_IN_BYTES, Fields.MEMORY, getMemory());
        builder.humanReadableField(Fields.TERMS_MEMORY_IN_BYTES, Fields.TERMS_MEMORY, getTermsMemory());
        builder.humanReadableField(Fields.STORED_FIELDS_MEMORY_IN_BYTES, Fields.STORED_FIELDS_MEMORY, getStoredFieldsMemory());
        builder.humanReadableField(Fields.TERM_VECTORS_MEMORY_IN_BYTES, Fields.TERM_VECTORS_MEMORY, getTermVectorsMemory());
        builder.humanReadableField(Fields.NORMS_MEMORY_IN_BYTES, Fields.NORMS_MEMORY, getNormsMemory());
        builder.humanReadableField(Fields.POINTS_MEMORY_IN_BYTES, Fields.POINTS_MEMORY, getPointsMemory());
        builder.humanReadableField(Fields.DOC_VALUES_MEMORY_IN_BYTES, Fields.DOC_VALUES_MEMORY, getDocValuesMemory());
        builder.humanReadableField(Fields.INDEX_WRITER_MEMORY_IN_BYTES, Fields.INDEX_WRITER_MEMORY, getIndexWriterMemory());
        builder.humanReadableField(Fields.VERSION_MAP_MEMORY_IN_BYTES, Fields.VERSION_MAP_MEMORY, getVersionMapMemory());
        builder.humanReadableField(Fields.FIXED_BIT_SET_MEMORY_IN_BYTES, Fields.FIXED_BIT_SET, getBitsetMemory());
        builder.field(Fields.MAX_UNSAFE_AUTO_ID_TIMESTAMP, maxUnsafeAutoIdTimestamp);
        builder.startObject(Fields.FILE_SIZES);
        for (ObjectObjectCursor<String, FileStats> entry : files) {
            entry.value.toXContent(builder, params);
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final String SEGMENTS = "segments";
        static final String COUNT = "count";
        static final String MEMORY = "memory";
        static final String MEMORY_IN_BYTES = "memory_in_bytes";
        static final String TERMS_MEMORY = "terms_memory";
        static final String TERMS_MEMORY_IN_BYTES = "terms_memory_in_bytes";
        static final String STORED_FIELDS_MEMORY = "stored_fields_memory";
        static final String STORED_FIELDS_MEMORY_IN_BYTES = "stored_fields_memory_in_bytes";
        static final String TERM_VECTORS_MEMORY = "term_vectors_memory";
        static final String TERM_VECTORS_MEMORY_IN_BYTES = "term_vectors_memory_in_bytes";
        static final String NORMS_MEMORY = "norms_memory";
        static final String NORMS_MEMORY_IN_BYTES = "norms_memory_in_bytes";
        static final String POINTS_MEMORY = "points_memory";
        static final String POINTS_MEMORY_IN_BYTES = "points_memory_in_bytes";
        static final String DOC_VALUES_MEMORY = "doc_values_memory";
        static final String DOC_VALUES_MEMORY_IN_BYTES = "doc_values_memory_in_bytes";
        static final String INDEX_WRITER_MEMORY = "index_writer_memory";
        static final String INDEX_WRITER_MEMORY_IN_BYTES = "index_writer_memory_in_bytes";
        static final String VERSION_MAP_MEMORY = "version_map_memory";
        static final String VERSION_MAP_MEMORY_IN_BYTES = "version_map_memory_in_bytes";
        static final String MAX_UNSAFE_AUTO_ID_TIMESTAMP = "max_unsafe_auto_id_timestamp";
        static final String FIXED_BIT_SET = "fixed_bit_set";
        static final String FIXED_BIT_SET_MEMORY_IN_BYTES = "fixed_bit_set_memory_in_bytes";
        static final String FILE_SIZES = "file_sizes";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(count);
        out.writeLong(memoryInBytes);
        out.writeLong(termsMemoryInBytes);
        out.writeLong(storedFieldsMemoryInBytes);
        out.writeLong(termVectorsMemoryInBytes);
        out.writeLong(normsMemoryInBytes);
        out.writeLong(pointsMemoryInBytes);
        out.writeLong(docValuesMemoryInBytes);
        out.writeLong(indexWriterMemoryInBytes);
        out.writeLong(versionMapMemoryInBytes);
        out.writeLong(bitsetMemoryInBytes);
        out.writeLong(maxUnsafeAutoIdTimestamp);

        out.writeVInt(files.size());
        for (ObjectCursor<FileStats> file : files.values()) {
            file.value.writeTo(out);
        }
    }

    public void clearFiles() {
        files = ImmutableOpenMap.of();
    }

    public static class FileStats implements Writeable, ToXContentFragment {

        private final String ext;
        private final long total;
        private final long count;
        private final long min;
        private final long max;

        FileStats(StreamInput in) throws IOException {
            if (in.getVersion().onOrAfter(Version.V_7_13_0)) {
                this.ext = in.readString();
                this.total = in.readVLong();
                this.count = in.readVLong();
                this.min = in.readVLong();
                this.max = in.readVLong();
            } else {
                this.ext = in.readString();
                this.total = in.readLong();
                this.count = 0L;
                this.min = 0L;
                this.max = 0L;
            }
        }

        public FileStats(String ext, long total, long count, long min, long max) {
            this.ext = ext;
            this.total = total;
            this.count = count;
            this.min = min;
            this.max = max;
        }

        public String getExt() {
            return ext;
        }

        public long getCount() {
            return count;
        }

        public long getTotal() {
            return total;
        }

        public long getMin() {
            return min;
        }

        public long getMax() {
            return max;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getVersion().onOrAfter(Version.V_7_13_0)) {
                out.writeString(ext);
                out.writeVLong(total);
                out.writeVLong(count);
                out.writeVLong(min);
                out.writeVLong(max);
            } else {
                out.writeString(ext);
                out.writeLong(total);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            final long roundedAverage = count > 0L ? Math.round((double) total / (double) count) : 0L;
            final LuceneFilesExtensions extension = LuceneFilesExtensions.fromExtension(ext);
            final String name = extension != null ? extension.getExtension() : "others";
            final String desc = extension != null ? extension.getDescription() : "Others";
            builder.startObject(name);
            {
                builder.field("description", desc);
                builder.humanReadableField("size_in_bytes", "size", ByteSizeValue.ofBytes(total));
                builder.humanReadableField("min_size_in_bytes", "min_size", ByteSizeValue.ofBytes(min));
                builder.humanReadableField("max_size_in_bytes", "max_size", ByteSizeValue.ofBytes(max));
                builder.humanReadableField("average_size_in_bytes", "average_size", ByteSizeValue.ofBytes(roundedAverage));
                builder.field("count", count);
            }
            builder.endObject();
            return builder;
        }

        public static FileStats merge(FileStats o1, FileStats o2) {
            assert o1 != null && o1.ext != null : o1;
            assert o2 != null && o2.ext != null : o2;
            assert o1.ext.equals(o2.ext) : o1 + " vs " + o2;
            return new FileStats(
                o1.ext,
                Math.addExact(o1.total, o2.total),
                Math.addExact(o1.count, o2.count),
                Math.min(o1.min, o2.min),
                Math.max(o1.max, o2.max)
            );
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }
}
