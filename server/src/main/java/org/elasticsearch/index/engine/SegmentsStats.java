/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.store.LuceneFilesExtensions;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class SegmentsStats implements Writeable, ToXContentFragment {

    private long count;
    private long indexWriterMemoryInBytes;
    private long versionMapMemoryInBytes;
    private long maxUnsafeAutoIdTimestamp = Long.MIN_VALUE;
    private long bitsetMemoryInBytes;
    private final Map<String, FileStats> files;

    public SegmentsStats() {
        files = new HashMap<>();
    }

    public SegmentsStats(StreamInput in) throws IOException {
        count = in.readVLong();
        if (in.getTransportVersion().before(TransportVersions.V_8_0_0)) {
            in.readLong(); // memoryInBytes
            in.readLong(); // termsMemoryInBytes
            in.readLong(); // storedFieldsMemoryInBytes
            in.readLong(); // termVectorsMemoryInBytes
            in.readLong(); // normsMemoryInBytes
            in.readLong(); // pointsMemoryInBytes
            in.readLong(); // docValuesMemoryInBytes
        }
        indexWriterMemoryInBytes = in.readLong();
        versionMapMemoryInBytes = in.readLong();
        bitsetMemoryInBytes = in.readLong();
        maxUnsafeAutoIdTimestamp = in.readLong();
        files = in.readMapValues(FileStats::new, FileStats::getExt);
    }

    public void add(long count) {
        this.count += count;
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

    public void addFiles(Map<String, FileStats> newFiles) {
        newFiles.forEach((k, v) -> files.merge(k, v, FileStats::merge));
    }

    public void add(SegmentsStats mergeStats) {
        if (mergeStats == null) {
            return;
        }
        updateMaxUnsafeAutoIdTimestamp(mergeStats.maxUnsafeAutoIdTimestamp);
        add(mergeStats.count);
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
     * Estimation of the memory usage by index writer
     */
    public long getIndexWriterMemoryInBytes() {
        return this.indexWriterMemoryInBytes;
    }

    public ByteSizeValue getIndexWriterMemory() {
        return ByteSizeValue.ofBytes(indexWriterMemoryInBytes);
    }

    /**
     * Estimation of the memory usage by version map
     */
    public long getVersionMapMemoryInBytes() {
        return this.versionMapMemoryInBytes;
    }

    public ByteSizeValue getVersionMapMemory() {
        return ByteSizeValue.ofBytes(versionMapMemoryInBytes);
    }

    /**
     * Estimation of how much the cached bit sets are taking. (which nested and p/c rely on)
     */
    public long getBitsetMemoryInBytes() {
        return bitsetMemoryInBytes;
    }

    public ByteSizeValue getBitsetMemory() {
        return ByteSizeValue.ofBytes(bitsetMemoryInBytes);
    }

    /**
     * Returns a mapping of file extension to statistics about files of that type.
     *
     * Note: This should only be used by tests.
     */
    public Map<String, FileStats> getFiles() {
        return Collections.unmodifiableMap(files);
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
        builder.humanReadableField(Fields.MEMORY_IN_BYTES, Fields.MEMORY, ByteSizeValue.ZERO);
        builder.humanReadableField(Fields.TERMS_MEMORY_IN_BYTES, Fields.TERMS_MEMORY, ByteSizeValue.ZERO);
        builder.humanReadableField(Fields.STORED_FIELDS_MEMORY_IN_BYTES, Fields.STORED_FIELDS_MEMORY, ByteSizeValue.ZERO);
        builder.humanReadableField(Fields.TERM_VECTORS_MEMORY_IN_BYTES, Fields.TERM_VECTORS_MEMORY, ByteSizeValue.ZERO);
        builder.humanReadableField(Fields.NORMS_MEMORY_IN_BYTES, Fields.NORMS_MEMORY, ByteSizeValue.ZERO);
        builder.humanReadableField(Fields.POINTS_MEMORY_IN_BYTES, Fields.POINTS_MEMORY, ByteSizeValue.ZERO);
        builder.humanReadableField(Fields.DOC_VALUES_MEMORY_IN_BYTES, Fields.DOC_VALUES_MEMORY, ByteSizeValue.ZERO);
        builder.humanReadableField(Fields.INDEX_WRITER_MEMORY_IN_BYTES, Fields.INDEX_WRITER_MEMORY, getIndexWriterMemory());
        builder.humanReadableField(Fields.VERSION_MAP_MEMORY_IN_BYTES, Fields.VERSION_MAP_MEMORY, getVersionMapMemory());
        builder.humanReadableField(Fields.FIXED_BIT_SET_MEMORY_IN_BYTES, Fields.FIXED_BIT_SET, getBitsetMemory());
        builder.field(Fields.MAX_UNSAFE_AUTO_ID_TIMESTAMP, maxUnsafeAutoIdTimestamp);
        builder.startObject(Fields.FILE_SIZES);
        for (Map.Entry<String, FileStats> entry : files.entrySet()) {
            entry.getValue().toXContent(builder, params);
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SegmentsStats that = (SegmentsStats) o;
        return count == that.count
            && indexWriterMemoryInBytes == that.indexWriterMemoryInBytes
            && versionMapMemoryInBytes == that.versionMapMemoryInBytes
            && maxUnsafeAutoIdTimestamp == that.maxUnsafeAutoIdTimestamp
            && bitsetMemoryInBytes == that.bitsetMemoryInBytes
            && Objects.equals(files, that.files);
    }

    @Override
    public int hashCode() {
        return Objects.hash(count, indexWriterMemoryInBytes, versionMapMemoryInBytes, maxUnsafeAutoIdTimestamp, bitsetMemoryInBytes, files);
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
        if (out.getTransportVersion().before(TransportVersions.V_8_0_0)) {
            out.writeLong(0L); // memoryInBytes
            out.writeLong(0L); // termsMemoryInBytes
            out.writeLong(0L); // storedFieldsMemoryInBytes
            out.writeLong(0L); // termVectorsMemoryInBytes
            out.writeLong(0L); // normsMemoryInBytes
            out.writeLong(0L); // pointsMemoryInBytes
            out.writeLong(0L); // docValuesMemoryInBytes
        }
        out.writeLong(indexWriterMemoryInBytes);
        out.writeLong(versionMapMemoryInBytes);
        out.writeLong(bitsetMemoryInBytes);
        out.writeLong(maxUnsafeAutoIdTimestamp);

        out.writeCollection(files.values());
    }

    public void clearFiles() {
        files.clear();
    }

    public static class FileStats implements Writeable, ToXContentFragment {

        private final String ext;
        private final long total;
        private final long count;
        private final long min;
        private final long max;

        FileStats(StreamInput in) throws IOException {
            this.ext = in.readString();
            this.total = in.readVLong();
            this.count = in.readVLong();
            this.min = in.readVLong();
            this.max = in.readVLong();
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
            out.writeString(ext);
            out.writeVLong(total);
            out.writeVLong(count);
            out.writeVLong(min);
            out.writeVLong(max);
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FileStats that = (FileStats) o;
            return Objects.equals(ext, that.ext) && total == that.total && count == that.count && min == that.min && max == that.max;
        }

        @Override
        public int hashCode() {
            return Objects.hash(ext, total, count, min, max);
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
