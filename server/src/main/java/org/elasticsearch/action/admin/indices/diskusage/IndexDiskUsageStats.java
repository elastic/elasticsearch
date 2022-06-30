/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.diskusage;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The result of analyzing disk usage of each field in a shard/index
 */
public final class IndexDiskUsageStats implements ToXContentFragment, Writeable {
    public static final String TOTAL = "total";
    public static final String TOTAL_IN_BYTES = "total_in_bytes";
    public static final String INVERTED_INDEX = "inverted_index";
    public static final String STORED_FIELDS = "stored_fields";
    public static final String STORED_FIELDS_IN_BYTES = "stored_fields_in_bytes";
    public static final String DOC_VALUES = "doc_values";
    public static final String DOC_VALUES_IN_BYTES = "doc_values_in_bytes";
    public static final String POINTS = "points";
    public static final String POINTS_IN_BYTES = "points_in_bytes";
    public static final String NORMS = "norms";
    public static final String NORMS_IN_BYTES = "norms_in_bytes";
    public static final String TERM_VECTORS = "term_vectors";
    public static final String TERM_VECTORS_IN_BYTES = "term_vectors_in_bytes";

    public static final String STORE_SIZE = "store_size";
    public static final String STORE_SIZE_IN_BYTES = "store_size_in_bytes";

    private final Map<String, PerFieldDiskUsage> fields;
    private long indexSizeInBytes;

    public IndexDiskUsageStats(long indexSizeInBytes) {
        fields = new HashMap<>();
        this.indexSizeInBytes = indexSizeInBytes;
    }

    public IndexDiskUsageStats(StreamInput in) throws IOException {
        this.fields = new HashMap<>(in.readMap(StreamInput::readString, PerFieldDiskUsage::new));
        this.indexSizeInBytes = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(fields, StreamOutput::writeString, (o, v) -> v.writeTo(o));
        out.writeVLong(indexSizeInBytes);
    }

    PerFieldDiskUsage total() {
        final PerFieldDiskUsage total = new PerFieldDiskUsage();
        for (PerFieldDiskUsage value : fields.values()) {
            total.add(value);
        }
        return total;
    }

    Map<String, PerFieldDiskUsage> getFields() {
        return fields;
    }

    long getIndexSizeInBytes() {
        return indexSizeInBytes;
    }

    private static void checkByteSize(long bytes) {
        if (bytes < 0) {
            throw new IllegalArgumentException("Bytes must be non-negative; got " + bytes);
        }
    }

    private PerFieldDiskUsage getOrAdd(String fieldName) {
        Objects.requireNonNull(fieldName, "fieldName must be non-null");
        return fields.computeIfAbsent(fieldName, k -> new PerFieldDiskUsage());
    }

    public void addInvertedIndex(String fieldName, long bytes) {
        checkByteSize(bytes);
        getOrAdd(fieldName).invertedIndexBytes += bytes;
    }

    public void addStoredField(String fieldName, long bytes) {
        checkByteSize(bytes);
        getOrAdd(fieldName).storedFieldBytes += bytes;
    }

    public void addDocValues(String fieldName, long bytes) {
        checkByteSize(bytes);
        getOrAdd(fieldName).docValuesBytes += bytes;
    }

    public void addPoints(String fieldName, long bytes) {
        checkByteSize(bytes);
        getOrAdd(fieldName).pointsBytes += bytes;
    }

    public void addNorms(String fieldName, long bytes) {
        checkByteSize(bytes);
        getOrAdd(fieldName).normsBytes += bytes;
    }

    public void addTermVectors(String fieldName, long bytes) {
        checkByteSize(bytes);
        getOrAdd(fieldName).termVectorsBytes += bytes;
    }

    public IndexDiskUsageStats add(IndexDiskUsageStats other) {
        other.fields.forEach((k, v) -> getOrAdd(k).add(v));
        this.indexSizeInBytes += other.indexSizeInBytes;
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        final PerFieldDiskUsage total = total();
        builder.field(STORE_SIZE, new ByteSizeValue(indexSizeInBytes));
        builder.field(STORE_SIZE_IN_BYTES, indexSizeInBytes);

        // all fields
        builder.startObject("all_fields");
        total.toXContent(builder, params);
        builder.endObject();

        // per field
        builder.startObject("fields");
        {
            final List<Map.Entry<String, PerFieldDiskUsage>> entries = fields.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .toList();
            for (Map.Entry<String, PerFieldDiskUsage> entry : entries) {
                builder.startObject(entry.getKey());
                entry.getValue().toXContent(builder, params);
                builder.endObject();
            }
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    /**
     * Disk usage stats for a single field
     */
    public static final class PerFieldDiskUsage implements ToXContentFragment, Writeable {
        private long invertedIndexBytes;
        private long storedFieldBytes;
        private long docValuesBytes;
        private long pointsBytes;
        private long normsBytes;
        private long termVectorsBytes;

        private PerFieldDiskUsage() {

        }

        private PerFieldDiskUsage(StreamInput in) throws IOException {
            invertedIndexBytes = in.readVLong();
            storedFieldBytes = in.readVLong();
            docValuesBytes = in.readVLong();
            pointsBytes = in.readVLong();
            normsBytes = in.readVLong();
            termVectorsBytes = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(invertedIndexBytes);
            out.writeVLong(storedFieldBytes);
            out.writeVLong(docValuesBytes);
            out.writeVLong(pointsBytes);
            out.writeVLong(normsBytes);
            out.writeVLong(termVectorsBytes);
        }

        private void add(PerFieldDiskUsage other) {
            invertedIndexBytes += other.invertedIndexBytes;
            storedFieldBytes += other.storedFieldBytes;
            docValuesBytes += other.docValuesBytes;
            pointsBytes += other.pointsBytes;
            normsBytes += other.normsBytes;
            termVectorsBytes += other.termVectorsBytes;
        }

        public long getInvertedIndexBytes() {
            return invertedIndexBytes;
        }

        public long getStoredFieldBytes() {
            return storedFieldBytes;
        }

        public long getDocValuesBytes() {
            return docValuesBytes;
        }

        public long getPointsBytes() {
            return pointsBytes;
        }

        public long getNormsBytes() {
            return normsBytes;
        }

        public long getTermVectorsBytes() {
            return termVectorsBytes;
        }

        long totalBytes() {
            return invertedIndexBytes + storedFieldBytes + docValuesBytes + pointsBytes + normsBytes + termVectorsBytes;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            final long totalBytes = totalBytes();
            builder.field(TOTAL, new ByteSizeValue(totalBytes));
            builder.field(TOTAL_IN_BYTES, totalBytes);

            builder.startObject(INVERTED_INDEX);
            builder.field(TOTAL, new ByteSizeValue(invertedIndexBytes));
            builder.field(TOTAL_IN_BYTES, invertedIndexBytes);
            builder.endObject();

            builder.field(STORED_FIELDS, new ByteSizeValue(storedFieldBytes));
            builder.field(STORED_FIELDS_IN_BYTES, storedFieldBytes);

            builder.field(DOC_VALUES, new ByteSizeValue(docValuesBytes));
            builder.field(DOC_VALUES_IN_BYTES, docValuesBytes);

            builder.field(POINTS, new ByteSizeValue(pointsBytes));
            builder.field(POINTS_IN_BYTES, pointsBytes);

            builder.field(NORMS, new ByteSizeValue(normsBytes));
            builder.field(NORMS_IN_BYTES, normsBytes);

            builder.field(TERM_VECTORS, new ByteSizeValue(termVectorsBytes));
            builder.field(TERM_VECTORS_IN_BYTES, termVectorsBytes);
            return builder;
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }
}
