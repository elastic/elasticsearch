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
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * The result of analyzing disk usage of each field in a shard/index
 */
public final class IndexDiskUsageStats implements ToXContentFragment, Writeable {
    public static final String TOTAL = "total";
    public static final String TOTAL_IN_BYTES = "total_in_bytes";
    public static final String TERMS = "terms";
    public static final String TERMS_IN_BYTES = "terms_in_bytes";
    public static final String POSTINGS = "postings";
    public static final String POSTINGS_IN_BYTES = "postings_in_bytes";
    public static final String PROXIMITY = "proximity";
    public static final String PROXIMITY_IN_BYTES = "proximity_in_bytes";
    public static final String STORED_FIELDS = "stored_fields";
    public static final String STORED_FIELDS_IN_BYTES = "stored_fields_in_bytes";
    public static final String DOC_VALUES = "doc_values";
    public static final String DOC_VALUES_IN_BYTES = "doc_values_in_bytes";
    public static final String POINTS = "points";
    public static final String POINTS_IN_BYTES = "points_in_bytes";
    public static final String NORMS = "norms";
    public static final String NORMS_IN_BYTES = "norms_in_bytes";
    public static final String VECTORS = "vectors";
    public static final String VECTORS_IN_BYTES = "vectors_in_bytes";

    private final Map<String, PerFieldDiskUsage> fields;

    public IndexDiskUsageStats() {
        fields = new HashMap<>();
    }

    public IndexDiskUsageStats(StreamInput in) throws IOException {
        this.fields = in.readMap(StreamInput::readString, PerFieldDiskUsage::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(fields, StreamOutput::writeString, (o, v) -> v.writeTo(o));
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

    private void checkByteSize(long bytes) {
        if (bytes < 0) {
            throw new IllegalArgumentException("Bytes must be non-negative; got " + bytes);
        }
    }

    private PerFieldDiskUsage getOrAdd(String fieldName) {
        Objects.requireNonNull(fieldName, "fieldName must be non-null");
        return fields.computeIfAbsent(fieldName, k -> new PerFieldDiskUsage());
    }

    public void addTerms(String fieldName, long bytes) {
        checkByteSize(bytes);
        getOrAdd(fieldName).termsBytes += bytes;
    }

    public void addPosting(String fieldName, long bytes) {
        checkByteSize(bytes);
        getOrAdd(fieldName).postingsBytes += bytes;
    }

    public void addProximity(String fieldName, long bytes) {
        checkByteSize(bytes);
        getOrAdd(fieldName).proximityBytes += bytes;
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

    public void addVectors(String fieldName, long bytes) {
        checkByteSize(bytes);
        getOrAdd(fieldName).vectorsBytes += bytes;
    }

    public IndexDiskUsageStats add(IndexDiskUsageStats other) {
        other.fields.forEach((k, v) -> getOrAdd(k).add(v));
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // _all
        {
            builder.startObject("_all");
            total().toXContent(builder, params);
            builder.endObject();
        }
        // per field
        {
            builder.startObject("fields");
            final List<String> sortedFields = fields.keySet().stream().sorted().collect(Collectors.toList());
            for (String field : sortedFields) {
                builder.startObject(field);
                fields.get(field).toXContent(builder, params);
                builder.endObject();
            }
            builder.endObject();
        }
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    /**
     * Disk usage stats for a single field
     */
    public static final class PerFieldDiskUsage implements Writeable, ToXContentFragment {
        private long termsBytes;
        private long postingsBytes;
        private long proximityBytes;
        private long storedFieldBytes;
        private long docValuesBytes;
        private long pointsBytes;
        private long normsBytes;
        private long vectorsBytes;

        private PerFieldDiskUsage() {

        }

        private PerFieldDiskUsage(StreamInput in) throws IOException {
            termsBytes = in.readZLong();
            postingsBytes = in.readZLong();
            proximityBytes = in.readZLong();
            storedFieldBytes = in.readZLong();
            docValuesBytes = in.readZLong();
            pointsBytes = in.readZLong();
            normsBytes = in.readZLong();
            vectorsBytes = in.readZLong();
        }

        private void add(PerFieldDiskUsage other) {
            termsBytes += other.termsBytes;
            postingsBytes += other.postingsBytes;
            proximityBytes += other.proximityBytes;
            storedFieldBytes += other.storedFieldBytes;
            docValuesBytes += other.docValuesBytes;
            pointsBytes += other.pointsBytes;
            normsBytes += other.normsBytes;
            vectorsBytes += other.vectorsBytes;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeZLong(termsBytes);
            out.writeZLong(postingsBytes);
            out.writeZLong(proximityBytes);
            out.writeZLong(storedFieldBytes);
            out.writeZLong(docValuesBytes);
            out.writeZLong(pointsBytes);
            out.writeZLong(normsBytes);
            out.writeZLong(vectorsBytes);
        }

        public long getPostingsBytes() {
            return postingsBytes;
        }

        public long getTermsBytes() {
            return termsBytes;
        }

        public long getProximityBytes() {
            return proximityBytes;
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

        public long getVectorsBytes() {
            return vectorsBytes;
        }

        long totalBytes() {
            return termsBytes + postingsBytes + proximityBytes +
                storedFieldBytes + docValuesBytes + pointsBytes + normsBytes + vectorsBytes;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            final long totalBytes = totalBytes();
            builder.field(TOTAL, new ByteSizeValue(totalBytes));
            builder.field(TOTAL_IN_BYTES, totalBytes);

            builder.field(TERMS, new ByteSizeValue(termsBytes));
            builder.field(TERMS_IN_BYTES, termsBytes);

            builder.field(POSTINGS, new ByteSizeValue(postingsBytes));
            builder.field(POSTINGS_IN_BYTES, postingsBytes);

            builder.field(PROXIMITY, new ByteSizeValue(proximityBytes));
            builder.field(PROXIMITY_IN_BYTES, proximityBytes);

            builder.field(STORED_FIELDS, new ByteSizeValue(storedFieldBytes));
            builder.field(STORED_FIELDS_IN_BYTES, storedFieldBytes);

            builder.field(DOC_VALUES, new ByteSizeValue(docValuesBytes));
            builder.field(DOC_VALUES_IN_BYTES, docValuesBytes);

            builder.field(POINTS, new ByteSizeValue(pointsBytes));
            builder.field(POINTS_IN_BYTES, pointsBytes);

            builder.field(NORMS, new ByteSizeValue(normsBytes));
            builder.field(NORMS_IN_BYTES, normsBytes);

            builder.field(VECTORS, new ByteSizeValue(vectorsBytes));
            builder.field(VECTORS_IN_BYTES, vectorsBytes);
            return builder;
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }
}
