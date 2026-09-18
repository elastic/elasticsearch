/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.shard;

import org.apache.lucene.codecs.KnnVectorsReader;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.codec.vectors.diskbbq.SegmentCalibrationParameters;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.unit.ByteSizeValue.ofBytes;

/**
 * Statistics about indexed dense vector
 */
public class DenseVectorStats implements Writeable, ToXContentFragment {

    static final TransportVersion DENSE_VECTOR_AUTO_CALIBRATION_STATS = TransportVersion.fromName("dense_vector_auto_calibration_stats");
    private static final TransportVersion DENSE_VECTOR_OFF_HEAP_STATS = TransportVersion.fromName("dense_vector_off_heap_stats");

    private long valueCount = 0;

    /** Per-field off-heap desired memory byte size, categorized by file extension. */
    Map<String, Map<String, Long>> offHeapStats;

    /** Per-field parameters applied by auto-calibration, provides aggregated counts by unique parameter set. */
    Map<String, List<AutoCalibrationEntry>> calibrationStats;

    public DenseVectorStats() {}

    public DenseVectorStats(long count) {
        this(count, null, null);
    }

    public DenseVectorStats(long count, Map<String, Map<String, Long>> offHeapStats) {
        this(count, offHeapStats, null);
    }

    public DenseVectorStats(
        long count,
        Map<String, Map<String, Long>> offHeapStats,
        Map<String, List<AutoCalibrationEntry>> calibrationStats
    ) {
        this.valueCount = count;
        this.offHeapStats = offHeapStats;
        this.calibrationStats = calibrationStats;
    }

    public DenseVectorStats(StreamInput in) throws IOException {
        this.valueCount = in.readVLong();
        if (in.getTransportVersion().supports(DENSE_VECTOR_OFF_HEAP_STATS)) {
            this.offHeapStats = readOptionalOffHeapStats(in);
        }
        if (in.getTransportVersion().supports(DENSE_VECTOR_AUTO_CALIBRATION_STATS)) {
            this.calibrationStats = readOptionalCalibrationStats(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(valueCount);
        if (out.getTransportVersion().supports(DENSE_VECTOR_OFF_HEAP_STATS)) {
            writeOptionalOffHeapStats(out);
        }
        if (out.getTransportVersion().supports(DENSE_VECTOR_AUTO_CALIBRATION_STATS)) {
            writeOptionalCalibrationStats(out);
        }
    }

    private Map<String, Map<String, Long>> readOptionalOffHeapStats(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            return in.readMap(v -> in.readMap(StreamInput::readLong));
        } else {
            return null;
        }
    }

    private void writeOptionalOffHeapStats(StreamOutput out) throws IOException {
        if (offHeapStats != null) {
            out.writeBoolean(true);
            out.writeMap(offHeapStats, StreamOutput::writeString, DenseVectorStats::writeFieldStatsMap);
        } else {
            out.writeBoolean(false);
        }
    }

    static void writeFieldStatsMap(StreamOutput out, Map<String, Long> map) throws IOException {
        out.writeMap(map, StreamOutput::writeString, StreamOutput::writeLong);
    }

    private Map<String, List<AutoCalibrationEntry>> readOptionalCalibrationStats(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            return in.readMap(v -> in.readCollectionAsList(AutoCalibrationEntry::new));
        } else {
            return null;
        }
    }

    private void writeOptionalCalibrationStats(StreamOutput out) throws IOException {
        if (calibrationStats != null) {
            out.writeBoolean(true);
            out.writeMap(calibrationStats, StreamOutput::writeString, (o, list) -> o.writeCollection(list));
        } else {
            out.writeBoolean(false);
        }
    }

    public void add(DenseVectorStats other) {
        if (other == null) {
            return;
        }
        this.valueCount += other.valueCount;
        if (other.offHeapStats != null) {
            if (this.offHeapStats == null) {
                this.offHeapStats = other.offHeapStats;
            } else {
                this.offHeapStats = Stream.of(this.offHeapStats, other.offHeapStats)
                    .flatMap(map -> map.entrySet().stream())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, KnnVectorsReader::mergeOffHeapByteSizeMaps));
            }
        }
        if (other.calibrationStats != null) {
            if (this.calibrationStats == null) {
                this.calibrationStats = other.calibrationStats;
            } else {
                Map<String, List<AutoCalibrationEntry>> merged = new HashMap<>(this.calibrationStats);
                for (Map.Entry<String, List<AutoCalibrationEntry>> entry : other.calibrationStats.entrySet()) {
                    merged.merge(entry.getKey(), entry.getValue(), DenseVectorStats::mergeCalibrationEntries);
                }
                this.calibrationStats = Collections.unmodifiableMap(merged);
            }
        }
    }

    private static List<AutoCalibrationEntry> mergeCalibrationEntries(
        List<AutoCalibrationEntry> existing,
        List<AutoCalibrationEntry> incoming
    ) {
        Map<SegmentCalibrationParameters, AutoCalibrationEntry> byKey = new HashMap<>();
        for (AutoCalibrationEntry e : existing) {
            byKey.merge(e.parameters, e, AutoCalibrationEntry::accumulate);
        }
        for (AutoCalibrationEntry e : incoming) {
            byKey.merge(e.parameters, e, AutoCalibrationEntry::accumulate);
        }
        return Collections.unmodifiableList(new ArrayList<>(byKey.values()));
    }

    public long getValueCount() {
        return valueCount;
    }

    public Map<String, Map<String, Long>> offHeapStats() {
        return offHeapStats;
    }

    public Map<String, List<AutoCalibrationEntry>> calibrationStats() {
        return calibrationStats;
    }

    private Map<String, Long> getTotalsByCategory() {
        if (offHeapStats == null) {
            return Map.of("veb", 0L, "vec", 0L, "veq", 0L, "vex", 0L, "cenivf", 0L, "clivf", 0L);
        } else {
            return offHeapStats.entrySet()
                .stream()
                .flatMap(map -> map.getValue().entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Long::sum));
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.NAME);
        builder.field(Fields.VALUE_COUNT, valueCount);
        if (params.paramAsBoolean(INCLUDE_OFF_HEAP, false)) {
            toXContentWithFields(builder, params);
        }
        builder.endObject();
        return builder;
    }

    private void toXContentWithFields(XContentBuilder builder, Params params) throws IOException {
        var totals = getTotalsByCategory();
        builder.startObject("off_heap");
        builder.humanReadableField("total_size_bytes", "total_size", ofBytes(totals.values().stream().mapToLong(Long::longValue).sum()));
        builder.humanReadableField("total_veb_size_bytes", "total_veb_size", ofBytes(totals.getOrDefault("veb", 0L)));
        builder.humanReadableField("total_vec_size_bytes", "total_vec_size", ofBytes(totals.getOrDefault("vec", 0L)));
        builder.humanReadableField("total_veq_size_bytes", "total_veq_size", ofBytes(totals.getOrDefault("veq", 0L)));
        builder.humanReadableField("total_vex_size_bytes", "total_vex_size", ofBytes(totals.getOrDefault("vex", 0L)));
        builder.humanReadableField("total_cenivf_size_bytes", "total_cenivf_size", ofBytes(totals.getOrDefault("cenivf", 0L)));
        builder.humanReadableField("total_clivf_size_bytes", "total_clivf_size", ofBytes(totals.getOrDefault("clivf", 0L)));
        if (params.paramAsBoolean(INCLUDE_PER_FIELD_STATS, false) && offHeapStats != null && offHeapStats.isEmpty() == false) {
            toXContentWithPerFieldStats(builder, params);
        }
        builder.endObject();
    }

    private void toXContentWithPerFieldStats(XContentBuilder builder, Params params) throws IOException {
        boolean includeAutoCalibration = params.paramAsBoolean(INCLUDE_AUTO_CALIBRATION, false);
        builder.startObject(Fields.FIELDS);
        for (var key : offHeapStats.keySet().stream().sorted().toList()) {
            Map<String, Long> entry = offHeapStats.get(key);
            if (entry.isEmpty() == false) {
                builder.startObject(key);
                for (var eKey : entry.keySet().stream().sorted().toList()) {
                    long value = entry.get(eKey);
                    assert value >= 0L;
                    builder.humanReadableField(eKey + "_size_bytes", eKey + "_size", ofBytes(value));
                }
                if (includeAutoCalibration && calibrationStats != null) {
                    List<AutoCalibrationEntry> calibEntries = calibrationStats.get(key);
                    if (calibEntries != null && calibEntries.isEmpty() == false) {
                        toXContentAutoCalibrationEntries(builder, calibEntries);
                    }
                }
                builder.endObject();
            }
        }
        builder.endObject();
    }

    private static void toXContentAutoCalibrationEntries(XContentBuilder builder, List<AutoCalibrationEntry> entries) throws IOException {
        builder.startArray(Fields.AUTO_CALIBRATION);
        for (AutoCalibrationEntry entry : entries) {
            entry.toXContent(builder, ToXContent.EMPTY_PARAMS);
        }
        builder.endArray();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DenseVectorStats that = (DenseVectorStats) o;
        return valueCount == that.valueCount
            && Objects.equals(offHeapStats, that.offHeapStats)
            && Objects.equals(calibrationStats, that.calibrationStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(valueCount, offHeapStats, calibrationStats);
    }

    public static final String INCLUDE_OFF_HEAP = "include_off_heap";
    public static final String INCLUDE_PER_FIELD_STATS = "include_per_field_stats";
    public static final String INCLUDE_AUTO_CALIBRATION = "include_auto_calibration";

    static final class Fields {
        static final String NAME = "dense_vector";
        static final String VALUE_COUNT = "value_count";
        static final String FIELDS = "fielddata";
        static final String AUTO_CALIBRATION = "auto_calibration";
    }

    /**
     * Stores aggregated auto-calibration data for one distinct parameter set.
     */
    public static final class AutoCalibrationEntry implements Writeable, ToXContentFragment {

        public final SegmentCalibrationParameters parameters;
        public final long numberOfVectors;
        public final long sizeInBytes;
        public final int numberOfSegments;

        public AutoCalibrationEntry(
            SegmentCalibrationParameters parameters,
            long numberOfVectors,
            long sizeInBytes,
            int numberOfSegments
        ) {
            this.parameters = parameters;
            this.numberOfVectors = numberOfVectors;
            this.sizeInBytes = sizeInBytes;
            this.numberOfSegments = numberOfSegments;
        }

        public AutoCalibrationEntry(StreamInput in) throws IOException {
            this.parameters = in.readBoolean()
                ? SegmentCalibrationParameters.readFrom(in)
                : new SegmentCalibrationParameters.Osq(null, false, Float.NaN);
            this.numberOfVectors = in.readVLong();
            this.sizeInBytes = in.readVLong();
            this.numberOfSegments = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(parameters.calibrated());
            if (parameters.calibrated()) {
                parameters.writeTo(out);
            }
            out.writeVLong(numberOfVectors);
            out.writeVLong(sizeInBytes);
            out.writeVInt(numberOfSegments);
        }

        public AutoCalibrationEntry accumulate(AutoCalibrationEntry other) {
            assert Objects.equals(this.parameters, other.parameters);
            return new AutoCalibrationEntry(
                this.parameters,
                this.numberOfVectors + other.numberOfVectors,
                this.sizeInBytes + other.sizeInBytes,
                this.numberOfSegments + other.numberOfSegments
            );
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("calibrated", parameters.calibrated());
            builder.field("type", parameters.type());
            builder.field("number_of_vectors", numberOfVectors);
            builder.humanReadableField("size_in_bytes", "size", ofBytes(sizeInBytes));
            builder.field("number_of_segments", numberOfSegments);
            if (parameters.calibrated()) {
                builder.startObject("parameters");
                parameters.toXContent(builder);
                builder.endObject();
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AutoCalibrationEntry that = (AutoCalibrationEntry) o;
            return numberOfVectors == that.numberOfVectors
                && sizeInBytes == that.sizeInBytes
                && numberOfSegments == that.numberOfSegments
                && Objects.equals(parameters, that.parameters);
        }

        @Override
        public int hashCode() {
            return Objects.hash(parameters, numberOfVectors, sizeInBytes, numberOfSegments);
        }
    }
}
