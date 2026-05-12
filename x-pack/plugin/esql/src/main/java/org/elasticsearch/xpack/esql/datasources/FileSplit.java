/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a byte range within a file for a file-based external source.
 * A single file may map to one or more splits. Carries Hive partition
 * key-value pairs extracted from the file path for partition pruning.
 */
public class FileSplit implements ExternalSplit {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        ExternalSplit.class,
        "FileSplit",
        FileSplit::new
    );

    static final TransportVersion ESQL_SPLIT_STATS_COMPACT = TransportVersion.fromName("esql_split_stats_compact");

    private final String sourceType;
    private final StoragePath path;
    private final long offset;
    private final long length;
    private final String format;
    private final Map<String, Object> config;
    private final Map<String, Object> partitionValues;
    @Nullable
    private final SchemaReconciliation.ColumnMapping columnMapping;
    @Nullable
    private final Map<String, Object> statistics;
    @Nullable
    private final SplitStats splitStats;

    public FileSplit(
        String sourceType,
        StoragePath path,
        long offset,
        long length,
        String format,
        Map<String, Object> config,
        Map<String, Object> partitionValues
    ) {
        this(sourceType, path, offset, length, format, config, partitionValues, null, null, null);
    }

    public FileSplit(
        String sourceType,
        StoragePath path,
        long offset,
        long length,
        String format,
        Map<String, Object> config,
        Map<String, Object> partitionValues,
        @Nullable SchemaReconciliation.ColumnMapping columnMapping
    ) {
        this(sourceType, path, offset, length, format, config, partitionValues, columnMapping, null, null);
    }

    public FileSplit(
        String sourceType,
        StoragePath path,
        long offset,
        long length,
        String format,
        Map<String, Object> config,
        Map<String, Object> partitionValues,
        @Nullable SchemaReconciliation.ColumnMapping columnMapping,
        @Nullable Map<String, Object> statistics
    ) {
        this(sourceType, path, offset, length, format, config, partitionValues, columnMapping, statistics, null);
    }

    /**
     * Creates a FileSplit with compact {@link SplitStats} instead of a raw statistics map.
     */
    public static FileSplit withSplitStats(
        String sourceType,
        StoragePath path,
        long offset,
        long length,
        String format,
        Map<String, Object> config,
        Map<String, Object> partitionValues,
        @Nullable SchemaReconciliation.ColumnMapping columnMapping,
        @Nullable SplitStats splitStats
    ) {
        return new FileSplit(sourceType, path, offset, length, format, config, partitionValues, columnMapping, null, splitStats);
    }

    private FileSplit(
        String sourceType,
        StoragePath path,
        long offset,
        long length,
        String format,
        Map<String, Object> config,
        Map<String, Object> partitionValues,
        @Nullable SchemaReconciliation.ColumnMapping columnMapping,
        @Nullable Map<String, Object> statistics,
        @Nullable SplitStats splitStats
    ) {
        if (sourceType == null) {
            throw new IllegalArgumentException("sourceType cannot be null");
        }
        if (path == null) {
            throw new IllegalArgumentException("path cannot be null");
        }
        if (statistics != null && splitStats != null) {
            throw new IllegalArgumentException("cannot set both statistics map and SplitStats");
        }
        this.sourceType = sourceType;
        this.path = path;
        this.offset = offset;
        this.length = length;
        this.format = format;
        this.config = config != null ? Map.copyOf(config) : Map.of();
        this.partitionValues = partitionValues != null && partitionValues.isEmpty() == false
            ? Collections.unmodifiableMap(new LinkedHashMap<>(partitionValues))
            : Map.of();
        this.columnMapping = columnMapping;
        // Normalize: eagerly convert legacy map to SplitStats when possible so that
        // equals/hashCode and serialization round-trips are stable.
        if (splitStats != null) {
            this.splitStats = splitStats;
            this.statistics = null;
        } else if (statistics != null) {
            SplitStats converted = SplitStats.of(statistics);
            if (converted != null) {
                this.splitStats = converted;
                this.statistics = null;
            } else {
                this.splitStats = null;
                this.statistics = Map.copyOf(statistics);
            }
        } else {
            this.splitStats = null;
            this.statistics = null;
        }
    }

    public FileSplit(StreamInput in) throws IOException {
        this.sourceType = in.readString();
        this.path = StoragePath.of(in.readString());
        this.offset = in.readVLong();
        this.length = in.readVLong();
        this.format = in.readOptionalString();
        this.config = in.readGenericMap();
        this.partitionValues = in.readGenericMap();
        if (in.readBoolean()) {
            this.columnMapping = new SchemaReconciliation.ColumnMapping(in);
        } else {
            this.columnMapping = null;
        }
        if (in.getTransportVersion().supports(ESQL_SPLIT_STATS_COMPACT)) {
            if (in.readBoolean()) {
                this.splitStats = new SplitStats(in);
                this.statistics = null;
            } else {
                this.splitStats = null;
                this.statistics = null;
            }
        } else {
            if (in.readBoolean()) {
                this.statistics = Map.copyOf(in.readGenericMap());
            } else {
                this.statistics = null;
            }
            this.splitStats = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(sourceType);
        out.writeString(path.toString());
        out.writeVLong(offset);
        out.writeVLong(length);
        out.writeOptionalString(format);
        out.writeGenericMap(config);
        out.writeGenericMap(partitionValues);
        if (columnMapping != null) {
            out.writeBoolean(true);
            columnMapping.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        if (out.getTransportVersion().supports(ESQL_SPLIT_STATS_COMPACT)) {
            if (splitStats != null) {
                out.writeBoolean(true);
                splitStats.writeTo(out);
            } else if (statistics != null) {
                // Legacy statistics map present but no SplitStats: convert on the fly
                SplitStats converted = SplitStats.of(statistics);
                if (converted != null) {
                    out.writeBoolean(true);
                    converted.writeTo(out);
                } else {
                    out.writeBoolean(false);
                }
            } else {
                out.writeBoolean(false);
            }
        } else {
            // Old format: write as map
            Map<String, Object> statsMap = splitStats != null ? splitStats.toMap() : statistics;
            if (statsMap != null) {
                out.writeBoolean(true);
                out.writeGenericMap(statsMap);
            } else {
                out.writeBoolean(false);
            }
        }
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public String sourceType() {
        return sourceType;
    }

    public StoragePath path() {
        return path;
    }

    public long offset() {
        return offset;
    }

    public long length() {
        return length;
    }

    public String format() {
        return format;
    }

    public Map<String, Object> config() {
        return config;
    }

    public Map<String, Object> partitionValues() {
        return partitionValues;
    }

    @Nullable
    public SchemaReconciliation.ColumnMapping columnMapping() {
        return columnMapping;
    }

    /**
     * Returns per-split statistics as a flat map using {@code _stats.*} keys. Delegates to
     * {@link SplitStats#toMap()} when compact stats are present; falls back to the legacy map.
     */
    @Nullable
    public Map<String, Object> statistics() {
        if (splitStats != null) {
            return splitStats.toMap();
        }
        return statistics;
    }

    /**
     * Returns the compact split stats, or {@code null} if only legacy map stats are available.
     * Overrides {@link ExternalSplit#splitStats()} with a covariant return type.
     */
    @Override
    @Nullable
    public SplitStats splitStats() {
        return splitStats;
    }

    @Override
    public long estimatedSizeInBytes() {
        return length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FileSplit that = (FileSplit) o;
        return offset == that.offset
            && length == that.length
            && Objects.equals(sourceType, that.sourceType)
            && Objects.equals(path, that.path)
            && Objects.equals(format, that.format)
            && Objects.equals(config, that.config)
            && Objects.equals(partitionValues, that.partitionValues)
            && Objects.equals(columnMapping, that.columnMapping)
            && Objects.equals(statistics, that.statistics)
            && Objects.equals(splitStats, that.splitStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceType, path, offset, length, format, config, partitionValues, columnMapping, statistics, splitStats);
    }

    @Override
    public String toString() {
        return "FileSplit[" + path + ", offset=" + offset + ", length=" + length + ", partitions=" + partitionValues + "]";
    }
}
