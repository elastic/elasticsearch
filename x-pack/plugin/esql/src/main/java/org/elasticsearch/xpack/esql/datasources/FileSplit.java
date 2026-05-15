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
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
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
    private static final TransportVersion ESQL_EXTERNAL_SOURCE_READ_SCHEMA = TransportVersion.fromName("esql_external_source_read_schema");

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
    /**
     * The schema the reader should use to interpret this file. Per-file, by nature — readers are per-file
     * entities. In FFW and STRICT, every split's {@code readSchema} carries the same value (the
     * anchor / validated common schema). In UBN, each split carries that file's coordinator-inferred
     * physical schema, which differs across files when files differ. {@code null} means "no pin — reader
     * may self-infer," preserving pre-PR behavior for older nodes or sources that don't compute one.
     */
    @Nullable
    private final List<Attribute> readSchema;

    public FileSplit(
        String sourceType,
        StoragePath path,
        long offset,
        long length,
        String format,
        Map<String, Object> config,
        Map<String, Object> partitionValues
    ) {
        this(sourceType, path, offset, length, format, config, partitionValues, null, null, null, null);
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
        this(sourceType, path, offset, length, format, config, partitionValues, columnMapping, null, null, null);
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
        this(sourceType, path, offset, length, format, config, partitionValues, columnMapping, statistics, null, null);
    }

    /**
     * Static factory that includes the per-file {@code readSchema} alongside the {@link SchemaReconciliation.ColumnMapping}.
     * Use this when building splits with the planner-resolved per-file schema so the reader can be pinned to the
     * coordinator's inference instead of self-inferring at runtime.
     * <p>Provided as a static factory (instead of a constructor overload) to avoid ambiguity with the
     * existing {@code (columnMapping, statistics)} constructor when callers pass {@code null}.
     */
    public static FileSplit withReadSchema(
        String sourceType,
        StoragePath path,
        long offset,
        long length,
        String format,
        Map<String, Object> config,
        Map<String, Object> partitionValues,
        @Nullable SchemaReconciliation.ColumnMapping columnMapping,
        @Nullable List<Attribute> readSchema
    ) {
        return new FileSplit(sourceType, path, offset, length, format, config, partitionValues, columnMapping, null, null, readSchema);
    }

    /**
     * Static factory that includes statistics (raw map) and the per-file {@code readSchema}.
     */
    public static FileSplit withStatisticsAndReadSchema(
        String sourceType,
        StoragePath path,
        long offset,
        long length,
        String format,
        Map<String, Object> config,
        Map<String, Object> partitionValues,
        @Nullable SchemaReconciliation.ColumnMapping columnMapping,
        @Nullable Map<String, Object> statistics,
        @Nullable List<Attribute> readSchema
    ) {
        return new FileSplit(
            sourceType,
            path,
            offset,
            length,
            format,
            config,
            partitionValues,
            columnMapping,
            statistics,
            null,
            readSchema
        );
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
        return new FileSplit(sourceType, path, offset, length, format, config, partitionValues, columnMapping, null, splitStats, null);
    }

    /**
     * Creates a FileSplit with compact {@link SplitStats} and a per-file {@code readSchema}.
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
        @Nullable SplitStats splitStats,
        @Nullable List<Attribute> readSchema
    ) {
        return new FileSplit(
            sourceType,
            path,
            offset,
            length,
            format,
            config,
            partitionValues,
            columnMapping,
            null,
            splitStats,
            readSchema
        );
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
        @Nullable SplitStats splitStats,
        @Nullable List<Attribute> readSchema
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
        // Empty list and null mean the same thing at this layer: "no schema pin." Collapse so the reader
        // does exactly one null-check downstream (mirrors FormatReadContext.readSchema's compact ctor).
        this.readSchema = (readSchema == null || readSchema.isEmpty()) ? null : List.copyOf(readSchema);
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
        if (in.getTransportVersion().supports(ESQL_EXTERNAL_SOURCE_READ_SCHEMA)) {
            if (in.readBoolean()) {
                int count = in.readVInt();
                List<Attribute> attrs = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    String name = in.readString();
                    // DataType.readFrom throws IOException on unknown; DataType.fromTypeName returns null.
                    DataType type = DataType.readFrom(in.readString());
                    Nullability nullability = in.readBoolean() ? Nullability.TRUE : Nullability.FALSE;
                    attrs.add(new ReferenceAttribute(Source.EMPTY, null, name, type, nullability, null, false));
                }
                // Local list never escapes; wrap rather than copy.
                this.readSchema = Collections.unmodifiableList(attrs);
            } else {
                this.readSchema = null;
            }
        } else {
            this.readSchema = null;
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
        if (out.getTransportVersion().supports(ESQL_EXTERNAL_SOURCE_READ_SCHEMA)) {
            if (readSchema != null) {
                out.writeBoolean(true);
                // Primitive (name, typeName, nullable). writeNamedWriteableCollection(Attribute) can't
                // be used: FileSplit travels on RecyclerBytesStreamOutput, not PlanStreamOutput.
                // Anything not provably non-null is written as nullable: UNKNOWN (planner-internal) maps to TRUE
                // on the wire so it can't be reconstituted as a stronger non-null guarantee than the source carried.
                out.writeVInt(readSchema.size());
                for (Attribute attr : readSchema) {
                    out.writeString(attr.name());
                    out.writeString(attr.dataType().typeName());
                    out.writeBoolean(attr.nullable() != Nullability.FALSE);
                }
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
     * Returns the per-file schema the reader should be pinned to, or {@code null} if no pin —
     * the reader is then free to self-infer (pre-PR behavior, preserved for older nodes and
     * sources that don't compute a per-file schema).
     */
    @Nullable
    public List<Attribute> readSchema() {
        return readSchema;
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
            && Objects.equals(splitStats, that.splitStats)
            && Objects.equals(readSchema, that.readSchema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            sourceType,
            path,
            offset,
            length,
            format,
            config,
            partitionValues,
            columnMapping,
            statistics,
            splitStats,
            readSchema
        );
    }

    @Override
    public String toString() {
        return "FileSplit[" + path + ", offset=" + offset + ", length=" + length + ", partitions=" + partitionValues + "]";
    }
}
