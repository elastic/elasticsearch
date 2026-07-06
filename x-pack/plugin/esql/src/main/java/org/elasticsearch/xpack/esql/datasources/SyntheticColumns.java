/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Helpers for reader-synthesized internal channels — columns with no backing source field, filled
 * by the format reader from a per-format value source. Today the only synthetic kind is
 * {@link Kind#ROW_POSITION} (column name {@link ColumnExtractor#ROW_POSITION_COLUMN}), which the
 * optimizer ({@code InjectRowPositionForExternalId}) injects whenever {@code _id} or
 * {@code _file.record_ref} is requested; format readers fill the slot from their own per-format
 * source (CSV counter, ORC {@code RecordReader#getRowNumber}, NDJSON byte offset, parquet-mr
 * packed extractor id) and {@code VirtualColumnIterator} consumes it before the data reaches the
 * user.
 *
 * <p>Distinct from <em>virtual</em> columns: virtual columns are user-visible names you can write
 * in a {@code METADATA} clause (the {@code MetadataAttribute.ATTRIBUTES_MAP} names — {@code _index},
 * {@code _id}, {@code _version}, … — plus the {@code _file.*} family); they implement the
 * {@link org.elasticsearch.xpack.esql.core.expression.VirtualAttribute} marker. Synthetic columns
 * are internal-only: the user cannot name {@code _rowPosition} in a query, but its value reaches
 * the user via the virtual {@code _file.record_ref} and through {@code _id}'s composition.
 *
 * <p>The {@link Kind} registry is the single source of truth for the set of synthetic columns,
 * their canonical names, and their attribute shapes — consumers derive a kind's behavior from its
 * accessors rather than dispatching per member.
 */
public final class SyntheticColumns {

    /**
     * Registry of reader-synthesized internal channels. Each kind carries the canonical column
     * name and the attribute shape ({@link DataType} + {@link Nullability}) the engine expects.
     * Adding a new kind here is the single change that grows the registry; consumers read the
     * shape off the member's accessors.
     */
    public enum Kind {
        /**
         * Per-record token. The substrate for {@code _id} composition (hashed with the file identity by {@code ExternalRowIdentity}) and
         * surfaced directly as the virtual {@code _file.record_ref} column. Format-defined
         * opaque value (file-global row index on columnar formats, file-global byte offset on
         * text formats). Nullability is UNKNOWN, not FALSE: readers without a row-position
         * channel (parquet-rs) null-splice this slot, so a never-null declaration would license
         * null-aware optimizer rules to mis-fold against those sources.
         */
        ROW_POSITION(ColumnExtractor.ROW_POSITION_COLUMN, DataType.LONG, Nullability.UNKNOWN);

        private final String columnName;
        private final DataType dataType;
        private final Nullability nullability;

        Kind(String columnName, DataType dataType, Nullability nullability) {
            this.columnName = columnName;
            this.dataType = dataType;
            this.nullability = nullability;
        }

        public String columnName() {
            return columnName;
        }

        public DataType dataType() {
            return dataType;
        }

        public Nullability nullability() {
            return nullability;
        }
    }

    private static final Map<String, Kind> BY_NAME;
    static {
        Map<String, Kind> m = new HashMap<>(Kind.values().length);
        for (Kind k : Kind.values()) {
            m.put(k.columnName(), k);
        }
        BY_NAME = Map.copyOf(m);
    }

    /**
     * Names of every reader-synthesized internal channel. The producer pipeline injects these,
     * and rendering code (e.g. {@code SynthesizeExternalSource}) excludes them from the rendered
     * {@code _source} object. User data columns whose names happen to start with {@code _}
     * (e.g. Spark's {@code _corrupt_record}, a user-supplied {@code _status}) are real data — a
     * leading underscore on its own is not the filter; membership in this set is.
     */
    public static final Set<String> NAMES = BY_NAME.keySet();

    private SyntheticColumns() {}

    /** The {@link Kind} matching {@code name}, or {@code null} when {@code name} is not synthetic. */
    public static Kind kindOf(String name) {
        return BY_NAME.get(name);
    }

    /**
     * Index of {@link ColumnExtractor#ROW_POSITION_COLUMN} in {@code projectedColumns}, or
     * {@code -1} when absent or the list is {@code null}.
     */
    public static int rowPositionIndexInNames(List<String> projectedColumns) {
        return projectedColumns == null ? -1 : projectedColumns.indexOf(ColumnExtractor.ROW_POSITION_COLUMN);
    }

    /**
     * Index of {@link ColumnExtractor#ROW_POSITION_COLUMN} in {@code attributes}, by attribute
     * name; {@code -1} when absent or the list is {@code null}.
     */
    public static int rowPositionIndexInAttributes(List<? extends Attribute> attributes) {
        if (attributes == null) {
            return -1;
        }
        for (int i = 0; i < attributes.size(); i++) {
            if (ColumnExtractor.ROW_POSITION_COLUMN.equals(attributes.get(i).name())) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Reader-side attribute shape for the slot of {@code kind}: a {@link ReferenceAttribute}
     * whose type and nullability come from the {@link Kind} record. Used by format readers
     * when they synthesize the projected attribute for a slot that has no source column behind
     * it (e.g. the {@code _rowPosition} channel in {@code CsvFormatReader}'s switch on
     * {@link Kind}).
     */
    public static Attribute newAttribute(Kind kind) {
        return new ReferenceAttribute(Source.EMPTY, null, kind.columnName(), kind.dataType(), kind.nullability(), null, false);
    }

    /** Shorthand for {@code newAttribute(Kind.ROW_POSITION)}; kept for the kind-specific call sites. */
    public static Attribute newRowPositionAttribute() {
        return newAttribute(Kind.ROW_POSITION);
    }

    /**
     * Optimizer-side attribute shape for the {@link Kind#ROW_POSITION} slot: a
     * {@link MetadataAttribute} typed and nullability-marked from the {@link Kind} record,
     * marked synthetic. Paired with {@link #newRowPositionAttribute()} so type and nullability
     * stay in lock-step between the optimizer injection site and the reader synthesis site.
     */
    public static MetadataAttribute newRowPositionMetadataAttribute(Source source) {
        Kind kind = Kind.ROW_POSITION;
        return new MetadataAttribute(source, kind.columnName(), kind.dataType(), kind.nullability(), null, true, false);
    }
}
