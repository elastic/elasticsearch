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

import java.util.List;
import java.util.Set;

/**
 * Helpers for reader-synthesized internal channels — columns with no backing source field, filled
 * by the format reader from a per-format value source. Today the only synthetic column is
 * {@link ColumnExtractor#ROW_POSITION_COLUMN}, which the optimizer
 * ({@code InjectRowPositionForExternalId}) injects into the projection whenever {@code _id} or
 * {@code _file.record_ref} is requested; format readers fill the slot from their own per-format
 * source (CSV counter, ORC {@code RecordReader#getRowNumber}, NDJSON byte offset, parquet-mr
 * packed extractor id) and {@code VirtualColumnIterator} consumes it before the data reaches the
 * user.
 *
 * <p>Distinct from <em>virtual</em> columns. Virtual columns are user-visible names you can write
 * in a {@code METADATA} clause (the {@code MetadataAttribute.ATTRIBUTES_MAP} names — {@code _index},
 * {@code _id}, {@code _version}, … — plus the {@code _file.*} family); they implement the
 * {@link org.elasticsearch.xpack.esql.core.expression.VirtualAttribute} marker. Synthetic columns
 * are internal-only: the user cannot name {@code _rowPosition} in a query, but its value reaches
 * the user via the virtual {@code _file.record_ref} and through {@code _id}'s composition.
 *
 * <p>When a second synthetic kind lands (e.g. {@code _rowGroup}), the natural extension here is a
 * {@code SyntheticKind} discriminator plus per-kind factories; today's single-kind shape stays
 * compact.
 */
public final class SyntheticColumns {

    /**
     * Names of every reader-synthesized internal channel. The producer pipeline injects these,
     * and rendering code (e.g. {@code SynthesizeExternalSource}) excludes them from the rendered
     * {@code _source} object. User data columns whose names happen to start with {@code _}
     * (e.g. Spark's {@code _corrupt_record}, a user-supplied {@code _status}) are real data — a
     * leading underscore on its own is not the filter; membership in this set is.
     */
    public static final Set<String> NAMES = Set.of(ColumnExtractor.ROW_POSITION_COLUMN);

    private SyntheticColumns() {}

    /** Whether {@code name} is a reader-synthesized internal channel. */
    public static boolean isSynthetic(String name) {
        return NAMES.contains(name);
    }

    /** Whether {@code projectedColumns} contains any reader-synthesized channel. */
    public static boolean containsAnySynthetic(List<String> projectedColumns) {
        if (projectedColumns == null) {
            return false;
        }
        for (String name : projectedColumns) {
            if (NAMES.contains(name)) {
                return true;
            }
        }
        return false;
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
     * Reader-side attribute shape for the synthetic {@link ColumnExtractor#ROW_POSITION_COLUMN}
     * slot: a {@link ReferenceAttribute} of {@link DataType#LONG}, non-nullable. Used by format
     * readers when they synthesize the projected attribute for a slot that has no source column
     * behind it.
     */
    public static Attribute newRowPositionAttribute() {
        return new ReferenceAttribute(
            Source.EMPTY,
            null,
            ColumnExtractor.ROW_POSITION_COLUMN,
            DataType.LONG,
            Nullability.FALSE,
            null,
            false
        );
    }

    /**
     * Optimizer-side attribute shape for the synthetic {@link ColumnExtractor#ROW_POSITION_COLUMN}
     * slot: a {@link MetadataAttribute} of {@link DataType#LONG}, non-nullable, marked
     * synthetic. Paired with {@link #newRowPositionAttribute()} so type and nullability stay in
     * lock-step between the optimizer injection site and the reader synthesis site.
     */
    public static MetadataAttribute newRowPositionMetadataAttribute(Source source) {
        return new MetadataAttribute(source, ColumnExtractor.ROW_POSITION_COLUMN, DataType.LONG, Nullability.FALSE, null, true, false);
    }
}
