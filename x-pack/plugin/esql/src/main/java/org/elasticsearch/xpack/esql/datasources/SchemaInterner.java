/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.action.ExternalPlanningReservation;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.HeapEstimates;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Shares file-schema objects inside one path resolve.
 * <p>
 * {@code toAttributes()} mints a fresh {@link Attribute} per column per file; this interner keeps one instance
 * of each distinct column, shape, mapping, and file schema, and charges the breaker only for the overflow above
 * the listing credit ({@code SCHEMA_MAP_BYTES_PER_FILE} per file), which is already held until query close.
 * A later pin, shadowed mapping, or overlay does not refund what it replaced. Nothing here is written
 * back into the schema cache, and nothing is shared across queries.
 * <p>
 * The metadata fan-out invokes {@link #canonicalize} from up to {@code metadataReadConcurrency} callbacks at once,
 * so every method takes the instance lock. Call {@link #setAllowance} before the first canonicalize when the file
 * count is not known at construction.
 */
public final class SchemaInterner {

    /** First time a shape list is kept: list shell plus one pointer per column. */
    private static final long SHAPE_BASE_BYTES = 32L;
    private static final long SHAPE_PER_COLUMN_BYTES = 8L;

    /** First time a mapping is kept: shell plus the index array, and the cast array when present. */
    private static final long MAPPING_BASE_BYTES = 24L;
    private static final long MAPPING_INDEX_SLOT_BYTES = 4L;
    private static final long MAPPING_CAST_SLOT_BYTES = 8L;

    /** First time an {@link ExternalSchema} is kept for a shape list. */
    private static final long SCHEMA_BYTES = 64L;

    @Nullable
    private final ExternalPlanningReservation reservation;
    private long allowance;
    private long retained;
    private long extraCharged;

    private final Map<ColumnKey, Attribute> columns = new HashMap<>();
    private final Map<List<Attribute>, List<Attribute>> shapes = new HashMap<>();
    private final Map<ColumnMapping, ColumnMapping> mappings = new HashMap<>();
    private final Map<List<Attribute>, ExternalSchema> schemas = new HashMap<>();

    /**
     * @param reservation query reservation, or {@code null} to share without charging (unit tests, and resolvers
     *                    that have no planning reservation)
     * @param allowance   bytes already credited for this resolve, typically
     *                    {@code fileCount * ExternalSourceResolver.SCHEMA_MAP_BYTES_PER_FILE}
     */
    public SchemaInterner(@Nullable ExternalPlanningReservation reservation, long allowance) {
        this.reservation = reservation;
        this.allowance = allowance;
    }

    /**
     * Sets the credited allowance once the listing's file count is known. Must run before any
     * {@link #canonicalize} on this instance; the listing credit is not measured schema size.
     */
    public synchronized void setAllowance(long allowance) {
        this.allowance = allowance;
    }

    /**
     * Credits {@code allowance} when this resolve has not already set one. A resolve that never took the
     * per-file listing credit passes {@link Long#MAX_VALUE}, so sharing does not open a queryHeld charge.
     */
    public synchronized void ensureAllowance(long allowance) {
        if (this.allowance == 0L) {
            this.allowance = allowance;
        }
    }

    /**
     * Bytes of one file's private attribute list: {@link HeapEstimates#columnBytes} per column, which includes the
     * column name, plus the list shell. Charged on a resolve-scoped run for the gather, not on {@code queryHeld}.
     * Not a measured deep size.
     *
     * @param namesShared the column name strings belong to a schema cache entry, which weighs them against the cache
     *                    budget, so only the attribute shells are charged here
     */
    static long privateListBytes(List<Attribute> columns, boolean namesShared) {
        long bytes = SHAPE_BASE_BYTES + SHAPE_PER_COLUMN_BYTES * columns.size();
        for (int i = 0; i < columns.size(); i++) {
            bytes += columnBytes(columns.get(i), namesShared);
        }
        return bytes;
    }

    private static long columnBytes(Attribute column, boolean nameShared) {
        return nameShared ? HeapEstimates.columnShellBytes() : HeapEstimates.columnBytes(column.name().length());
    }

    /**
     * {@link #canonicalize(List, boolean)} for a list whose column names are not owned by a schema cache entry.
     */
    public List<Attribute> canonicalize(List<Attribute> raw) {
        return canonicalize(raw, false);
    }

    /**
     * Returns the canonical attribute list for {@code raw}: each column is the first instance kept for its key
     * (name, type, nullability, synthetic — not {@code NameId}), and the sequence itself is the first list kept
     * for that series of instances.
     *
     * @param namesShared the column name strings of {@code raw} belong to a schema cache entry, which weighs them
     *                    against the cache budget, so a column kept from {@code raw} is charged its shell only
     */
    public synchronized List<Attribute> canonicalize(List<Attribute> raw, boolean namesShared) {
        List<Attribute> existing = shapes.get(raw);
        if (existing != null) {
            return existing;
        }
        List<Attribute> interned = new ArrayList<>(raw.size());
        for (int i = 0; i < raw.size(); i++) {
            interned.add(internColumn(raw.get(i), namesShared));
        }
        List<Attribute> candidate = List.copyOf(interned);
        existing = shapes.get(candidate);
        if (existing != null) {
            return existing;
        }
        retain(SHAPE_BASE_BYTES + SHAPE_PER_COLUMN_BYTES * candidate.size());
        shapes.put(candidate, candidate);
        return candidate;
    }

    /**
     * Returns the first {@link ColumnMapping} equal to {@code mapping}.
     */
    public synchronized ColumnMapping intern(ColumnMapping mapping) {
        ColumnMapping existing = mappings.get(mapping);
        if (existing != null) {
            return existing;
        }
        retain(mappingBytes(mapping));
        mappings.put(mapping, mapping);
        return mapping;
    }

    /**
     * Returns the one {@link ExternalSchema} for {@code canonicalList}. {@code canonicalList} must be a list
     * already returned by {@link #canonicalize}.
     */
    public synchronized ExternalSchema intern(List<Attribute> canonicalList) {
        ExternalSchema existing = schemas.get(canonicalList);
        if (existing != null) {
            return existing;
        }
        ExternalSchema candidate = new ExternalSchema(canonicalList);
        retain(SCHEMA_BYTES);
        schemas.put(canonicalList, candidate);
        return candidate;
    }

    private Attribute internColumn(Attribute raw, boolean nameShared) {
        ColumnKey key = new ColumnKey(raw.name(), raw.dataType(), raw.nullable(), raw.synthetic());
        Attribute existing = columns.get(key);
        if (existing != null) {
            return existing;
        }
        retain(columnBytes(raw, nameShared));
        columns.put(key, raw);
        return raw;
    }

    private static long mappingBytes(ColumnMapping mapping) {
        long bytes = MAPPING_BASE_BYTES + MAPPING_INDEX_SLOT_BYTES * mapping.width();
        int castLength = mapping.castArrayLength();
        if (castLength > 0) {
            bytes += MAPPING_CAST_SLOT_BYTES * castLength;
        }
        return bytes;
    }

    /**
     * Charges the overflow of {@code cost} above {@link #allowance} against the running retained total, then
     * records the cost. {@code chargeQuery} runs before the caller publishes, and a throw leaves both the maps
     * and this total unchanged. Replaced objects stay in the total until query close.
     */
    private void retain(long cost) {
        // No listing credit was taken. Overflow stays 0; do not subtract from Long.MAX_VALUE.
        if (allowance == Long.MAX_VALUE) {
            retained += cost;
            return;
        }
        long charge = Math.max(0L, retained + cost - allowance) - extraCharged;
        if (reservation != null && charge > 0) {
            reservation.chargeQuery(charge);
        }
        extraCharged += charge;
        retained += cost;
    }

    private record ColumnKey(String name, DataType type, Nullability nullability, boolean synthetic) {}
}
