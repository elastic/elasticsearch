/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.xpack.cluster.routing.allocation.mapper.DataTierFieldMapper;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Registry of the standard ES index metadata names ({@code _id}, {@code _index},
 * {@code _version}, ...) that the external-source pipeline knows how to materialise on
 * external datasets. The data types are sourced from
 * {@link MetadataAttribute#ATTRIBUTES_MAP} so the binding here and in the analyzer
 * always agree (including snapshot-only entries such as {@code _tier}).
 * <p>
 * Sibling to {@link FileMetadataColumns} ({@code _file.*}). Both families are request-driven
 * (the user names them in {@code METADATA}) and materialized by {@link VirtualColumnIterator}
 * on the producer thread; the split exists because {@code _file.*} comes from per-file stat
 * while the standard names route through {@link MetadataAttribute#ATTRIBUTES_MAP}.
 * <p>
 * Two materialisation lanes exist: see {@link #PER_FILE_CONSTANT_NAMES} for the canonical
 * per-file-constant set; the remaining standard names ({@code _id} via
 * {@link ExternalRowIdentity}, {@code _source} via {@link SynthesizeExternalSource}) are
 * per-row composed.
 */
public final class ExternalMetadataColumns {

    // Aliased from MetadataAttribute where it exports the name, so this registry cannot drift
    // from the analyzer's binding source. The remainder have no exported constant there.
    public static final String ID = "_id";
    public static final String INDEX = MetadataAttribute.INDEX;
    public static final String VERSION = "_version";
    public static final String SCORE = MetadataAttribute.SCORE;
    public static final String SOURCE = "_source";
    public static final String IGNORED = "_ignored";
    public static final String INDEX_MODE = "_index_mode";
    public static final String TSID = MetadataAttribute.TSID_FIELD;
    public static final String SIZE = MetadataAttribute.SIZE;
    public static final String SLICE = SliceIndexing.PARAM_NAME;

    /**
     * Names of standard metadata columns that are materialised by the producer-side
     * constant-block path (per-file values, including SQL {@code NULL} where unavailable).
     * The other standard names ({@link #ID}, {@link #SOURCE}) are not in this set — they go
     * through per-row composition operators ({@link ExternalRowIdentity},
     * {@link SynthesizeExternalSource}).
     */
    public static final Set<String> PER_FILE_CONSTANT_NAMES;

    static {
        // Preserve a deterministic iteration order matching the natural projection order so any
        // diagnostic / explain output is stable across runs.
        var names = new LinkedHashSet<String>();
        names.add(INDEX);
        names.add(VERSION);
        names.add(SCORE);
        names.add(IGNORED);
        names.add(INDEX_MODE);
        names.add(TSID);
        names.add(SIZE);
        // _tier is snapshot-only in MetadataAttribute.ATTRIBUTES_MAP; gate matches.
        if (EsqlCapabilities.Cap.METADATA_TIER_FIELD.isEnabled()) {
            names.add(DataTierFieldMapper.NAME);
        }
        // _slice is backed by _routing doc values on slice-enabled indices; not available on external datasets.
        if (EsqlCapabilities.Cap.METADATA_SLICE.isEnabled()) {
            names.add(SLICE);
        }
        PER_FILE_CONSTANT_NAMES = Collections.unmodifiableSet(names);
    }

    /**
     * Every standard metadata name an external relation can bind — {@link #PER_FILE_CONSTANT_NAMES}
     * plus the per-row composed pair ({@link #ID}, {@link #SOURCE}). This is the dedicated
     * namespace: dataset layouts (Hive partition directories, data columns) cannot claim these
     * names; see {@code HivePartitionDetector} for the rename that enforces it.
     */
    public static final Set<String> STANDARD_NAMES;

    static {
        var names = new LinkedHashSet<>(PER_FILE_CONSTANT_NAMES);
        names.add(ID);
        names.add(SOURCE);
        STANDARD_NAMES = Collections.unmodifiableSet(names);
    }

    /**
     * The dedicated metadata namespace for reservation/rename purposes: {@link #STANDARD_NAMES}
     * plus every standard name that is only snapshot-gated for binding ({@code _tier}). Reservation
     * must not flip with build mode — a dataset layout claiming {@code _tier} is renamed to
     * {@code _partition._tier} in EVERY build, even where {@code METADATA _tier} itself is not yet
     * exposed — so a Hive dataset surfaces the same column names regardless of snapshot vs release.
     * Use this set for namespace protection; use {@link #STANDARD_NAMES} for what a relation may
     * actually bind.
     */
    public static final Set<String> RESERVED_NAMES;

    static {
        var names = new LinkedHashSet<>(STANDARD_NAMES);
        names.add(DataTierFieldMapper.NAME); // unconditional: reservation is build-mode-independent
        names.add(SLICE); // unconditional: reservation is flag-state-independent
        RESERVED_NAMES = Collections.unmodifiableSet(names);
    }

    private ExternalMetadataColumns() {}

    /**
     * Build the per-file constant values for the standard metadata names listed in
     * {@link #PER_FILE_CONSTANT_NAMES}. The map is suitable for merging into a partition-value map
     * consumed by {@link VirtualColumnIterator}. Values are:
     * <ul>
     *     <li>{@code _index} — {@code datasetName} when known, otherwise {@code null}
     *         (bare-glob {@code FROM} queries have no dataset identity).</li>
     *     <li>{@code _version} — {@link FileList#lastModifiedMillis(int)} as a {@code Long};
     *         {@code 0L} is treated as "unknown" and yields {@code null} per the precedent set
     *         by {@link FileMetadataColumns#extractValues(FileList, int)}.</li>
     *     <li>Every other standard name — {@code null}. They are not addressable on external
     *         data (no relevance scoring, no per-row {@code _ignored} list, etc.).</li>
     * </ul>
     * Callers must call this once per file; the result is meant to overlay onto the
     * partition-value map so {@link VirtualColumnIterator} renders constant blocks of the
     * correct type ({@link DataType}) — null values are turned into
     * {@code newConstantNullBlock} by the iterator's existing path.
     */
    public static Map<String, Object> extractPerFileConstants(@Nullable String datasetName, FileList fileList, int index) {
        long modifiedMillis = fileList.lastModifiedMillis(index);
        Long version = modifiedMillis == 0L ? null : Long.valueOf(modifiedMillis);
        return buildPerFileConstants(datasetName, version);
    }

    /**
     * Variant for callers that already hold the file's last-modified epoch-millis (e.g. the
     * slice-queue path, which reuses {@link FileMetadataColumns#MODIFIED} previously stuffed
     * into the {@code FileSplit}'s partition values). A {@code null} {@code lastModifiedMillis}
     * yields a {@code null} {@code _version}; zero is treated as "unknown" per
     * {@link FileMetadataColumns#extractValues(FileList, int)} precedent.
     */
    public static Map<String, Object> extractPerFileConstants(@Nullable String datasetName, @Nullable Long lastModifiedMillis) {
        Long version = lastModifiedMillis == null || lastModifiedMillis == 0L ? null : lastModifiedMillis;
        return buildPerFileConstants(datasetName, version);
    }

    private static Map<String, Object> buildPerFileConstants(@Nullable String datasetName, @Nullable Long version) {
        var values = new LinkedHashMap<String, Object>(PER_FILE_CONSTANT_NAMES.size());
        for (String name : PER_FILE_CONSTANT_NAMES) {
            values.put(name, perFileValue(name, datasetName, version));
        }
        return Collections.unmodifiableMap(values);
    }

    private static Object perFileValue(String name, @Nullable String datasetName, @Nullable Long version) {
        return switch (name) {
            case INDEX -> datasetName != null ? new BytesRef(datasetName) : null;
            case VERSION -> version;
            case SCORE, IGNORED, INDEX_MODE, TSID, SIZE, DataTierFieldMapper.NAME, SLICE -> null;
            default -> throw new AssertionError("Unhandled per-file constant name: " + name);
        };
    }
}
