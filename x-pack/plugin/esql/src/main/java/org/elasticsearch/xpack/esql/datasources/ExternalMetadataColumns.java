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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Registry of the standard ES index metadata names ({@code _index}, {@code _score},
 * {@code _ignored}, ...) that the external-source pipeline knows how to materialise on
 * external datasets. The data types are sourced from
 * {@link MetadataAttribute#ATTRIBUTES_MAP} so the binding here and in the analyzer
 * always agree (including snapshot-only entries such as {@code _tier}).
 * <p>
 * Sibling to {@link FileMetadataColumns} ({@code _file.*}). Both families are request-driven
 * (the user names them in {@code METADATA}) and materialized by {@link VirtualColumnIterator}
 * on the producer thread; the split exists because {@code _file.*} comes from per-file stat
 * while the standard names route through {@link MetadataAttribute#ATTRIBUTES_MAP}.
 * <p>
 * Every standard name a dataset can bind is a per-file constant; see
 * {@link #PER_FILE_CONSTANT_NAMES}. {@code _id}, {@code _version} and {@code _source} are not
 * among them: a file holds no such fact, so a dataset does not answer them at all. They stay in
 * {@link #RESERVED_NAMES} so a dataset layout cannot claim the names.
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
    public static final String SLICE = SliceIndexing.FIELD_NAME;

    /**
     * Names of standard metadata columns that are materialised by the producer-side
     * constant-block path (per-file values, including SQL {@code NULL} where unavailable).
     * {@link #ID}, {@link #VERSION} and {@link #SOURCE} are not in this set and are not bindable
     * on a dataset at all — a file carries no document identity, no document version and no
     * stored source.
     */
    public static final Set<String> PER_FILE_CONSTANT_NAMES;

    static {
        // Preserve a deterministic iteration order matching the natural projection order so any
        // diagnostic / explain output is stable across runs.
        var names = new LinkedHashSet<String>();
        names.add(INDEX);
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
     * Every standard metadata name an external relation can bind. {@code Analyzer.bindMetadataFields}
     * consults this set, so a standard name outside it resolves the way an unknown name does.
     * It currently has the same membership as {@link #PER_FILE_CONSTANT_NAMES} — every name a
     * dataset can answer happens to be a per-file constant — but the two say different things and
     * a name backed by a real per-row source would join this set without joining that one.
     * For namespace protection use {@link #RESERVED_NAMES}, which is wider.
     */
    public static final Set<String> STANDARD_NAMES;

    static {
        STANDARD_NAMES = Collections.unmodifiableSet(new LinkedHashSet<>(PER_FILE_CONSTANT_NAMES));
    }

    /**
     * Whether {@code name} is a standard metadata name that a dataset cannot answer: registered in
     * {@link MetadataAttribute#ATTRIBUTES_MAP}, and so parsed into a RESOLVED attribute, but outside
     * {@link #STANDARD_NAMES}. The analyzer has to refuse such a name explicitly — forwarding a resolved
     * attribute onto the unresolved list drops it in silence.
     * <p>
     * Derived from the registry rather than listed, so a name added to {@code ATTRIBUTES_MAP} tomorrow
     * without a dataset-side value is refused loudly instead of disappearing. Today it answers true for
     * exactly {@code _id}, {@code _version} and {@code _source} — a file holds no document identity, no
     * document version and no stored source. It answers false for {@code _doc}, which is not in the
     * registry at all: {@code InfoCommandPlanUtils} injects that one directly for TS_INFO / METRICS_INFO,
     * and it keeps its existing pass-through.
     */
    public static boolean isRegisteredButUnbindable(String name) {
        return MetadataAttribute.isSupported(name) && STANDARD_NAMES.contains(name) == false;
    }

    /**
     * The dedicated metadata namespace for reservation/rename purposes: {@link #STANDARD_NAMES}
     * plus every standard name that is not bindable on a dataset. Reservation is wider than
     * binding on purpose and must not flip with build mode or flag state — a dataset layout
     * claiming {@code _tier} is renamed to {@code _partition._tier} in EVERY build, even where
     * {@code METADATA _tier} itself is not yet exposed, so a Hive dataset surfaces the same column
     * names either way. {@code _id}, {@code _version} and {@code _source} are here for the same
     * reason: a dataset cannot answer them, but a layout must not be able to claim the names.
     * Use this set for namespace protection; use {@link #STANDARD_NAMES} for what a relation may
     * actually bind.
     */
    public static final Set<String> RESERVED_NAMES;

    static {
        var names = new LinkedHashSet<>(STANDARD_NAMES);
        names.add(DataTierFieldMapper.NAME); // unconditional: reservation is build-mode-independent
        names.add(SLICE); // unconditional: reservation is flag-state-independent
        // Not bindable on a dataset, but reserved so a layout cannot claim the name.
        names.add(ID);
        names.add(VERSION);
        names.add(SOURCE);
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
     *     <li>Every other name in the set — {@code null}. They are not addressable on external
     *         data (no relevance scoring, no per-row {@code _ignored} list, etc.).</li>
     * </ul>
     * The values depend only on the dataset name; nothing here is derived from the file. The
     * result is meant to overlay onto the partition-value map so {@link VirtualColumnIterator}
     * renders constant blocks of the correct type ({@link DataType}) — null values are turned into
     * {@code newConstantNullBlock} by the iterator's existing path.
     */
    public static Map<String, Object> extractPerFileConstants(@Nullable String datasetName) {
        var values = new LinkedHashMap<String, Object>(PER_FILE_CONSTANT_NAMES.size());
        for (String name : PER_FILE_CONSTANT_NAMES) {
            values.put(name, perFileValue(name, datasetName));
        }
        return Collections.unmodifiableMap(values);
    }

    private static Object perFileValue(String name, @Nullable String datasetName) {
        return switch (name) {
            case INDEX -> datasetName != null ? new BytesRef(datasetName) : null;
            case SCORE, IGNORED, INDEX_MODE, TSID, SIZE, DataTierFieldMapper.NAME, SLICE -> null;
            default -> throw new AssertionError("Unhandled per-file constant name: " + name);
        };
    }
}
