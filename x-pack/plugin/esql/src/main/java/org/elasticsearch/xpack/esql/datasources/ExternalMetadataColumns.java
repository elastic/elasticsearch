/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Registry of the standard ES index metadata names ({@code _id}, {@code _index},
 * {@code _version}, ...) that the external-source pipeline knows how to materialise on
 * external datasets. The data types are sourced from
 * {@link MetadataAttribute#ATTRIBUTES_MAP} so the binding here and in the analyzer
 * always agree (including snapshot-only entries such as {@code _data_tier}).
 * <p>
 * Sibling to {@link FileMetadataColumns}, which covers the {@code _file.*} virtual
 * columns. The two registries are kept separate because they have different lifecycles:
 * the {@code _file.*} names are always materialised by
 * {@link VirtualColumnIterator} (always-enrich), whereas the standard metadata names
 * here are only injected when the user requests them via the {@code METADATA} clause.
 * <p>
 * Two members of {@link MetadataAttribute#ATTRIBUTES_MAP} are deliberately excluded
 * from {@link #PER_FILE_CONSTANT_NAMES} because they cannot be produced by a per-file constant: {@code _id}
 * requires a per-row {@code <location>:<rowPosition>} composition (see
 * {@code ExternalRowIdentity}) and {@code _source} requires a per-row JSON synthesis
 * from the row's data columns (see {@code SynthesizeExternalSource}). They are still
 * bound to {@link ExternalMetadataColumns} at analysis time via
 * {@link #isStandardMetadataColumn(String)} so the analyzer doesn't reject them; the
 * runtime takes a different code path for each.
 */
public final class ExternalMetadataColumns {

    public static final String ID = "_id";
    public static final String INDEX = "_index";
    public static final String VERSION = "_version";
    public static final String SCORE = "_score";
    public static final String SOURCE = "_source";
    public static final String IGNORED = "_ignored";
    public static final String INDEX_MODE = "_index_mode";
    public static final String TSID = "_tsid";
    public static final String SIZE = "_size";
    public static final String DATA_TIER = "_data_tier";

    /**
     * Names of standard metadata columns that are materialised by the producer-side
     * constant-block path (per-file values, including SQL {@code NULL} where unavailable).
     * Excludes {@link #ID} and {@link #SOURCE}: those require per-row composition and are
     * produced by separate operator steps.
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
        // _data_tier is snapshot-only in MetadataAttribute.ATTRIBUTES_MAP; gate matches.
        if (MetadataAttribute.isSupported(DATA_TIER)) {
            names.add(DATA_TIER);
        }
        PER_FILE_CONSTANT_NAMES = Collections.unmodifiableSet(names);
    }

    private ExternalMetadataColumns() {}

    /**
     * Whether {@code name} is a standard ES metadata column the analyzer should bind on
     * external datasets. Delegates to {@link MetadataAttribute#isSupported(String)} so the
     * answer always matches the registry the analyzer uses for ES-backed sources.
     */
    public static boolean isStandardMetadataColumn(String name) {
        return MetadataAttribute.isSupported(name);
    }

    /**
     * The {@link DataType} the analyzer must bind for {@code name}, or {@code null} if the
     * name is not a standard metadata column. Sourced from
     * {@link MetadataAttribute#ATTRIBUTES_MAP} so the type the analyzer assigns matches
     * what ES-backed sources produce.
     */
    public static DataType dataType(String name) {
        return MetadataAttribute.dataType(name);
    }
}
