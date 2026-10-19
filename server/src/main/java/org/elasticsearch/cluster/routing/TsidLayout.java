/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;

import java.util.Objects;

/**
 * The per-index inputs that select how {@link TsidBuilder#buildTsid(TsidLayout)} lays out a tsid: the index creation version, which
 * gates every layout change, and the {@link IndexSettings#TIME_SERIES_TSID_DIMENSIONS_PREFIX} setting.
 * <p>
 * Resolve it once per index with {@link #forIndex(IndexMetadata)} and build every tsid of that index with it, on the coordinating
 * node and on the data node alike: the tsid is both the shard routing key and the primary index sort key, so two layouts for one
 * index would scatter one time series over several shards.
 *
 * @param indexVersion     the index creation version
 * @param dimensionsPrefix the raw value of {@link IndexSettings#TIME_SERIES_TSID_DIMENSIONS_PREFIX}; empty disables the prefixed
 *                         dimensions hash
 */
public record TsidLayout(IndexVersion indexVersion, String dimensionsPrefix) {

    public TsidLayout {
        Objects.requireNonNull(indexVersion, "indexVersion");
        Objects.requireNonNull(dimensionsPrefix, "dimensionsPrefix");
    }

    /** The layout of the given index. */
    public static TsidLayout forIndex(IndexMetadata metadata) {
        return new TsidLayout(metadata.getCreationVersion(), IndexSettings.TIME_SERIES_TSID_DIMENSIONS_PREFIX.get(metadata.getSettings()));
    }

    /**
     * The layout of an index created at {@code indexVersion} with default settings. Meant for tests and for callers that have no
     * index to resolve the settings from yet; callers that have the {@link IndexMetadata} must use {@link #forIndex} instead.
     */
    public static TsidLayout forVersion(IndexVersion indexVersion) {
        return new TsidLayout(indexVersion, IndexSettings.TIME_SERIES_TSID_DIMENSIONS_PREFIX.getDefault(Settings.EMPTY));
    }

    /** Whether the tsid uses the 16-byte layout with a single similarity byte prefix, see {@link TsidBuilder#useSingleBytePrefixLayout}. */
    public boolean singleBytePrefix() {
        return TsidBuilder.useSingleBytePrefixLayout(indexVersion);
    }

    /**
     * The path prefix of the dimensions that are hashed into the second byte of the tsid, or {@code null} when that
     * byte comes from the hash over all dimensions: for indices created before {@link IndexVersions#TSID_PREFIXED_DIMENSIONS_HASH}
     * and for an empty {@link #dimensionsPrefix()}.
     */
    @Nullable
    public String effectiveDimensionsPrefix() {
        return TsidBuilder.usePrefixedDimensionsHash(indexVersion) && dimensionsPrefix.isEmpty() == false ? dimensionsPrefix : null;
    }
}
