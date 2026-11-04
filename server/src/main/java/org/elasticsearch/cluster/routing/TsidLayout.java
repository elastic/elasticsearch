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
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;

import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * The per-index inputs that select how {@link TsidBuilder#buildTsid(TsidLayout)} lays out a tsid: the index creation version, which
 * gates every layout change, and the {@link IndexSettings#TIME_SERIES_TSID_PREFIXED_DIMENSIONS} setting.
 * <p>
 * Resolve it once per index with {@link #forIndex(IndexMetadata)} and build every tsid of that index with it, on the coordinating
 * node and on the data node alike: the tsid is both the shard routing key and the primary index sort key, so two layouts for one
 * index would scatter one time series over several shards. Callers that build tsids before the index exists resolve the layout the
 * index will get from its template with {@link #forNewIndex(ProjectMetadata, String)}.
 */
public final class TsidLayout {

    private final IndexVersion indexVersion;
    private final List<String> prefixedDimensions;
    @Nullable
    private final Predicate<String> prefixedDimensionMatcher;

    /**
     * @param indexVersion       the index creation version
     * @param prefixedDimensions the raw value of {@link IndexSettings#TIME_SERIES_TSID_PREFIXED_DIMENSIONS}; empty disables the
     *                           prefixed dimensions hash
     */
    public TsidLayout(IndexVersion indexVersion, List<String> prefixedDimensions) {
        this.indexVersion = Objects.requireNonNull(indexVersion, "indexVersion");
        this.prefixedDimensions = List.copyOf(Objects.requireNonNull(prefixedDimensions, "prefixedDimensions"));
        this.prefixedDimensionMatcher = TsidBuilder.usePrefixedDimensionsHash(indexVersion) && this.prefixedDimensions.isEmpty() == false
            ? Regex.simpleMatcher(this.prefixedDimensions.toArray(String[]::new))
            : null;
    }

    /** The layout of the given index. */
    public static TsidLayout forIndex(IndexMetadata metadata) {
        return new TsidLayout(
            metadata.getCreationVersion(),
            IndexSettings.TIME_SERIES_TSID_PREFIXED_DIMENSIONS.get(metadata.getSettings())
        );
    }

    /**
     * The layout of an index created at {@code indexVersion} with default settings, that is without prefixed dimensions. Meant for
     * tests; production callers must use {@link #forIndex} or {@link #forNewIndex}.
     */
    public static TsidLayout forVersion(IndexVersion indexVersion) {
        return new TsidLayout(indexVersion, IndexSettings.TIME_SERIES_TSID_PREFIXED_DIMENSIONS.getDefault(Settings.EMPTY));
    }

    /**
     * The layout of the index that indexing into {@code indexName} would create: the current version, and the
     * {@link IndexSettings#TIME_SERIES_TSID_PREFIXED_DIMENSIONS} setting of the index template that matches the name, if any. Meant
     * for callers that build tsids for an index or data stream that does not exist yet; callers that have the
     * {@link IndexMetadata} must use {@link #forIndex} instead.
     */
    public static TsidLayout forNewIndex(ProjectMetadata projectMetadata, String indexName) {
        String templateName = MetadataIndexTemplateService.findV2Template(projectMetadata, indexName, false);
        if (templateName == null) {
            return forVersion(IndexVersion.current());
        }
        Settings settings = MetadataIndexTemplateService.resolveSettings(projectMetadata, templateName);
        return new TsidLayout(IndexVersion.current(), IndexSettings.TIME_SERIES_TSID_PREFIXED_DIMENSIONS.get(settings));
    }

    public IndexVersion indexVersion() {
        return indexVersion;
    }

    /** The raw value of {@link IndexSettings#TIME_SERIES_TSID_PREFIXED_DIMENSIONS}. */
    public List<String> prefixedDimensions() {
        return prefixedDimensions;
    }

    /** Whether the tsid uses the 16-byte layout with a single similarity byte prefix, see {@link TsidBuilder#useSingleBytePrefixLayout}. */
    public boolean singleBytePrefix() {
        return TsidBuilder.useSingleBytePrefixLayout(indexVersion);
    }

    /**
     * Matches the paths of the dimensions that are hashed into the second and third bytes of the tsid, or {@code null} when those
     * bytes come from the hash over all dimensions: for indices created before {@link IndexVersions#TSID_PREFIXED_DIMENSIONS_HASH}
     * and for an empty {@link #prefixedDimensions()}.
     */
    @Nullable
    public Predicate<String> prefixedDimensionMatcher() {
        return prefixedDimensionMatcher;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof TsidLayout other
            && indexVersion.equals(other.indexVersion)
            && prefixedDimensions.equals(other.prefixedDimensions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexVersion, prefixedDimensions);
    }

    @Override
    public String toString() {
        return "TsidLayout[indexVersion=" + indexVersion + ", prefixedDimensions=" + prefixedDimensions + "]";
    }
}
