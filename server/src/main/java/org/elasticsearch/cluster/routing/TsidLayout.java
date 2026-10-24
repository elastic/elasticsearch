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
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;

/**
 * The byte layout of the tsids of one index, resolved once from the index metadata so that the coordinating node, the data node and
 * the OTLP ingest path all build the same bytes for the same dimensions.
 *
 * @param creationVersion the index creation version, which selects between the legacy multi-byte prefix and the single prefix byte
 * @param dimensionGroupSort whether the index sorts on {@code _metric_name_hash} and {@code _dimension_group} ahead of the tsid (see
 *                           {@link IndexSettings#TIME_SERIES_DIMENSION_GROUP_FIELD}). The metric similarity that the prefix byte
 *                           used to provide is then carried by {@code _metric_name_hash}, so the tsid is the plain full hash.
 */
public record TsidLayout(IndexVersion creationVersion, boolean dimensionGroupSort) {

    public enum Kind {
        /** A name similarity byte, up to four value similarity bytes, then the full hash. */
        MULTI_BYTE_PREFIX,
        /** The full hash, with its first byte replaced by the metric similarity byte. */
        SINGLE_BYTE_PREFIX,
        /** The full hash only. */
        NO_PREFIX
    }

    public static TsidLayout forIndex(IndexMetadata metadata) {
        return forIndex(metadata.getCreationVersion(), metadata.getSettings());
    }

    /**
     * @param indexSettings the index settings; see {@link IndexSettings#timeSeriesDimensionGroupSortEnabled(IndexVersion, Settings)}
     *                      for what is read, the mode and creation version are not required to be present
     */
    public static TsidLayout forIndex(IndexVersion creationVersion, Settings indexSettings) {
        return new TsidLayout(creationVersion, IndexSettings.timeSeriesDimensionGroupSortEnabled(creationVersion, indexSettings));
    }

    /** The layout of an index created at {@code creationVersion} with default settings. */
    public static TsidLayout forVersion(IndexVersion creationVersion) {
        return forIndex(creationVersion, Settings.EMPTY);
    }

    public Kind kind() {
        if (TsidBuilder.useSingleBytePrefixLayout(creationVersion) == false) {
            return Kind.MULTI_BYTE_PREFIX;
        }
        return dimensionGroupSort ? Kind.NO_PREFIX : Kind.SINGLE_BYTE_PREFIX;
    }
}
