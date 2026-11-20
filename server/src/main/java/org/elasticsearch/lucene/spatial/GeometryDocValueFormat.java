/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.spatial;

import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;

/**
 * The binary layout of geometry doc-values. The bytes don't record which layout they use, so every reader must be
 * told: persisted doc-values use the layout of the index version that created the index, which is the same for every
 * document in it, and transient encodings (query shapes, runtime fields, the aggregation {@code missing} value) are
 * written and read as {@link #LEGACY}.
 */
public enum GeometryDocValueFormat {
    /** Triangle tree with inline coordinate deltas. Cannot reconstruct the original geometry, except for points. */
    LEGACY,
    /** Vertex lookup table, a triangle tree of vertex ordinals and connectivity, which can reconstruct the original geometry. */
    V2;

    /** The layout of the doc-values written by a shape field mapper in an index created on the given version. */
    public static GeometryDocValueFormat forIndexVersion(IndexVersion indexCreatedVersion) {
        return indexCreatedVersion.onOrAfter(IndexVersions.GEOMETRY_DOC_VALUES_V2) ? V2 : LEGACY;
    }
}
