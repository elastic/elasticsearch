/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.startree;

/**
 * Constants for star-tree codec format.
 */
public final class StarTreeConstants {

    private StarTreeConstants() {}

    /** Name of the codec */
    public static final String CODEC_NAME = "ESStarTree";

    /** Extension for star-tree metadata file */
    public static final String META_EXTENSION = "star";

    /** Extension for star-tree values file (pre-aggregated metrics) */
    public static final String VALUES_EXTENSION = "stv";

    /** Extension for star-tree dimension ordinals file */
    public static final String DIMS_EXTENSION = "std";

    /** Extension for star-tree index file (skip list for random access) */
    public static final String INDEX_EXTENSION = "sti";

    /** Codec name for metadata */
    public static final String META_CODEC = "ESStarTreeMeta";

    /** Codec name for values */
    public static final String VALUES_CODEC = "ESStarTreeValues";

    /** Codec name for dimension ordinals */
    public static final String DIMS_CODEC = "ESStarTreeDims";

    /** Codec name for skip index */
    public static final String INDEX_CODEC = "ESStarTreeIndex";

    /** Initial version */
    public static final int VERSION_START = 0;

    /** Current version */
    public static final int VERSION_CURRENT = VERSION_START;

    /** Magic value to identify a star node (aggregates across all dimension values) */
    public static final long STAR_NODE_ORDINAL = -1L;

    /** Magic value for NULL/missing dimension values */
    public static final long NULL_DIMENSION_ORDINAL = -2L;

    /** Magic value indicating no more children */
    public static final int NO_MORE_CHILDREN = -1;

    /** Maximum depth of the star-tree (equal to number of dimensions) */
    public static final int MAX_TREE_DEPTH = 32;

    /** Node type markers */
    public static final byte NODE_TYPE_LEAF = 0;
    public static final byte NODE_TYPE_INTERNAL = 1;
    public static final byte NODE_TYPE_STAR = 2;
}
