/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.block;

import java.util.EnumSet;

/**
 * Defines the different levels at which cluster blocks can restrict operations.
 * Each level represents a category of operations that can be independently blocked.
 *
 * <p><b>Block Levels:</b></p>
 * <ul>
 *   <li><b>READ</b> - Blocks read operations (searches, get requests)</li>
 *   <li><b>WRITE</b> - Blocks write operations (indexing, updates, deletes)</li>
 *   <li><b>METADATA_READ</b> - Blocks metadata read operations</li>
 *   <li><b>METADATA_WRITE</b> - Blocks metadata write operations (mappings, settings)</li>
 *   <li><b>REFRESH</b> - Blocks refresh operations</li>
 * </ul>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Create a block for write operations only
 * EnumSet<ClusterBlockLevel> writeLevels = EnumSet.of(ClusterBlockLevel.WRITE);
 *
 * // Create a block for all read and write operations
 * EnumSet<ClusterBlockLevel> readWriteLevels = ClusterBlockLevel.READ_WRITE;
 *
 * // Create a block for all operations
 * EnumSet<ClusterBlockLevel> allLevels = ClusterBlockLevel.ALL;
 * }</pre>
 *
 * @see org.elasticsearch.cluster.block.ClusterBlock
 */
public enum ClusterBlockLevel {
    /** Blocks data read operations such as searches and get requests */
    READ,

    /** Blocks data write operations such as indexing, updates, and deletes */
    WRITE,

    /** Blocks metadata read operations */
    METADATA_READ,

    /** Blocks metadata write operations such as mapping and settings updates */
    METADATA_WRITE,

    /** Blocks refresh operations */
    REFRESH;

    /** A set containing all possible cluster block levels */
    public static final EnumSet<ClusterBlockLevel> ALL = EnumSet.allOf(ClusterBlockLevel.class);

    /** A set containing both read and write data operation levels */
    public static final EnumSet<ClusterBlockLevel> READ_WRITE = EnumSet.of(READ, WRITE);
}
