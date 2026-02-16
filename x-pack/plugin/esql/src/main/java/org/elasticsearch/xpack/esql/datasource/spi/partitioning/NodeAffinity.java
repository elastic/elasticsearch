/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.spi.partitioning;

/**
 * Node affinity for a split or partition — where it should (or must) execute.
 *
 * <p>Two types of affinity reflect two different data access patterns:
 *
 * <ul>
 *   <li><b>Required</b> ({@link #require}) — data only exists on this node (local filesystem,
 *       node-local resources). The grouping algorithm never mixes required-affinity splits
 *       from different nodes. This is always enforced regardless of
 *       {@link DistributionHints#preferDataLocality()}.</li>
 *   <li><b>Preferred</b> ({@link #prefer}) — data is faster to read locally but accessible
 *       from anywhere (HDFS replicas, S3 with warm cache). The grouping algorithm groups
 *       these by node when {@link DistributionHints#preferDataLocality()} is true,
 *       but may mix them for balance. When the flag is false, preference is ignored.</li>
 * </ul>
 *
 * @param nodeId The target node identifier, or null for no affinity
 * @param required Whether this is a hard constraint (true) or a soft preference (false)
 * @see DataSourceSplit#nodeAffinity()
 * @see org.elasticsearch.xpack.esql.datasource.spi.DataSourcePartition#nodeAffinity()
 */
public record NodeAffinity(String nodeId, boolean required) {

    /**
     * No node affinity — the split can execute on any node.
     */
    public static final NodeAffinity NONE = new NodeAffinity(null, false);

    /**
     * Soft affinity: data is faster to read on this node but accessible from anywhere.
     *
     * <p>Examples: HDFS block with a local replica, S3 object with warm local cache.
     * Honored when {@link DistributionHints#preferDataLocality()} is true; ignored otherwise.
     *
     * @param nodeId The preferred node identifier
     */
    public static NodeAffinity prefer(String nodeId) {
        return new NodeAffinity(nodeId, false);
    }

    /**
     * Hard affinity: data only exists on this node.
     *
     * <p>Examples: local filesystem files, node-local resources.
     * Always enforced regardless of {@link DistributionHints#preferDataLocality()}.
     *
     * @param nodeId The required node identifier
     */
    public static NodeAffinity require(String nodeId) {
        return new NodeAffinity(nodeId, true);
    }

    /**
     * Whether this affinity specifies a node (either required or preferred).
     */
    public boolean hasAffinity() {
        return nodeId != null;
    }
}
