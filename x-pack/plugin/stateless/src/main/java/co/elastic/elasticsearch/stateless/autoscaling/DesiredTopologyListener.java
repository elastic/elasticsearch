/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.autoscaling;

/**
 * Used in conjunction with {@link DesiredTopologyContext} to react to changes in the desired cluster topology.
 */
public interface DesiredTopologyListener {
    /**
     * Called when a desired cluster topology becomes available for the first time.
     * <p>
     * This method is invoked on the master node when the topology transitions from {@code null} to a non-null state. This will most often
     * occur when a node is a newly-elected master node, and it receives its first topology via
     * {@code POST /_internal/serverless/autoscaling}. It is not called for subsequent topology updates.
     *
     * @param topology the newly available desired cluster topology, never {@code null}
     */
    void onDesiredTopologyAvailable(DesiredClusterTopology topology);
}
