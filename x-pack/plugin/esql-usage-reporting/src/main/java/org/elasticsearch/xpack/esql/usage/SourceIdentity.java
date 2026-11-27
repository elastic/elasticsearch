/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage;

/**
 * Who is reporting. The node id comes from the node itself; the deployment id, cluster id, cloud provider and region are injected
 * by Cloud through settings. Hosted records that have no cluster id are dropped downstream, and the Usage API rejects a whole batch that
 * contains a record with an incomplete identity, so nothing is built from an identity that is not {@link #isComplete() complete}.
 */
public record SourceIdentity(String nodeId, String deploymentId, String clusterId, String provider, String region) {

    /** True when every value is present, which is what is needed to attribute a record. */
    public boolean isComplete() {
        return isPresent(nodeId) && isPresent(deploymentId) && isPresent(clusterId) && isPresent(provider) && isPresent(region);
    }

    private static boolean isPresent(String value) {
        return value != null && value.isBlank() == false;
    }
}
