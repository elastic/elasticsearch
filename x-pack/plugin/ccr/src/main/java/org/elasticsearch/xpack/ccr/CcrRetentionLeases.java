/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.index.Index;

import java.util.Locale;

public class CcrRetentionLeases {

    /**
     * The retention lease ID used by followers.
     *
     * @param localClusterName   the local cluster name
     * @param followerIndex      the follower index
     * @param remoteClusterAlias the remote cluster alias
     * @param leaderIndex        the leader index
     * @return the retention lease ID
     */
    public static String retentionLeaseId(
            final String localClusterName,
            final Index followerIndex,
            final String remoteClusterAlias,
            final Index leaderIndex) {
        return String.format(
                Locale.ROOT,
                "%s/%s/%s-following-%s/%s/%s",
                localClusterName,
                followerIndex.getName(),
                followerIndex.getUUID(),
                remoteClusterAlias,
                leaderIndex.getName(),
                leaderIndex.getUUID());
    }

}
