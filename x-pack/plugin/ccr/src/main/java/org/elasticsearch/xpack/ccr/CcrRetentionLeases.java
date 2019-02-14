/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr;

import java.util.Locale;

public class CcrRetentionLeases {

    /**
     * The retention lease ID used by followers.
     *
     * @param followerUUID the follower index UUID
     * @param leaderUUID   the leader index UUID
     * @return the retention lease ID
     */
    public static String retentionLeaseId(
            final String localClusterName,
            final String remoteClusterName,
            final String followerIndexName,
            final String followerUUID,
            final String leaderIndexName,
            final String leaderUUID) {
        return String.format(
                Locale.ROOT,
                "%s/%s/%s-following-%s/%s/%s",
                localClusterName,
                followerIndexName,
                followerUUID,
                remoteClusterName,
                leaderIndexName,
                leaderUUID);
    }

}
