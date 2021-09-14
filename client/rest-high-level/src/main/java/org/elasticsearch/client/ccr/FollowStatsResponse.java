/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ccr;

import org.elasticsearch.common.xcontent.XContentParser;

public final class FollowStatsResponse {

    public static FollowStatsResponse fromXContent(XContentParser parser) {
        return new FollowStatsResponse(IndicesFollowStats.PARSER.apply(parser, null));
    }

    private final IndicesFollowStats indicesFollowStats;

    public FollowStatsResponse(IndicesFollowStats indicesFollowStats) {
        this.indicesFollowStats = indicesFollowStats;
    }

    public IndicesFollowStats getIndicesFollowStats() {
        return indicesFollowStats;
    }
}
