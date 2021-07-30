/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ccr;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

public final class CcrStatsResponse {

    static final ParseField AUTO_FOLLOW_STATS_FIELD = new ParseField("auto_follow_stats");
    static final ParseField FOLLOW_STATS_FIELD = new ParseField("follow_stats");

    private static final ConstructingObjectParser<CcrStatsResponse, Void> PARSER = new ConstructingObjectParser<>(
        "indices",
        true,
        args -> {
            AutoFollowStats autoFollowStats = (AutoFollowStats) args[0];
            IndicesFollowStats indicesFollowStats = (IndicesFollowStats) args[1];
            return new CcrStatsResponse(autoFollowStats, indicesFollowStats);
        });

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), AutoFollowStats.STATS_PARSER, AUTO_FOLLOW_STATS_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), IndicesFollowStats.PARSER, FOLLOW_STATS_FIELD);
    }

    public static CcrStatsResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final AutoFollowStats autoFollowStats;
    private final IndicesFollowStats indicesFollowStats;

    public CcrStatsResponse(AutoFollowStats autoFollowStats, IndicesFollowStats indicesFollowStats) {
        this.autoFollowStats = autoFollowStats;
        this.indicesFollowStats = indicesFollowStats;
    }

    public AutoFollowStats getAutoFollowStats() {
        return autoFollowStats;
    }

    public IndicesFollowStats getIndicesFollowStats() {
        return indicesFollowStats;
    }
}
