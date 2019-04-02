/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.ccr;

import org.elasticsearch.common.ParseField;
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
