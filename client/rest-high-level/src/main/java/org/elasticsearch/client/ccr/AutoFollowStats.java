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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

public final class AutoFollowStats {

    static final ParseField NUMBER_OF_SUCCESSFUL_INDICES_AUTO_FOLLOWED = new ParseField("number_of_successful_follow_indices");
    static final ParseField NUMBER_OF_FAILED_INDICES_AUTO_FOLLOWED = new ParseField("number_of_failed_follow_indices");
    static final ParseField NUMBER_OF_FAILED_REMOTE_CLUSTER_STATE_REQUESTS =
        new ParseField("number_of_failed_remote_cluster_state_requests");
    static final ParseField RECENT_AUTO_FOLLOW_ERRORS = new ParseField("recent_auto_follow_errors");
    static final ParseField LEADER_INDEX = new ParseField("leader_index");
    static final ParseField AUTO_FOLLOW_EXCEPTION = new ParseField("auto_follow_exception");

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<AutoFollowStats, Void> STATS_PARSER = new ConstructingObjectParser<>("auto_follow_stats",
        args -> new AutoFollowStats(
            (Long) args[0],
            (Long) args[1],
            (Long) args[2],
            new TreeMap<>(
                ((List<Map.Entry<String, ElasticsearchException>>) args[3])
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
        ));

    private static final ConstructingObjectParser<Map.Entry<String, ElasticsearchException>, Void> AUTO_FOLLOW_EXCEPTIONS_PARSER =
        new ConstructingObjectParser<>(
            "auto_follow_stats_errors",
            args -> new AbstractMap.SimpleEntry<>((String) args[0], (ElasticsearchException) args[1]));

    static {
        AUTO_FOLLOW_EXCEPTIONS_PARSER.declareString(ConstructingObjectParser.constructorArg(), LEADER_INDEX);
        AUTO_FOLLOW_EXCEPTIONS_PARSER.declareObject(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> ElasticsearchException.fromXContent(p),
            AUTO_FOLLOW_EXCEPTION);

        STATS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), NUMBER_OF_FAILED_INDICES_AUTO_FOLLOWED);
        STATS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), NUMBER_OF_FAILED_REMOTE_CLUSTER_STATE_REQUESTS);
        STATS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), NUMBER_OF_SUCCESSFUL_INDICES_AUTO_FOLLOWED);
        STATS_PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), AUTO_FOLLOW_EXCEPTIONS_PARSER,
            RECENT_AUTO_FOLLOW_ERRORS);
    }

    private final long numberOfFailedFollowIndices;
    private final long numberOfFailedRemoteClusterStateRequests;
    private final long numberOfSuccessfulFollowIndices;
    private final NavigableMap<String, ElasticsearchException> recentAutoFollowErrors;

    AutoFollowStats(long numberOfFailedFollowIndices,
                    long numberOfFailedRemoteClusterStateRequests,
                    long numberOfSuccessfulFollowIndices,
                    NavigableMap<String, ElasticsearchException> recentAutoFollowErrors) {
        this.numberOfFailedFollowIndices = numberOfFailedFollowIndices;
        this.numberOfFailedRemoteClusterStateRequests = numberOfFailedRemoteClusterStateRequests;
        this.numberOfSuccessfulFollowIndices = numberOfSuccessfulFollowIndices;
        this.recentAutoFollowErrors = recentAutoFollowErrors;
    }

    public long getNumberOfFailedFollowIndices() {
        return numberOfFailedFollowIndices;
    }

    public long getNumberOfFailedRemoteClusterStateRequests() {
        return numberOfFailedRemoteClusterStateRequests;
    }

    public long getNumberOfSuccessfulFollowIndices() {
        return numberOfSuccessfulFollowIndices;
    }

    public NavigableMap<String, ElasticsearchException> getRecentAutoFollowErrors() {
        return recentAutoFollowErrors;
    }

}
