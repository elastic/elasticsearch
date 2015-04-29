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

package org.elasticsearch.action.search;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexException;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.*;

/**
 *
 */
public class SearchPhaseExecutionException extends ElasticsearchException {
    private final String phaseName;

    private ShardSearchFailure[] shardFailures;

    public SearchPhaseExecutionException(String phaseName, String msg, ShardSearchFailure[] shardFailures) {
        super(msg);
        this.phaseName = phaseName;
        this.shardFailures = shardFailures;
    }

    public SearchPhaseExecutionException(String phaseName, String msg, Throwable cause, ShardSearchFailure[] shardFailures) {
        super(msg, cause);
        this.phaseName = phaseName;
        this.shardFailures = shardFailures;
    }

    @Override
    public RestStatus status() {
        if (shardFailures.length == 0) {
            // if no successful shards, it means no active shards, so just return SERVICE_UNAVAILABLE
            return RestStatus.SERVICE_UNAVAILABLE;
        }
        RestStatus status = shardFailures[0].status();
        if (shardFailures.length > 1) {
            for (int i = 1; i < shardFailures.length; i++) {
                if (shardFailures[i].status().getStatus() >= 500) {
                    status = shardFailures[i].status();
                }
            }
        }
        return status;
    }

    public ShardSearchFailure[] shardFailures() {
        return shardFailures;
    }

    private static String buildMessage(String phaseName, String msg, ShardSearchFailure[] shardFailures) {
        StringBuilder sb = new StringBuilder();
        sb.append("Failed to execute phase [").append(phaseName).append("], ").append(msg);
        if (shardFailures != null && shardFailures.length > 0) {
            sb.append("; shardFailures ");
            for (ShardSearchFailure shardFailure : shardFailures) {
                if (shardFailure.shard() != null) {
                    sb.append("{").append(shardFailure.shard()).append(": ").append(shardFailure.reason()).append("}");
                } else {
                    sb.append("{").append(shardFailure.reason()).append("}");
                }
            }
        }
        return sb.toString();
    }

    @Override
    protected void innerToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("phase", phaseName);
        final boolean group = params.paramAsBoolean("group_shard_failures", true); // we group by default
        builder.field("grouped", group); // notify that it's grouped
        builder.field("failed_shards");
        builder.startArray();
        ShardSearchFailure[] failures = params.paramAsBoolean("group_shard_failures", true) ? groupBy(shardFailures) : shardFailures;
        for (ShardSearchFailure failure : failures) {
            builder.startObject();
            failure.toXContent(builder, params);
            builder.endObject();
        }
        builder.endArray();
        super.innerToXContent(builder, params);

    }

    private ShardSearchFailure[] groupBy(ShardSearchFailure[] failures) {
        List<ShardSearchFailure> uniqueFailures = new ArrayList<>();
        Set<GroupBy> reasons = new HashSet<>();
        for (ShardSearchFailure failure : failures) {
            GroupBy reason = new GroupBy(failure.getCause());
            if (reasons.contains(reason) == false) {
                reasons.add(reason);
                uniqueFailures.add(failure);
            }
        }
        return uniqueFailures.toArray(new ShardSearchFailure[0]);

    }

    @Override
    public ElasticsearchException[] guessRootCauses() {
        ShardSearchFailure[] failures = groupBy(shardFailures);
        List<ElasticsearchException> rootCauses = new ArrayList<>(failures.length);
        for (ShardSearchFailure failure : failures) {
            ElasticsearchException[] guessRootCauses = ElasticsearchException.guessRootCauses(failure.getCause());
            rootCauses.addAll(Arrays.asList(guessRootCauses));
        }
        return rootCauses.toArray(new ElasticsearchException[0]);
    }

    @Override
    public String toString() {
        return buildMessage(phaseName, getMessage(), shardFailures);
    }

    static class GroupBy {
        final String reason;
        final Index index;
        final Class<? extends Throwable> causeType;

        public GroupBy(Throwable t) {
            if (t instanceof IndexException) {
                index = ((IndexException) t).index();
            } else {
                index = null;
            }
            reason = t.getMessage();
            causeType = t.getClass();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            GroupBy groupBy = (GroupBy) o;

            if (!causeType.equals(groupBy.causeType)) return false;
            if (index != null ? !index.equals(groupBy.index) : groupBy.index != null) return false;
            if (reason != null ? !reason.equals(groupBy.reason) : groupBy.reason != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = reason != null ? reason.hashCode() : 0;
            result = 31 * result + (index != null ? index.hashCode() : 0);
            result = 31 * result + causeType.hashCode();
            return result;
        }
    }
}
