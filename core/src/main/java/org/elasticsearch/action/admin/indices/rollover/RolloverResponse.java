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

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.stream.Collectors;

public final class RolloverResponse extends ActionResponse implements ToXContentObject {

    private static final String NEW_INDEX = "new_index";
    private static final String OLD_INDEX = "old_index";
    private static final String DRY_RUN = "dry_run";
    private static final String ROLLED_OVER = "rolled_over";
    private static final String CONDITIONS = "conditions";
    private static final String ACKNOWLEDGED = "acknowledged";
    private static final String SHARDS_ACKED = "shards_acknowledged";

    static class SingleAliasRolloverResponse implements ToXContentObject {

        private String alias;
        private String oldIndex;
        private String newIndex;
        private Set<Map.Entry<String, Boolean>> conditionStatus;
        private boolean dryRun;
        private boolean rolledOver;
        private boolean acknowledged;
        private boolean shardsAcked;

        private SingleAliasRolloverResponse() {
        }

        SingleAliasRolloverResponse(String alias, String oldIndex, String newIndex, Set<Condition.Result> conditionResults,
                                    boolean dryRun, boolean rolledOver, boolean acknowledged, boolean shardsAcked) {
            this.alias = alias;
            this.oldIndex = oldIndex;
            this.newIndex = newIndex;
            this.dryRun = dryRun;
            this.rolledOver = rolledOver;
            this.acknowledged = acknowledged;
            this.shardsAcked = shardsAcked;
            this.conditionStatus = conditionResults.stream()
                .map(result -> new AbstractMap.SimpleEntry<>(result.condition.toString(), result.matched))
                .collect(Collectors.toSet());
        }

        /**
         * Returns the name of the index that the alias was pointing to
         */
        String getOldIndex() {
            return oldIndex;
        }

        /**
         * Returns the name of the index that the alias currently points to
         */
        String getNewIndex() {
            return newIndex;
        }

        /**
         * Returns the statuses of all the request conditions
         */
        Set<Map.Entry<String, Boolean>> getConditionStatus() {
            return conditionStatus;
        }

        /**
         * Returns if the rollover execution was skipped even when conditions were met
         */
        boolean isDryRun() {
            return dryRun;
        }

        /**
         * Returns true if the rollover was not simulated and the conditions were met
         */
        boolean isRolledOver() {
            return rolledOver;
        }

        /**
         * Returns true if the creation of the new rollover index and switching of the
         * alias to the newly created index was successful, and returns false otherwise.
         * If {@link #isDryRun()} is true, then this will also return false. If this
         * returns false, then {@link #isShardsAcked()} will also return false.
         */
        boolean isAcknowledged() {
            return acknowledged;
        }

        /**
         * Returns true if the requisite number of shards were started in the newly
         * created rollover index before returning.  If {@link #isAcknowledged()} is
         * false, then this will also return false.
         */
        boolean isShardsAcked() {
            return shardsAcked;
        }

        /**
         * Returns name of rolled over alias
         */
        String getAlias() {
            return alias;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(OLD_INDEX, oldIndex);
            builder.field(NEW_INDEX, newIndex);
            builder.field(ROLLED_OVER, rolledOver);
            builder.field(DRY_RUN, dryRun);
            builder.field(ACKNOWLEDGED, acknowledged);
            builder.field(SHARDS_ACKED, shardsAcked);
            builder.startObject(CONDITIONS);
            for (Map.Entry<String, Boolean> entry : conditionStatus) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
            return builder;
        }
    }

    private final List<SingleAliasRolloverResponse> responses;
    private final Map<String, Exception> failures;

    RolloverResponse() {
        this.responses = new ArrayList<>();
        this.failures = new HashMap<>();
    }

    RolloverResponse(List<SingleAliasRolloverResponse> responses, Map<String, Exception> failures) {
        if (responses.isEmpty() && failures.isEmpty()) {
            throw new IllegalStateException("Response for at least 1 alias is expected");
        }
        this.responses = responses;
        this.failures = failures;
    }

    List<SingleAliasRolloverResponse> responses() {
        return responses;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int responsesSize = in.readVInt();
        for (int j = 0; j < responsesSize; j++) {
            final SingleAliasRolloverResponse response = new SingleAliasRolloverResponse();
            response.alias = in.readString();
            response.oldIndex = in.readString();
            response.newIndex = in.readString();
            int conditionSize = in.readVInt();
            Set<Map.Entry<String, Boolean>> conditions = new HashSet<>(conditionSize);
            for (int i = 0; i < conditionSize; i++) {
                String condition = in.readString();
                boolean satisfied = in.readBoolean();
                conditions.add(new AbstractMap.SimpleEntry<>(condition, satisfied));
            }
            response.conditionStatus = conditions;
            response.dryRun = in.readBoolean();
            response.rolledOver = in.readBoolean();
            response.acknowledged = in.readBoolean();
            response.shardsAcked = in.readBoolean();
            responses.add(response);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(responses.size());
        for (SingleAliasRolloverResponse response: responses) {
            out.writeString(response.alias);
            out.writeString(response.oldIndex);
            out.writeString(response.newIndex);
            out.writeVInt(response.conditionStatus.size());
            for (Map.Entry<String, Boolean> entry : response.conditionStatus) {
                out.writeString(entry.getKey());
                out.writeBoolean(entry.getValue());
            }
            out.writeBoolean(response.dryRun);
            out.writeBoolean(response.rolledOver);
            out.writeBoolean(response.acknowledged);
            out.writeBoolean(response.shardsAcked);
        }
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        // keep compatibility with single alias Rollover
        if (responses.size() == 1) {
            responses.get(0).toXContent(builder, params);
        }

        builder.startObject("rolled_over_aliases");
        for(SingleAliasRolloverResponse response: this.responses) {
            builder.startObject(response.getAlias());
            response.toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();

        builder.startObject("failed_aliases");
        for (Map.Entry<String, Exception> entry : failures.entrySet()) {
            builder.startObject(entry.getKey());
            ElasticsearchException.generateThrowableXContent(builder, params, entry.getValue());
            builder.endObject();
        }
        builder.endObject();

        builder.endObject();
        return builder;
    }
}
