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

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.Collections;
import java.util.stream.Collectors;

public class RolloverResponse extends ActionResponse implements ToXContentObject {

    private static final String NEW_INDEX = "new_index";
    private static final String OLD_INDEX = "old_index";
    private static final String DRY_RUN = "dry_run";
    private static final String ROLLED_OVER = "rolled_over";
    private static final String CONDITIONS = "conditions";
    private static final String ACKNOWLEDGED = "acknowledged";
    private static final String SHARDS_ACKED = "shards_acknowledged";

    private final List<SingleAliasRolloverResponse> responses;

    RolloverResponse() {
        this.responses = Collections.emptyList();
    }

    RolloverResponse(List<SingleAliasRolloverResponse> responses) {
        if (0 == responses.size()) {
            throw new IllegalArgumentException("we need to match at least 1 alias"); // todo maybe better with optionals in method getOldIndex
        }
        this.responses = responses;
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DRY_RUN, responses.get(0).isDryRun());

        if (responses.size() == 1) {
            // keep compatibility with
            responses.get(0).toXContent(builder, params);
        }

        builder.startObject("matched_aliases");
        for(SingleAliasRolloverResponse response: this.responses) {
            builder.startObject(response.getAlias());
            builder.field(OLD_INDEX, response.getOldIndex());
            builder.field(NEW_INDEX, response.getNewIndex());
            builder.field(ROLLED_OVER, response.isRolledOver());
            builder.field(ACKNOWLEDGED, response.isAcknowledged());
            builder.field(SHARDS_ACKED, response.isShardsAcked());
            builder.startObject(CONDITIONS);
            for (Map.Entry<String, Boolean> entry : response.getConditionStatus()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
            builder.endObject();
        }

        builder.endObject();
        builder.endObject();

        return builder;
    }

    //todo override ActionResponseMethods

    static class SingleAliasRolloverResponse {

        private String alias;
        private String oldIndex;
        private String newIndex;
        private Set<Map.Entry<String, Boolean>> conditionStatus;
        private boolean dryRun;
        private boolean rolledOver;
        private boolean acknowledged;
        private boolean shardsAcked;

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

        String getAlias() {
            return alias;
        }

        String getOldIndex() {
            return oldIndex;
        }

        String getNewIndex() {
            return newIndex;
        }

        Set<Map.Entry<String, Boolean>> getConditionStatus() {
            return conditionStatus;
        }

        boolean isDryRun() {
            return dryRun;
        }

        boolean isRolledOver() {
            return rolledOver;
        }

        boolean isAcknowledged() {
            return acknowledged;
        }

        boolean isShardsAcked() {
            return shardsAcked;
        }

        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(OLD_INDEX, oldIndex);
            builder.field(NEW_INDEX, newIndex);
            builder.field(ROLLED_OVER, rolledOver);
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

    /**
     * Returns the name of the index that the request alias was pointing to
     */
    String getOldIndex() {
        return responses.get(0).getOldIndex();
    } // todo only if size == 1, null if size == 0 , exception otherwise

    /**
     * Returns the name of the index that the request alias currently points to
     */
    String getNewIndex() {
        return responses.get(0).getNewIndex();
    }

    /**
     * Returns the statuses of all the request conditions
     */
    Set<Map.Entry<String, Boolean>> getConditionStatus() {
        return responses.get(0).getConditionStatus();
    }

    /**
     * Returns if the rollover execution was skipped even when conditions were met
     */
    boolean isDryRun() {
        return responses.get(0).isDryRun();
    }

    /**
     * Returns true if the rollover was not simulated and the conditions were met
     */
    boolean isRolledOver() {
        return responses.get(0).isRolledOver();
    }

    /**
     * Returns true if the creation of the new rollover index and switching of the
     * alias to the newly created index was successful, and returns false otherwise.
     * If {@link #isDryRun()} is true, then this will also return false. If this
     * returns false, then {@link #isShardsAcked()} will also return false.
     */
    boolean isAcknowledged() {
        return responses.get(0).isAcknowledged();
    }

    /**
     * Returns true if the requisite number of shards were started in the newly
     * created rollover index before returning.  If {@link #isAcknowledged()} is
     * false, then this will also return false.
     */
    boolean isShardsAcked() {
        return responses.get(0).isShardsAcked();
    }
}
