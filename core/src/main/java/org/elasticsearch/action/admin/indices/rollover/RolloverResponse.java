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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public final class RolloverResponse extends ActionResponse implements ToXContent {

    private String oldIndex;
    private String newIndex;
    private Set<Map.Entry<String, Boolean>> conditionStatus;
    private boolean dryRun;
    private boolean rolledOver;
    private boolean rolloverIndexCreated;

    RolloverResponse() {
    }

    RolloverResponse(String oldIndex, String newIndex, Set<Condition.Result> conditionResults,
                     boolean dryRun, boolean rolledOver, boolean rolloverIndexCreated) {
        this.oldIndex = oldIndex;
        this.newIndex = newIndex;
        this.dryRun = dryRun;
        this.rolledOver = rolledOver;
        this.rolloverIndexCreated = rolloverIndexCreated;
        this.conditionStatus = conditionResults.stream()
            .map(result -> new AbstractMap.SimpleEntry<>(result.condition.toString(), result.matched))
            .collect(Collectors.toSet());
    }

    /**
     * Returns the name of the index that the request alias was pointing to
     */
    public String getOldIndex() {
        return oldIndex;
    }

    /**
     * Returns the name of the index that the request alias currently points to
     */
    public String getNewIndex() {
        return newIndex;
    }

    /**
     * Returns the statuses of all the request conditions
     */
    public Set<Map.Entry<String, Boolean>> getConditionStatus() {
        return conditionStatus;
    }

    /**
     * Returns if the rollover execution was skipped even when conditions were met
     */
    public boolean isDryRun() {
        return dryRun;
    }

    /**
     * Returns if the rollover was not simulated and the conditions were met
     */
    public boolean isRolledOver() {
        return rolledOver;
    }

    /**
     * Returns if the rollover index had to be explicitly created
     */
    public boolean isRolloverIndexCreated() {
        return rolloverIndexCreated;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        oldIndex = in.readString();
        newIndex = in.readString();
        int conditionSize = in.readVInt();
        Set<Map.Entry<String, Boolean>> conditions = new HashSet<>(conditionSize);
        for (int i = 0; i < conditionSize; i++) {
            String condition = in.readString();
            boolean satisfied = in.readBoolean();
            conditions.add(new AbstractMap.SimpleEntry<>(condition, satisfied));
        }
        conditionStatus = conditions;
        dryRun = in.readBoolean();
        rolledOver = in.readBoolean();
        rolloverIndexCreated = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(oldIndex);
        out.writeString(newIndex);
        out.writeVInt(conditionStatus.size());
        for (Map.Entry<String, Boolean> entry : conditionStatus) {
            out.writeString(entry.getKey());
            out.writeBoolean(entry.getValue());
        }
        out.writeBoolean(dryRun);
        out.writeBoolean(rolledOver);
        out.writeBoolean(rolloverIndexCreated);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.OLD_INDEX, oldIndex);
        builder.field(Fields.NEW_INDEX, newIndex);
        builder.field(Fields.ROLLED_OVER, rolledOver);
        builder.field(Fields.DRY_RUN, dryRun);
        builder.field(Fields.ROLLOVER_INDEX_CREATED, rolloverIndexCreated);
        builder.startObject(Fields.CONDITIONS);
        for (Map.Entry<String, Boolean> entry : conditionStatus) {
            builder.field(entry.getKey(), entry.getValue());
        }
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final String NEW_INDEX = "new_index";
        static final String OLD_INDEX = "old_index";
        static final String DRY_RUN = "dry_run";
        static final String ROLLED_OVER = "rolled_over";
        static final String ROLLOVER_INDEX_CREATED = "rollover_index_created";
        static final String CONDITIONS = "conditions";
    }
}
