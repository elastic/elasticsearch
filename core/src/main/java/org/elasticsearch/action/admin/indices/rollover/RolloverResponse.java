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
    private boolean simulate;
    private boolean rolledOver;
    private boolean rolloverIndexCreated;

    RolloverResponse() {
    }

    RolloverResponse(String oldIndex, String newIndex, Set<Map.Entry<String, Boolean>> conditionStatus,
                     boolean simulate, boolean rolledOver, boolean rolloverIndexCreated) {
        this.oldIndex = oldIndex;
        this.newIndex = newIndex;
        this.simulate = simulate;
        this.rolledOver = rolledOver;
        this.rolloverIndexCreated = rolloverIndexCreated;
        this.conditionStatus = conditionStatus;
    }

    public String getOldIndex() {
        return oldIndex;
    }

    public String getNewIndex() {
        return newIndex;
    }

    public Set<Map.Entry<String, Boolean>> getConditionStatus() {
        return conditionStatus;
    }

    public boolean isSimulate() {
        return simulate;
    }

    public boolean isRolledOver() {
        return rolledOver;
    }

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
        simulate = in.readBoolean();
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
        out.writeBoolean(simulate);
        out.writeBoolean(rolledOver);
        out.writeBoolean(rolloverIndexCreated);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.OLD_INDEX, oldIndex);
        builder.field(Fields.NEW_INDEX, newIndex);
        builder.field(Fields.ROLLED_OVER, rolledOver);
        builder.field(Fields.SIMULATED, simulate);
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
        static final String SIMULATED = "simulated";
        static final String ROLLED_OVER = "rolled_over";
        static final String ROLLOVER_INDEX_CREATED = "rollover_index_created";
        static final String CONDITIONS = "conditions";
    }
}
