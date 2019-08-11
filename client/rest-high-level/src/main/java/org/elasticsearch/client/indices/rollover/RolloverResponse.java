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

package org.elasticsearch.client.indices.rollover;

import org.elasticsearch.action.support.master.ShardsAcknowledgedResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Response object for {@link RolloverRequest} API
 */
public final class RolloverResponse extends ShardsAcknowledgedResponse {

    private static final ParseField NEW_INDEX = new ParseField("new_index");
    private static final ParseField OLD_INDEX = new ParseField("old_index");
    private static final ParseField DRY_RUN = new ParseField("dry_run");
    private static final ParseField ROLLED_OVER = new ParseField("rolled_over");
    private static final ParseField CONDITIONS = new ParseField("conditions");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<RolloverResponse, Void> PARSER = new ConstructingObjectParser<>("rollover",
            true, args -> new RolloverResponse((String) args[0], (String) args[1], (Map<String,Boolean>) args[2],
            (Boolean)args[3], (Boolean)args[4], (Boolean) args[5], (Boolean) args[6]));

    static {
        PARSER.declareString(constructorArg(), OLD_INDEX);
        PARSER.declareString(constructorArg(), NEW_INDEX);
        PARSER.declareObject(constructorArg(), (parser, context) -> parser.map(), CONDITIONS);
        PARSER.declareBoolean(constructorArg(), DRY_RUN);
        PARSER.declareBoolean(constructorArg(), ROLLED_OVER);
        declareAcknowledgedAndShardsAcknowledgedFields(PARSER);
    }

    private final String oldIndex;
    private final String newIndex;
    private final Map<String, Boolean> conditionStatus;
    private final boolean dryRun;
    private final boolean rolledOver;

    public RolloverResponse(String oldIndex, String newIndex, Map<String, Boolean> conditionResults,
                            boolean dryRun, boolean rolledOver, boolean acknowledged, boolean shardsAcknowledged) {
        super(acknowledged, shardsAcknowledged);
        this.oldIndex = oldIndex;
        this.newIndex = newIndex;
        this.dryRun = dryRun;
        this.rolledOver = rolledOver;
        this.conditionStatus = conditionResults;
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
    public Map<String, Boolean> getConditionStatus() {
        return conditionStatus;
    }

    /**
     * Returns if the rollover execution was skipped even when conditions were met
     */
    public boolean isDryRun() {
        return dryRun;
    }

    /**
     * Returns true if the rollover was not simulated and the conditions were met
     */
    public boolean isRolledOver() {
        return rolledOver;
    }

    public static RolloverResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o)) {
            RolloverResponse that = (RolloverResponse) o;
            return dryRun == that.dryRun &&
                    rolledOver == that.rolledOver &&
                    Objects.equals(oldIndex, that.oldIndex) &&
                    Objects.equals(newIndex, that.newIndex) &&
                    Objects.equals(conditionStatus, that.conditionStatus);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), oldIndex, newIndex, conditionStatus, dryRun, rolledOver);
    }
}
