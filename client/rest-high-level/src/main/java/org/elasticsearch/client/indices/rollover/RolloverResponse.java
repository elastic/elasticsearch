/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.indices.rollover;

import org.elasticsearch.action.support.master.ShardsAcknowledgedResponse;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

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
    private static final ConstructingObjectParser<RolloverResponse, Void> PARSER = new ConstructingObjectParser<>(
        "rollover",
        true,
        args -> new RolloverResponse(
            (String) args[0],
            (String) args[1],
            (Map<String, Boolean>) args[2],
            (Boolean) args[3],
            (Boolean) args[4],
            (Boolean) args[5],
            (Boolean) args[6]
        )
    );

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

    public RolloverResponse(
        String oldIndex,
        String newIndex,
        Map<String, Boolean> conditionResults,
        boolean dryRun,
        boolean rolledOver,
        boolean acknowledged,
        boolean shardsAcknowledged
    ) {
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
            return dryRun == that.dryRun
                && rolledOver == that.rolledOver
                && Objects.equals(oldIndex, that.oldIndex)
                && Objects.equals(newIndex, that.newIndex)
                && Objects.equals(conditionStatus, that.conditionStatus);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), oldIndex, newIndex, conditionStatus, dryRun, rolledOver);
    }
}
