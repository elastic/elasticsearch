/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * This class is used for returning information for lists of query rulesets, to avoid including all
 * {@link QueryRuleset} information which can be retrieved using subsequent GetQueryRuleset requests.
 */
public class QueryRulesetListItem implements Writeable, ToXContentObject {

    public static final ParseField RULESET_ID_FIELD = new ParseField("ruleset_id");
    public static final ParseField NUM_RULES_FIELD = new ParseField("num_rules");

    private final String rulesetId;
    private final int numRules;

    /**
     * Constructs a QueryRulesetListItem.
     *
     * @param rulesetId The unique identifier for the ruleset
     * @param numRules  The number of rules contained within the ruleset.
     */
    public QueryRulesetListItem(String rulesetId, int numRules) {
        Objects.requireNonNull(rulesetId, "rulesetId cannot be null on a QueryRuleListItem");
        this.rulesetId = rulesetId;
        this.numRules = numRules;
    }

    public QueryRulesetListItem(StreamInput in) throws IOException {
        this.rulesetId = in.readString();
        this.numRules = in.readInt();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(RULESET_ID_FIELD.getPreferredName(), rulesetId);
        builder.field(NUM_RULES_FIELD.getPreferredName(), numRules);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(rulesetId);
        out.writeInt(numRules);
    }

    /**
     * Returns the rulesetId of the {@link QueryRulesetListItem}.
     *
     * @return the rulesetId.
     */
    public String rulesetId() {
        return rulesetId;
    }

    /**
     * Returns the number of rules associated with the {@link QueryRulesetListItem}.
     *
     * @return the total number of rules.
     */
    public int numRules() {
        return numRules;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryRulesetListItem that = (QueryRulesetListItem) o;
        return numRules == that.numRules && Objects.equals(rulesetId, that.rulesetId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rulesetId, numRules);
    }
}
