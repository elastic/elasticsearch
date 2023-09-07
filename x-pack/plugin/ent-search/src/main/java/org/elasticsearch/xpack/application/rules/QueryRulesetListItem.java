/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * This class is used for returning information for lists of query rulesets, to avoid including all
 * {@link QueryRuleset} information which can be retrieved using subsequent GetQueryRuleset requests.
 */
public class QueryRulesetListItem implements Writeable, ToXContentObject {

    // TODO we need to actually bump transport version, but there's no point until main is merged. Placeholder for now.
    public static final TransportVersion EXPANDED_RULESET_COUNT_TRANSPORT_VERSION = TransportVersions.V_8_500_052;

    public static final ParseField RULESET_ID_FIELD = new ParseField("ruleset_id");
    public static final ParseField RULE_TOTAL_COUNT_FIELD = new ParseField("rule_total_count");
    public static final ParseField RULE_CRITERIA_TYPE_COUNTS_FIELD = new ParseField("rule_criteria_types_counts");

    private final String rulesetId;
    private final int ruleTotalCount;
    private final Map<QueryRuleCriteriaType, Integer> criteriaTypeToCountMap;

    /**
     * Constructs a QueryRulesetListItem.
     *
     * @param rulesetId The unique identifier for the ruleset
     * @param ruleTotalCount  The number of rules contained within the ruleset.
     * @param criteriaTypeToCountMap A map of criteria type to the number of rules of that type.
     */
    public QueryRulesetListItem(String rulesetId, int ruleTotalCount, Map<QueryRuleCriteriaType, Integer> criteriaTypeToCountMap) {
        Objects.requireNonNull(rulesetId, "rulesetId cannot be null on a QueryRuleListItem");
        this.rulesetId = rulesetId;
        this.ruleTotalCount = ruleTotalCount;
        this.criteriaTypeToCountMap = criteriaTypeToCountMap;
    }

    public QueryRulesetListItem(StreamInput in) throws IOException {
        this.rulesetId = in.readString();
        this.ruleTotalCount = in.readInt();
        if (in.getTransportVersion().onOrAfter(EXPANDED_RULESET_COUNT_TRANSPORT_VERSION)) {
            this.criteriaTypeToCountMap = in.readMap(m -> in.readEnum(QueryRuleCriteriaType.class), StreamInput::readInt);
        } else {
            this.criteriaTypeToCountMap = Map.of();
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(RULESET_ID_FIELD.getPreferredName(), rulesetId);
        builder.field(RULE_TOTAL_COUNT_FIELD.getPreferredName(), ruleTotalCount);
        builder.startObject(RULE_CRITERIA_TYPE_COUNTS_FIELD.getPreferredName());
        for (QueryRuleCriteriaType criteriaType : criteriaTypeToCountMap.keySet()) {
            builder.field(criteriaType.name().toLowerCase(Locale.ROOT), criteriaTypeToCountMap.get(criteriaType));
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(rulesetId);
        out.writeInt(ruleTotalCount);
        if (out.getTransportVersion().onOrAfter(EXPANDED_RULESET_COUNT_TRANSPORT_VERSION)) {
            out.writeMap(criteriaTypeToCountMap, StreamOutput::writeEnum, StreamOutput::writeInt);
        }
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
    public int ruleTotalCount() {
        return ruleTotalCount;
    }

    public Map<QueryRuleCriteriaType, Integer> criteriaTypeToCountMap() {
        return criteriaTypeToCountMap;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryRulesetListItem that = (QueryRulesetListItem) o;
        return ruleTotalCount == that.ruleTotalCount
            && Objects.equals(rulesetId, that.rulesetId)
            && Objects.equals(criteriaTypeToCountMap, that.criteriaTypeToCountMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rulesetId, ruleTotalCount, criteriaTypeToCountMap);
    }
}
