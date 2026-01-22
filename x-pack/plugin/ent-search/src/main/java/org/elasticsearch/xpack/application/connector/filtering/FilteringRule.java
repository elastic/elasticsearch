/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.filtering;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.connector.ConnectorUtils;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Represents a single rule used for filtering in a data processing or querying context.
 * Each {@link FilteringRule} includes details such as its creation and update timestamps,
 * the specific field it applies to, an identifier, and its order in a set of rules.
 * Additionally, it encapsulates the filtering policy, the condition under which the rule applies,
 * and the value associated with the rule.
 */
public class FilteringRule implements Writeable, ToXContentObject {

    private final Instant createdAt;
    private final String field;
    private final String id;
    private final Integer order;
    private final FilteringPolicy policy;
    private final FilteringRuleCondition rule;
    private final Instant updatedAt;
    private final String value;

    /**
     * Constructs a new FilteringRule instance.
     *
     * @param createdAt The creation timestamp of the filtering rule.
     * @param field     The field associated with the filtering rule.
     * @param id        The identifier of the filtering rule.
     * @param order     The order of the filtering rule.
     * @param policy    The {@link FilteringPolicy} of the filtering rule.
     * @param rule      The specific {@link FilteringRuleCondition}
     * @param updatedAt The update timestamp of the filtering rule.
     * @param value     The value associated with the filtering rule.
     */
    public FilteringRule(
        Instant createdAt,
        String field,
        String id,
        Integer order,
        FilteringPolicy policy,
        FilteringRuleCondition rule,
        Instant updatedAt,
        String value
    ) {
        this.createdAt = createdAt;
        this.field = field;
        this.id = id;
        this.order = order;
        this.policy = policy;
        this.rule = rule;
        this.updatedAt = updatedAt;
        this.value = value;
    }

    public FilteringRule(StreamInput in) throws IOException {
        this.createdAt = in.readOptionalInstant();
        this.field = in.readString();
        this.id = in.readString();
        this.order = in.readInt();
        this.policy = in.readEnum(FilteringPolicy.class);
        this.rule = in.readEnum(FilteringRuleCondition.class);
        this.updatedAt = in.readOptionalInstant();
        this.value = in.readString();
    }

    private static final ParseField CREATED_AT_FIELD = new ParseField("created_at");
    private static final ParseField FIELD_FIELD = new ParseField("field");
    private static final ParseField ID_FIELD = new ParseField("id");
    private static final ParseField ORDER_FIELD = new ParseField("order");
    private static final ParseField POLICY_FIELD = new ParseField("policy");
    private static final ParseField RULE_FIELD = new ParseField("rule");
    private static final ParseField UPDATED_AT_FIELD = new ParseField("updated_at");
    private static final ParseField VALUE_FIELD = new ParseField("value");

    private static final ConstructingObjectParser<FilteringRule, Void> PARSER = new ConstructingObjectParser<>(
        "connector_filtering_rule",
        true,
        args -> new Builder().setCreatedAt((Instant) args[0])
            .setField((String) args[1])
            .setId((String) args[2])
            .setOrder((Integer) args[3])
            .setPolicy((FilteringPolicy) args[4])
            .setRule((FilteringRuleCondition) args[5])
            .setUpdatedAt((Instant) args[6])
            .setValue((String) args[7])
            .build()
    );

    static {
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> ConnectorUtils.parseInstant(p, CREATED_AT_FIELD.getPreferredName()),
            CREATED_AT_FIELD,
            ObjectParser.ValueType.STRING
        );
        PARSER.declareString(constructorArg(), FIELD_FIELD);
        PARSER.declareString(constructorArg(), ID_FIELD);
        PARSER.declareInt(constructorArg(), ORDER_FIELD);
        PARSER.declareField(
            constructorArg(),
            (p, c) -> FilteringPolicy.filteringPolicy(p.text()),
            POLICY_FIELD,
            ObjectParser.ValueType.STRING
        );
        PARSER.declareField(
            constructorArg(),
            (p, c) -> FilteringRuleCondition.filteringRuleCondition(p.text()),
            RULE_FIELD,
            ObjectParser.ValueType.STRING
        );
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> ConnectorUtils.parseInstant(p, UPDATED_AT_FIELD.getPreferredName()),
            UPDATED_AT_FIELD,
            ObjectParser.ValueType.STRING
        );
        PARSER.declareString(constructorArg(), VALUE_FIELD);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CREATED_AT_FIELD.getPreferredName(), createdAt);
        builder.field(FIELD_FIELD.getPreferredName(), field);
        builder.field(ID_FIELD.getPreferredName(), id);
        builder.field(ORDER_FIELD.getPreferredName(), order);
        builder.field(POLICY_FIELD.getPreferredName(), policy.toString());
        builder.field(RULE_FIELD.getPreferredName(), rule.toString());
        builder.field(UPDATED_AT_FIELD.getPreferredName(), updatedAt);
        builder.field(VALUE_FIELD.getPreferredName(), value);
        builder.endObject();
        return builder;
    }

    public static FilteringRule fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalInstant(createdAt);
        out.writeString(field);
        out.writeString(id);
        out.writeInt(order);
        out.writeEnum(policy);
        out.writeEnum(rule);
        out.writeOptionalInstant(updatedAt);
        out.writeString(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FilteringRule that = (FilteringRule) o;
        return Objects.equals(createdAt, that.createdAt)
            && Objects.equals(field, that.field)
            && Objects.equals(id, that.id)
            && Objects.equals(order, that.order)
            && policy == that.policy
            && rule == that.rule
            && Objects.equals(updatedAt, that.updatedAt)
            && Objects.equals(value, that.value);
    }

    /**
     * Compares this {@code FilteringRule} to another rule for equality, ignoring differences
     * in created_at, updated_at timestamps and order.
    */
    public boolean equalsExceptForTimestampsAndOrder(FilteringRule that) {
        return Objects.equals(field, that.field)
            && Objects.equals(id, that.id)
            && policy == that.policy
            && rule == that.rule
            && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(createdAt, field, id, order, policy, rule, updatedAt, value);
    }

    public Integer getOrder() {
        return order;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public static class Builder {

        private Instant createdAt;
        private String field;
        private String id;
        private Integer order;
        private FilteringPolicy policy;
        private FilteringRuleCondition rule;
        private Instant updatedAt;
        private String value;
        private final Instant currentTimestamp = Instant.now();

        public Builder setCreatedAt(Instant createdAt) {
            this.createdAt = Objects.requireNonNullElse(createdAt, currentTimestamp);
            return this;
        }

        public Builder setField(String field) {
            this.field = field;
            return this;
        }

        public Builder setId(String id) {
            this.id = id;
            return this;
        }

        public Builder setOrder(Integer order) {
            this.order = order;
            return this;
        }

        public Builder setPolicy(FilteringPolicy policy) {
            this.policy = policy;
            return this;
        }

        public Builder setRule(FilteringRuleCondition rule) {
            this.rule = rule;
            return this;
        }

        public Builder setUpdatedAt(Instant updatedAt) {
            this.updatedAt = Objects.requireNonNullElse(updatedAt, currentTimestamp);
            return this;
        }

        public Builder setValue(String value) {
            this.value = value;
            return this;
        }

        public FilteringRule build() {
            return new FilteringRule(createdAt, field, id, order, policy, rule, updatedAt, value);
        }
    }
}
