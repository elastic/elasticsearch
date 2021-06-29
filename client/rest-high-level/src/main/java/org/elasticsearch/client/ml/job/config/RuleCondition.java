/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.job.config;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

public class RuleCondition implements ToXContentObject {

    public static final ParseField RULE_CONDITION_FIELD = new ParseField("rule_condition");

    public static final ParseField APPLIES_TO_FIELD = new ParseField("applies_to");
    public static final ParseField VALUE_FIELD = new ParseField("value");

    public static final ConstructingObjectParser<RuleCondition, Void> PARSER =
        new ConstructingObjectParser<>(RULE_CONDITION_FIELD.getPreferredName(), true,
            a -> new RuleCondition((AppliesTo) a[0], (Operator) a[1], (double) a[2]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), AppliesTo::fromString, APPLIES_TO_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Operator::fromString, Operator.OPERATOR_FIELD);
        PARSER.declareDouble(ConstructingObjectParser.constructorArg(), VALUE_FIELD);
    }

    private final AppliesTo appliesTo;
    private final Operator operator;
    private final double value;

    public RuleCondition(AppliesTo appliesTo, Operator operator, double value) {
        this.appliesTo = appliesTo;
        this.operator = operator;
        this.value = value;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(APPLIES_TO_FIELD.getPreferredName(), appliesTo);
        builder.field(Operator.OPERATOR_FIELD.getPreferredName(), operator);
        builder.field(VALUE_FIELD.getPreferredName(), value);
        builder.endObject();
        return builder;
    }

    public AppliesTo getAppliesTo() {
        return appliesTo;
    }

    public Operator getOperator() {
        return operator;
    }

    public double getValue() {
        return value;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof RuleCondition == false) {
            return false;
        }

        RuleCondition other = (RuleCondition) obj;
        return appliesTo == other.appliesTo && operator == other.operator && value == other.value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(appliesTo, operator, value);
    }

    public static RuleCondition createTime(Operator operator, long epochSeconds) {
        return new RuleCondition(AppliesTo.TIME, operator, epochSeconds);
    }

    public enum AppliesTo {
        ACTUAL,
        TYPICAL,
        DIFF_FROM_TYPICAL,
        TIME;

        public static AppliesTo fromString(String value) {
            return valueOf(value.toUpperCase(Locale.ROOT));
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }
}
