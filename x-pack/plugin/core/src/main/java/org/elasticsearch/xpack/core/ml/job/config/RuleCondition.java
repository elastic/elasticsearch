/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.config;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

public class RuleCondition implements ToXContentObject, Writeable {

    public static final ParseField RULE_CONDITION_FIELD = new ParseField("rule_condition");

    public static final ParseField APPLIES_TO_FIELD = new ParseField("applies_to");
    public static final ParseField VALUE_FIELD = new ParseField("value");

    // These parsers follow the pattern that metadata is parsed leniently (to allow for enhancements), whilst config is parsed strictly
    public static final ConstructingObjectParser<RuleCondition, Void> LENIENT_PARSER = createParser(true);
    public static final ConstructingObjectParser<RuleCondition, Void> STRICT_PARSER = createParser(false);

    private static ConstructingObjectParser<RuleCondition, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<RuleCondition, Void> parser = new ConstructingObjectParser<>(
            RULE_CONDITION_FIELD.getPreferredName(),
            ignoreUnknownFields,
            a -> new RuleCondition((AppliesTo) a[0], (Operator) a[1], (double) a[2])
        );

        parser.declareString(ConstructingObjectParser.constructorArg(), AppliesTo::fromString, APPLIES_TO_FIELD);
        parser.declareString(ConstructingObjectParser.constructorArg(), Operator::fromString, Operator.OPERATOR_FIELD);
        parser.declareDouble(ConstructingObjectParser.constructorArg(), VALUE_FIELD);

        return parser;
    }

    private final AppliesTo appliesTo;
    private final Operator operator;
    private final double value;

    public RuleCondition(StreamInput in) throws IOException {
        appliesTo = AppliesTo.readFromStream(in);
        operator = Operator.readFromStream(in);
        value = in.readDouble();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        appliesTo.writeTo(out);
        operator.writeTo(out);
        out.writeDouble(value);
    }

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

    public enum AppliesTo implements Writeable {
        ACTUAL,
        TYPICAL,
        DIFF_FROM_TYPICAL,
        TIME;

        public static AppliesTo fromString(String value) {
            return valueOf(value.toUpperCase(Locale.ROOT));
        }

        public static AppliesTo readFromStream(StreamInput in) throws IOException {
            return in.readEnum(AppliesTo.class);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(this);
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }
}
