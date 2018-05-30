/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.job.config;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.MlParserType;

import java.io.IOException;
import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public class RuleCondition implements ToXContentObject, Writeable {
    public static final ParseField APPLIES_TO_FIELD = new ParseField("applies_to");
    public static final ParseField RULE_CONDITION_FIELD = new ParseField("rule_condition");

    // These parsers follow the pattern that metadata is parsed leniently (to allow for enhancements), whilst config is parsed strictly
    public static final ConstructingObjectParser<RuleCondition, Void> METADATA_PARSER =
            new ConstructingObjectParser<>(RULE_CONDITION_FIELD.getPreferredName(), true,
                    a -> new RuleCondition((AppliesTo) a[0], (Condition) a[1]));
    public static final ConstructingObjectParser<RuleCondition, Void> CONFIG_PARSER =
            new ConstructingObjectParser<>(RULE_CONDITION_FIELD.getPreferredName(), false,
                    a -> new RuleCondition((AppliesTo) a[0], (Condition) a[1]));
    public static final Map<MlParserType, ConstructingObjectParser<RuleCondition, Void>> PARSERS =
            new EnumMap<>(MlParserType.class);

    static {
        PARSERS.put(MlParserType.METADATA, METADATA_PARSER);
        PARSERS.put(MlParserType.CONFIG, CONFIG_PARSER);
        for (MlParserType parserType : MlParserType.values()) {
            ConstructingObjectParser<RuleCondition, Void> parser = PARSERS.get(parserType);
            assert parser != null;
            parser.declareField(ConstructingObjectParser.constructorArg(), p -> {
                if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                    return AppliesTo.fromString(p.text());
                }
                throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
            }, APPLIES_TO_FIELD, ValueType.STRING);
            parser.declareObject(ConstructingObjectParser.optionalConstructorArg(), Condition.PARSER, Condition.CONDITION_FIELD);
        }
    }

    private final AppliesTo appliesTo;
    private final Condition condition;

    public RuleCondition(StreamInput in) throws IOException {
        appliesTo = AppliesTo.readFromStream(in);
        condition = in.readOptionalWriteable(Condition::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        appliesTo.writeTo(out);
        out.writeOptionalWriteable(condition);
    }

    public RuleCondition(AppliesTo appliesTo, Condition condition) {
        this.appliesTo = appliesTo;
        this.condition = condition;
    }

    public RuleCondition(RuleCondition ruleCondition) {
        this.appliesTo = ruleCondition.appliesTo;
        this.condition = ruleCondition.condition;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(APPLIES_TO_FIELD.getPreferredName(), appliesTo);
        if (condition != null) {
            builder.field(Condition.CONDITION_FIELD.getPreferredName(), condition);
        }
        builder.endObject();
        return builder;
    }

    public AppliesTo getAppliesTo() {
        return appliesTo;
    }

    public Condition getCondition() {
        return condition;
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
        return Objects.equals(appliesTo, other.appliesTo) && Objects.equals(condition, other.condition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(appliesTo, condition);
    }

    public static RuleCondition createTime(Operator operator, long epochSeconds) {
        return new RuleCondition(AppliesTo.TIME, new Condition(operator, epochSeconds));
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
