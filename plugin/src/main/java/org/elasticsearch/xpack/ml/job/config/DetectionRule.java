/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.config;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.MlParserType;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class DetectionRule implements ToXContentObject, Writeable {

    public static final ParseField DETECTION_RULE_FIELD = new ParseField("detection_rule");
    public static final ParseField RULE_ACTION_FIELD = new ParseField("rule_action");
    public static final ParseField TARGET_FIELD_NAME_FIELD = new ParseField("target_field_name");
    public static final ParseField TARGET_FIELD_VALUE_FIELD = new ParseField("target_field_value");
    public static final ParseField CONDITIONS_CONNECTIVE_FIELD = new ParseField("conditions_connective");
    public static final ParseField RULE_CONDITIONS_FIELD = new ParseField("rule_conditions");

    // These parsers follow the pattern that metadata is parsed leniently (to allow for enhancements), whilst config is parsed strictly
    public static final ObjectParser<Builder, Void> METADATA_PARSER =
            new ObjectParser<>(DETECTION_RULE_FIELD.getPreferredName(), true, Builder::new);
    public static final ObjectParser<Builder, Void> CONFIG_PARSER =
            new ObjectParser<>(DETECTION_RULE_FIELD.getPreferredName(), false, Builder::new);
    public static final Map<MlParserType, ObjectParser<Builder, Void>> PARSERS = new EnumMap<>(MlParserType.class);

    static {
        PARSERS.put(MlParserType.METADATA, METADATA_PARSER);
        PARSERS.put(MlParserType.CONFIG, CONFIG_PARSER);
        for (MlParserType parserType : MlParserType.values()) {
            ObjectParser<Builder, Void> parser = PARSERS.get(parserType);
            assert parser != null;
            parser.declareField(Builder::setRuleAction, p -> {
                if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                    return RuleAction.fromString(p.text());
                }
                throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
            }, RULE_ACTION_FIELD, ValueType.STRING);
            parser.declareString(Builder::setTargetFieldName, TARGET_FIELD_NAME_FIELD);
            parser.declareString(Builder::setTargetFieldValue, TARGET_FIELD_VALUE_FIELD);
            parser.declareField(Builder::setConditionsConnective, p -> {
                if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                    return Connective.fromString(p.text());
                }
                throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
            }, CONDITIONS_CONNECTIVE_FIELD, ValueType.STRING);
            parser.declareObjectArray(Builder::setRuleConditions, (p, c) ->
                    RuleCondition.PARSERS.get(parserType).apply(p, c), RULE_CONDITIONS_FIELD);
        }
    }

    private final RuleAction ruleAction;
    private final String targetFieldName;
    private final String targetFieldValue;
    private final Connective conditionsConnective;
    private final List<RuleCondition> ruleConditions;

    private DetectionRule(RuleAction ruleAction, @Nullable String targetFieldName, @Nullable String targetFieldValue,
                          Connective conditionsConnective, List<RuleCondition> ruleConditions) {
        this.ruleAction = Objects.requireNonNull(ruleAction);
        this.targetFieldName = targetFieldName;
        this.targetFieldValue = targetFieldValue;
        this.conditionsConnective = Objects.requireNonNull(conditionsConnective);
        this.ruleConditions = Collections.unmodifiableList(ruleConditions);
    }

    public DetectionRule(StreamInput in) throws IOException {
        ruleAction = RuleAction.readFromStream(in);
        conditionsConnective = Connective.readFromStream(in);
        int size = in.readVInt();
        ruleConditions = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            ruleConditions.add(new RuleCondition(in));
        }
        targetFieldName = in.readOptionalString();
        targetFieldValue = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        ruleAction.writeTo(out);
        conditionsConnective.writeTo(out);
        out.writeVInt(ruleConditions.size());
        for (RuleCondition condition : ruleConditions) {
            condition.writeTo(out);
        }
        out.writeOptionalString(targetFieldName);
        out.writeOptionalString(targetFieldValue);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(RULE_ACTION_FIELD.getPreferredName(), ruleAction);
        builder.field(CONDITIONS_CONNECTIVE_FIELD.getPreferredName(), conditionsConnective);
        builder.field(RULE_CONDITIONS_FIELD.getPreferredName(), ruleConditions);
        if (targetFieldName != null) {
            builder.field(TARGET_FIELD_NAME_FIELD.getPreferredName(), targetFieldName);
        }
        if (targetFieldValue != null) {
            builder.field(TARGET_FIELD_VALUE_FIELD.getPreferredName(), targetFieldValue);
        }
        builder.endObject();
        return builder;
    }

    public RuleAction getRuleAction() {
        return ruleAction;
    }

    @Nullable
    public String getTargetFieldName() {
        return targetFieldName;
    }

    @Nullable
    public String getTargetFieldValue() {
        return targetFieldValue;
    }

    public Connective getConditionsConnective() {
        return conditionsConnective;
    }

    public List<RuleCondition> getRuleConditions() {
        return ruleConditions;
    }

    public Set<String> extractReferencedFilters() {
        return ruleConditions.stream().map(RuleCondition::getValueFilter).filter(Objects::nonNull).collect(Collectors.toSet());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof DetectionRule == false) {
            return false;
        }

        DetectionRule other = (DetectionRule) obj;
        return Objects.equals(ruleAction, other.ruleAction)
                && Objects.equals(targetFieldName, other.targetFieldName)
                && Objects.equals(targetFieldValue, other.targetFieldValue)
                && Objects.equals(conditionsConnective, other.conditionsConnective)
                && Objects.equals(ruleConditions, other.ruleConditions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ruleAction, targetFieldName, targetFieldValue, conditionsConnective, ruleConditions);
    }

    public static class Builder {
        private RuleAction ruleAction = RuleAction.FILTER_RESULTS;
        private String targetFieldName;
        private String targetFieldValue;
        private Connective conditionsConnective = Connective.OR;
        private List<RuleCondition> ruleConditions = Collections.emptyList();

        public Builder(List<RuleCondition> ruleConditions) {
            this.ruleConditions = ExceptionsHelper.requireNonNull(ruleConditions, RULE_CONDITIONS_FIELD.getPreferredName());
        }

        private Builder() {
        }

        public Builder setRuleAction(RuleAction ruleAction) {
            this.ruleAction = ExceptionsHelper.requireNonNull(ruleAction, RULE_ACTION_FIELD.getPreferredName());
            return this;
        }

        public Builder setTargetFieldName(String targetFieldName) {
            this.targetFieldName = targetFieldName;
            return this;
        }

        public Builder setTargetFieldValue(String targetFieldValue) {
            this.targetFieldValue = targetFieldValue;
            return this;
        }

        public Builder setConditionsConnective(Connective connective) {
            this.conditionsConnective = ExceptionsHelper.requireNonNull(connective, CONDITIONS_CONNECTIVE_FIELD.getPreferredName());
            return this;
        }

        public Builder setRuleConditions(List<RuleCondition> ruleConditions) {
            this.ruleConditions = ExceptionsHelper.requireNonNull(ruleConditions, RULE_ACTION_FIELD.getPreferredName());
            return this;
        }

        public DetectionRule build() {
            if (targetFieldValue != null && targetFieldName == null) {
                String msg = Messages.getMessage(Messages.JOB_CONFIG_DETECTION_RULE_MISSING_TARGET_FIELD_NAME, targetFieldValue);
                throw ExceptionsHelper.badRequestException(msg);
            }
            if (ruleConditions == null || ruleConditions.isEmpty()) {
                String msg = Messages.getMessage(Messages.JOB_CONFIG_DETECTION_RULE_REQUIRES_AT_LEAST_ONE_CONDITION);
                throw ExceptionsHelper.badRequestException(msg);
            }
            for (RuleCondition condition : ruleConditions) {
                if (condition.getConditionType() == RuleConditionType.CATEGORICAL && targetFieldName != null) {
                    String msg = Messages.getMessage(Messages.JOB_CONFIG_DETECTION_RULE_CONDITION_CATEGORICAL_INVALID_OPTION,
                            DetectionRule.TARGET_FIELD_NAME_FIELD.getPreferredName());
                    throw ExceptionsHelper.badRequestException(msg);
                }
            }
            return new DetectionRule(ruleAction, targetFieldName, targetFieldValue, conditionsConnective, ruleConditions);
        }
    }
}
