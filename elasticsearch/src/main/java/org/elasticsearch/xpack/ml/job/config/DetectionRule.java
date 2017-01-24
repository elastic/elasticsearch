/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.config;

import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.job.messages.Messages;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class DetectionRule extends ToXContentToBytes implements Writeable {
    public static final ParseField DETECTION_RULE_FIELD = new ParseField("detection_rule");
    public static final ParseField RULE_ACTION_FIELD = new ParseField("rule_action");
    public static final ParseField TARGET_FIELD_NAME_FIELD = new ParseField("target_field_name");
    public static final ParseField TARGET_FIELD_VALUE_FIELD = new ParseField("target_field_value");
    public static final ParseField CONDITIONS_CONNECTIVE_FIELD = new ParseField("conditions_connective");
    public static final ParseField RULE_CONDITIONS_FIELD = new ParseField("rule_conditions");

    public static final ConstructingObjectParser<DetectionRule, Void> PARSER = new ConstructingObjectParser<>(
            DETECTION_RULE_FIELD.getPreferredName(),
            arr -> {
                @SuppressWarnings("unchecked")
                List<RuleCondition> rules = (List<RuleCondition>) arr[3];
                return new DetectionRule((String) arr[0], (String) arr[1], (Connective) arr[2], rules);
            }
            );

    static {
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), TARGET_FIELD_NAME_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), TARGET_FIELD_VALUE_FIELD);
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(), p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return Connective.fromString(p.text());
            }
            throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
        }, CONDITIONS_CONNECTIVE_FIELD, ValueType.STRING);
        PARSER.declareObjectArray(ConstructingObjectParser.optionalConstructorArg(),
                (parser, parseFieldMatcher) -> RuleCondition.PARSER.apply(parser, parseFieldMatcher), RULE_CONDITIONS_FIELD);
    }

    private final RuleAction ruleAction = RuleAction.FILTER_RESULTS;
    private final String targetFieldName;
    private final String targetFieldValue;
    private final Connective conditionsConnective;
    private final List<RuleCondition> ruleConditions;

    public DetectionRule(StreamInput in) throws IOException {
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
        builder.field(CONDITIONS_CONNECTIVE_FIELD.getPreferredName(), conditionsConnective.getName());
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

    public DetectionRule(String targetFieldName, String targetFieldValue, Connective conditionsConnective,
            List<RuleCondition> ruleConditions) {
        if (targetFieldValue != null && targetFieldName == null) {
            String msg = Messages.getMessage(Messages.JOB_CONFIG_DETECTION_RULE_MISSING_TARGET_FIELD_NAME, targetFieldValue);
            throw new IllegalArgumentException(msg);
        }
        if (ruleConditions == null || ruleConditions.isEmpty()) {
            String msg = Messages.getMessage(Messages.JOB_CONFIG_DETECTION_RULE_REQUIRES_AT_LEAST_ONE_CONDITION);
            throw new IllegalArgumentException(msg);
        }
        for (RuleCondition condition : ruleConditions) {
            if (condition.getConditionType() == RuleConditionType.CATEGORICAL && targetFieldName != null) {
                String msg = Messages.getMessage(Messages.JOB_CONFIG_DETECTION_RULE_CONDITION_CATEGORICAL_INVALID_OPTION,
                        DetectionRule.TARGET_FIELD_NAME_FIELD.getPreferredName());
                throw new IllegalArgumentException(msg);
            }
        }

        this.targetFieldName = targetFieldName;
        this.targetFieldValue = targetFieldValue;
        this.conditionsConnective = conditionsConnective != null ? conditionsConnective : Connective.OR;
        this.ruleConditions = Collections.unmodifiableList(ruleConditions);
    }

    public RuleAction getRuleAction() {
        return ruleAction;
    }

    public String getTargetFieldName() {
        return targetFieldName;
    }

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
        return Objects.equals(ruleAction, other.ruleAction) && Objects.equals(targetFieldName, other.targetFieldName)
                && Objects.equals(targetFieldValue, other.targetFieldValue)
                && Objects.equals(conditionsConnective, other.conditionsConnective) && Objects.equals(ruleConditions, other.ruleConditions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ruleAction, targetFieldName, targetFieldValue, conditionsConnective, ruleConditions);
    }
}
