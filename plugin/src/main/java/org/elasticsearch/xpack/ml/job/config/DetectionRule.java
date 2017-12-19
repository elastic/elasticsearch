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
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class DetectionRule implements ToXContentObject, Writeable {

    public static final ParseField DETECTION_RULE_FIELD = new ParseField("detection_rule");
    public static final ParseField ACTIONS_FIELD = new ParseField("actions");
    public static final ParseField TARGET_FIELD_NAME_FIELD = new ParseField("target_field_name");
    public static final ParseField TARGET_FIELD_VALUE_FIELD = new ParseField("target_field_value");
    public static final ParseField CONDITIONS_CONNECTIVE_FIELD = new ParseField("conditions_connective");
    public static final ParseField CONDITIONS_FIELD = new ParseField("conditions");

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
            parser.declareStringArray(Builder::setActions, ACTIONS_FIELD);
            parser.declareString(Builder::setTargetFieldName, TARGET_FIELD_NAME_FIELD);
            parser.declareString(Builder::setTargetFieldValue, TARGET_FIELD_VALUE_FIELD);
            parser.declareField(Builder::setConditionsConnective, p -> {
                if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                    return Connective.fromString(p.text());
                }
                throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
            }, CONDITIONS_CONNECTIVE_FIELD, ValueType.STRING);
            parser.declareObjectArray(Builder::setConditions, (p, c) ->
                    RuleCondition.PARSERS.get(parserType).apply(p, c), CONDITIONS_FIELD);
        }
    }

    private final EnumSet<RuleAction> actions;
    private final String targetFieldName;
    private final String targetFieldValue;
    private final Connective conditionsConnective;
    private final List<RuleCondition> conditions;

    private DetectionRule(EnumSet<RuleAction> actions, @Nullable String targetFieldName, @Nullable String targetFieldValue,
                          Connective conditionsConnective, List<RuleCondition> conditions) {
        this.actions = Objects.requireNonNull(actions);
        this.targetFieldName = targetFieldName;
        this.targetFieldValue = targetFieldValue;
        this.conditionsConnective = Objects.requireNonNull(conditionsConnective);
        this.conditions = Collections.unmodifiableList(conditions);
    }

    public DetectionRule(StreamInput in) throws IOException {
        int actionsCount = in.readVInt();
        actions = EnumSet.noneOf(RuleAction.class);
        for (int i = 0; i < actionsCount; ++i) {
            actions.add(RuleAction.readFromStream(in));
        }

        conditionsConnective = Connective.readFromStream(in);
        int size = in.readVInt();
        conditions = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            conditions.add(new RuleCondition(in));
        }
        targetFieldName = in.readOptionalString();
        targetFieldValue = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(actions.size());
        for (RuleAction action : actions) {
            action.writeTo(out);
        }

        conditionsConnective.writeTo(out);
        out.writeVInt(conditions.size());
        for (RuleCondition condition : conditions) {
            condition.writeTo(out);
        }
        out.writeOptionalString(targetFieldName);
        out.writeOptionalString(targetFieldValue);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ACTIONS_FIELD.getPreferredName(), actions);
        builder.field(CONDITIONS_CONNECTIVE_FIELD.getPreferredName(), conditionsConnective);
        builder.field(CONDITIONS_FIELD.getPreferredName(), conditions);
        if (targetFieldName != null) {
            builder.field(TARGET_FIELD_NAME_FIELD.getPreferredName(), targetFieldName);
        }
        if (targetFieldValue != null) {
            builder.field(TARGET_FIELD_VALUE_FIELD.getPreferredName(), targetFieldValue);
        }
        builder.endObject();
        return builder;
    }

    public EnumSet<RuleAction> getActions() {
        return actions;
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

    public List<RuleCondition> getConditions() {
        return conditions;
    }

    public Set<String> extractReferencedFilters() {
        return conditions.stream().map(RuleCondition::getFilterId).filter(Objects::nonNull).collect(Collectors.toSet());
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
        return Objects.equals(actions, other.actions)
                && Objects.equals(targetFieldName, other.targetFieldName)
                && Objects.equals(targetFieldValue, other.targetFieldValue)
                && Objects.equals(conditionsConnective, other.conditionsConnective)
                && Objects.equals(conditions, other.conditions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(actions, targetFieldName, targetFieldValue, conditionsConnective, conditions);
    }

    public static class Builder {
        private EnumSet<RuleAction> actions = EnumSet.of(RuleAction.FILTER_RESULTS);
        private String targetFieldName;
        private String targetFieldValue;
        private Connective conditionsConnective = Connective.OR;
        private List<RuleCondition> conditions = Collections.emptyList();

        public Builder(List<RuleCondition> conditions) {
            this.conditions = ExceptionsHelper.requireNonNull(conditions, CONDITIONS_FIELD.getPreferredName());
        }

        private Builder() {
        }

        public Builder setActions(List<String> actions) {
            this.actions.clear();
            actions.stream().map(RuleAction::fromString).forEach(this.actions::add);
            return this;
        }

        public Builder setActions(EnumSet<RuleAction> actions) {
            this.actions = Objects.requireNonNull(actions, ACTIONS_FIELD.getPreferredName());
            return this;
        }

        public Builder setActions(RuleAction... actions) {
            this.actions.clear();
            Arrays.stream(actions).forEach(this.actions::add);
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

        public Builder setConditions(List<RuleCondition> conditions) {
            this.conditions = ExceptionsHelper.requireNonNull(conditions, CONDITIONS_FIELD.getPreferredName());
            return this;
        }

        public DetectionRule build() {
            if (targetFieldValue != null && targetFieldName == null) {
                String msg = Messages.getMessage(Messages.JOB_CONFIG_DETECTION_RULE_MISSING_TARGET_FIELD_NAME, targetFieldValue);
                throw ExceptionsHelper.badRequestException(msg);
            }
            if (conditions == null || conditions.isEmpty()) {
                String msg = Messages.getMessage(Messages.JOB_CONFIG_DETECTION_RULE_REQUIRES_AT_LEAST_ONE_CONDITION);
                throw ExceptionsHelper.badRequestException(msg);
            }
            for (RuleCondition condition : conditions) {
                if (condition.getType() == RuleConditionType.CATEGORICAL && targetFieldName != null) {
                    String msg = Messages.getMessage(Messages.JOB_CONFIG_DETECTION_RULE_CONDITION_CATEGORICAL_INVALID_OPTION,
                            DetectionRule.TARGET_FIELD_NAME_FIELD.getPreferredName());
                    throw ExceptionsHelper.badRequestException(msg);
                }
            }
            return new DetectionRule(actions, targetFieldName, targetFieldValue, conditionsConnective, conditions);
        }
    }
}
