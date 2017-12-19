/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.config;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.MlParserType;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;

public class RuleCondition implements ToXContentObject, Writeable {
    public static final ParseField TYPE_FIELD = new ParseField("type");
    public static final ParseField RULE_CONDITION_FIELD = new ParseField("rule_condition");
    public static final ParseField FIELD_NAME_FIELD = new ParseField("field_name");
    public static final ParseField FIELD_VALUE_FIELD = new ParseField("field_value");

    // These parsers follow the pattern that metadata is parsed leniently (to allow for enhancements), whilst config is parsed strictly
    public static final ConstructingObjectParser<RuleCondition, Void> METADATA_PARSER =
            new ConstructingObjectParser<>(RULE_CONDITION_FIELD.getPreferredName(), true,
                    a -> new RuleCondition((RuleConditionType) a[0], (String) a[1], (String) a[2], (Condition) a[3], (String) a[4]));
    public static final ConstructingObjectParser<RuleCondition, Void> CONFIG_PARSER =
            new ConstructingObjectParser<>(RULE_CONDITION_FIELD.getPreferredName(), false,
                    a -> new RuleCondition((RuleConditionType) a[0], (String) a[1], (String) a[2], (Condition) a[3], (String) a[4]));
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
                    return RuleConditionType.fromString(p.text());
                }
                throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
            }, TYPE_FIELD, ValueType.STRING);
            parser.declareStringOrNull(ConstructingObjectParser.optionalConstructorArg(), FIELD_NAME_FIELD);
            parser.declareStringOrNull(ConstructingObjectParser.optionalConstructorArg(), FIELD_VALUE_FIELD);
            parser.declareObject(ConstructingObjectParser.optionalConstructorArg(), Condition.PARSER, Condition.CONDITION_FIELD);
            parser.declareStringOrNull(ConstructingObjectParser.optionalConstructorArg(), MlFilter.ID);
        }
    }

    private final RuleConditionType type;
    private final String fieldName;
    private final String fieldValue;
    private final Condition condition;
    private final String filterId;

    public RuleCondition(StreamInput in) throws IOException {
        type = RuleConditionType.readFromStream(in);
        condition = in.readOptionalWriteable(Condition::new);
        fieldName = in.readOptionalString();
        fieldValue = in.readOptionalString();
        filterId = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        type.writeTo(out);
        out.writeOptionalWriteable(condition);
        out.writeOptionalString(fieldName);
        out.writeOptionalString(fieldValue);
        out.writeOptionalString(filterId);
    }

    RuleCondition(RuleConditionType type, String fieldName, String fieldValue, Condition condition, String filterId) {
        this.type = type;
        this.fieldName = fieldName;
        this.fieldValue = fieldValue;
        this.condition = condition;
        this.filterId = filterId;

        verifyFieldsBoundToType(this);
        verifyFieldValueRequiresFieldName(this);
    }

    public RuleCondition(RuleCondition ruleCondition) {
        this.type = ruleCondition.type;
        this.fieldName = ruleCondition.fieldName;
        this.fieldValue = ruleCondition.fieldValue;
        this.condition = ruleCondition.condition;
        this.filterId = ruleCondition.filterId;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TYPE_FIELD.getPreferredName(), type);
        if (condition != null) {
            builder.field(Condition.CONDITION_FIELD.getPreferredName(), condition);
        }
        if (fieldName != null) {
            builder.field(FIELD_NAME_FIELD.getPreferredName(), fieldName);
        }
        if (fieldValue != null) {
            builder.field(FIELD_VALUE_FIELD.getPreferredName(), fieldValue);
        }
        if (filterId != null) {
            builder.field(MlFilter.ID.getPreferredName(), filterId);
        }
        builder.endObject();
        return builder;
    }

    public RuleConditionType getType() {
        return type;
    }

    /**
     * The field name for which the rule applies. Can be null, meaning rule
     * applies to all results.
     */
    public String getFieldName() {
        return fieldName;
    }

    /**
     * The value of the field name for which the rule applies. When set, the
     * rule applies only to the results that have the fieldName/fieldValue pair.
     * When null, the rule applies to all values for of the specified field
     * name. Only applicable when fieldName is not null.
     */
    public String getFieldValue() {
        return fieldValue;
    }

    public Condition getCondition() {
        return condition;
    }

    /**
     * The unique identifier of a filter. Required when the rule type is
     * categorical. Should be null for all other types.
     */
    public String getFilterId() {
        return filterId;
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
        return Objects.equals(type, other.type) && Objects.equals(fieldName, other.fieldName)
                && Objects.equals(fieldValue, other.fieldValue) && Objects.equals(condition, other.condition)
                && Objects.equals(filterId, other.filterId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, fieldName, fieldValue, condition, filterId);
    }

    public static RuleCondition createCategorical(String fieldName, String valueFilter) {
        return new RuleCondition(RuleConditionType.CATEGORICAL, fieldName, null, null, valueFilter);
    }

    public static RuleCondition createNumerical(RuleConditionType conditionType, String fieldName, String fieldValue,
                                                Condition condition ) {
        if (conditionType.isNumerical() == false) {
            throw new IllegalStateException("Rule condition type [" +  conditionType + "] not valid for a numerical condition");
        }
        return new RuleCondition(conditionType, fieldName, fieldValue, condition, null);
    }

    public static RuleCondition createTime(Operator operator, long epochSeconds) {
        return new RuleCondition(RuleConditionType.TIME, null, null, new Condition(operator, Long.toString(epochSeconds)), null);
    }

    private static void verifyFieldsBoundToType(RuleCondition ruleCondition) throws ElasticsearchParseException {
        switch (ruleCondition.getType()) {
        case CATEGORICAL:
            verifyCategorical(ruleCondition);
            break;
        case NUMERICAL_ACTUAL:
        case NUMERICAL_TYPICAL:
        case NUMERICAL_DIFF_ABS:
            verifyNumerical(ruleCondition);
            break;
        case TIME:
            verifyTimeRule(ruleCondition);
            break;
        default:
            throw new IllegalStateException();
        }
    }

    private static void verifyCategorical(RuleCondition ruleCondition) throws ElasticsearchParseException {
        checkCategoricalHasNoField(Condition.CONDITION_FIELD.getPreferredName(), ruleCondition.getCondition());
        checkCategoricalHasNoField(RuleCondition.FIELD_VALUE_FIELD.getPreferredName(), ruleCondition.getFieldValue());
        checkCategoricalHasField(MlFilter.ID.getPreferredName(), ruleCondition.getFilterId());
    }

    private static void checkCategoricalHasNoField(String fieldName, Object fieldValue) throws ElasticsearchParseException {
        if (fieldValue != null) {
            String msg = Messages.getMessage(Messages.JOB_CONFIG_DETECTION_RULE_CONDITION_CATEGORICAL_INVALID_OPTION, fieldName);
            throw ExceptionsHelper.badRequestException(msg);
        }
    }

    private static void checkCategoricalHasField(String fieldName, Object fieldValue) throws ElasticsearchParseException {
        if (fieldValue == null) {
            String msg = Messages.getMessage(Messages.JOB_CONFIG_DETECTION_RULE_CONDITION_CATEGORICAL_MISSING_OPTION, fieldName);
            throw ExceptionsHelper.badRequestException(msg);
        }
    }

    private static void verifyNumerical(RuleCondition ruleCondition) throws ElasticsearchParseException {
        checkNumericalHasNoField(MlFilter.ID.getPreferredName(), ruleCondition.getFilterId());
        checkNumericalHasField(Condition.CONDITION_FIELD.getPreferredName(), ruleCondition.getCondition());
        if (ruleCondition.getFieldName() != null && ruleCondition.getFieldValue() == null) {
            String msg = Messages.getMessage(Messages.JOB_CONFIG_DETECTION_RULE_CONDITION_NUMERICAL_WITH_FIELD_NAME_REQUIRES_FIELD_VALUE);
            throw ExceptionsHelper.badRequestException(msg);
        }
        checkNumericalConditionOparatorsAreValid(ruleCondition);
    }

    private static void checkNumericalHasNoField(String fieldName, Object fieldValue) throws ElasticsearchParseException {
        if (fieldValue != null) {
            String msg = Messages.getMessage(Messages.JOB_CONFIG_DETECTION_RULE_CONDITION_NUMERICAL_INVALID_OPTION, fieldName);
            throw ExceptionsHelper.badRequestException(msg);
        }
    }

    private static void checkNumericalHasField(String fieldName, Object fieldValue) throws ElasticsearchParseException {
        if (fieldValue == null) {
            String msg = Messages.getMessage(Messages.JOB_CONFIG_DETECTION_RULE_CONDITION_NUMERICAL_MISSING_OPTION, fieldName);
            throw ExceptionsHelper.badRequestException(msg);
        }
    }

    private static void verifyFieldValueRequiresFieldName(RuleCondition ruleCondition) throws ElasticsearchParseException {
        if (ruleCondition.getFieldValue() != null && ruleCondition.getFieldName() == null) {
            String msg = Messages.getMessage(Messages.JOB_CONFIG_DETECTION_RULE_CONDITION_MISSING_FIELD_NAME,
                    ruleCondition.getFieldValue());
            throw ExceptionsHelper.badRequestException(msg);
        }
    }

    static EnumSet<Operator> VALID_CONDITION_OPERATORS = EnumSet.of(Operator.LT, Operator.LTE, Operator.GT, Operator.GTE);

    private static void checkNumericalConditionOparatorsAreValid(RuleCondition ruleCondition) throws ElasticsearchParseException {
        Operator operator = ruleCondition.getCondition().getOperator();
        if (!VALID_CONDITION_OPERATORS.contains(operator)) {
            String msg = Messages.getMessage(Messages.JOB_CONFIG_DETECTION_RULE_CONDITION_NUMERICAL_INVALID_OPERATOR, operator);
            throw ExceptionsHelper.badRequestException(msg);
        }
    }

    private static void verifyTimeRule(RuleCondition ruleCondition) {
        checkNumericalConditionOparatorsAreValid(ruleCondition);
    }
}
