/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml.job.config;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

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
        PARSER.declareField(ConstructingObjectParser.constructorArg(), p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return AppliesTo.fromString(p.text());
            }
            throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
        }, APPLIES_TO_FIELD, ValueType.STRING);
        PARSER.declareField(ConstructingObjectParser.constructorArg(), p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return Operator.fromString(p.text());
            }
            throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
        }, Operator.OPERATOR_FIELD, ValueType.STRING);
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
