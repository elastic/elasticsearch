/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.config;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class DetectionRule implements ToXContentObject, Writeable {

    public static final ParseField DETECTION_RULE_FIELD = new ParseField("detection_rule");
    public static final ParseField ACTIONS_FIELD = new ParseField("actions");
    public static final ParseField SCOPE_FIELD = new ParseField("scope");
    public static final ParseField CONDITIONS_FIELD = new ParseField("conditions");
    public static final ParseField PARAMS_FIELD = new ParseField("params");

    // These parsers follow the pattern that metadata is parsed leniently (to allow for enhancements), whilst config is parsed strictly
    public static final ObjectParser<Builder, Void> LENIENT_PARSER = createParser(true);
    public static final ObjectParser<Builder, Void> STRICT_PARSER = createParser(false);

    private static ObjectParser<Builder, Void> createParser(boolean ignoreUnknownFields) {
        ObjectParser<Builder, Void> parser = new ObjectParser<>(DETECTION_RULE_FIELD.getPreferredName(), ignoreUnknownFields, Builder::new);

        parser.declareStringArray(Builder::setActions, ACTIONS_FIELD);
        parser.declareObject(Builder::setScope, RuleScope.parser(ignoreUnknownFields), SCOPE_FIELD);
        parser.declareObjectArray(
            Builder::setConditions,
            ignoreUnknownFields ? RuleCondition.LENIENT_PARSER : RuleCondition.STRICT_PARSER,
            CONDITIONS_FIELD
        );
        parser.declareObject(Builder::setParams, ignoreUnknownFields ? RuleParams.LENIENT_PARSER : RuleParams.STRICT_PARSER, PARAMS_FIELD);

        return parser;
    }

    private final EnumSet<RuleAction> actions;
    private final RuleScope scope;
    private final List<RuleCondition> conditions;
    private final RuleParams params;

    private DetectionRule(EnumSet<RuleAction> actions, RuleScope scope, List<RuleCondition> conditions, RuleParams params) {
        this.actions = Objects.requireNonNull(actions);
        this.scope = Objects.requireNonNull(scope);
        this.conditions = Collections.unmodifiableList(conditions);
        this.params = params;
    }

    public DetectionRule(StreamInput in) throws IOException {
        actions = in.readEnumSet(RuleAction.class);
        scope = new RuleScope(in);
        conditions = in.readCollectionAsList(RuleCondition::new);
        if (in.getTransportVersion().onOrAfter(TransportVersions.ML_ADD_DETECTION_RULE_PARAMS)) {
            params = new RuleParams(in);
        } else {
            params = new RuleParams();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnumSet(actions);
        scope.writeTo(out);
        out.writeCollection(conditions);
        if (out.getTransportVersion().onOrAfter(TransportVersions.ML_ADD_DETECTION_RULE_PARAMS)) {
            params.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ACTIONS_FIELD.getPreferredName(), actions);
        if (scope.isEmpty() == false) {
            builder.field(SCOPE_FIELD.getPreferredName(), scope);
        }
        if (conditions.isEmpty() == false) {
            builder.field(CONDITIONS_FIELD.getPreferredName(), conditions);
        }
        if (this.params.isEmpty() == false) {
            builder.field(PARAMS_FIELD.getPreferredName(), this.params);
        }
        builder.endObject();
        return builder;
    }

    public EnumSet<RuleAction> getActions() {
        return actions;
    }

    public RuleScope getScope() {
        return scope;
    }

    public List<RuleCondition> getConditions() {
        return conditions;
    }

    public RuleParams getParams() {
        return params;
    }

    public Set<String> extractReferencedFilters() {
        return scope.getReferencedFilters();
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
            && Objects.equals(scope, other.scope)
            && Objects.equals(conditions, other.conditions)
            && Objects.equals(params, other.params);
    }

    @Override
    public int hashCode() {
        return Objects.hash(actions, scope, conditions, params);
    }

    public static class Builder {
        private EnumSet<RuleAction> actions = EnumSet.of(RuleAction.SKIP_RESULT);
        private RuleScope scope = new RuleScope();
        private List<RuleCondition> conditions = Collections.emptyList();
        private RuleParams params = new RuleParams();

        public Builder(RuleScope.Builder scope) {
            this.scope = scope.build();
        }

        public Builder(List<RuleCondition> conditions) {
            this.conditions = ExceptionsHelper.requireNonNull(conditions, CONDITIONS_FIELD.getPreferredName());
        }

        Builder() {}

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

        public Builder setScope(RuleScope scope) {
            this.scope = Objects.requireNonNull(scope);
            return this;
        }

        public Builder setConditions(List<RuleCondition> conditions) {
            this.conditions = ExceptionsHelper.requireNonNull(conditions, CONDITIONS_FIELD.getPreferredName());
            return this;
        }

        public Builder setParams(RuleParams params) {
            this.params = params;
            return this;
        }

        public DetectionRule build() {
            if (scope.isEmpty() && conditions.isEmpty()) {
                String msg = Messages.getMessage(Messages.JOB_CONFIG_DETECTION_RULE_REQUIRES_SCOPE_OR_CONDITION);
                throw ExceptionsHelper.badRequestException(msg);
            }
            // if actions contain FORCE_TIME_SHIFT, then params must contain RuleParamsForForceTimeShift
            if (actions.contains(RuleAction.FORCE_TIME_SHIFT) && params.getForceTimeShift() == null) {
                String msg = Messages.getMessage(Messages.JOB_CONFIG_DETECTION_RULE_REQUIRES_FORCE_TIME_SHIFT_PARAMS);
                throw ExceptionsHelper.badRequestException(msg);
            }
            // Return error if params must contain RuleParamsForForceTimeShift, but actions do not contain FORCE_TIME_SHIFT
            if (actions.contains(RuleAction.FORCE_TIME_SHIFT) == false && params.getForceTimeShift() != null) {
                String msg = Messages.getMessage(Messages.JOB_CONFIG_DETECTION_RULE_PARAMS_FORCE_TIME_SHIFT_NOT_REQUIRED);
                throw ExceptionsHelper.badRequestException(msg);
            }
            return new DetectionRule(actions, scope, conditions, params);
        }
    }
}
