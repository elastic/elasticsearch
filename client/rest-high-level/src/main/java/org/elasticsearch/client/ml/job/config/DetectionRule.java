/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.job.config;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;

public class DetectionRule implements ToXContentObject {

    public static final ParseField DETECTION_RULE_FIELD = new ParseField("detection_rule");
    public static final ParseField ACTIONS_FIELD = new ParseField("actions");
    public static final ParseField SCOPE_FIELD = new ParseField("scope");
    public static final ParseField CONDITIONS_FIELD = new ParseField("conditions");

    public static final ObjectParser<Builder, Void> PARSER =
        new ObjectParser<>(DETECTION_RULE_FIELD.getPreferredName(), true, Builder::new);

    static {
        PARSER.declareStringArray(Builder::setActions, ACTIONS_FIELD);
        PARSER.declareObject(Builder::setScope, RuleScope.parser(), SCOPE_FIELD);
        PARSER.declareObjectArray(Builder::setConditions, RuleCondition.PARSER, CONDITIONS_FIELD);
    }

    private final EnumSet<RuleAction> actions;
    private final RuleScope scope;
    private final List<RuleCondition> conditions;

    private DetectionRule(EnumSet<RuleAction> actions, RuleScope scope, List<RuleCondition> conditions) {
        this.actions = Objects.requireNonNull(actions);
        this.scope = Objects.requireNonNull(scope);
        this.conditions = Collections.unmodifiableList(conditions);
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
                && Objects.equals(conditions, other.conditions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(actions, scope, conditions);
    }

    public static class Builder {
        private EnumSet<RuleAction> actions = EnumSet.of(RuleAction.SKIP_RESULT);
        private RuleScope scope = new RuleScope();
        private List<RuleCondition> conditions = Collections.emptyList();

        public Builder(RuleScope.Builder scope) {
            this.scope = scope.build();
        }

        public Builder(List<RuleCondition> conditions) {
            this.conditions = Objects.requireNonNull(conditions);
        }

        Builder() {
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

        public Builder setScope(RuleScope scope) {
            this.scope = Objects.requireNonNull(scope);
            return this;
        }

        public Builder setConditions(List<RuleCondition> conditions) {
            this.conditions = Objects.requireNonNull(conditions);
            return this;
        }

        public DetectionRule build() {
            return new DetectionRule(actions, scope, conditions);
        }
    }
}
