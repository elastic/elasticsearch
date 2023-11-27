/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public class ConnectorFiltering implements Writeable, ToXContentObject {

    private final FilteringRules active;
    private final String domain;
    private final FilteringRules draft;

    /**
     * Constructs a new ConnectorFiltering instance.
     *
     * @param active The active filtering rules.
     * @param domain The domain associated with the filtering.
     * @param draft  The draft filtering rules.
     */
    public ConnectorFiltering(FilteringRules active, String domain, FilteringRules draft) {
        this.active = active;
        this.domain = domain;
        this.draft = draft;
    }

    public ConnectorFiltering(StreamInput in) throws IOException {
        this.active = new FilteringRules(in);
        this.domain = in.readString();
        this.draft = new FilteringRules(in);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field("active", active);
            builder.field("domain", domain);
            builder.field("draft", draft);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        active.writeTo(out);
        out.writeString(domain);
        draft.writeTo(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConnectorFiltering that = (ConnectorFiltering) o;
        return Objects.equals(active, that.active) && Objects.equals(domain, that.domain) && Objects.equals(draft, that.draft);
    }

    @Override
    public int hashCode() {
        return Objects.hash(active, domain, draft);
    }

    public static class Builder {

        private FilteringRules active;
        private String domain;
        private FilteringRules draft;

        public Builder setActive(FilteringRules active) {
            this.active = active;
            return this;
        }

        public Builder setDomain(String domain) {
            this.domain = domain;
            return this;
        }

        public Builder setDraft(FilteringRules draft) {
            this.draft = draft;
            return this;
        }

        public ConnectorFiltering build() {
            return new ConnectorFiltering(active, domain, draft);
        }
    }

    public static class FilteringRules implements Writeable, ToXContentObject {

        private final Instant advancedSnippetCreatedAt;
        private final Instant advancedSnippetUpdatedAt;
        private final Map<String, Object> advancedSnippetValue;
        private final List<FilteringRule> rules;
        private final List<FilteringValidation> validationErrors;
        private final FilteringValidationState validationState;

        /**
         * Constructs a new FilteringRules instance.
         *
         * @param advancedSnippetCreatedAt The creation timestamp of the advanced snippet.
         * @param advancedSnippetUpdatedAt The update timestamp of the advanced snippet.
         * @param advancedSnippetValue The map of the advanced snippet.
         * @param rules The list of {@link FilteringRule} objects
         * @param validationErrors The list of {@link FilteringValidation} errors for the filtering rules.
         * @param validationState The {@link FilteringValidationState} of the filtering rules.
         */
        public FilteringRules(
            Instant advancedSnippetCreatedAt,
            Instant advancedSnippetUpdatedAt,
            Map<String, Object> advancedSnippetValue,
            List<FilteringRule> rules,
            List<FilteringValidation> validationErrors,
            FilteringValidationState validationState
        ) {
            this.advancedSnippetCreatedAt = advancedSnippetCreatedAt;
            this.advancedSnippetUpdatedAt = advancedSnippetUpdatedAt;
            this.advancedSnippetValue = advancedSnippetValue;
            this.rules = rules;
            this.validationErrors = validationErrors;
            this.validationState = validationState;
        }

        public FilteringRules(StreamInput in) throws IOException {
            this.advancedSnippetCreatedAt = in.readInstant();
            this.advancedSnippetUpdatedAt = in.readInstant();
            this.advancedSnippetValue = in.readMap(StreamInput::readString, StreamInput::readGenericValue);
            this.rules = in.readCollectionAsList(FilteringRule::new);
            this.validationErrors = in.readCollectionAsList(FilteringValidation::new);
            this.validationState = in.readEnum(FilteringValidationState.class);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.startObject("advanced_snippet");
                {
                    builder.field("created_at", advancedSnippetCreatedAt);
                    builder.field("updated_at", advancedSnippetUpdatedAt);
                    builder.field("value", advancedSnippetValue);
                }
                builder.endObject();

                builder.startArray("rules");
                for (FilteringRule rule : rules) {
                    rule.toXContent(builder, params);
                }
                builder.endArray();

                builder.startObject("validation");
                {
                    builder.startArray("errors");
                    for (FilteringValidation error : validationErrors) {
                        error.toXContent(builder, params);
                    }
                    builder.endArray();
                    builder.field("state", validationState.toString());
                }
                builder.endObject();
            }
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInstant(advancedSnippetCreatedAt);
            out.writeInstant(advancedSnippetUpdatedAt);
            out.writeMap(advancedSnippetValue, StreamOutput::writeString, StreamOutput::writeGenericValue);
            out.writeCollection(rules);
            out.writeCollection(validationErrors);
            out.writeEnum(validationState);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FilteringRules that = (FilteringRules) o;
            return Objects.equals(advancedSnippetCreatedAt, that.advancedSnippetCreatedAt)
                && Objects.equals(advancedSnippetUpdatedAt, that.advancedSnippetUpdatedAt)
                && Objects.equals(advancedSnippetValue, that.advancedSnippetValue)
                && Objects.equals(rules, that.rules)
                && Objects.equals(validationErrors, that.validationErrors)
                && validationState == that.validationState;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                advancedSnippetCreatedAt,
                advancedSnippetUpdatedAt,
                advancedSnippetValue,
                rules,
                validationErrors,
                validationState
            );
        }

        public static class Builder {

            private Instant advancedSnippetCreatedAt;
            private Instant advancedSnippetUpdatedAt;
            private Map<String, Object> advancedSnippetValue;
            private List<FilteringRule> rules;
            private List<FilteringValidation> validationErrors;
            private FilteringValidationState validationState;

            public Builder setAdvancedSnippetCreatedAt(Instant advancedSnippetCreatedAt) {
                this.advancedSnippetCreatedAt = advancedSnippetCreatedAt;
                return this;
            }

            public Builder setAdvancedSnippetUpdatedAt(Instant advancedSnippetUpdatedAt) {
                this.advancedSnippetUpdatedAt = advancedSnippetUpdatedAt;
                return this;
            }

            public Builder setAdvancedSnippetValue(Map<String, Object> advancedSnippetValue) {
                this.advancedSnippetValue = advancedSnippetValue;
                return this;
            }

            public Builder setRules(List<FilteringRule> rules) {
                this.rules = rules;
                return this;
            }

            public Builder setValidationErrors(List<FilteringValidation> validationErrors) {
                this.validationErrors = validationErrors;
                return this;
            }

            public Builder setValidationState(FilteringValidationState validationState) {
                this.validationState = validationState;
                return this;
            }

            public FilteringRules build() {
                return new FilteringRules(
                    advancedSnippetCreatedAt,
                    advancedSnippetUpdatedAt,
                    advancedSnippetValue,
                    rules,
                    validationErrors,
                    validationState
                );
            }
        }
    }

    public static class FilteringRule implements Writeable, ToXContentObject {

        private final Instant createdAt;
        private final String field;
        private final String id;
        private final Integer order;
        private final FilteringPolicy policy;
        private final FilteringRuleCondition rule;
        private final Instant updatedAt;
        private final String value;

        /**
         * Constructs a new FilteringRule instance.
         *
         * @param createdAt The creation timestamp of the filtering rule.
         * @param field     The field associated with the filtering rule.
         * @param id        The identifier of the filtering rule.
         * @param order     The order of the filtering rule.
         * @param policy    The {@link FilteringPolicy} of the filtering rule.
         * @param rule      The specific {@link FilteringRuleCondition}
         * @param updatedAt The update timestamp of the filtering rule.
         * @param value     The value associated with the filtering rule.
         */
        public FilteringRule(
            Instant createdAt,
            String field,
            String id,
            Integer order,
            FilteringPolicy policy,
            FilteringRuleCondition rule,
            Instant updatedAt,
            String value
        ) {
            this.createdAt = createdAt;
            this.field = field;
            this.id = id;
            this.order = order;
            this.policy = policy;
            this.rule = rule;
            this.updatedAt = updatedAt;
            this.value = value;
        }

        public FilteringRule(StreamInput in) throws IOException {
            this.createdAt = in.readInstant();
            this.field = in.readString();
            this.id = in.readString();
            this.order = in.readInt();
            this.policy = in.readEnum(FilteringPolicy.class);
            this.rule = in.readEnum(FilteringRuleCondition.class);
            this.updatedAt = in.readInstant();
            this.value = in.readString();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("createdAt", createdAt);
            builder.field("field", field);
            builder.field("id", id);
            builder.field("order", order);
            builder.field("policy", policy.toString());
            builder.field("rule", rule.toString());
            builder.field("updatedAt", updatedAt);
            builder.field("value", value);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInstant(createdAt);
            out.writeString(field);
            out.writeString(id);
            out.writeInt(order);
            out.writeEnum(policy);
            out.writeEnum(rule);
            out.writeInstant(updatedAt);
            out.writeString(value);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FilteringRule that = (FilteringRule) o;
            return Objects.equals(createdAt, that.createdAt)
                && Objects.equals(field, that.field)
                && Objects.equals(id, that.id)
                && Objects.equals(order, that.order)
                && policy == that.policy
                && rule == that.rule
                && Objects.equals(updatedAt, that.updatedAt)
                && Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(createdAt, field, id, order, policy, rule, updatedAt, value);
        }

        public static class Builder {

            private Instant createdAt;
            private String field;
            private String id;
            private Integer order;
            private FilteringPolicy policy;
            private FilteringRuleCondition rule;
            private Instant updatedAt;
            private String value;

            public Builder setCreatedAt(Instant createdAt) {
                this.createdAt = createdAt;
                return this;
            }

            public Builder setField(String field) {
                this.field = field;
                return this;
            }

            public Builder setId(String id) {
                this.id = id;
                return this;
            }

            public Builder setOrder(Integer order) {
                this.order = order;
                return this;
            }

            public Builder setPolicy(FilteringPolicy policy) {
                this.policy = policy;
                return this;
            }

            public Builder setRule(FilteringRuleCondition rule) {
                this.rule = rule;
                return this;
            }

            public Builder setUpdatedAt(Instant updatedAt) {
                this.updatedAt = updatedAt;
                return this;
            }

            public Builder setValue(String value) {
                this.value = value;
                return this;
            }

            public FilteringRule build() {
                return new FilteringRule(createdAt, field, id, order, policy, rule, updatedAt, value);
            }
        }
    }

    public static class FilteringValidation implements Writeable, ToXContentObject {
        private final List<String> ids;
        private final List<String> messages;

        /**
         * Constructs a new FilteringValidation instance.
         *
         * @param ids      The list of identifiers associated with the validation.
         * @param messages The list of messages describing the validation results.
         */
        public FilteringValidation(List<String> ids, List<String> messages) {
            this.ids = ids;
            this.messages = messages;
        }

        public FilteringValidation(StreamInput in) throws IOException {
            this.ids = in.readStringCollectionAsList();
            this.messages = in.readStringCollectionAsList();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.stringListField("ids", ids);
                builder.stringListField("messages", messages);
            }
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringCollection(ids);
            out.writeStringCollection(messages);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FilteringValidation that = (FilteringValidation) o;
            return Objects.equals(ids, that.ids) && Objects.equals(messages, that.messages);
        }

        @Override
        public int hashCode() {
            return Objects.hash(ids, messages);
        }

        public static class Builder {

            private List<String> ids;
            private List<String> messages;

            public Builder setIds(List<String> ids) {
                this.ids = ids;
                return this;
            }

            public Builder setMessages(List<String> messages) {
                this.messages = messages;
                return this;
            }

            public FilteringValidation build() {
                return new FilteringValidation(ids, messages);
            }
        }
    }

    public enum FilteringValidationState {
        EDITED,
        INVALID,
        VALID;

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    public enum FilteringPolicy {
        EXCLUDE,
        INCLUDE;

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    public enum FilteringRuleCondition {
        CONTAINS("contains"),
        ENDS_WITH("ends_with"),
        EQUALS("equals"),
        GT(">"),
        LT("<"),
        REGEX("regex"),
        STARTS_WITH("starts_with");

        private final String value;

        FilteringRuleCondition(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return this.value;
        }
    }

    public static ConnectorFiltering getDefaultConnectorFilteringConfig() {

        Instant currentTimestamp = Instant.now();

        return new ConnectorFiltering.Builder().setActive(
            new FilteringRules.Builder().setAdvancedSnippetCreatedAt(currentTimestamp)
                .setAdvancedSnippetUpdatedAt(currentTimestamp)
                .setAdvancedSnippetValue(Collections.emptyMap())
                .setRules(
                    List.of(
                        new FilteringRule.Builder().setCreatedAt(currentTimestamp)
                            .setField("_")
                            .setId("DEFAULT")
                            .setOrder(0)
                            .setPolicy(FilteringPolicy.INCLUDE)
                            .setRule(FilteringRuleCondition.REGEX)
                            .setUpdatedAt(currentTimestamp)
                            .setValue(".*")
                            .build()
                    )
                )
                .setValidationErrors(Collections.emptyList())
                .setValidationState(FilteringValidationState.VALID)
                .build()
        )
            .setDomain("DEFAULT")
            .setDraft(
                new FilteringRules.Builder().setAdvancedSnippetCreatedAt(currentTimestamp)
                    .setAdvancedSnippetUpdatedAt(currentTimestamp)
                    .setAdvancedSnippetValue(Collections.emptyMap())
                    .setRules(
                        List.of(
                            new FilteringRule.Builder().setCreatedAt(currentTimestamp)
                                .setField("_")
                                .setId("DEFAULT")
                                .setOrder(0)
                                .setPolicy(FilteringPolicy.INCLUDE)
                                .setRule(FilteringRuleCondition.REGEX)
                                .setUpdatedAt(currentTimestamp)
                                .setValue(".*")
                                .build()
                        )
                    )
                    .setValidationErrors(Collections.emptyList())
                    .setValidationState(FilteringValidationState.VALID)
                    .build()
            )
            .build();
    }
}
