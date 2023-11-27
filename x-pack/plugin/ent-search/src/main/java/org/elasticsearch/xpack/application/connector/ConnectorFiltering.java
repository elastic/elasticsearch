/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

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

    private static final ParseField ACTIVE_FIELD = new ParseField("active");
    private static final ParseField DOMAIN_FIELD = new ParseField("domain");
    private static final ParseField DRAFT_FIELD = new ParseField("draft");

    private static final ConstructingObjectParser<ConnectorFiltering, Void> PARSER = new ConstructingObjectParser<>(
        "connector_filtering",
        true,
        args -> new ConnectorFiltering.Builder().setActive((FilteringRules) args[0])
            .setDomain((String) args[1])
            .setDraft((FilteringRules) args[2])
            .build()
    );

    static {
        PARSER.declareObject(constructorArg(), (p, c) -> FilteringRules.fromXContent(p), ACTIVE_FIELD);
        PARSER.declareString(constructorArg(), DOMAIN_FIELD);
        PARSER.declareObject(constructorArg(), (p, c) -> FilteringRules.fromXContent(p), DRAFT_FIELD);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(ACTIVE_FIELD.getPreferredName(), active);
            builder.field(DOMAIN_FIELD.getPreferredName(), domain);
            builder.field(DRAFT_FIELD.getPreferredName(), draft);
        }
        builder.endObject();
        return builder;
    }

    public static ConnectorFiltering fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public static ConnectorFiltering fromXContentBytes(BytesReference source, XContentType xContentType) {
        try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, source, xContentType)) {
            return ConnectorFiltering.fromXContent(parser);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse a connector filtering.", e);
        }
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

        private final AdvancedSnippet advancedSnippet;
        private final List<FilteringRule> rules;

        private final FilteringValidationInfo filteringValidationInfo;

        /**
         * Constructs a new FilteringRules instance.
         *
         * @param advancedSnippet The {@link AdvancedSnippet} object.
         * @param rules The list of {@link FilteringRule} objects
         * @param filteringValidationInfo The {@link FilteringValidationInfo} object.
         */
        public FilteringRules(AdvancedSnippet advancedSnippet, List<FilteringRule> rules, FilteringValidationInfo filteringValidationInfo) {
            this.advancedSnippet = advancedSnippet;
            this.rules = rules;
            this.filteringValidationInfo = filteringValidationInfo;
        }

        public FilteringRules(StreamInput in) throws IOException {
            this.advancedSnippet = new AdvancedSnippet(in);
            this.rules = in.readCollectionAsList(FilteringRule::new);
            this.filteringValidationInfo = new FilteringValidationInfo(in);
        }

        private static final ParseField ADVANCED_SNIPPET_FIELD = new ParseField("advanced_snippet");
        private static final ParseField RULES_FIELD = new ParseField("rules");
        private static final ParseField VALIDATION_FIELD = new ParseField("validation");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<FilteringRules, Void> PARSER = new ConstructingObjectParser<>(
            "connector_filtering_rules",
            true,
            args -> new FilteringRules.Builder().setAdvancedSnippet((AdvancedSnippet) args[0])
                .setRules((List<FilteringRule>) args[1])
                .setFilteringValidationInfo((FilteringValidationInfo) args[2])
                .build()
        );

        static {
            PARSER.declareObject(constructorArg(), (p, c) -> AdvancedSnippet.fromXContent(p), ADVANCED_SNIPPET_FIELD);
            PARSER.declareObjectArray(constructorArg(), (p, c) -> FilteringRule.fromXContent(p), RULES_FIELD);
            PARSER.declareObject(constructorArg(), (p, c) -> FilteringValidationInfo.fromXContent(p), VALIDATION_FIELD);

        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field(ADVANCED_SNIPPET_FIELD.getPreferredName(), advancedSnippet);
                builder.xContentList(RULES_FIELD.getPreferredName(), rules);
                builder.field(VALIDATION_FIELD.getPreferredName(), filteringValidationInfo);
            }
            builder.endObject();
            return builder;
        }

        public static FilteringRules fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            advancedSnippet.writeTo(out);
            out.writeCollection(rules);
            filteringValidationInfo.writeTo(out);

        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FilteringRules that = (FilteringRules) o;
            return Objects.equals(advancedSnippet, that.advancedSnippet)
                && Objects.equals(rules, that.rules)
                && Objects.equals(filteringValidationInfo, that.filteringValidationInfo);
        }

        @Override
        public int hashCode() {
            return Objects.hash(advancedSnippet, rules, filteringValidationInfo);
        }

        public static class Builder {

            private AdvancedSnippet advancedSnippet;
            private List<FilteringRule> rules;
            private FilteringValidationInfo filteringValidationInfo;

            public Builder setAdvancedSnippet(AdvancedSnippet advancedSnippet) {
                this.advancedSnippet = advancedSnippet;
                return this;
            }

            public Builder setRules(List<FilteringRule> rules) {
                this.rules = rules;
                return this;
            }

            public Builder setFilteringValidationInfo(FilteringValidationInfo filteringValidationInfo) {
                this.filteringValidationInfo = filteringValidationInfo;
                return this;
            }

            public FilteringRules build() {
                return new FilteringRules(advancedSnippet, rules, filteringValidationInfo);
            }
        }
    }

    public static class AdvancedSnippet implements Writeable, ToXContentObject {

        private final Instant advancedSnippetCreatedAt;
        private final Instant advancedSnippetUpdatedAt;
        private final Map<String, Object> advancedSnippetValue;

        /**
         * @param advancedSnippetCreatedAt The creation timestamp of the advanced snippet.
         * @param advancedSnippetUpdatedAt The update timestamp of the advanced snippet.
         * @param advancedSnippetValue     The map of the advanced snippet.
         */
        private AdvancedSnippet(
            Instant advancedSnippetCreatedAt,
            Instant advancedSnippetUpdatedAt,
            Map<String, Object> advancedSnippetValue
        ) {
            this.advancedSnippetCreatedAt = advancedSnippetCreatedAt;
            this.advancedSnippetUpdatedAt = advancedSnippetUpdatedAt;
            this.advancedSnippetValue = advancedSnippetValue;
        }

        public AdvancedSnippet(StreamInput in) throws IOException {
            this.advancedSnippetCreatedAt = in.readInstant();
            this.advancedSnippetUpdatedAt = in.readInstant();
            this.advancedSnippetValue = in.readMap(StreamInput::readString, StreamInput::readGenericValue);
        }

        private static final ParseField CREATED_AT_FIELD = new ParseField("created_at");
        private static final ParseField UPDATED_AT_FIELD = new ParseField("updated_at");
        private static final ParseField VALUE_FIELD = new ParseField("value");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<AdvancedSnippet, Void> PARSER = new ConstructingObjectParser<>(
            "connector_filtering_advanced_snippet",
            true,
            args -> new Builder().setAdvancedSnippetCreatedAt((Instant) args[0])
                .setAdvancedSnippetUpdatedAt((Instant) args[1])
                .setAdvancedSnippetValue((Map<String, Object>) args[2])
                .build()
        );

        static {
            PARSER.declareField(constructorArg(), (p, c) -> Instant.parse(p.text()), CREATED_AT_FIELD, ObjectParser.ValueType.STRING);
            PARSER.declareField(constructorArg(), (p, c) -> Instant.parse(p.text()), UPDATED_AT_FIELD, ObjectParser.ValueType.STRING);
            PARSER.declareField(constructorArg(), (p, c) -> p.map(), VALUE_FIELD, ObjectParser.ValueType.OBJECT);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field(CREATED_AT_FIELD.getPreferredName(), advancedSnippetCreatedAt);
                builder.field(UPDATED_AT_FIELD.getPreferredName(), advancedSnippetUpdatedAt);
                builder.field(VALUE_FIELD.getPreferredName(), advancedSnippetValue);
            }
            builder.endObject();
            return builder;
        }

        public static AdvancedSnippet fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInstant(advancedSnippetCreatedAt);
            out.writeInstant(advancedSnippetUpdatedAt);
            out.writeMap(advancedSnippetValue, StreamOutput::writeString, StreamOutput::writeGenericValue);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AdvancedSnippet that = (AdvancedSnippet) o;
            return Objects.equals(advancedSnippetCreatedAt, that.advancedSnippetCreatedAt)
                && Objects.equals(advancedSnippetUpdatedAt, that.advancedSnippetUpdatedAt)
                && Objects.equals(advancedSnippetValue, that.advancedSnippetValue);
        }

        @Override
        public int hashCode() {
            return Objects.hash(advancedSnippetCreatedAt, advancedSnippetUpdatedAt, advancedSnippetValue);
        }

        public static class Builder {

            private Instant advancedSnippetCreatedAt;
            private Instant advancedSnippetUpdatedAt;
            private Map<String, Object> advancedSnippetValue;

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

            public AdvancedSnippet build() {
                return new AdvancedSnippet(advancedSnippetCreatedAt, advancedSnippetUpdatedAt, advancedSnippetValue);
            }
        }
    }

    public static class FilteringValidationInfo implements Writeable, ToXContentObject {

        private final List<FilteringValidation> validationErrors;
        private final FilteringValidationState validationState;

        /**
         * @param validationErrors The list of {@link FilteringValidation} errors for the filtering rules.
         * @param validationState  The {@link FilteringValidationState} of the filtering rules.
         */
        public FilteringValidationInfo(List<FilteringValidation> validationErrors, FilteringValidationState validationState) {
            this.validationErrors = validationErrors;
            this.validationState = validationState;
        }

        public FilteringValidationInfo(StreamInput in) throws IOException {
            this.validationErrors = in.readCollectionAsList(FilteringValidation::new);
            this.validationState = in.readEnum(FilteringValidationState.class);
        }

        private static final ParseField ERRORS_FIELD = new ParseField("errors");
        private static final ParseField STATE_FIELD = new ParseField("state");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<FilteringValidationInfo, Void> PARSER = new ConstructingObjectParser<>(
            "filtering_validation_info",
            true,
            args -> new Builder().setValidationErrors((List<FilteringValidation>) args[0])
                .setValidationState((FilteringValidationState) args[1])
                .build()
        );

        static {
            PARSER.declareObjectArray(constructorArg(), (p, c) -> FilteringValidation.fromXContent(p), ERRORS_FIELD);
            PARSER.declareField(
                constructorArg(),
                (p, c) -> FilteringValidationState.filteringValidationState(p.text()),
                STATE_FIELD,
                ObjectParser.ValueType.STRING
            );
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field(ERRORS_FIELD.getPreferredName(), validationErrors);
                builder.field(STATE_FIELD.getPreferredName(), validationState);
            }
            builder.endObject();
            return builder;
        }

        public static FilteringValidationInfo fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(validationErrors);
            out.writeEnum(validationState);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FilteringValidationInfo that = (FilteringValidationInfo) o;
            return Objects.equals(validationErrors, that.validationErrors) && validationState == that.validationState;
        }

        @Override
        public int hashCode() {
            return Objects.hash(validationErrors, validationState);
        }

        public static class Builder {

            private List<FilteringValidation> validationErrors;
            private FilteringValidationState validationState;

            public Builder setValidationErrors(List<FilteringValidation> validationErrors) {
                this.validationErrors = validationErrors;
                return this;
            }

            public Builder setValidationState(FilteringValidationState validationState) {
                this.validationState = validationState;
                return this;
            }

            public FilteringValidationInfo build() {
                return new FilteringValidationInfo(validationErrors, validationState);
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

        private static final ParseField CREATED_AT_FIELD = new ParseField("created_at");
        private static final ParseField FIELD_FIELD = new ParseField("field");
        private static final ParseField ID_FIELD = new ParseField("id");
        private static final ParseField ORDER_FIELD = new ParseField("order");
        private static final ParseField POLICY_FIELD = new ParseField("policy");
        private static final ParseField RULE_FIELD = new ParseField("rule");
        private static final ParseField UPDATED_AT_FIELD = new ParseField("updated_at");
        private static final ParseField VALUE_FIELD = new ParseField("value");

        private static final ConstructingObjectParser<FilteringRule, Void> PARSER = new ConstructingObjectParser<>(
            "connector_filtering_rule",
            true,
            args -> new FilteringRule.Builder().setCreatedAt((Instant) args[0])
                .setField((String) args[1])
                .setId((String) args[2])
                .setOrder((Integer) args[3])
                .setPolicy((FilteringPolicy) args[4])
                .setRule((FilteringRuleCondition) args[5])
                .setUpdatedAt((Instant) args[6])
                .setValue((String) args[7])
                .build()
        );

        static {
            PARSER.declareField(constructorArg(), (p, c) -> Instant.parse(p.text()), CREATED_AT_FIELD, ObjectParser.ValueType.STRING);
            PARSER.declareString(constructorArg(), FIELD_FIELD);
            PARSER.declareString(constructorArg(), ID_FIELD);
            PARSER.declareInt(constructorArg(), ORDER_FIELD);
            PARSER.declareField(
                constructorArg(),
                (p, c) -> FilteringPolicy.filteringPolicy(p.text()),
                POLICY_FIELD,
                ObjectParser.ValueType.STRING
            );
            PARSER.declareField(
                constructorArg(),
                (p, c) -> FilteringRuleCondition.filteringRuleCondition(p.text()),
                RULE_FIELD,
                ObjectParser.ValueType.STRING
            );
            PARSER.declareField(constructorArg(), (p, c) -> Instant.parse(p.text()), UPDATED_AT_FIELD, ObjectParser.ValueType.STRING);
            PARSER.declareString(constructorArg(), VALUE_FIELD);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(CREATED_AT_FIELD.getPreferredName(), createdAt);
            builder.field(FIELD_FIELD.getPreferredName(), field);
            builder.field(ID_FIELD.getPreferredName(), id);
            builder.field(ORDER_FIELD.getPreferredName(), order);
            builder.field(POLICY_FIELD.getPreferredName(), policy.toString());
            builder.field(RULE_FIELD.getPreferredName(), rule.toString());
            builder.field(UPDATED_AT_FIELD.getPreferredName(), updatedAt);
            builder.field(VALUE_FIELD.getPreferredName(), value);
            builder.endObject();
            return builder;
        }

        public static FilteringRule fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
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

        private static final ParseField IDS_FIELD = new ParseField("ids");
        private static final ParseField MESSAGES_FIELD = new ParseField("messages");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<FilteringValidation, Void> PARSER = new ConstructingObjectParser<>(
            "connector_filtering_validation",
            true,
            args -> new FilteringValidation.Builder().setIds((List<String>) args[0]).setMessages((List<String>) args[1]).build()
        );

        static {
            PARSER.declareStringArray(constructorArg(), IDS_FIELD);
            PARSER.declareStringArray(constructorArg(), MESSAGES_FIELD);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.stringListField(IDS_FIELD.getPreferredName(), ids);
                builder.stringListField(MESSAGES_FIELD.getPreferredName(), messages);
            }
            return builder;
        }

        public static FilteringValidation fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
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

        public static FilteringValidationState filteringValidationState(String validationState) {
            for (FilteringValidationState filteringValidationState : FilteringValidationState.values()) {
                if (filteringValidationState.name().equalsIgnoreCase(validationState)) {
                    return filteringValidationState;
                }
            }
            throw new IllegalArgumentException("Unknown FilteringValidationState: " + validationState);
        }
    }

    public enum FilteringPolicy {
        EXCLUDE,
        INCLUDE;

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }

        public static FilteringPolicy filteringPolicy(String policy) {
            for (FilteringPolicy filteringPolicy : FilteringPolicy.values()) {
                if (filteringPolicy.name().equalsIgnoreCase(policy)) {
                    return filteringPolicy;
                }
            }
            throw new IllegalArgumentException("Unknown FilteringPolicy: " + policy);
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

        public static FilteringRuleCondition filteringRuleCondition(String condition) {
            for (FilteringRuleCondition filteringRuleCondition : FilteringRuleCondition.values()) {
                if (filteringRuleCondition.name().equalsIgnoreCase(condition)) {
                    return filteringRuleCondition;
                }
            }
            throw new IllegalArgumentException("Unknown FilteringRuleCondition: " + condition);
        }
    }

    public static ConnectorFiltering getDefaultConnectorFilteringConfig() {

        Instant currentTimestamp = Instant.now();

        return new ConnectorFiltering.Builder().setActive(
            new FilteringRules.Builder().setAdvancedSnippet(
                new AdvancedSnippet.Builder().setAdvancedSnippetCreatedAt(currentTimestamp)
                    .setAdvancedSnippetUpdatedAt(currentTimestamp)
                    .setAdvancedSnippetValue(Collections.emptyMap())
                    .build()
            )
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
                .setFilteringValidationInfo(
                    new FilteringValidationInfo.Builder().setValidationErrors(Collections.emptyList())
                        .setValidationState(FilteringValidationState.VALID)
                        .build()
                )
                .build()
        )
            .setDomain("DEFAULT")
            .setDraft(
                new FilteringRules.Builder().setAdvancedSnippet(
                    new AdvancedSnippet.Builder().setAdvancedSnippetCreatedAt(currentTimestamp)
                        .setAdvancedSnippetUpdatedAt(currentTimestamp)
                        .setAdvancedSnippetValue(Collections.emptyMap())
                        .build()
                )
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
                    .setFilteringValidationInfo(
                        new FilteringValidationInfo.Builder().setValidationErrors(Collections.emptyList())
                            .setValidationState(FilteringValidationState.VALID)
                            .build()
                    )
                    .build()
            )
            .build();
    }
}
