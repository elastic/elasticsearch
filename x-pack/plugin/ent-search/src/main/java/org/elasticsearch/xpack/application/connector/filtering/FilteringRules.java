/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.filtering;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * The {@link FilteringRules} class encapsulates the rules and configurations for filtering operations in a connector.
 * It includes an advanced snippet for complex filtering logic, a list of individual filtering rules, and validation
 * information for these rules.
 */
public class FilteringRules implements Writeable, ToXContentObject {

    private final FilteringAdvancedSnippet advancedSnippet;
    private final List<FilteringRule> rules;

    private final FilteringValidationInfo filteringValidationInfo;

    /**
     * Constructs a new FilteringRules instance.
     *
     * @param advancedSnippet         The {@link FilteringAdvancedSnippet} object.
     * @param rules                   The list of {@link FilteringRule} objects
     * @param filteringValidationInfo The {@link FilteringValidationInfo} object.
     */
    public FilteringRules(
        FilteringAdvancedSnippet advancedSnippet,
        List<FilteringRule> rules,
        FilteringValidationInfo filteringValidationInfo
    ) {
        this.advancedSnippet = advancedSnippet;
        this.rules = rules;
        this.filteringValidationInfo = filteringValidationInfo;
    }

    public FilteringRules(StreamInput in) throws IOException {
        this.advancedSnippet = new FilteringAdvancedSnippet(in);
        this.rules = in.readCollectionAsList(FilteringRule::new);
        this.filteringValidationInfo = new FilteringValidationInfo(in);
    }

    public FilteringAdvancedSnippet getAdvancedSnippet() {
        return advancedSnippet;
    }

    public List<FilteringRule> getRules() {
        return rules;
    }

    public FilteringValidationInfo getFilteringValidationInfo() {
        return filteringValidationInfo;
    }

    public static final ParseField ADVANCED_SNIPPET_FIELD = new ParseField("advanced_snippet");
    public static final ParseField RULES_FIELD = new ParseField("rules");

    public static final ParseField VALIDATION_FIELD = new ParseField("validation");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<FilteringRules, Void> PARSER = new ConstructingObjectParser<>(
        "connector_filtering_rules",
        true,
        args -> new Builder().setAdvancedSnippet((FilteringAdvancedSnippet) args[0])
            .setRules((List<FilteringRule>) args[1])
            .setFilteringValidationInfo((FilteringValidationInfo) args[2])
            .build()
    );

    static {
        PARSER.declareObject(constructorArg(), (p, c) -> FilteringAdvancedSnippet.fromXContent(p), ADVANCED_SNIPPET_FIELD);
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

        private FilteringAdvancedSnippet advancedSnippet;
        private List<FilteringRule> rules;
        private FilteringValidationInfo filteringValidationInfo;

        public Builder setAdvancedSnippet(FilteringAdvancedSnippet advancedSnippet) {
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
