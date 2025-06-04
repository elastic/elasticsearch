/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.application.connector.filtering.FilteringAdvancedSnippet;
import org.elasticsearch.xpack.application.connector.filtering.FilteringPolicy;
import org.elasticsearch.xpack.application.connector.filtering.FilteringRule;
import org.elasticsearch.xpack.application.connector.filtering.FilteringRuleCondition;
import org.elasticsearch.xpack.application.connector.filtering.FilteringRules;
import org.elasticsearch.xpack.application.connector.filtering.FilteringValidationInfo;
import org.elasticsearch.xpack.application.connector.filtering.FilteringValidationState;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Represents filtering configurations for a connector, encapsulating both active and draft rules.
 * The {@link ConnectorFiltering} class stores the current active filtering rules, a domain associated
 * with these rules, and any draft filtering rules that are yet to be applied.
 */
public class ConnectorFiltering implements Writeable, ToXContentObject {

    private FilteringRules active;
    private final String domain = "DEFAULT"; // Connectors always use DEFAULT domain, users should not modify it via API
    private FilteringRules draft;

    /**
     * Constructs a new ConnectorFiltering instance.
     *
     * @param active The active filtering rules.
     * @param draft  The draft filtering rules.
     */
    public ConnectorFiltering(FilteringRules active, FilteringRules draft) {
        this.active = active;
        this.draft = draft;
    }

    public ConnectorFiltering(StreamInput in) throws IOException {
        this.active = new FilteringRules(in);
        this.draft = new FilteringRules(in);
    }

    public FilteringRules getActive() {
        return active;
    }

    public String getDomain() {
        return domain;
    }

    public FilteringRules getDraft() {
        return draft;
    }

    public ConnectorFiltering setActive(FilteringRules active) {
        this.active = active;
        return this;
    }

    public ConnectorFiltering setDraft(FilteringRules draft) {
        this.draft = draft;
        return this;
    }

    private static final ParseField ACTIVE_FIELD = new ParseField("active");
    private static final ParseField DRAFT_FIELD = new ParseField("draft");

    private static final ConstructingObjectParser<ConnectorFiltering, Void> PARSER = new ConstructingObjectParser<>(
        "connector_filtering",
        true,
        args -> new ConnectorFiltering.Builder().setActive((FilteringRules) args[0]).setDraft((FilteringRules) args[1]).build()
    );

    static {
        PARSER.declareObject(constructorArg(), (p, c) -> FilteringRules.fromXContent(p), ACTIVE_FIELD);
        PARSER.declareObject(constructorArg(), (p, c) -> FilteringRules.fromXContent(p), DRAFT_FIELD);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(ACTIVE_FIELD.getPreferredName(), active);
            builder.field("domain", domain); // We still want to write the DEFAULT domain to the index
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

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<List<ConnectorFiltering>, Void> CONNECTOR_FILTERING_PARSER =
        new ConstructingObjectParser<>(
            "connector_filtering_parser",
            true,
            args -> (List<ConnectorFiltering>) args[0]

        );

    static {
        CONNECTOR_FILTERING_PARSER.declareObjectArray(
            constructorArg(),
            (p, c) -> ConnectorFiltering.fromXContent(p),
            Connector.FILTERING_FIELD
        );
    }

    /**
     * Deserializes the {@link ConnectorFiltering} property from a {@link Connector} byte representation.
     *
     * @param source       Byte representation of the {@link Connector}.
     * @param xContentType {@link XContentType} of the content (e.g., JSON).
     * @return             List of {@link ConnectorFiltering} objects.
     */
    public static List<ConnectorFiltering> fromXContentBytesConnectorFiltering(BytesReference source, XContentType xContentType) {
        try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, source, xContentType)) {
            return CONNECTOR_FILTERING_PARSER.parse(parser, null);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse a connector filtering.", e);
        }
    }

    public static class Builder {

        private FilteringRules active;
        private FilteringRules draft;

        public Builder setActive(FilteringRules active) {
            this.active = active;
            return this;
        }

        public Builder setDraft(FilteringRules draft) {
            this.draft = draft;
            return this;
        }

        public ConnectorFiltering build() {
            return new ConnectorFiltering(active, draft);
        }
    }

    public static FilteringRule getDefaultFilteringRule(Instant timestamp, Integer order) {
        return new FilteringRule.Builder().setCreatedAt(timestamp)
            .setField("_")
            .setId("DEFAULT")
            .setOrder(order)
            .setPolicy(FilteringPolicy.INCLUDE)
            .setRule(FilteringRuleCondition.REGEX)
            .setUpdatedAt(timestamp)
            .setValue(".*")
            .build();
    }

    public static FilteringRule getDefaultFilteringRuleWithOrder(Integer order) {
        return getDefaultFilteringRule(null, order);
    }

    public static boolean isDefaultFilteringRule(FilteringRule rule) {
        return rule.equalsExceptForTimestampsAndOrder(ConnectorFiltering.getDefaultFilteringRuleWithOrder(0));
    }

    /**
     * Sorts the list of {@link List<FilteringRule> } by order, ensuring that the default rule is always the last one.
     * If the rules list is empty, a default rule is added with order 0, otherwise default rule is added with order
     * equal to the last rule's order + 1.
     *
     * @param rules The list of filtering rules to be sorted.
     * @return The sorted list of filtering rules.
     */
    public static List<FilteringRule> sortFilteringRulesByOrder(List<FilteringRule> rules) {
        if (rules.isEmpty()) {
            return List.of(getDefaultFilteringRuleWithOrder(0));
        }

        Optional<FilteringRule> defaultRuleTimeStamp = rules.stream().filter(ConnectorFiltering::isDefaultFilteringRule).findFirst();

        List<FilteringRule> sortedRules = rules.stream()
            .filter(rule -> ConnectorFiltering.isDefaultFilteringRule(rule) == false)
            .sorted(Comparator.comparingInt(FilteringRule::getOrder))
            .collect(Collectors.toList());

        sortedRules.add(
            getDefaultFilteringRule(
                defaultRuleTimeStamp.map(FilteringRule::getCreatedAt).orElse(null),
                sortedRules.isEmpty() ? 0 : sortedRules.get(sortedRules.size() - 1).getOrder() + 1
            )
        );
        return sortedRules;
    }

    public static ConnectorFiltering getDefaultConnectorFilteringConfig() {

        Instant currentTimestamp = Instant.now();

        return new ConnectorFiltering.Builder().setActive(
            new FilteringRules.Builder().setAdvancedSnippet(
                new FilteringAdvancedSnippet.Builder().setAdvancedSnippetCreatedAt(currentTimestamp)
                    .setAdvancedSnippetUpdatedAt(currentTimestamp)
                    .setAdvancedSnippetValue(Collections.emptyMap())
                    .build()
            )
                .setRules(List.of(getDefaultFilteringRule(currentTimestamp, 0)))
                .setFilteringValidationInfo(
                    new FilteringValidationInfo.Builder().setValidationErrors(Collections.emptyList())
                        .setValidationState(FilteringValidationState.VALID)
                        .build()
                )
                .build()
        )
            .setDraft(
                new FilteringRules.Builder().setAdvancedSnippet(
                    new FilteringAdvancedSnippet.Builder().setAdvancedSnippetCreatedAt(currentTimestamp)
                        .setAdvancedSnippetUpdatedAt(currentTimestamp)
                        .setAdvancedSnippetValue(Collections.emptyMap())
                        .build()
                )
                    .setRules(List.of(getDefaultFilteringRule(currentTimestamp, 0)))
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
