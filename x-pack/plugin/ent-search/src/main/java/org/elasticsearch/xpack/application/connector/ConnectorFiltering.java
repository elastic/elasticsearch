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
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Represents filtering configurations for a connector, encapsulating both active and draft rules.
 * The {@link ConnectorFiltering} class stores the current active filtering rules, a domain associated
 * with these rules, and any draft filtering rules that are yet to be applied.
 */
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

    public FilteringRules getActive() {
        return active;
    }

    public String getDomain() {
        return domain;
    }

    public FilteringRules getDraft() {
        return draft;
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

    public static ConnectorFiltering getDefaultConnectorFilteringConfig() {

        Instant currentTimestamp = Instant.now();

        return new ConnectorFiltering.Builder().setActive(
            new FilteringRules.Builder().setAdvancedSnippet(
                new FilteringAdvancedSnippet.Builder().setAdvancedSnippetCreatedAt(currentTimestamp)
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
                    new FilteringAdvancedSnippet.Builder().setAdvancedSnippetCreatedAt(currentTimestamp)
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
