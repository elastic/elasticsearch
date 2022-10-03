/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.CombinedFieldsQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.relevancesearch.relevance.RelevanceSettings;
import org.elasticsearch.xpack.relevancesearch.relevance.RelevanceSettingsService;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;

/**
 * Parses and builds relevance_match queries
 */
public class RelevanceMatchQueryBuilder extends AbstractQueryBuilder<RelevanceMatchQueryBuilder> {

    public static final String NAME = "relevance_match";

    private static final ParseField QUERY_FIELD = new ParseField("query");
    private static final ParseField RELEVANCE_SETTINGS_FIELD = new ParseField("relevance_settings");

    private static final ObjectParser<RelevanceMatchQueryBuilder, Void> PARSER = new ObjectParser<>(NAME, RelevanceMatchQueryBuilder::new);

    private RelevanceSettingsService relevanceSettingsService;

    private final QueryFieldsResolver queryFieldsResolver = new QueryFieldsResolver();

    static {
        declareStandardFields(PARSER);

        PARSER.declareString(RelevanceMatchQueryBuilder::setQuery, QUERY_FIELD);
        PARSER.declareStringOrNull(RelevanceMatchQueryBuilder::setRelevanceSettingsId, RELEVANCE_SETTINGS_FIELD);
    }

    private String query;

    private String relevanceSettingsId;

    public RelevanceMatchQueryBuilder() {}

    public RelevanceMatchQueryBuilder(RelevanceSettingsService relevanceSettingsService, StreamInput in) throws IOException {
        super(in);

        this.relevanceSettingsService = relevanceSettingsService;
        query = in.readString();
    }

    public void setRelevanceSettingsService(RelevanceSettingsService relevanceSettingsService) {
        this.relevanceSettingsService = relevanceSettingsService;
    }

    public static RelevanceMatchQueryBuilder fromXContent(final XContentParser parser, RelevanceSettingsService relevanceSettingsService) {

        final RelevanceMatchQueryBuilder builder;
        try {
            builder = PARSER.apply(parser, null);
        } catch (final IllegalArgumentException e) {
            throw new ParsingException(parser.getTokenLocation(), e.getMessage(), e);
        }

        if (builder.query == null) {
            throw new ParsingException(parser.getTokenLocation(), "[relevance_match] requires a query, none specified");
        }

        builder.setRelevanceSettingsService(relevanceSettingsService);

        return builder;
    }

    @Override
    protected void doWriteTo(final StreamOutput out) throws IOException {
        out.writeString(query);
        out.writeOptionalString(relevanceSettingsId);
    }

    @Override
    protected void doXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject(NAME);

        builder.field(QUERY_FIELD.getPreferredName(), query);
        if (relevanceSettingsId != null) {
            builder.field(RELEVANCE_SETTINGS_FIELD.getPreferredName(), relevanceSettingsId);
        }

        builder.endObject();
    }

    @Override
    protected Query doToQuery(final SearchExecutionContext context) throws IOException {

        Collection<String> fields;
        if (relevanceSettingsId != null) {
            try {
                RelevanceSettings relevanceSettings = relevanceSettingsService.getRelevanceSettings(relevanceSettingsId);
                fields = relevanceSettings.getFields();
            } catch (RelevanceSettingsService.RelevanceSettingsNotFoundException e) {
                throw new IllegalArgumentException("[relevance_match] query can't find search settings: " + relevanceSettingsId);
            }
        } else {
            fields = queryFieldsResolver.getQueryFields(context);
            if (fields.isEmpty()) {
                throw new IllegalArgumentException("[relevance_match] query cannot find text fields in the index");
            }
        }

        final CombinedFieldsQueryBuilder builder = new CombinedFieldsQueryBuilder(query, fields.toArray(new String[0]));

        return builder.toQuery(context);
    }

    @Override
    protected boolean doEquals(final RelevanceMatchQueryBuilder other) {
        return Objects.equals(this.query, other.query);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(query);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_8_6_0;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public void setRelevanceSettingsId(String relevanceSettingsId) {
        this.relevanceSettingsId = relevanceSettingsId;
    }
}
