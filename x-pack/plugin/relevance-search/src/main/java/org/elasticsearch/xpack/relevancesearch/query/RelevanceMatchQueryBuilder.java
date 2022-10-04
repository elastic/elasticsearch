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
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.CombinedFieldsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.relevancesearch.relevance.curations.CurationSettings;
import org.elasticsearch.xpack.relevancesearch.relevance.curations.CurationsService;
import org.elasticsearch.xpack.relevancesearch.relevance.settings.RelevanceSettings;
import org.elasticsearch.xpack.relevancesearch.relevance.settings.RelevanceSettingsService;
import org.elasticsearch.xpack.searchbusinessrules.PinnedQueryBuilder;
import org.elasticsearch.xpack.relevancesearch.relevance.QueryConfiguration;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Parses and builds relevance_match queries
 */
public class RelevanceMatchQueryBuilder extends AbstractQueryBuilder<RelevanceMatchQueryBuilder> {

    public static final String NAME = "relevance_match";

    private static final ParseField QUERY_FIELD = new ParseField("query");
    private static final ParseField RELEVANCE_SETTINGS_FIELD = new ParseField("relevance_settings");
    private static final ParseField CURATIONS_SETTINGS_FIELD = new ParseField("curations");

    private static final ObjectParser<RelevanceMatchQueryBuilder, Void> PARSER = new ObjectParser<>(NAME, RelevanceMatchQueryBuilder::new);

    private RelevanceSettingsService relevanceSettingsService;
    private CurationsService curationsService;

    private final QueryFieldsResolver queryFieldsResolver = new QueryFieldsResolver();

    static {
        declareStandardFields(PARSER);

        PARSER.declareString(RelevanceMatchQueryBuilder::setQuery, QUERY_FIELD);
        PARSER.declareStringOrNull(RelevanceMatchQueryBuilder::setRelevanceSettingsId, RELEVANCE_SETTINGS_FIELD);
        PARSER.declareStringOrNull(RelevanceMatchQueryBuilder::setCurationsSettingsId, CURATIONS_SETTINGS_FIELD);
    }

    private String query;

    private String relevanceSettingsId;

    private String curationsSettingsId;

    public RelevanceMatchQueryBuilder() {
        super();
    }

    public RelevanceMatchQueryBuilder(RelevanceSettingsService relevanceSettingsService, CurationsService curationsService, StreamInput in)
        throws IOException {
        super(in);

        this.relevanceSettingsService = relevanceSettingsService;
        this.curationsService = curationsService;
        query = in.readString();
    }

    public void setRelevanceSettingsService(RelevanceSettingsService relevanceSettingsService) {
        this.relevanceSettingsService = relevanceSettingsService;
    }

    public static RelevanceMatchQueryBuilder fromXContent(
        final XContentParser parser,
        RelevanceSettingsService relevanceSettingsService,
        CurationsService curationsService
    ) {

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
        builder.setCurationsService(curationsService);

        return builder;
    }

    public void setCurationsService(CurationsService curationsService) {
        this.curationsService = curationsService;
    }

    public void setCurationsSettingsId(String curationsSettingsId) {
        this.curationsSettingsId = curationsSettingsId;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getQuery() {
        return query;
    }

    public void setRelevanceSettingsId(String relevanceSettingsId) {
        this.relevanceSettingsId = relevanceSettingsId;
    }

    @Override
    protected void doWriteTo(final StreamOutput out) throws IOException {
        out.writeString(query);
        out.writeOptionalString(relevanceSettingsId);
        out.writeOptionalString(curationsSettingsId);
    }

    @Override
    protected void doXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject(NAME);

        builder.field(QUERY_FIELD.getPreferredName(), query);
        if (relevanceSettingsId != null) {
            builder.field(RELEVANCE_SETTINGS_FIELD.getPreferredName(), relevanceSettingsId);
        }
        if (curationsSettingsId != null) {
            builder.field(RELEVANCE_SETTINGS_FIELD.getPreferredName(), curationsSettingsId);
        }

        builder.endObject();
    }

    @Override
    protected Query doToQuery(final SearchExecutionContext context) throws IOException {

        Map<String, Float> fieldsAndBoosts;
        if (relevanceSettingsId != null) {
            try {
                RelevanceSettings relevanceSettings = relevanceSettingsService.getRelevanceSettings(relevanceSettingsId);
                QueryConfiguration queryConfiguration = relevanceSettings.getQueryConfiguration();
                fieldsAndBoosts = queryConfiguration.getFieldsAndBoosts();
            } catch (RelevanceSettingsService.RelevanceSettingsNotFoundException e) {
                throw new IllegalArgumentException("[relevance_match] query can't find search settings: " + relevanceSettingsId);
            } catch (RelevanceSettingsService.RelevanceSettingsInvalidException e) {
                throw new IllegalArgumentException("[relevance_match] invalid relevance search settings for: " + relevanceSettingsId);
            }
        } else {
            Collection<String> fields = queryFieldsResolver.getQueryFields(context);
            if (fields.isEmpty()) {
                throw new IllegalArgumentException("[relevance_match] query cannot find text fields in the index");
            }
            fieldsAndBoosts = fields.stream().collect(Collectors.toMap(Function.identity(), (field) -> AbstractQueryBuilder.DEFAULT_BOOST));
        }

        final CombinedFieldsQueryBuilder combinedFieldsBuilder = new CombinedFieldsQueryBuilder(query, fieldsAndBoosts);
        QueryBuilder queryBuilder = combinedFieldsBuilder;

        if (curationsSettingsId != null) {
            try {
                CurationSettings curationSettings = curationsService.getCurationsSettings(curationsSettingsId);
                final boolean conditionMatch = curationSettings.conditions().stream().anyMatch(c -> c.match(this));
                if (conditionMatch) {

                    final PinnedQueryBuilder.Item[] items = curationSettings.pinnedDocs()
                        .stream()
                        .map(docRef -> new PinnedQueryBuilder.Item(docRef.index(), docRef.id()))
                        .toArray(PinnedQueryBuilder.Item[]::new);
                    PinnedQueryBuilder pinnedQueryBuilder = new PinnedQueryBuilder(combinedFieldsBuilder, items);
                    queryBuilder = pinnedQueryBuilder;

                    if (curationSettings.hiddenDocs().isEmpty() == false) {
                        BoolQueryBuilder booleanQueryBuilder = new BoolQueryBuilder();
                        booleanQueryBuilder.should(pinnedQueryBuilder);

                        BoolQueryBuilder hiddenDocsBuilder = new BoolQueryBuilder();
                        for (CurationSettings.DocumentReference hiddenDoc : curationSettings.hiddenDocs()) {
                            BoolQueryBuilder mustQueryBuilder = new BoolQueryBuilder();
                            mustQueryBuilder.must(new TermsQueryBuilder("_index", hiddenDoc.index()));
                            mustQueryBuilder.must(new TermsQueryBuilder("_id", hiddenDoc.id()));

                            hiddenDocsBuilder.mustNot(mustQueryBuilder);
                        }
                        booleanQueryBuilder.filter(hiddenDocsBuilder);

                        queryBuilder = booleanQueryBuilder;
                    }
                }

            } catch (CurationsService.CurationsSettingsNotFoundException e) {
                throw new IllegalArgumentException("[relevance_match] query cannot find curation settings: " + curationsSettingsId);
            }
        }

        return queryBuilder.toQuery(context);
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

}
