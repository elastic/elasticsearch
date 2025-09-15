/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.kql.parser.KqlParser;
import org.elasticsearch.xpack.kql.parser.KqlParsingContext;
import org.elasticsearch.xpack.kql.parser.KqlParsingException;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class KqlQueryBuilder extends AbstractQueryBuilder<KqlQueryBuilder> {
    public static final String NAME = "kql";
    public static final ParseField QUERY_FIELD = new ParseField("query");
    public static final ParseField CASE_INSENSITIVE_FIELD = new ParseField("case_insensitive");
    public static final ParseField TIME_ZONE_FIELD = new ParseField("time_zone");
    public static final ParseField DEFAULT_FIELD_FIELD = new ParseField("default_field");

    private static final Logger log = LogManager.getLogger(KqlQueryBuilder.class);
    private static final ConstructingObjectParser<KqlQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(NAME, a -> {
        KqlQueryBuilder kqlQuery = new KqlQueryBuilder((String) a[0]);

        if (a[1] != null) {
            kqlQuery.caseInsensitive((Boolean) a[1]);
        }

        if (a[2] != null) {
            kqlQuery.timeZone((String) a[2]);
        }

        if (a[3] != null) {
            kqlQuery.defaultField((String) a[3]);
        }

        return kqlQuery;
    });

    static {
        PARSER.declareString(constructorArg(), QUERY_FIELD);
        PARSER.declareBoolean(optionalConstructorArg(), CASE_INSENSITIVE_FIELD);
        PARSER.declareString(optionalConstructorArg(), TIME_ZONE_FIELD);
        PARSER.declareString(optionalConstructorArg(), DEFAULT_FIELD_FIELD);
        declareStandardFields(PARSER);
    }

    private final String query;
    private boolean caseInsensitive = true;
    private ZoneId timeZone;
    private String defaultField;

    public KqlQueryBuilder(String query) {
        this.query = Objects.requireNonNull(query, "query can not be null");
    }

    public KqlQueryBuilder(StreamInput in) throws IOException {
        super(in);
        query = in.readString();
        caseInsensitive = in.readBoolean();
        timeZone = in.readOptionalZoneId();
        defaultField = in.readOptionalString();
    }

    public static KqlQueryBuilder fromXContent(XContentParser parser) {
        try {
            return PARSER.apply(parser, null);
        } catch (IllegalArgumentException e) {
            throw new ParsingException(parser.getTokenLocation(), e.getMessage(), e);
        }
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.KQL_QUERY_TECH_PREVIEW;
    }

    public String queryString() {
        return query;
    }

    public boolean caseInsensitive() {
        return caseInsensitive;
    }

    public KqlQueryBuilder caseInsensitive(boolean caseInsensitive) {
        this.caseInsensitive = caseInsensitive;
        return this;
    }

    public ZoneId timeZone() {
        return timeZone;
    }

    public KqlQueryBuilder timeZone(String timeZone) {
        this.timeZone = timeZone != null ? ZoneId.of(timeZone) : null;
        return this;
    }

    public String defaultField() {
        return defaultField;
    }

    public KqlQueryBuilder defaultField(String defaultField) {
        this.defaultField = defaultField;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        {
            builder.field(QUERY_FIELD.getPreferredName(), query);
            builder.field(CASE_INSENSITIVE_FIELD.getPreferredName(), caseInsensitive);

            if (defaultField != null) {
                builder.field(DEFAULT_FIELD_FIELD.getPreferredName(), defaultField);
            }

            if (timeZone != null) {
                builder.field(TIME_ZONE_FIELD.getPreferredName(), timeZone.getId());
            }

            boostAndQueryNameToXContent(builder);
        }
        builder.endObject();
    }

    @Override
    protected QueryBuilder doIndexMetadataRewrite(QueryRewriteContext context) throws IOException {
        try {
            KqlParser parser = new KqlParser();
            QueryBuilder rewrittenQuery = parser.parseKqlQuery(query, createKqlParserContext(context));

            log.trace(() -> Strings.format("KQL query %s translated to Query DSL: %s", query, Strings.toString(rewrittenQuery)));

            return rewrittenQuery;
        } catch (KqlParsingException e) {
            throw new QueryShardException(context, "Failed to parse KQL query [{}]", e, query);
        }
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        throw new IllegalStateException("The query should have been rewritten");
    }

    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(query);
        out.writeBoolean(caseInsensitive);
        out.writeOptionalZoneId(timeZone);
        out.writeOptionalString(defaultField);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(query, caseInsensitive, timeZone, defaultField);
    }

    @Override
    protected boolean doEquals(KqlQueryBuilder other) {
        return Objects.equals(query, other.query)
            && Objects.equals(timeZone, other.timeZone)
            && Objects.equals(defaultField, other.defaultField)
            && caseInsensitive == other.caseInsensitive;
    }

    private KqlParsingContext createKqlParserContext(QueryRewriteContext queryRewriteContext) {
        return KqlParsingContext.builder(queryRewriteContext)
            .caseInsensitive(caseInsensitive)
            .timeZone(timeZone)
            .defaultField(defaultField)
            .build();
    }
}
