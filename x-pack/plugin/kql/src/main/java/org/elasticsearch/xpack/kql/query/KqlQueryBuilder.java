/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.query;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.kql.parser.KqlParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;


public class KqlQueryBuilder extends AbstractQueryBuilder<KqlQueryBuilder> {


    private static final Logger log = LogManager.getLogger(KqlQueryBuilder.class);

    public static final String NAME = "kql";
    public static final ParseField QUERY_FIELD = new ParseField("query");

    private static final ConstructingObjectParser<KqlQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(NAME, a -> {
        return new KqlQueryBuilder((String) a[0]);
    });
    static {
        PARSER.declareString(constructorArg(), QUERY_FIELD);
        declareStandardFields(PARSER);
    }

    public static KqlQueryBuilder fromXContent(XContentParser parser) {
        try {
            return PARSER.apply(parser, null);
        } catch (IllegalArgumentException e) {
            throw new ParsingException(parser.getTokenLocation(), e.getMessage(), e);
        }
    }

    private final String query;

    public KqlQueryBuilder(String query) {
        this.query = Objects.requireNonNull(query, "query can not be null");
    }

    public KqlQueryBuilder(StreamInput in) throws IOException {
        super(in);
        query = in.readString();
    }
    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(QUERY_FIELD.getPreferredName(), query);
        boostAndQueryNameToXContent(builder);
        builder.endObject();
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        QueryBuilder queryBuilder = new KqlParser().parseKqlQuery(query, context);

        if (log.isTraceEnabled()) {
            log.trace("KQL query {} translated to Query DSL: {}", query, Strings.toString(queryBuilder));
        }

        return queryBuilder.toQuery(context);
    }

    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(query);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(query);
    }

    @Override
    protected boolean doEquals(KqlQueryBuilder other) {
        return Objects.equals(query, other.query);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        // TODO: Create a transport versions.
        return TransportVersion.current();
    }

}
