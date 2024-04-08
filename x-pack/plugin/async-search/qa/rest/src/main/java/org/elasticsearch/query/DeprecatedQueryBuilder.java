/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.query;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class DeprecatedQueryBuilder extends AbstractQueryBuilder<DeprecatedQueryBuilder> {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(DeprecatedQueryBuilder.class);

    public static final String NAME = "deprecated";

    public DeprecatedQueryBuilder() {}

    DeprecatedQueryBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected void doWriteTo(StreamOutput out) {}

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.endObject();
    }

    private static final ObjectParser<DeprecatedQueryBuilder, Void> PARSER = new ObjectParser<>(NAME, DeprecatedQueryBuilder::new);

    public static DeprecatedQueryBuilder fromXContent(XContentParser parser) {
        try {
            PARSER.apply(parser, null);
            return new DeprecatedQueryBuilder();
        } catch (IllegalArgumentException e) {
            throw new ParsingException(parser.getTokenLocation(), e.getMessage(), e);
        }
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) {
        deprecationLogger.warn(DeprecationCategory.QUERIES, "to_query", "[deprecated] query");
        return new MatchAllDocsQuery();
    }

    @Override
    protected boolean doEquals(DeprecatedQueryBuilder other) {
        return false;
    }

    @Override
    protected int doHashCode() {
        return 0;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ZERO;
    }
}
