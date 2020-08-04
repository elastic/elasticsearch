/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.query;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;

public class DeprecatedQueryBuilder extends AbstractQueryBuilder<DeprecatedQueryBuilder> {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger("Deprecated");

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
    protected Query doToQuery(QueryShardContext context) {
        deprecationLogger.deprecate("to_query", "[deprecated] query");
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
}
