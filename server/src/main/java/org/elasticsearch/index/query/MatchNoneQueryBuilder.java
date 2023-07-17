/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

/**
 * A query that matches no document.
 */
public class MatchNoneQueryBuilder extends AbstractQueryBuilder<MatchNoneQueryBuilder> {
    public static final String NAME = "match_none";

    private String rewriteReason;

    public MatchNoneQueryBuilder() {}

    public MatchNoneQueryBuilder(String rewriteReason) {
        this.rewriteReason = rewriteReason;
    }

    /**
     * Read from a stream.
     */
    public MatchNoneQueryBuilder(StreamInput in) throws IOException {
        super(in);
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_500_029)) {
            rewriteReason = in.readOptionalString();
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_500_029)) {
            out.writeOptionalString(rewriteReason);
        }
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public static MatchNoneQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String currentFieldName = null;
        XContentParser.Token token;
        String queryName = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        while (((token = parser.nextToken()) != XContentParser.Token.END_OBJECT && token != XContentParser.Token.END_ARRAY)) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "[" + MatchNoneQueryBuilder.NAME + "] query does not support [" + currentFieldName + "]"
                    );
                }
            } else {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "[" + MatchNoneQueryBuilder.NAME + "] unknown token [" + token + "] after [" + currentFieldName + "]"
                );
            }
        }

        MatchNoneQueryBuilder matchNoneQueryBuilder = new MatchNoneQueryBuilder();
        matchNoneQueryBuilder.boost(boost);
        matchNoneQueryBuilder.queryName(queryName);
        return matchNoneQueryBuilder;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        if (rewriteReason != null) {
            return Queries.newMatchNoDocsQuery(rewriteReason);
        }
        return Queries.newMatchNoDocsQuery("User requested \"" + getName() + "\" query.");
    }

    @Override
    protected boolean doEquals(MatchNoneQueryBuilder other) {
        return true;
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
        return TransportVersion.ZERO;
    }
}
