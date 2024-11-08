/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.core.UpdateForV9;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class CommonTermsQueryBuilder extends AbstractQueryBuilder<CommonTermsQueryBuilder> {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(CommonTermsQueryBuilder.class);
    public static final String COMMON_TERMS_QUERY_DEPRECATION_MSG = "Common Terms Query usage is not supported. "
        + "Use [match] query which can efficiently skip blocks of documents if the total number of hits is not tracked.";

    @UpdateForV9(owner = UpdateForV9.Owner.SEARCH_RELEVANCE) // v7 REST API no longer exists: eliminate ref to RestApiVersion.V_7
    public static ParseField NAME_V7 = new ParseField("common").withAllDeprecated(COMMON_TERMS_QUERY_DEPRECATION_MSG)
        .forRestApiVersion(RestApiVersion.equalTo(RestApiVersion.V_7));

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("common_term_query is not meant to be serialized.");
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {}

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        return null;
    }

    @Override
    protected boolean doEquals(CommonTermsQueryBuilder other) {
        return false;
    }

    @Override
    protected int doHashCode() {
        return 0;
    }

    @Override
    public String getWriteableName() {
        return null;
    }

    public static CommonTermsQueryBuilder fromXContent(XContentParser parser) throws IOException {
        deprecationLogger.compatibleCritical("common_term_query", COMMON_TERMS_QUERY_DEPRECATION_MSG);
        throw new ParsingException(parser.getTokenLocation(), COMMON_TERMS_QUERY_DEPRECATION_MSG);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ZERO;
    }
}
