/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform;

import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

/*
 * Utility test class to write a deprecation message on usage
 */
public class MockDeprecatedQueryBuilder extends AbstractQueryBuilder<MockDeprecatedQueryBuilder> {

    public static final String NAME = "deprecated_match_all";
    public static final String DEPRECATION_MESSAGE = "expected deprecation message from MockDeprecatedQueryBuilder";

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(MockDeprecatedQueryBuilder.class);

    private static final ObjectParser<MockDeprecatedQueryBuilder, Void> PARSER = new ObjectParser<>(NAME, MockDeprecatedQueryBuilder::new);

    static {
        declareStandardFields(PARSER);
    }

    public MockDeprecatedQueryBuilder() {}

    public MockDeprecatedQueryBuilder(StreamInput in) throws IOException {
        super(in);
    }

    public static MockDeprecatedQueryBuilder fromXContent(XContentParser parser) {
        try {
            deprecationLogger.warn(DeprecationCategory.OTHER, "deprecated_mock", DEPRECATION_MESSAGE);

            return PARSER.apply(parser, null);
        } catch (IllegalArgumentException e) {
            throw new ParsingException(parser.getTokenLocation(), e.getMessage(), e);
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {}

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        return Queries.newMatchAllQuery();
    }

    @Override
    protected boolean doEquals(MockDeprecatedQueryBuilder other) {
        return true;
    }

    @Override
    protected int doHashCode() {
        return 0;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_EMPTY;
    }
}
