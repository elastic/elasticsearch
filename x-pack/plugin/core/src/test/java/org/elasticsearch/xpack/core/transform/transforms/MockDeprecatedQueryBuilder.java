/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;

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

    public MockDeprecatedQueryBuilder() {
    }

    public MockDeprecatedQueryBuilder(StreamInput in) throws IOException {
        super(in);
    }

    public static MockDeprecatedQueryBuilder fromXContent(XContentParser parser) {
        try {
            deprecationLogger.deprecate("deprecated_mock", DEPRECATION_MESSAGE);

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
    protected void doWriteTo(StreamOutput out) throws IOException {
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
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
}
