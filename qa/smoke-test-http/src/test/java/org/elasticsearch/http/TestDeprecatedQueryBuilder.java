/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.http;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;

/**
 * A query that performs a <code>match_all</code> query, but with each <em>index</em> touched getting a unique deprecation warning.
 * <p>
 * This makes it easy to test multiple unique responses for a single request.
 */
public class TestDeprecatedQueryBuilder extends AbstractQueryBuilder<TestDeprecatedQueryBuilder> {
    public static final String NAME = "deprecated_match_all";

    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(
        LogManager.getLogger(TestDeprecatedQueryBuilder.class));

    public TestDeprecatedQueryBuilder() {
        // nothing to do
    }

    /**
     * Read from a stream.
     */
    public TestDeprecatedQueryBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        // nothing to do
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME).endObject();
    }

    public static TestDeprecatedQueryBuilder fromXContent(XContentParser parser) throws IOException, ParsingException {
        if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "[{}] query does not have any fields", NAME);
        }

        return new TestDeprecatedQueryBuilder();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        deprecationLogger.deprecated("[{}] query is deprecated, but used on [{}] index", NAME, context.index().getName());

        return Queries.newMatchAllQuery();
    }

    @Override
    public int doHashCode() {
        return 0;
    }

    @Override
    protected boolean doEquals(TestDeprecatedQueryBuilder other) {
        return true;
    }

}
