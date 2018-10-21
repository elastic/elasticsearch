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

package org.elasticsearch.index.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 * A query that matches on all documents.
 */
public class MatchAllQueryBuilder extends AbstractQueryBuilder<MatchAllQueryBuilder> {
    public static final String NAME = "match_all";

    public MatchAllQueryBuilder() {
    }

    /**
     * Read from a stream.
     */
    public MatchAllQueryBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected void doWriteTo(StreamOutput out) {
        // only superclass has state
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    private static final ObjectParser<MatchAllQueryBuilder, Void> PARSER = new ObjectParser<>(NAME, MatchAllQueryBuilder::new);

    static {
        declareStandardFields(PARSER);
    }

    public static MatchAllQueryBuilder fromXContent(XContentParser parser) {
        try {
            return PARSER.apply(parser, null);
        } catch (IllegalArgumentException e) {
            throw new ParsingException(parser.getTokenLocation(), e.getMessage(), e);
        }
    }

    @Override
    protected Query doToQuery(QueryShardContext context) {
        return Queries.newMatchAllQuery();
    }

    @Override
    protected boolean doEquals(MatchAllQueryBuilder other) {
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
}
