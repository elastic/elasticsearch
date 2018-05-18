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

package org.elasticsearch.index.query.plugin;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.plugin.DummyQueryParserPlugin.DummyQuery;

import java.io.IOException;

public class DummyQueryBuilder extends AbstractQueryBuilder<DummyQueryBuilder> {
    public static final String NAME = "dummy";

    public DummyQueryBuilder() {
    }

    public DummyQueryBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        // only the superclass has state
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME).endObject();
    }

    public static DummyQueryBuilder fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();
        assert token == XContentParser.Token.END_OBJECT;
        return new DummyQueryBuilder();
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        return new DummyQuery(context.isFilter());
    }

    @Override
    protected int doHashCode() {
        return 0;
    }

    @Override
    protected boolean doEquals(DummyQueryBuilder other) {
        return true;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}