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
import org.apache.lucene.search.spans.SpanFirstQuery;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.index.query.QueryBuilders.spanTermQuery;
import static org.hamcrest.CoreMatchers.*;

public class SpanFirstQueryBuilderTest extends BaseQueryTestCase<SpanFirstQueryBuilder> {

    @Override
    protected SpanFirstQueryBuilder doCreateTestQueryBuilder() {
        SpanTermQueryBuilder[] spanTermQueries = new SpanTermQueryBuilderTest().createSpanTermQueryBuilders(1);
        return new SpanFirstQueryBuilder(spanTermQueries[0], randomIntBetween(0, 1000));
    }

    @Override
    protected void doAssertLuceneQuery(SpanFirstQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        assertThat(query, instanceOf(SpanFirstQuery.class));
    }

    @Test
    public void testValidate() {
        int totalExpectedErrors = 0;
        SpanQueryBuilder innerSpanQueryBuilder;
        if (randomBoolean()) {
            if (randomBoolean()) {
                innerSpanQueryBuilder = new SpanTermQueryBuilder("", "test");
            } else {
                innerSpanQueryBuilder = null;
            }
            totalExpectedErrors++;
        } else {
            innerSpanQueryBuilder = new SpanTermQueryBuilder("name", "value");
        }
        int end = randomIntBetween(0, 10);
        if (randomBoolean()) {
            end = randomIntBetween(-10, -1);
            totalExpectedErrors++;
        }
        SpanFirstQueryBuilder queryBuilder = new SpanFirstQueryBuilder(innerSpanQueryBuilder, end);
        assertValidate(queryBuilder, totalExpectedErrors);
    }

    /**
     * test exception on missing `end` and `match` parameter in parser
     */
    @Test
    public void testParseEnd() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.startObject(SpanFirstQueryBuilder.NAME);
        builder.field("match");
        spanTermQuery("description", "jumped").toXContent(builder, null);
        builder.endObject();
        builder.endObject();

        QueryParseContext context = createParseContext();
        XContentParser parser = XContentFactory.xContent(builder.string()).createParser(builder.string());
        context.reset(parser);
        assertQueryHeader(parser, SpanFirstQueryBuilder.NAME);
        try {
            new SpanFirstQueryParser().fromXContent(context);
            fail("missing [end] parameter should raise exception");
        } catch (QueryParsingException e) {
            assertTrue(e.getMessage().contains("spanFirst must have [end] set"));
        }

        builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.startObject(SpanFirstQueryBuilder.NAME);
        builder.field("end", 10);
        builder.endObject();
        builder.endObject();

        context = createParseContext();
        parser = XContentFactory.xContent(builder.string()).createParser(builder.string());
        context.reset(parser);
        assertQueryHeader(parser, SpanFirstQueryBuilder.NAME);
        try {
            new SpanFirstQueryParser().fromXContent(context);
            fail("missing [match] parameter should raise exception");
        } catch (QueryParsingException e) {
            assertTrue(e.getMessage().contains("spanFirst must have [match] span query clause"));
        }
    }
}
