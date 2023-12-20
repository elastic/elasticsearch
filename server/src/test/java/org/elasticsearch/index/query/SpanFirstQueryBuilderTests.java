/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.queries.spans.SpanFirstQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;

import static org.elasticsearch.index.query.QueryBuilders.spanTermQuery;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

public class SpanFirstQueryBuilderTests extends AbstractQueryTestCase<SpanFirstQueryBuilder> {
    @Override
    protected SpanFirstQueryBuilder doCreateTestQueryBuilder() {
        SpanTermQueryBuilder[] spanTermQueries = new SpanTermQueryBuilderTests().createSpanTermQueryBuilders(1);
        return new SpanFirstQueryBuilder(spanTermQueries[0], randomIntBetween(0, 1000));
    }

    @Override
    protected SpanFirstQueryBuilder createQueryWithInnerQuery(QueryBuilder queryBuilder) {
        if (queryBuilder instanceof SpanFirstQueryBuilder) {
            return new SpanFirstQueryBuilder((SpanFirstQueryBuilder) queryBuilder, 1);
        }
        return new SpanFirstQueryBuilder(new SpanTermQueryBuilder("field", "value"), 1);
    }

    @Override
    protected void doAssertLuceneQuery(SpanFirstQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        assertThat(query, instanceOf(SpanFirstQuery.class));
    }

    /**
     * test exception on missing `end` and `match` parameter in parser
     */
    public void testParseEnd() throws IOException {
        {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            builder.startObject(SpanFirstQueryBuilder.NAME);
            builder.field("match");
            spanTermQuery("description", "jumped").toXContent(builder, null);
            builder.endObject();
            builder.endObject();

            ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(Strings.toString(builder)));
            assertTrue(e.getMessage().contains("span_first must have [end] set"));
        }
        {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            builder.startObject(SpanFirstQueryBuilder.NAME);
            builder.field("end", 10);
            builder.endObject();
            builder.endObject();

            ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(Strings.toString(builder)));
            assertTrue(e.getMessage().contains("span_first must have [match] span query clause"));
        }
    }

    public void testFromJson() throws IOException {
        String json = """
            {
              "span_first" : {
                "match" : {
                  "span_term" : {
                    "user" : {
                      "value" : "kimchy"
                    }
                  }
                },
                "end" : 3
              }
            }""";

        SpanFirstQueryBuilder parsed = (SpanFirstQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, 3, parsed.end());
        assertEquals(json, "kimchy", ((SpanTermQueryBuilder) parsed.innerQuery()).value());
    }

    public void testFromJsonWithNonDefaultBoostInMatchQuery() {
        String json = """
            {
              "span_first" : {
                "match" : {
                  "span_term" : {
                    "user" : {
                      "value" : "kimchy",
                      "boost" : 2.0
                    }
                  }
                },
                "end" : 3,
                "boost" : 1.0
              }
            }""";

        Exception exception = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertThat(exception.getMessage(), equalTo("span_first [match] as a nested span clause can't have non-default boost value [2.0]"));
    }
}
