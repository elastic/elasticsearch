/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.queries.spans.SpanNotQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;

import static org.elasticsearch.index.query.QueryBuilders.spanNearQuery;
import static org.elasticsearch.index.query.QueryBuilders.spanTermQuery;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class SpanNotQueryBuilderTests extends AbstractQueryTestCase<SpanNotQueryBuilder> {
    @Override
    protected SpanNotQueryBuilder doCreateTestQueryBuilder() {
        SpanTermQueryBuilder[] spanTermQueries = new SpanTermQueryBuilderTests().createSpanTermQueryBuilders(2);
        SpanNotQueryBuilder queryBuilder = new SpanNotQueryBuilder(spanTermQueries[0], spanTermQueries[1]);
        if (randomBoolean()) {
            // also test negative values, they should implicitly be changed to 0
            queryBuilder.dist(randomIntBetween(-2, 10));
        } else {
            if (randomBoolean()) {
                queryBuilder.pre(randomIntBetween(-2, 10));
            }
            if (randomBoolean()) {
                queryBuilder.post(randomIntBetween(-2, 10));
            }
        }
        return queryBuilder;
    }

    @Override
    protected void doAssertLuceneQuery(SpanNotQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        assertThat(query, instanceOf(SpanNotQuery.class));
        SpanNotQuery spanNotQuery = (SpanNotQuery) query;
        assertThat(spanNotQuery.getExclude(), equalTo(queryBuilder.excludeQuery().toQuery(context)));
        assertThat(spanNotQuery.getInclude(), equalTo(queryBuilder.includeQuery().toQuery(context)));
    }

    public void testIllegalArgument() {
        SpanTermQueryBuilder spanTermQuery = new SpanTermQueryBuilder("field", "value");
        expectThrows(IllegalArgumentException.class, () -> new SpanNotQueryBuilder(null, spanTermQuery));
        expectThrows(IllegalArgumentException.class, () -> new SpanNotQueryBuilder(spanTermQuery, null));
    }

    public void testDist() {
        SpanNotQueryBuilder builder = new SpanNotQueryBuilder(
            new SpanTermQueryBuilder("name1", "value1"),
            new SpanTermQueryBuilder("name2", "value2")
        );
        assertThat(builder.pre(), equalTo(0));
        assertThat(builder.post(), equalTo(0));
        builder.dist(-4);
        assertThat(builder.pre(), equalTo(0));
        assertThat(builder.post(), equalTo(0));
        builder.dist(4);
        assertThat(builder.pre(), equalTo(4));
        assertThat(builder.post(), equalTo(4));
    }

    public void testPrePost() {
        SpanNotQueryBuilder builder = new SpanNotQueryBuilder(
            new SpanTermQueryBuilder("name1", "value1"),
            new SpanTermQueryBuilder("name2", "value2")
        );
        assertThat(builder.pre(), equalTo(0));
        assertThat(builder.post(), equalTo(0));
        builder.pre(-4).post(-4);
        assertThat(builder.pre(), equalTo(0));
        assertThat(builder.post(), equalTo(0));
        builder.pre(1).post(2);
        assertThat(builder.pre(), equalTo(1));
        assertThat(builder.post(), equalTo(2));
    }

    /**
     * test correct parsing of `dist` parameter, this should create builder with pre/post set to same value
     */
    public void testParseDist() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.startObject(SpanNotQueryBuilder.NAME);
        builder.field("exclude");
        spanTermQuery("description", "jumped").toXContent(builder, null);
        builder.field("include");
        spanNearQuery(QueryBuilders.spanTermQuery("description", "quick"), 1).addClause(QueryBuilders.spanTermQuery("description", "fox"))
            .toXContent(builder, null);
        builder.field("dist", 3);
        builder.endObject();
        builder.endObject();
        SpanNotQueryBuilder query = (SpanNotQueryBuilder) parseQuery(Strings.toString(builder));
        assertThat(query.pre(), equalTo(3));
        assertThat(query.post(), equalTo(3));
        assertNotNull(query.includeQuery());
        assertNotNull(query.excludeQuery());
    }

    /**
     * test exceptions for three types of broken json, missing include / exclude and both dist and pre/post specified
     */
    public void testParserExceptions() throws IOException {
        {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            builder.startObject(SpanNotQueryBuilder.NAME);
            builder.field("exclude");
            spanTermQuery("description", "jumped").toXContent(builder, null);
            builder.field("dist", 2);
            builder.endObject();
            builder.endObject();

            ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(Strings.toString(builder)));
            assertThat(e.getDetailedMessage(), containsString("span_not must have [include]"));
        }
        {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            builder.startObject(SpanNotQueryBuilder.NAME);
            builder.field("include");
            spanNearQuery(QueryBuilders.spanTermQuery("description", "quick"), 1).addClause(
                QueryBuilders.spanTermQuery("description", "fox")
            ).toXContent(builder, null);
            builder.field("dist", 2);
            builder.endObject();
            builder.endObject();

            ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(Strings.toString(builder)));
            assertThat(e.getDetailedMessage(), containsString("span_not must have [exclude]"));
        }
        {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            builder.startObject(SpanNotQueryBuilder.NAME);
            builder.field("include");
            spanNearQuery(QueryBuilders.spanTermQuery("description", "quick"), 1).addClause(
                QueryBuilders.spanTermQuery("description", "fox")
            ).toXContent(builder, null);
            builder.field("exclude");
            spanTermQuery("description", "jumped").toXContent(builder, null);
            builder.field("dist", 2);
            builder.field("pre", 2);
            builder.endObject();
            builder.endObject();

            ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(Strings.toString(builder)));
            assertThat(e.getDetailedMessage(), containsString("span_not can either use [dist] or [pre] & [post] (or none)"));
        }
    }

    public void testFromJson() throws IOException {
        String json = """
            {
              "span_not" : {
                "include" : {
                  "span_term" : {
                    "field1" : {
                      "value" : "hoya",
                      "boost" : 1.0
                    }
                  }
                },
                "exclude" : {
                  "span_near" : {
                    "clauses" : [ {
                      "span_term" : {
                        "field1" : {
                          "value" : "la",
                          "boost" : 1.0
                        }
                      }
                    }, {
                      "span_term" : {
                        "field1" : {
                          "value" : "hoya",
                          "boost" : 1.0
                        }
                      }
                    } ],
                    "slop" : 0,
                    "in_order" : true,
                    "boost" : 1.0
                  }
                },
                "pre" : 0,
                "post" : 0,
                "boost" : 2.0
              }
            }""";

        SpanNotQueryBuilder parsed = (SpanNotQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, "hoya", ((SpanTermQueryBuilder) parsed.includeQuery()).value());
        assertEquals(json, 2, ((SpanNearQueryBuilder) parsed.excludeQuery()).clauses().size());
        assertEquals(json, 2.0, parsed.boost(), 0.0);
    }

    public void testFromJsonWithNonDefaultBoostInIncludeQuery() {
        String json = """
            {
              "span_not" : {
                "exclude" : {
                  "span_term" : {
                    "field1" : {
                      "value" : "hoya",
                      "boost" : 1.0
                    }
                  }
                },
                "include" : {
                  "span_near" : {
                    "clauses" : [ {
                      "span_term" : {
                        "field1" : {
                          "value" : "la",
                          "boost" : 1.0
                        }
                      }
                    }, {
                      "span_term" : {
                        "field1" : {
                          "value" : "hoya",
                          "boost" : 1.0
                        }
                      }
                    } ],
                    "slop" : 0,
                    "in_order" : true,
                    "boost" : 2.0
                  }
                },
                "pre" : 0,
                "post" : 0,
                "boost" : 1.0
              }
            }""";

        Exception exception = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertThat(exception.getMessage(), equalTo("span_not [include] as a nested span clause can't have non-default boost value [2.0]"));
    }

    public void testFromJsonWithNonDefaultBoostInExcludeQuery() {
        String json = """
            {
              "span_not" : {
                "include" : {
                  "span_term" : {
                    "field1" : {
                      "value" : "hoya",
                      "boost" : 1.0
                    }
                  }
                },
                "exclude" : {
                  "span_near" : {
                    "clauses" : [ {
                      "span_term" : {
                        "field1" : {
                          "value" : "la",
                          "boost" : 1.0
                        }
                      }
                    }, {
                      "span_term" : {
                        "field1" : {
                          "value" : "hoya",
                          "boost" : 1.0
                        }
                      }
                    } ],
                    "slop" : 0,
                    "in_order" : true,
                    "boost" : 2.0
                  }
                },
                "pre" : 0,
                "post" : 0,
                "boost" : 1.0
              }
            }""";

        Exception exception = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertThat(exception.getMessage(), equalTo("span_not [exclude] as a nested span clause can't have non-default boost value [2.0]"));
    }
}
