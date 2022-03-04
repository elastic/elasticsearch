/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.queries.spans.SpanContainingQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

public class SpanContainingQueryBuilderTests extends AbstractQueryTestCase<SpanContainingQueryBuilder> {
    @Override
    protected SpanContainingQueryBuilder doCreateTestQueryBuilder() {
        SpanTermQueryBuilder[] spanTermQueries = new SpanTermQueryBuilderTests().createSpanTermQueryBuilders(2);
        return new SpanContainingQueryBuilder(spanTermQueries[0], spanTermQueries[1]);
    }

    @Override
    protected void doAssertLuceneQuery(SpanContainingQueryBuilder queryBuilder, Query query, SearchExecutionContext context) {
        assertThat(query, instanceOf(SpanContainingQuery.class));
    }

    public void testIllegalArguments() {
        SpanTermQueryBuilder spanTermQuery = new SpanTermQueryBuilder("field", "value");
        expectThrows(IllegalArgumentException.class, () -> new SpanContainingQueryBuilder(null, spanTermQuery));
        expectThrows(IllegalArgumentException.class, () -> new SpanContainingQueryBuilder(spanTermQuery, null));
    }

    public void testFromJson() throws IOException {
        String json = """
            {
              "span_containing" : {
                "big" : {
                  "span_near" : {
                    "clauses" : [ {
                      "span_term" : {
                        "field1" : {
                          "value" : "bar",
                          "boost" : 1.0
                        }
                      }
                    }, {
                      "span_term" : {
                        "field1" : {
                          "value" : "baz",
                          "boost" : 1.0
                        }
                      }
                    } ],
                    "slop" : 5,
                    "in_order" : true,
                    "boost" : 1.0
                  }
                },
                "little" : {
                  "span_term" : {
                    "field1" : {
                      "value" : "foo",
                      "boost" : 1.0
                    }
                  }
                },
                "boost" : 2.0
              }
            }""";

        SpanContainingQueryBuilder parsed = (SpanContainingQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, 2, ((SpanNearQueryBuilder) parsed.bigQuery()).clauses().size());
        assertEquals(json, "foo", ((SpanTermQueryBuilder) parsed.littleQuery()).value());
        assertEquals(json, 2.0, parsed.boost(), 0.0);
    }

    public void testFromJsoWithNonDefaultBoostInBigQuery() {
        String json = """
            {
              "span_containing" : {
                "big" : {
                  "span_near" : {
                    "clauses" : [ {
                      "span_term" : {
                        "field1" : {
                          "value" : "bar",
                          "boost" : 1.0
                        }
                      }
                    }, {
                      "span_term" : {
                        "field1" : {
                          "value" : "baz",
                          "boost" : 1.0
                        }
                      }
                    } ],
                    "slop" : 5,
                    "in_order" : true,
                    "boost" : 2.0
                  }
                },
                "little" : {
                  "span_term" : {
                    "field1" : {
                      "value" : "foo",
                      "boost" : 1.0
                    }
                  }
                },
                "boost" : 1.0
              }
            }""";

        Exception exception = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertThat(
            exception.getMessage(),
            equalTo("span_containing [big] as a nested span clause can't have non-default boost value [2.0]")
        );
    }

    public void testFromJsonWithNonDefaultBoostInLittleQuery() {
        String json = """
            {
              "span_containing" : {
                "little" : {
                  "span_near" : {
                    "clauses" : [ {
                      "span_term" : {
                        "field1" : {
                          "value" : "bar",
                          "boost" : 1.0
                        }
                      }
                    }, {
                      "span_term" : {
                        "field1" : {
                          "value" : "baz",
                          "boost" : 1.0
                        }
                      }
                    } ],
                    "slop" : 5,
                    "in_order" : true,
                    "boost" : 2.0
                  }
                },
                "big" : {
                  "span_term" : {
                    "field1" : {
                      "value" : "foo",
                      "boost" : 1.0
                    }
                  }
                },
                "boost" : 1.0
              }
            }""";

        Exception exception = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertThat(
            exception.getMessage(),
            equalTo("span_containing [little] as a nested span clause can't have non-default boost value [2.0]")
        );
    }
}
