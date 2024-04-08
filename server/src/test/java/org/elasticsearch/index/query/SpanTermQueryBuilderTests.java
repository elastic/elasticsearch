/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.index.Term;
import org.apache.lucene.queries.spans.SpanTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.xcontent.json.JsonStringEncoder;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

public class SpanTermQueryBuilderTests extends AbstractTermQueryTestCase<SpanTermQueryBuilder> {

    @Override
    protected SpanTermQueryBuilder doCreateTestQueryBuilder() {
        String fieldName = randomFrom(TEXT_FIELD_NAME, TEXT_ALIAS_FIELD_NAME, randomAlphaOfLengthBetween(1, 10));

        Object value;
        if (frequently()) {
            value = randomAlphaOfLengthBetween(1, 10);
        } else {
            // generate unicode string in 10% of cases
            JsonStringEncoder encoder = JsonStringEncoder.getInstance();
            value = new String(encoder.quoteAsString(randomUnicodeOfLength(10)));
        }
        return createQueryBuilder(fieldName, value);
    }

    @Override
    protected SpanTermQueryBuilder createQueryBuilder(String fieldName, Object value) {
        return new SpanTermQueryBuilder(fieldName, value);
    }

    @Override
    protected void doAssertLuceneQuery(SpanTermQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        assertThat(query, instanceOf(SpanTermQuery.class));
        SpanTermQuery spanTermQuery = (SpanTermQuery) query;

        String expectedFieldName = expectedFieldName(queryBuilder.fieldName);
        assertThat(spanTermQuery.getTerm().field(), equalTo(expectedFieldName));

        MappedFieldType mapper = context.getFieldType(queryBuilder.fieldName());
        if (mapper != null) {
            Term term = ((TermQuery) mapper.termQuery(queryBuilder.value(), null)).getTerm();
            assertThat(spanTermQuery.getTerm(), equalTo(term));
        } else {
            assertThat(spanTermQuery.getTerm().bytes(), equalTo(BytesRefs.toBytesRef(queryBuilder.value())));
        }
    }

    /**
     * @param amount a number of clauses that will be returned
     * @return the array of random {@link SpanTermQueryBuilder} with same field name
     */
    public SpanTermQueryBuilder[] createSpanTermQueryBuilders(int amount) {
        SpanTermQueryBuilder[] clauses = new SpanTermQueryBuilder[amount];
        SpanTermQueryBuilder first = createTestQueryBuilder(false, true);
        clauses[0] = first;
        for (int i = 1; i < amount; i++) {
            // we need same field name in all clauses, so we only randomize value
            SpanTermQueryBuilder spanTermQuery = new SpanTermQueryBuilder(first.fieldName(), getRandomValueForFieldName(first.fieldName()));
            if (randomBoolean()) {
                spanTermQuery.queryName(randomAlphaOfLengthBetween(1, 10));
            }
            clauses[i] = spanTermQuery;
        }
        return clauses;
    }

    public void testFromJson() throws IOException {
        String json = "{    \"span_term\" : { \"user\" : { \"value\" : \"kimchy\", \"boost\" : 2.0 } }}";
        SpanTermQueryBuilder parsed = (SpanTermQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertEquals(json, "kimchy", parsed.value());
        assertEquals(json, 2.0, parsed.boost(), 0.0001);
    }

    public void testParseFailsWithMultipleFields() throws IOException {
        String json = """
            {
              "span_term" : {
                "message1" : {
                  "term" : "this"
                },
                "message2" : {
                  "term" : "this"
                }
              }
            }""";
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertEquals("[span_term] query doesn't support multiple fields, found [message1] and [message2]", e.getMessage());

        String shortJson = """
            {
              "span_term" : {
                "message1" : "this",
                "message2" : "this"
              }
            }""";
        e = expectThrows(ParsingException.class, () -> parseQuery(shortJson));
        assertEquals("[span_term] query doesn't support multiple fields, found [message1] and [message2]", e.getMessage());
    }

    public void testWithMetadataField() throws IOException {
        SearchExecutionContext context = createSearchExecutionContext();
        for (String field : new String[] { "field1", "field2" }) {
            SpanTermQueryBuilder spanTermQueryBuilder = new SpanTermQueryBuilder(field, "toto");
            Query query = spanTermQueryBuilder.toQuery(context);
            Query expected = new SpanTermQuery(new Term(field, "toto"));
            assertEquals(expected, query);
        }
    }
}
