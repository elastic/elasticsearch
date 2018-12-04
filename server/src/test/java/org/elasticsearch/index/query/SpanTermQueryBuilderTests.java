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

import com.fasterxml.jackson.core.io.JsonStringEncoder;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

public class SpanTermQueryBuilderTests extends AbstractTermQueryTestCase<SpanTermQueryBuilder> {

    @Override
    protected SpanTermQueryBuilder doCreateTestQueryBuilder() {
        String fieldName = randomFrom(STRING_FIELD_NAME,
            STRING_ALIAS_FIELD_NAME,
            randomAlphaOfLengthBetween(1, 10));

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
    protected void doAssertLuceneQuery(SpanTermQueryBuilder queryBuilder, Query query, SearchContext context) throws IOException {
        assertThat(query, instanceOf(SpanTermQuery.class));
        SpanTermQuery spanTermQuery = (SpanTermQuery) query;

        String expectedFieldName = expectedFieldName(queryBuilder.fieldName);
        assertThat(spanTermQuery.getTerm().field(), equalTo(expectedFieldName));

        MappedFieldType mapper = context.getQueryShardContext().fieldMapper(queryBuilder.fieldName());
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
        String json = "{\n" +
                "  \"span_term\" : {\n" +
                "    \"message1\" : {\n" +
                "      \"term\" : \"this\"\n" +
                "    },\n" +
                "    \"message2\" : {\n" +
                "      \"term\" : \"this\"\n" +
                "    }\n" +
                "  }\n" +
                "}";
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertEquals("[span_term] query doesn't support multiple fields, found [message1] and [message2]", e.getMessage());

        String shortJson = "{\n" +
                "  \"span_term\" : {\n" +
                "    \"message1\" : \"this\",\n" +
                "    \"message2\" : \"this\"\n" +
                "  }\n" +
                "}";
        e = expectThrows(ParsingException.class, () -> parseQuery(shortJson));
        assertEquals("[span_term] query doesn't support multiple fields, found [message1] and [message2]", e.getMessage());
    }

    public void testWithMetaDataField() throws IOException {
        QueryShardContext context = createShardContext();
        for (String field : new String[]{"field1", "field2"}) {
            SpanTermQueryBuilder spanTermQueryBuilder = new SpanTermQueryBuilder(field, "toto");
            Query query = spanTermQueryBuilder.toQuery(context);
            Query expected = new SpanTermQuery(new Term(field, "toto"));
            assertEquals(expected, query);
        }
    }
}
