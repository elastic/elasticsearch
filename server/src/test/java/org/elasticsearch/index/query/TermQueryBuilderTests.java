/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import com.fasterxml.jackson.core.io.JsonStringEncoder;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.either;

public class TermQueryBuilderTests extends AbstractTermQueryTestCase<TermQueryBuilder> {

    @Override
    protected TermQueryBuilder doCreateTestQueryBuilder() {
        String fieldName = null;
        Object value;
        switch (randomIntBetween(0, 3)) {
            case 0:
                if (randomBoolean()) {
                    fieldName = BOOLEAN_FIELD_NAME;
                }
                value = randomBoolean();
                break;
            case 1:
                if (randomBoolean()) {
                    fieldName = randomFrom(TEXT_FIELD_NAME, TEXT_ALIAS_FIELD_NAME);
                }
                if (frequently()) {
                    value = randomAlphaOfLengthBetween(1, 10);
                } else {
                    // generate unicode string in 10% of cases
                    JsonStringEncoder encoder = JsonStringEncoder.getInstance();
                    value = new String(encoder.quoteAsString(randomUnicodeOfLength(10)));
                }
                break;
            case 2:
                if (randomBoolean()) {
                    fieldName = INT_FIELD_NAME;
                }
                value = randomInt(10000);
                break;
            case 3:
                if (randomBoolean()) {
                    fieldName = DOUBLE_FIELD_NAME;
                }
                value = randomDouble();
                break;
            default:
                throw new UnsupportedOperationException();
        }

        if (fieldName == null) {
            fieldName = randomAlphaOfLengthBetween(1, 10);
        }
        return createQueryBuilder(fieldName, value);
    }

    /**
     * @return a TermQuery with random field name and value, optional random boost and queryname
     */
    @Override
    protected TermQueryBuilder createQueryBuilder(String fieldName, Object value) {
        TermQueryBuilder result = new TermQueryBuilder(fieldName, value);
        return result;
    }

    @Override
    protected void doAssertLuceneQuery(TermQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        assertThat(query, either(instanceOf(TermQuery.class)).or(instanceOf(PointRangeQuery.class)).or(instanceOf(MatchNoDocsQuery.class))
            .or(instanceOf(AutomatonQuery.class)));
        MappedFieldType mapper = context.getFieldType(queryBuilder.fieldName());
        if (query instanceof TermQuery) {
            TermQuery termQuery = (TermQuery) query;

            String expectedFieldName = expectedFieldName(queryBuilder.fieldName());
            assertThat(termQuery.getTerm().field(), equalTo(expectedFieldName));

            Term term = ((TermQuery) termQuery(mapper, queryBuilder.value(), queryBuilder.caseInsensitive())).getTerm();
            assertThat(termQuery.getTerm(), equalTo(term));
        } else if (mapper != null) {
            assertEquals(query, termQuery(mapper, queryBuilder.value(), queryBuilder.caseInsensitive()));
        } else {
            assertThat(query, instanceOf(MatchNoDocsQuery.class));
        }
    }

    private Query termQuery(MappedFieldType mapper, Object value, boolean caseInsensitive) {
        if (caseInsensitive) {
            return mapper.termQueryCaseInsensitive(value, null);
        }
        return mapper.termQuery(value, null);
    }

    public void testTermArray() throws IOException {
        String queryAsString = "{\n" +
                "    \"term\": {\n" +
                "        \"age\": [34, 35]\n" +
                "    }\n" +
                "}";
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(queryAsString));
        assertEquals("[term] query does not support array of values", e.getMessage());
    }

    public void testFromJson() throws IOException {
        String json =
                "{\n" +
                "  \"term\" : {\n" +
                "    \"exact_value\" : {\n" +
                "      \"value\" : \"Quick Foxes!\",\n" +
                "      \"case_insensitive\" : true,\n" +
                "      \"boost\" : 1.0\n" +
                "    }\n" +
                "  }\n" +
                "}";

        TermQueryBuilder parsed = (TermQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertEquals(json, "Quick Foxes!", parsed.value());
    }

    public void testGeo() throws Exception {
        TermQueryBuilder query = new TermQueryBuilder(GEO_POINT_FIELD_NAME, "2,3");
        SearchExecutionContext context = createSearchExecutionContext();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> query.toQuery(context));
        assertEquals("Geometry fields do not support exact searching, "
                + "use dedicated geometry queries instead: [mapped_geo_point]", e.getMessage());
    }

    public void testParseFailsWithMultipleFields() {
        String json = "{\n" +
                "  \"term\" : {\n" +
                "    \"message1\" : {\n" +
                "      \"value\" : \"this\"\n" +
                "    },\n" +
                "    \"message2\" : {\n" +
                "      \"value\" : \"this\"\n" +
                "    }\n" +
                "  }\n" +
                "}";
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertEquals("[term] query doesn't support multiple fields, found [message1] and [message2]", e.getMessage());

        String shortJson = "{\n" +
                "  \"term\" : {\n" +
                "    \"message1\" : \"this\",\n" +
                "    \"message2\" : \"this\"\n" +
                "  }\n" +
                "}";
        e = expectThrows(ParsingException.class, () -> parseQuery(shortJson));
        assertEquals("[term] query doesn't support multiple fields, found [message1] and [message2]", e.getMessage());
    }

    public void testParseAndSerializeBigInteger() throws IOException {
        String json = "{\n" +
                "  \"term\" : {\n" +
                "    \"foo\" : {\n" +
                "      \"value\" : 80315953321748200608\n" +
                "    }\n" +
                "  }\n" +
                "}";
        QueryBuilder parsedQuery = parseQuery(json);
        assertSerialization(parsedQuery);
    }

    public void testRewriteIndexQueryToMatchNone() throws IOException {
        TermQueryBuilder query = QueryBuilders.termQuery("_index", "does_not_exist");
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        QueryBuilder rewritten = query.rewrite(searchExecutionContext);
        assertThat(rewritten, instanceOf(MatchNoneQueryBuilder.class));
    }

    public void testRewriteIndexQueryToNotMatchNone() throws IOException {
        TermQueryBuilder query = QueryBuilders.termQuery("_index", getIndex().getName());
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        QueryBuilder rewritten = query.rewrite(searchExecutionContext);
        assertThat(rewritten, instanceOf(MatchAllQueryBuilder.class));
    }

    @Override
    public void testMustRewrite() throws IOException {
        SearchExecutionContext context = createSearchExecutionContext();
        context.setAllowUnmappedFields(true);
        TermQueryBuilder queryBuilder = new TermQueryBuilder("unmapped_field", "foo");
        IllegalStateException e = expectThrows(IllegalStateException.class,
                () -> queryBuilder.toQuery(context));
        assertEquals("Rewrite first", e.getMessage());
    }
}
