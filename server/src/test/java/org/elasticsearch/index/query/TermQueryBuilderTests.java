/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.xcontent.json.JsonStringEncoder;
import org.hamcrest.CoreMatchers;

import java.io.IOException;
import java.util.Locale;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;

public class TermQueryBuilderTests extends AbstractTermQueryTestCase<TermQueryBuilder> {

    @Override
    protected TermQueryBuilder doCreateTestQueryBuilder() {
        String fieldName = null;
        Object value;
        switch (randomIntBetween(0, 3)) {
            case 0 -> {
                if (randomBoolean()) {
                    fieldName = BOOLEAN_FIELD_NAME;
                }
                value = randomBoolean();
            }
            case 1 -> {
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
            }
            case 2 -> {
                if (randomBoolean()) {
                    fieldName = INT_FIELD_NAME;
                }
                value = randomInt(10000);
            }
            case 3 -> {
                if (randomBoolean()) {
                    fieldName = DOUBLE_FIELD_NAME;
                }
                value = randomDouble();
            }
            default -> throw new UnsupportedOperationException();
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
        assertThat(
            query,
            either(instanceOf(TermQuery.class)).or(instanceOf(PointRangeQuery.class))
                .or(instanceOf(MatchNoDocsQuery.class))
                .or(instanceOf(AutomatonQuery.class))
        );
        MappedFieldType mapper = context.getFieldType(queryBuilder.fieldName());
        if (query instanceof TermQuery termQuery) {

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
            return mapper.termQueryCaseInsensitive(value, FieldTypeTestCase.MOCK_CONTEXT);
        }
        return mapper.termQuery(value, FieldTypeTestCase.MOCK_CONTEXT);
    }

    public void testTermArray() throws IOException {
        String queryAsString = """
            {
                "term": {
                    "age": [34, 35]
                }
            }""";
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(queryAsString));
        assertEquals("[term] query does not support array of values", e.getMessage());
    }

    public void testFromJson() throws IOException {
        String json = """
            {
              "term" : {
                "exact_value" : {
                  "value" : "Quick Foxes!",
                  "case_insensitive" : true
                }
              }
            }""";

        TermQueryBuilder parsed = (TermQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertEquals(json, "Quick Foxes!", parsed.value());
    }

    public void testGeo() throws Exception {
        TermQueryBuilder query = new TermQueryBuilder(GEO_POINT_FIELD_NAME, "2,3");
        SearchExecutionContext context = createSearchExecutionContext();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> query.toQuery(context));
        assertEquals(
            "Geometry fields do not support exact searching, use dedicated geometry queries instead: [mapped_geo_point]",
            e.getMessage()
        );
    }

    public void testParseFailsWithMultipleFields() {
        String json = """
            {
              "term" : {
                "message1" : {
                  "value" : "this"
                },
                "message2" : {
                  "value" : "this"
                }
              }
            }""";
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertEquals("[term] query doesn't support multiple fields, found [message1] and [message2]", e.getMessage());

        String shortJson = """
            {
              "term" : {
                "message1" : "this",
                "message2" : "this"
              }
            }""";
        e = expectThrows(ParsingException.class, () -> parseQuery(shortJson));
        assertEquals("[term] query doesn't support multiple fields, found [message1] and [message2]", e.getMessage());
    }

    public void testParseFailsWithMultipleValues() {
        String json = """
            {
              "term" : {
                "message1" : {
                  "value" : ["this", "that"]
                }
              }
            }""";
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertEquals(
            "[term] query does not support arrays for value - use a bool query with multiple term clauses "
                + "in the should section or use a Terms query if scoring is not required",
            e.getMessage()
        );
    }

    public void testParseAndSerializeBigInteger() throws IOException {
        String json = """
            {
              "term" : {
                "foo" : {
                  "value" : 80315953321748200608
                }
              }
            }""";
        QueryBuilder parsedQuery = parseQuery(json);
        assertSerialization(parsedQuery);
    }

    public void testRewriteIndexQueryToMatchNone() throws IOException {
        TermQueryBuilder query = QueryBuilders.termQuery("_index", "does_not_exist");
        for (QueryRewriteContext context : new QueryRewriteContext[] { createSearchExecutionContext(), createQueryRewriteContext() }) {
            QueryBuilder rewritten = query.rewrite(context);
            assertThat(rewritten, instanceOf(MatchNoneQueryBuilder.class));
        }
    }

    public void testRewriteIndexQueryToNotMatchNone() throws IOException {
        TermQueryBuilder query = QueryBuilders.termQuery("_index", getIndex().getName());
        for (QueryRewriteContext context : new QueryRewriteContext[] { createSearchExecutionContext(), createQueryRewriteContext() }) {
            QueryBuilder rewritten = query.rewrite(context);
            assertThat(rewritten, instanceOf(MatchAllQueryBuilder.class));
        }
    }

    @Override
    public void testMustRewrite() throws IOException {
        SearchExecutionContext context = createSearchExecutionContext();
        context.setAllowUnmappedFields(true);
        TermQueryBuilder queryBuilder = new TermQueryBuilder("unmapped_field", "foo");
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> queryBuilder.toQuery(context));
        assertEquals("Rewrite first", e.getMessage());
    }

    public void testLongTerm() throws IOException {
        String longTerm = "a".repeat(IndexWriter.MAX_TERM_LENGTH + 1);
        Exception e = expectThrows(IllegalArgumentException.class, () -> parseQuery(String.format(Locale.ROOT, """
            { "term" : { "foo" : "%s" } }""", longTerm)));
        assertThat(e.getMessage(), containsString("term starting with [aaaaa"));
    }

    public void testCoordinatorTierRewriteToMatchAll() throws IOException {
        QueryBuilder query = new TermQueryBuilder("_tier", "data_frozen");
        final String timestampFieldName = "@timestamp";
        long minTimestamp = 1685714000000L;
        long maxTimestamp = 1685715000000L;
        final CoordinatorRewriteContext coordinatorRewriteContext = createCoordinatorRewriteContext(
            new DateFieldMapper.DateFieldType(timestampFieldName),
            minTimestamp,
            maxTimestamp,
            "data_frozen"
        );

        QueryBuilder rewritten = query.rewrite(coordinatorRewriteContext);
        assertThat(rewritten, CoreMatchers.instanceOf(MatchAllQueryBuilder.class));
    }

    public void testCoordinatorTierRewriteToMatchNone() throws IOException {
        QueryBuilder query = QueryBuilders.boolQuery().mustNot(new TermQueryBuilder("_tier", "data_frozen"));
        final String timestampFieldName = "@timestamp";
        long minTimestamp = 1685714000000L;
        long maxTimestamp = 1685715000000L;
        final CoordinatorRewriteContext coordinatorRewriteContext = createCoordinatorRewriteContext(
            new DateFieldMapper.DateFieldType(timestampFieldName),
            minTimestamp,
            maxTimestamp,
            "data_frozen"
        );

        QueryBuilder rewritten = query.rewrite(coordinatorRewriteContext);
        assertThat(rewritten, CoreMatchers.instanceOf(MatchNoneQueryBuilder.class));
    }
}
