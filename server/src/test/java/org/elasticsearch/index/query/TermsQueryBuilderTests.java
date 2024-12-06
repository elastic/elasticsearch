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
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PointInSetQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.indices.TermsLookup;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.hamcrest.CoreMatchers;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.instanceOf;

public class TermsQueryBuilderTests extends AbstractQueryTestCase<TermsQueryBuilder> {
    private List<Object> randomTerms;
    private String termsPath;

    @Before
    public void randomTerms() {
        List<Object> randomTerms = new ArrayList<>();
        String[] strings = generateRandomStringArray(10, 10, false, true);
        for (String string : strings) {
            randomTerms.add(string);
            if (rarely()) {
                randomTerms.add(null);
            }
        }
        this.randomTerms = randomTerms;
        termsPath = randomAlphaOfLength(10).replace('.', '_');
    }

    @Override
    protected TermsQueryBuilder doCreateTestQueryBuilder() {
        TermsQueryBuilder query;
        // terms query or lookup query
        if (randomBoolean()) {
            // make between 0 and 5 different values of the same type
            String fieldName = randomValueOtherThanMany(
                choice -> choice.equals(GEO_POINT_FIELD_NAME)
                    || choice.equals(BINARY_FIELD_NAME)
                    || choice.equals(GEO_POINT_ALIAS_FIELD_NAME)
                    || choice.equals(INT_RANGE_FIELD_NAME)
                    || choice.equals(DATE_ALIAS_FIELD_NAME)
                    || choice.equals(DATE_RANGE_FIELD_NAME)
                    || choice.equals(DATE_NANOS_FIELD_NAME), // TODO: needs testing for date_nanos type
                AbstractQueryTestCase::getRandomFieldName
            );
            Object[] values = new Object[randomInt(5)];
            for (int i = 0; i < values.length; i++) {
                values[i] = getRandomValueForFieldName(fieldName);
            }
            query = new TermsQueryBuilder(fieldName, values);
        } else {
            // right now the mock service returns us a list of strings
            query = new TermsQueryBuilder(randomBoolean() ? randomAlphaOfLengthBetween(1, 10) : TEXT_FIELD_NAME, randomTermsLookup());
        }
        return query;
    }

    private TermsLookup randomTermsLookup() {
        TermsLookup lookup = new TermsLookup(randomAlphaOfLength(10), randomAlphaOfLength(10), termsPath);
        lookup.routing(randomBoolean() ? randomAlphaOfLength(10) : null);
        return lookup;
    }

    @Override
    protected void doAssertLuceneQuery(TermsQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        if (queryBuilder.termsLookup() == null && (queryBuilder.values() == null || queryBuilder.values().isEmpty())) {
            assertThat(query, instanceOf(MatchNoDocsQuery.class));
        } else if (queryBuilder.termsLookup() != null && randomTerms.isEmpty()) {
            assertThat(query, instanceOf(MatchNoDocsQuery.class));
        } else {
            assertThat(
                query,
                either(instanceOf(TermInSetQuery.class)).or(instanceOf(PointInSetQuery.class))
                    .or(instanceOf(ConstantScoreQuery.class))
                    .or(instanceOf(MatchNoDocsQuery.class))
            );
            // if (true) throw new IllegalArgumentException(randomTerms.toString());
            if (query instanceof ConstantScoreQuery) {
                assertThat(((ConstantScoreQuery) query).getQuery(), instanceOf(BooleanQuery.class));
            }

            // we only do the check below for string fields (otherwise we'd have to decode the values)
            if (queryBuilder.fieldName().equals(INT_FIELD_NAME)
                || queryBuilder.fieldName().equals(INT_ALIAS_FIELD_NAME)
                || queryBuilder.fieldName().equals(DOUBLE_FIELD_NAME)
                || queryBuilder.fieldName().equals(BOOLEAN_FIELD_NAME)
                || queryBuilder.fieldName().equals(DATE_FIELD_NAME)) {
                return;
            }

            // expected returned terms depending on whether we have a terms query or a terms lookup query
            List<Object> terms;
            if (queryBuilder.termsLookup() != null) {
                terms = randomTerms;
            } else {
                terms = queryBuilder.values();
            }

            String fieldName = expectedFieldName(queryBuilder.fieldName());
            Query expected;
            if (context.getFieldType(fieldName) != null) {
                expected = new TermInSetQuery(
                    fieldName,
                    terms.stream().filter(Objects::nonNull).map(Object::toString).map(BytesRef::new).toList()
                );
            } else {
                expected = new MatchNoDocsQuery();
            }
            assertEquals(expected, query);
        }
    }

    public void testEmptyFieldName() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new TermsQueryBuilder(null, "term"));
        assertEquals("field name cannot be null.", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> new TermsQueryBuilder("", "term"));
        assertEquals("field name cannot be null.", e.getMessage());
    }

    public void testEmptyTermsLookup() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new TermsQueryBuilder("field", (TermsLookup) null));
        assertEquals("No value or termsLookup specified for terms query", e.getMessage());
    }

    public void testNullValues() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new TermsQueryBuilder("field", (String[]) null));
        assertThat(e.getMessage(), containsString("No value specified for terms query"));
        e = expectThrows(IllegalArgumentException.class, () -> new TermsQueryBuilder("field", (int[]) null));
        assertThat(e.getMessage(), containsString("No value specified for terms query"));
        e = expectThrows(IllegalArgumentException.class, () -> new TermsQueryBuilder("field", (long[]) null));
        assertThat(e.getMessage(), containsString("No value specified for terms query"));
        e = expectThrows(IllegalArgumentException.class, () -> new TermsQueryBuilder("field", (float[]) null));
        assertThat(e.getMessage(), containsString("No value specified for terms query"));
        e = expectThrows(IllegalArgumentException.class, () -> new TermsQueryBuilder("field", (double[]) null));
        assertThat(e.getMessage(), containsString("No value specified for terms query"));
        e = expectThrows(IllegalArgumentException.class, () -> new TermsQueryBuilder("field", (Object[]) null));
        assertThat(e.getMessage(), containsString("No value specified for terms query"));
    }

    public void testBothValuesAndLookupSet() throws IOException {
        String query = """
            {
              "terms": {
                "field": [
                  "blue",
                  "pill"
                ],
                "field_lookup": {
                  "index": "pills",
                  "type": "red",
                  "id": "3",
                  "path": "white rabbit"
                }
              }
            }""";

        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(query));
        assertThat(e.getMessage(), containsString("[" + TermsQueryBuilder.NAME + "] query does not support more than one field."));
    }

    @Override
    public GetResponse executeGet(GetRequest getRequest) {
        String json;
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject();
            builder.array(termsPath, randomTerms.toArray(Object[]::new));
            builder.endObject();
            json = Strings.toString(builder);
        } catch (IOException ex) {
            throw new ElasticsearchException("boom", ex);
        }
        return new GetResponse(new GetResult(getRequest.index(), getRequest.id(), 0, 1, 0, true, new BytesArray(json), null, null));
    }

    public void testNumeric() throws IOException {
        {
            TermsQueryBuilder builder = new TermsQueryBuilder("foo", new int[] { 1, 3, 4 });
            TermsQueryBuilder copy = (TermsQueryBuilder) assertSerialization(builder);
            List<Object> values = copy.values();
            assertEquals(Arrays.asList(1, 3, 4), values);
        }
        {
            TermsQueryBuilder builder = new TermsQueryBuilder("foo", new double[] { 1, 3, 4 });
            TermsQueryBuilder copy = (TermsQueryBuilder) assertSerialization(builder);
            List<Object> values = copy.values();
            assertEquals(Arrays.asList(1d, 3d, 4d), values);
        }
        {
            TermsQueryBuilder builder = new TermsQueryBuilder("foo", new float[] { 1, 3, 4 });
            TermsQueryBuilder copy = (TermsQueryBuilder) assertSerialization(builder);
            List<Object> values = copy.values();
            assertEquals(Arrays.asList(1f, 3f, 4f), values);
        }
        {
            TermsQueryBuilder builder = new TermsQueryBuilder("foo", new long[] { 1, 3, 4 });
            TermsQueryBuilder copy = (TermsQueryBuilder) assertSerialization(builder);
            List<Object> values = copy.values();
            assertEquals(Arrays.asList(1L, 3L, 4L), values);
        }
    }

    public void testTermsQueryWithMultipleFields() throws IOException {
        String query = Strings.toString(
            XContentFactory.jsonBuilder().startObject().startObject("terms").array("foo", 123).array("bar", 456).endObject().endObject()
        );
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(query));
        assertEquals("[" + TermsQueryBuilder.NAME + "] query does not support multiple fields", e.getMessage());
    }

    public void testFromJson() throws IOException {
        String json = """
            {
              "terms" : {
                "user" : [ "kimchy", "elasticsearch" ],
                "boost" : 1.0
              }
            }""";

        TermsQueryBuilder parsed = (TermsQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertEquals(json, 2, parsed.values().size());
    }

    @Override
    public void testMustRewrite() throws IOException {
        TermsQueryBuilder termsQueryBuilder = new TermsQueryBuilder(TEXT_FIELD_NAME, randomTermsLookup());
        UnsupportedOperationException e = expectThrows(
            UnsupportedOperationException.class,
            () -> termsQueryBuilder.toQuery(createSearchExecutionContext())
        );
        assertEquals("query must be rewritten first", e.getMessage());

        // terms lookup removes null values
        List<Object> nonNullTerms = randomTerms.stream().filter(Objects::nonNull).toList();
        QueryBuilder expected;
        if (nonNullTerms.isEmpty()) {
            expected = new MatchNoneQueryBuilder();
        } else {
            expected = new TermsQueryBuilder(TEXT_FIELD_NAME, nonNullTerms);
        }
        assertEquals(expected, rewriteAndFetch(termsQueryBuilder, createSearchExecutionContext()));
    }

    public void testGeo() throws Exception {
        TermsQueryBuilder query = new TermsQueryBuilder(GEO_POINT_FIELD_NAME, "2,3");
        SearchExecutionContext context = createSearchExecutionContext();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> query.toQuery(context));
        assertEquals(
            "Geometry fields do not support exact searching, use dedicated geometry queries instead: [mapped_geo_point]",
            e.getMessage()
        );
    }

    public void testSerializationFailsUnlessFetched() throws IOException {
        QueryBuilder builder = new TermsQueryBuilder(TEXT_FIELD_NAME, randomTermsLookup());
        QueryBuilder termsQueryBuilder = Rewriteable.rewrite(builder, createSearchExecutionContext());
        IllegalStateException ise = expectThrows(IllegalStateException.class, () -> termsQueryBuilder.writeTo(new BytesStreamOutput(10)));
        assertEquals(ise.getMessage(), "supplier must be null, can't serialize suppliers, missing a rewriteAndFetch?");
        builder = rewriteAndFetch(builder, createSearchExecutionContext());
        builder.writeTo(new BytesStreamOutput(10));
    }

    public void testRewriteIndexQueryToMatchNone() throws IOException {
        TermsQueryBuilder query = new TermsQueryBuilder("_index", "does_not_exist", "also_does_not_exist");
        for (QueryRewriteContext context : new QueryRewriteContext[] { createSearchExecutionContext(), createQueryRewriteContext() }) {
            QueryBuilder rewritten = query.rewrite(context);
            assertThat(rewritten, instanceOf(MatchNoneQueryBuilder.class));
        }
    }

    public void testRewriteIndexQueryToNotMatchNone() throws IOException {
        // At least one name is good
        TermsQueryBuilder query = new TermsQueryBuilder("_index", "does_not_exist", getIndex().getName());
        for (QueryRewriteContext context : new QueryRewriteContext[] { createSearchExecutionContext(), createQueryRewriteContext() }) {
            QueryBuilder rewritten = query.rewrite(context);
            assertThat(rewritten, instanceOf(MatchAllQueryBuilder.class));
        }
    }

    public void testLongTerm() throws IOException {
        String longTerm = "a".repeat(IndexWriter.MAX_TERM_LENGTH + 1);
        Exception e = expectThrows(IllegalArgumentException.class, () -> parseQuery(String.format(Locale.getDefault(), """
            { "terms" : { "foo" : [ "q", "%s" ] } }""", longTerm)));
        assertThat(e.getMessage(), containsString("term starting with [aaaaa"));
    }

    public void testCoordinatorTierRewriteToMatchAll() throws IOException {
        QueryBuilder query = new TermsQueryBuilder("_tier", "data_frozen");
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
        QueryBuilder query = QueryBuilders.boolQuery().mustNot(new TermsQueryBuilder("_tier", "data_frozen"));
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

    @Override
    protected QueryBuilder parseQuery(XContentParser parser) throws IOException {
        QueryBuilder query = super.parseQuery(parser);
        assertThat(query, CoreMatchers.instanceOf(TermsQueryBuilder.class));
        return query;
    }
}
