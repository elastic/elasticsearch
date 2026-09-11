/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.WildcardQuery;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.CoreMatchers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class WildcardQueryBuilderTests extends AbstractQueryTestCase<WildcardQueryBuilder> {

    @Override
    protected WildcardQueryBuilder doCreateTestQueryBuilder() {
        WildcardQueryBuilder query = randomWildcardQuery();
        if (randomBoolean()) {
            query.rewrite(randomFrom(getRandomRewriteMethod()));
        }
        return query;
    }

    @Override
    protected Map<String, WildcardQueryBuilder> getAlternateVersions() {
        Map<String, WildcardQueryBuilder> alternateVersions = new HashMap<>();
        WildcardQueryBuilder wildcardQuery = randomWildcardQuery();
        String contentString = Strings.format("""
            {
                "wildcard" : {
                    "%s" : "%s"
                }
            }""", wildcardQuery.fieldName(), wildcardQuery.value());
        alternateVersions.put(contentString, wildcardQuery);
        return alternateVersions;
    }

    private static WildcardQueryBuilder randomWildcardQuery() {
        String fieldName = randomFrom(TEXT_FIELD_NAME, TEXT_ALIAS_FIELD_NAME, randomAlphaOfLengthBetween(1, 10));
        String text = randomAlphaOfLengthBetween(1, 10);

        return new WildcardQueryBuilder(fieldName, text);
    }

    @Override
    protected void doAssertLuceneQuery(WildcardQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        String expectedFieldName = expectedFieldName(queryBuilder.fieldName());

        if (expectedFieldName.equals(TEXT_FIELD_NAME)) {
            if (queryBuilder.caseInsensitive()) {
                assertThat(query, instanceOf(AutomatonQuery.class));
            } else {
                assertThat(query, instanceOf(WildcardQuery.class));
                WildcardQuery wildcardQuery = (WildcardQuery) query;

                assertThat(wildcardQuery.getField(), equalTo(expectedFieldName));
                assertThat(wildcardQuery.getTerm().field(), equalTo(expectedFieldName));
                // wildcard queries get normalized
                String text = wildcardQuery.getTerm().text().toLowerCase(Locale.ROOT);
                assertThat(text, equalTo(text));
            }
        } else {
            Query expected = Queries.NO_DOCS_INSTANCE;
            assertEquals(expected, query);
        }
    }

    public void testIllegalArguments() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new WildcardQueryBuilder(null, "text"));
        assertEquals("field name is null or empty", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> new WildcardQueryBuilder("", "text"));
        assertEquals("field name is null or empty", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> new WildcardQueryBuilder("field", null));
        assertEquals("value cannot be null", e.getMessage());
    }

    public void testEmptyValue() throws IOException {
        SearchExecutionContext context = createSearchExecutionContext();
        context.setAllowUnmappedFields(true);
        WildcardQueryBuilder wildcardQueryBuilder = new WildcardQueryBuilder(TEXT_FIELD_NAME, "");
        assertEquals(wildcardQueryBuilder.toQuery(context).getClass(), WildcardQuery.class);
    }

    public void testFromJson() throws IOException {
        String json = """
            {
              "wildcard": {
                "user": {
                  "wildcard": "ki*y",
                  "case_insensitive": true,
                  "boost": 2.0
                }
              }
            }""";
        WildcardQueryBuilder parsed = (WildcardQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertEquals(json, "ki*y", parsed.value());
        assertEquals(json, 2.0, parsed.boost(), 0.0001);
        assertEquals(new WildcardQueryBuilder("user", "ki*y", false).caseInsensitive(true).boost(2.0f), parsed);
    }

    public void testParseFailsWithMultipleFields() throws IOException {
        String json = """
            {
                "wildcard": {
                  "user1": {
                    "wildcard": "ki*y"
                  },
                  "user2": {
                    "wildcard": "ki*y"
                  }
                }
            }""";
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertEquals("[wildcard] query doesn't support multiple fields, found [user1] and [user2]", e.getMessage());

        String shortJson = """
            {
                "wildcard": {
                  "user1": "ki*y",
                  "user2": "ki*y"
                }
            }""";
        e = expectThrows(ParsingException.class, () -> parseQuery(shortJson));
        assertEquals("[wildcard] query doesn't support multiple fields, found [user1] and [user2]", e.getMessage());
    }

    public void testRewriteIndexQueryToMatchNone() throws IOException {
        WildcardQueryBuilder query = new WildcardQueryBuilder("_index", "does_not_exist");
        for (QueryRewriteContext context : new QueryRewriteContext[] { createSearchExecutionContext(), createQueryRewriteContext() }) {
            QueryBuilder rewritten = query.rewrite(context);
            assertThat(rewritten, instanceOf(MatchNoneQueryBuilder.class));
        }
    }

    public void testRewriteIndexQueryNotMatchNone() throws IOException {
        String fullIndexName = getIndex().getName();
        String firstHalfOfIndexName = fullIndexName.substring(0, fullIndexName.length() / 2);
        WildcardQueryBuilder query = new WildcardQueryBuilder("_index", firstHalfOfIndexName + "*");
        for (QueryRewriteContext context : new QueryRewriteContext[] { createSearchExecutionContext(), createQueryRewriteContext() }) {
            QueryBuilder rewritten = query.rewrite(context);
            assertThat(rewritten, instanceOf(MatchAllQueryBuilder.class));
        }
    }

    @Override
    public void testMustRewrite() throws IOException {
        SearchExecutionContext context = createSearchExecutionContext();
        context.setAllowUnmappedFields(true);
        WildcardQueryBuilder queryBuilder = new WildcardQueryBuilder("unmapped_field", "foo");
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> queryBuilder.toQuery(context));
        assertEquals("Rewrite first", e.getMessage());
    }

    public void testCoordinatorTierRewriteToMatchAll() throws IOException {
        QueryBuilder query = new WildcardQueryBuilder("_tier", "data_fr*");
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
        QueryBuilder query = QueryBuilders.boolQuery().mustNot(new WildcardQueryBuilder("_tier", "data_fro*"));
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

    public void testWildcardQueryCircuitBreakerAccounting() throws IOException {
        assertCircuitBreakerAccountsForQuery(new WildcardQueryBuilder(TEXT_FIELD_NAME, "test*pattern*with*wildcards*"));
    }

    public void testWildcardQueryContinuouslyAccountedDuringConstruction() {
        assertCircuitBreakerContinuouslyAccountsDuringConstruction(
            context -> context.getFieldType(TEXT_FIELD_NAME).wildcardQuery("test*pattern*with*many*wildcards*", null, false, context)
        );
    }

    public void testWildcardQueryNoBreakerDipUnderConcurrency() throws Exception {
        assertNoBreakerDipUnderConcurrentConstruction(
            context -> context.getFieldType(TEXT_FIELD_NAME).wildcardQuery("test*pattern*with*many*wildcards*", null, false, context)
        );
    }

    public void testCircuitBreakerTripsWithLowLimit() {
        assertCircuitBreakerTripsOnQueryConstruction("1mb", () -> {
            BoolQueryBuilder boolQuery = new BoolQueryBuilder();
            IntStream.range(0, 100)
                .forEach(i -> boolQuery.should(new WildcardQueryBuilder(TEXT_FIELD_NAME, "*a*b*c*d*e*f*g*h*i*j*k*l*m*n*o*p*" + i + "*")));
            return boolQuery;
        });
    }

    public void testRewriteBreakerEstimate() throws IOException {
        // Setting rewrite increases the estimate by exactly rewrite.length()*2+64.
        // A breaker sized for the estimate without rewrite trips when rewrite is present.
        WildcardQueryBuilder base = new WildcardQueryBuilder(TEXT_FIELD_NAME, "fo*");
        long noRewriteEstimate = base.parseTimeBreakerEstimate();
        String rewriteMethod = "constant_score";
        assertEquals(noRewriteEstimate + rewriteMethod.length() * 2L + 64L, base.rewrite(rewriteMethod).parseTimeBreakerEstimate());
        long limit = noRewriteEstimate;
        LimitedBreaker limitedBreaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofBytes(limit));
        AbstractQueryBuilder.setQueryParsingBreaker(limitedBreaker);
        try {
            WildcardQueryBuilder withRewrite = new WildcardQueryBuilder(TEXT_FIELD_NAME, "fo*").rewrite(rewriteMethod);
            for (XContentType type : new XContentType[] { XContentType.JSON, XContentType.SMILE }) {
                BytesReference bytes = XContentHelper.toXContent(withRewrite, type, false);
                try (XContentParser parser = createParser(type.xContent(), bytes)) {
                    expectThrows(CircuitBreakingException.class, () -> parseQuery(parser));
                }
            }
        } finally {
            AbstractQueryBuilder.setQueryParsingBreaker(null);
        }
    }

    public void testFieldValueBreakerEstimate() throws IOException {
        // WildcardQueryBuilder stores value as String: estimateValue = s.length()*2 + 64.
        // TEXT_FIELD_NAME = "mapped_string" (13 chars): fieldName cost = 13*2+64 = 90.
        // "hi" → estimateValue = 2*2+64 = 68. Small cost = BASELINE + 90 + 68 = 414.
        long baseline = AbstractQueryBuilder.QUERY_BUILDER_SIZE_ESTIMATE_BYTES;
        String shortValue = "hi";
        long smallCost = baseline + 13 * 2L + 64L + shortValue.length() * 2L + 64L;
        long limit = smallCost; // equal to limit does not trip (LimitedBreaker uses strict >)
        LimitedBreaker limitedBreaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofBytes(limit));
        AbstractQueryBuilder.setQueryParsingBreaker(limitedBreaker);
        try {
            WildcardQueryBuilder small = new WildcardQueryBuilder(TEXT_FIELD_NAME, shortValue);
            WildcardQueryBuilder big = new WildcardQueryBuilder(TEXT_FIELD_NAME, "x".repeat(500));
            for (XContentType type : new XContentType[] { XContentType.JSON, XContentType.SMILE }) {
                BytesReference bytes = XContentHelper.toXContent(small, type, false);
                try (XContentParser parser = createParser(type.xContent(), bytes)) {
                    parseQuery(parser); // must not throw
                }
                BytesReference bigBytes = XContentHelper.toXContent(big, type, false);
                try (XContentParser parser = createParser(type.xContent(), bigBytes)) {
                    expectThrows(CircuitBreakingException.class, () -> parseQuery(parser));
                }
            }
        } finally {
            AbstractQueryBuilder.setQueryParsingBreaker(null);
        }
    }
}
