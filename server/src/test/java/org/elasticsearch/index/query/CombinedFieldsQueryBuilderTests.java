/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.sandbox.search.CombinedFieldQuery;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.index.query.QueryBuilders.combinedFieldsQuery;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.instanceOf;

public class CombinedFieldsQueryBuilderTests extends AbstractQueryTestCase<CombinedFieldsQueryBuilder> {
    private static final String MISSING_WILDCARD_FIELD_NAME = "missing_*";
    private static final String MISSING_FIELD_NAME = "missing";

    @Override
    protected CombinedFieldsQueryBuilder doCreateTestQueryBuilder() {
        Object value = getRandomQueryText();
        String field = randomFrom(TEXT_FIELD_NAME, TEXT_ALIAS_FIELD_NAME, MISSING_FIELD_NAME, MISSING_WILDCARD_FIELD_NAME);
        CombinedFieldsQueryBuilder query = combinedFieldsQuery(value, field);

        if (randomBoolean()) {
            query.field(field);
        } else {
            query.field(field, 1.0f + randomFloat());
        }

        if (randomBoolean()) {
            query.operator(randomFrom(Operator.values()));
        }
        if (randomBoolean()) {
            query.minimumShouldMatch(randomMinimumShouldMatch());
        }
        if (randomBoolean()) {
            query.zeroTermsQuery(randomFrom(ZeroTermsQueryOption.NONE, ZeroTermsQueryOption.ALL));
        }
        if (randomBoolean()) {
            query.autoGenerateSynonymsPhraseQuery(randomBoolean());
        }
        return query;
    }

    /**
     * This check is very light, instead the parsing is tested in detail in {@link CombinedFieldsQueryParsingTests}.
     */
    @Override
    protected void doAssertLuceneQuery(CombinedFieldsQueryBuilder queryBuilder, Query query, SearchExecutionContext context) {
        assertThat(
            query,
            anyOf(
                Arrays.asList(
                    instanceOf(BooleanQuery.class),
                    instanceOf(TermQuery.class),
                    instanceOf(MatchAllDocsQuery.class),
                    instanceOf(MatchNoDocsQuery.class),
                    instanceOf(CombinedFieldQuery.class)
                )
            )
        );
    }

    public void testValuesFromXContent() throws IOException {
        String json = """
            {
              "combined_fields" : {
                "query" : "quick brown fox",
                "fields" : [ "abstract^1.0", "body^1.0", "title^1.0" ],
                "operator" : "AND",
                "zero_terms_query" : "ALL",
                "auto_generate_synonyms_phrase_query" : false,
                "boost" : 2.0
              }
            }""";

        CombinedFieldsQueryBuilder parsed = (CombinedFieldsQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, "quick brown fox", parsed.value());
        assertEquals(json, 3, parsed.fields().size());
        assertEquals(json, Operator.AND, parsed.operator());
        assertEquals(json, 2.0, parsed.boost, 1e-6);
    }

    /**
     * We parse `minimum_should_match` to a String but other queries supporting this parameter also accept integer values and null
     */
    public void testMinumumShouldMatchFromXContent() throws IOException {
        Object[] testValues = new Object[] { 2, "\"2\"", "\"2%\"", null };
        Object[] expectedValues = new Object[] { "2", "2", "2%", null };
        int i = 0;
        for (Object value : testValues) {
            String json = Strings.format("""
                {
                  "combined_fields" : {
                    "query" : "quick brown fox",
                    "minimum_should_match" : %s
                  }
                }""", value);

            CombinedFieldsQueryBuilder parsed = (CombinedFieldsQueryBuilder) parseQuery(json);

            assertEquals(json, "quick brown fox", parsed.value());
            assertEquals(json, expectedValues[i], parsed.minimumShouldMatch());
            i++;
        }
    }

    public void testQueryValueBreakerEstimate() throws IOException {
        // BASELINE + estimateValue(value) + estimateValue(fieldsAndBoosts)
        // value="hi" (String): 2*2+64=68. fieldsAndBoosts={"mapped_string":1.0f}: 32+1*48+(13*2+64)+8=178.
        // small: 256+68+178=502. large: value="x"×500 → 1064; total=1498.
        long fieldsCost = 32L + 48L + 13 * 2L + 64L + 8L; // TEXT_FIELD_NAME = "mapped_string" (13 chars) + Float 8
        long limit = AbstractQueryBuilder.QUERY_BUILDER_SIZE_ESTIMATE_BYTES + (2 * 2L + 64L) + fieldsCost;
        LimitedBreaker breaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofBytes(limit));
        AbstractQueryBuilder.setQueryParsingBreaker(breaker);
        try {
            CombinedFieldsQueryBuilder small = new CombinedFieldsQueryBuilder("hi", TEXT_FIELD_NAME);
            CombinedFieldsQueryBuilder big = new CombinedFieldsQueryBuilder("x".repeat(500), TEXT_FIELD_NAME);
            for (XContentType type : new XContentType[] { XContentType.JSON, XContentType.SMILE }) {
                BytesReference bytes = XContentHelper.toXContent(small, type, false);
                try (XContentParser parser = createParser(type.xContent(), bytes)) {
                    parseQuery(parser);
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
