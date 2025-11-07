/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public abstract class AbstractPrefilteredQueryTestCase<QB extends AbstractQueryBuilder<QB> & PrefilteredQuery<QB>> extends
    AbstractQueryTestCase<QB> {

    protected abstract QB createQueryBuilderForPrefilteredRewriteTest(Supplier<QueryBuilder> prefilteredQuerySupplier);

    protected abstract void assertRewrittenHasPropagatedPrefilters(QueryBuilder rewritten, List<QueryBuilder> prefilters);

    public void testSerializationPrefiltersBwc() throws Exception {
        QB originalQuery = createTestQueryBuilder();
        originalQuery.setPrefilters(randomList(1, 5, () -> RandomQueryBuilder.createQuery(random())));

        for (int i = 0; i < 100; i++) {
            TransportVersion transportVersion = TransportVersionUtils.randomVersionBetween(
                random(),
                originalQuery.getMinimalSupportedVersion().id() == 0
                    ? TransportVersions.V_8_0_0 // The first major before introducing prefiltering
                    : originalQuery.getMinimalSupportedVersion(),
                TransportVersionUtils.getPreviousVersion(TransportVersion.current())
            );

            @SuppressWarnings("unchecked")
            QB deserializedQuery = (QB) copyNamedWriteable(originalQuery, namedWriteableRegistry(), QueryBuilder.class, transportVersion);

            if (transportVersion.supports(PrefilteredQuery.QUERY_PREFILTERING)) {
                assertThat(deserializedQuery, equalTo(originalQuery));
            } else {
                QB originalQueryWithoutPrefilters = copyQuery(originalQuery).setPrefilters(List.of());
                assertThat(deserializedQuery, equalTo(originalQueryWithoutPrefilters));
            }
        }
    }

    public void testEqualsAndHashcodeForPrefilters() throws IOException {
        QB originalQuery = createTestQueryBuilder();
        originalQuery.setPrefilters(randomList(1, 5, () -> RandomQueryBuilder.createQuery(random())));

        @SuppressWarnings("unchecked")
        QB deserializedQuery = (QB) copyNamedWriteable(originalQuery, namedWriteableRegistry(), QueryBuilder.class);

        assertThat(deserializedQuery, equalTo(originalQuery));
        assertThat(deserializedQuery.hashCode(), equalTo(originalQuery.hashCode()));

        deserializedQuery.setPrefilters(List.of());
        assertThat(deserializedQuery, not(equalTo(originalQuery)));
        assertThat(deserializedQuery.hashCode(), not(equalTo(originalQuery.hashCode())));
    }

    public void testRewriteWithPrefilters() throws IOException {
        QueryRewriteContext queryRewriteContext = createQueryRewriteContext();
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();

        for (int i = 0; i < 100; i++) {
            QB queryBuilder = createQueryBuilderForPrefilteredRewriteTest(() -> createRandomPrefilteredQuery());
            if (queryBuilder == null) {
                return;
            }
            setRandomPrefilters(queryBuilder);

            QueryBuilder rewritten = rewriteQuery(queryBuilder, queryRewriteContext, searchExecutionContext);

            assertRewrittenHasPropagatedPrefilters(rewritten, queryBuilder.getPrefilters());
        }
    }

    private static void setRandomPrefilters(PrefilteredQuery<?> queryBuilder) {
        List<QueryBuilder> filters = new ArrayList<>();
        int numFilters = randomIntBetween(1, 5);
        for (int i = 0; i < numFilters; i++) {
            filters.add(randomFrom(randomTermQuery(), createRandomPrefilteredQuery()));
        }
        queryBuilder.setPrefilters(filters);
    }

    private static QueryBuilder randomTermQuery() {
        String filterFieldName = randomFrom(KEYWORD_FIELD_NAME, TEXT_FIELD_NAME);
        return QueryBuilders.termQuery(filterFieldName, randomAlphaOfLength(10));
    }

    private static QueryBuilder createRandomPrefilteredQuery() {
        return switch (randomFrom(PrefilteredQueryType.values())) {
            case BOOL -> QueryBuilders.boolQuery().must(randomTermQuery());
            case BOOSTING -> QueryBuilders.boostingQuery(randomTermQuery(), randomTermQuery());
            case CONSTANT_SCORE -> QueryBuilders.constantScoreQuery(randomTermQuery());
            case DIS_MAX -> QueryBuilders.disMaxQuery().add(randomTermQuery()).add(randomTermQuery());
            case FUNCTION_SCORE -> QueryBuilders.functionScoreQuery(randomTermQuery());
            case NESTED -> QueryBuilders.nestedQuery(OBJECT_FIELD_NAME, randomTermQuery(), randomFrom(ScoreMode.values()));
        };
    }

    private enum PrefilteredQueryType {
        // We only include query types that have child queries.
        BOOL,
        BOOSTING,
        CONSTANT_SCORE,
        DIS_MAX,
        FUNCTION_SCORE,
        NESTED
    }
}
