/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.DistanceFeatureQueryBuilder;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MultiTermQueryBuilder;
import org.elasticsearch.index.query.PrefixQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.SpanQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.indices.TermsLookup;
import org.elasticsearch.script.Script;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTests;
import org.elasticsearch.xpack.security.authc.ApiKeyService;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.security.support.ApiKeyFieldNameTranslators.FIELD_NAME_TRANSLATORS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ApiKeyBoolQueryBuilderTests extends ESTestCase {

    public void testBuildFromSimpleQuery() {
        final Authentication authentication = randomBoolean() ? AuthenticationTests.randomAuthentication(null, null) : null;
        final QueryBuilder q1 = randomSimpleQuery(randomFrom("name", "creation", "expiration"));
        final ApiKeyBoolQueryBuilder apiKeyQb1 = ApiKeyBoolQueryBuilder.build(q1, authentication);
        assertCommonFilterQueries(apiKeyQb1, authentication);
        final List<QueryBuilder> mustQueries = apiKeyQb1.must();
        assertThat(mustQueries.size(), equalTo(1));
        assertThat(mustQueries.get(0), equalTo(q1));
        assertTrue(apiKeyQb1.should().isEmpty());
        assertTrue(apiKeyQb1.mustNot().isEmpty());
    }

    public void testBuildFromBoolQuery() {
        final Authentication authentication = randomBoolean() ? AuthenticationTests.randomAuthentication(null, null) : null;
        final BoolQueryBuilder bq1 = QueryBuilders.boolQuery();

        if (randomBoolean()) {
            bq1.must(QueryBuilders.prefixQuery("name", "prod-"));
        }
        if (randomBoolean()) {
            bq1.should(QueryBuilders.wildcardQuery("name", "*-east-*"));
        }
        if (randomBoolean()) {
            bq1.filter(QueryBuilders.termsQuery("name",
                randomArray(3, 8, String[]::new, () -> "prod-" + randomInt() + "-east-" + randomInt())));
        }
        if (randomBoolean()) {
            bq1.mustNot(QueryBuilders.idsQuery().addIds(randomArray(1, 3, String[]::new, () -> randomAlphaOfLength(22))));
        }
        if (randomBoolean()) {
            bq1.minimumShouldMatch(randomIntBetween(1, 2));
        }
        final ApiKeyBoolQueryBuilder apiKeyQb1 = ApiKeyBoolQueryBuilder.build(bq1, authentication);
        assertCommonFilterQueries(apiKeyQb1, authentication);

        assertThat(apiKeyQb1.must(), hasSize(1));
        assertThat(apiKeyQb1.should(), empty());
        assertThat(apiKeyQb1.mustNot(), empty());
        assertThat(apiKeyQb1.filter(), hasItem(QueryBuilders.termQuery("doc_type", "api_key")));
        assertThat(apiKeyQb1.must().get(0).getClass(), is(BoolQueryBuilder.class));
        final BoolQueryBuilder processed = (BoolQueryBuilder) apiKeyQb1.must().get(0);
        assertThat(processed.must(), equalTo(bq1.must()));
        assertThat(processed.should(), equalTo(bq1.should()));
        assertThat(processed.mustNot(), equalTo(bq1.mustNot()));
        assertThat(processed.minimumShouldMatch(), equalTo(bq1.minimumShouldMatch()));
        assertThat(processed.filter(), equalTo(bq1.filter()));
    }

    public void testFieldNameTranslation() {
        final Authentication authentication = randomBoolean() ? AuthenticationTests.randomAuthentication(null, null) : null;

        // metadata
        final String metadataKey = randomAlphaOfLengthBetween(3, 8);
        final TermQueryBuilder q1 = QueryBuilders.termQuery("metadata." + metadataKey, randomAlphaOfLengthBetween(3, 8));
        final ApiKeyBoolQueryBuilder apiKeyQb1 = ApiKeyBoolQueryBuilder.build(q1, authentication);
        assertCommonFilterQueries(apiKeyQb1, authentication);
        assertThat(apiKeyQb1.must().get(0), equalTo(QueryBuilders.termQuery("metadata_flattened." + metadataKey, q1.value())));

        // username
        final PrefixQueryBuilder q2 = QueryBuilders.prefixQuery("username", randomAlphaOfLength(3));
        final ApiKeyBoolQueryBuilder apiKeyQb2 = ApiKeyBoolQueryBuilder.build(q2, authentication);
        assertCommonFilterQueries(apiKeyQb2, authentication);
        assertThat(apiKeyQb2.must().get(0), equalTo(QueryBuilders.prefixQuery("creator.principal", q2.value())));

        // realm name
        final WildcardQueryBuilder q3 = QueryBuilders.wildcardQuery("realm_name", "*" + randomAlphaOfLength(3));
        final ApiKeyBoolQueryBuilder apiKeyQb3 = ApiKeyBoolQueryBuilder.build(q3, authentication);
        assertCommonFilterQueries(apiKeyQb3, authentication);
        assertThat(apiKeyQb3.must().get(0), equalTo(QueryBuilders.wildcardQuery("creator.realm", q3.value())));
    }

    public void testAllowListOfFieldNames() {
        final Authentication authentication = randomBoolean() ? AuthenticationTests.randomAuthentication(null, null) : null;

        final String randomFieldName = randomValueOtherThanMany(s -> FIELD_NAME_TRANSLATORS.stream().anyMatch(t -> t.supports(s)),
            () -> randomAlphaOfLengthBetween(3, 20));
        final String fieldName = randomFrom(
                        randomFieldName,
                        "api_key_hash",
                        "api_key_invalidated",
                        "doc_type",
                        "role_descriptors",
                        "limited_by_role_descriptors",
                        "version",
            "creator", "creator.metadata");

        final QueryBuilder q1 = randomValueOtherThanMany(
            q -> q.getClass() == IdsQueryBuilder.class || q.getClass() == MatchAllQueryBuilder.class,
            () -> randomSimpleQuery(fieldName));
        final IllegalArgumentException e1 =
            expectThrows(IllegalArgumentException.class, () -> ApiKeyBoolQueryBuilder.build(q1, authentication));

        assertThat(e1.getMessage(), containsString("Field [" + fieldName + "] is not allowed for API Key query"));
    }

    public void testTermsLookupIsNotAllowed() {
        final Authentication authentication = randomBoolean() ? AuthenticationTests.randomAuthentication(null, null) : null;
        final TermsQueryBuilder q1 = QueryBuilders.termsLookupQuery("name", new TermsLookup("lookup", "1", "names"));
        final IllegalArgumentException e1 =
            expectThrows(IllegalArgumentException.class, () -> ApiKeyBoolQueryBuilder.build(q1, authentication));
        assertThat(e1.getMessage(), containsString("terms query with terms lookup is not supported for API Key query"));
    }

    public void testRangeQueryWithRelationIsNotAllowed() {
        final Authentication authentication = randomBoolean() ? AuthenticationTests.randomAuthentication(null, null) : null;
        final RangeQueryBuilder q1 = QueryBuilders.rangeQuery("creation").relation("contains");
        final IllegalArgumentException e1 =
            expectThrows(IllegalArgumentException.class, () -> ApiKeyBoolQueryBuilder.build(q1, authentication));
        assertThat(e1.getMessage(), containsString("range query with relation is not supported for API Key query"));
    }

    public void testDisallowedQueryTypes() {
        final Authentication authentication = randomBoolean() ? AuthenticationTests.randomAuthentication(null, null) : null;

        final AbstractQueryBuilder<? extends AbstractQueryBuilder<?>> q1 = randomFrom(
            QueryBuilders.matchQuery(randomAlphaOfLength(5), randomAlphaOfLength(5)),
            QueryBuilders.constantScoreQuery(mock(QueryBuilder.class)),
            QueryBuilders.existsQuery(randomAlphaOfLength(5)),
            QueryBuilders.boostingQuery(mock(QueryBuilder.class), mock(QueryBuilder.class)),
            QueryBuilders.queryStringQuery("q=a:42"),
            QueryBuilders.simpleQueryStringQuery(randomAlphaOfLength(5)),
            QueryBuilders.combinedFieldsQuery(randomAlphaOfLength(5)),
            QueryBuilders.disMaxQuery(),
            QueryBuilders.distanceFeatureQuery(randomAlphaOfLength(5),
                mock(DistanceFeatureQueryBuilder.Origin.class),
                randomAlphaOfLength(5)),
            QueryBuilders.fieldMaskingSpanQuery(mock(SpanQueryBuilder.class), randomAlphaOfLength(5)),
            QueryBuilders.functionScoreQuery(mock(QueryBuilder.class)),
            QueryBuilders.fuzzyQuery(randomAlphaOfLength(5), randomAlphaOfLength(5)),
            QueryBuilders.wrapperQuery(randomAlphaOfLength(5)),
            QueryBuilders.matchBoolPrefixQuery(randomAlphaOfLength(5), randomAlphaOfLength(5)),
            QueryBuilders.matchPhraseQuery(randomAlphaOfLength(5), randomAlphaOfLength(5)),
            QueryBuilders.matchPhrasePrefixQuery(randomAlphaOfLength(5), randomAlphaOfLength(5)),
            QueryBuilders.moreLikeThisQuery(randomArray(1, 3, String[]::new, () -> randomAlphaOfLength(5))),
            QueryBuilders.regexpQuery(randomAlphaOfLength(5), randomAlphaOfLength(5)),
            QueryBuilders.spanTermQuery(randomAlphaOfLength(5), randomAlphaOfLength(5)),
            QueryBuilders.spanOrQuery(mock(SpanQueryBuilder.class)),
            QueryBuilders.spanContainingQuery(mock(SpanQueryBuilder.class), mock(SpanQueryBuilder.class)),
            QueryBuilders.spanFirstQuery(mock(SpanQueryBuilder.class), randomIntBetween(1, 3)),
            QueryBuilders.spanMultiTermQueryBuilder(mock(MultiTermQueryBuilder.class)),
            QueryBuilders.spanNotQuery(mock(SpanQueryBuilder.class), mock(SpanQueryBuilder.class)),
            QueryBuilders.scriptQuery(new Script(randomAlphaOfLength(5))),
            QueryBuilders.scriptScoreQuery(mock(QueryBuilder.class), new Script(randomAlphaOfLength(5))),
            QueryBuilders.geoWithinQuery(randomAlphaOfLength(5), randomAlphaOfLength(5)),
            QueryBuilders.geoBoundingBoxQuery(randomAlphaOfLength(5)),
            QueryBuilders.geoDisjointQuery(randomAlphaOfLength(5), randomAlphaOfLength(5)),
            QueryBuilders.geoDistanceQuery(randomAlphaOfLength(5)),
            QueryBuilders.geoIntersectionQuery(randomAlphaOfLength(5), randomAlphaOfLength(5)),
            QueryBuilders.geoShapeQuery(randomAlphaOfLength(5), randomAlphaOfLength(5))
        );

        final IllegalArgumentException e1 =
            expectThrows(IllegalArgumentException.class, () -> ApiKeyBoolQueryBuilder.build(q1, authentication));
        assertThat(e1.getMessage(), containsString("Query type [" + q1.getName() + "] is not supported for API Key query"));
    }

    public void testWillSetAllowedFields() throws IOException {
        final ApiKeyBoolQueryBuilder apiKeyQb1 = ApiKeyBoolQueryBuilder.build(randomSimpleQuery("name"),
            randomBoolean() ? AuthenticationTests.randomAuthentication(null, null) : null);

        final SearchExecutionContext context1 = mock(SearchExecutionContext.class);
        doAnswer(invocationOnMock -> {
            final Object[] args = invocationOnMock.getArguments();
            @SuppressWarnings("unchecked")
            final Predicate<String> predicate = (Predicate<String>) args[0];
            assertTrue(predicate.getClass().getName().startsWith(ApiKeyBoolQueryBuilder.class.getName()));
            testAllowedIndexFieldName(predicate);
            return null;
        }).when(context1).setAllowedFields(any());
        try {
            if (randomBoolean()) {
                apiKeyQb1.doToQuery(context1);
            } else {
                apiKeyQb1.doRewrite(context1);
            }
        } catch (Exception e) {
            // just ignore any exception from superclass since we only need verify the allowedFields are set
        } finally {
            verify(context1).setAllowedFields(any());
        }
    }

    private void testAllowedIndexFieldName(Predicate<String> predicate) {
        final String allowedField = randomFrom(
            "doc_type",
            "name",
            "api_key_invalidated",
            "creation",
            "expiration",
            "metadata_flattened." + randomAlphaOfLengthBetween(1, 10),
            "creator." + randomAlphaOfLengthBetween(1, 10));
        assertTrue(predicate.test(allowedField));

        final String disallowedField = randomBoolean() ? (randomAlphaOfLengthBetween(1, 3) + allowedField) : (allowedField.substring(1));
        assertFalse(predicate.test(disallowedField));
    }

    private void assertCommonFilterQueries(ApiKeyBoolQueryBuilder qb, Authentication authentication) {
        final List<TermQueryBuilder> tqb = qb.filter()
            .stream()
            .filter(q -> q.getClass() == TermQueryBuilder.class)
            .map(q -> (TermQueryBuilder) q)
            .collect(Collectors.toUnmodifiableList());
        assertTrue(tqb.stream().anyMatch(q -> q.equals(QueryBuilders.termQuery("doc_type", "api_key"))));
        if (authentication == null) {
            return;
        }
        assertTrue(tqb.stream()
            .anyMatch(q -> q.equals(QueryBuilders.termQuery("creator.principal", authentication.getUser().principal()))));
        assertTrue(tqb.stream()
            .anyMatch(q -> q.equals(QueryBuilders.termQuery("creator.realm", ApiKeyService.getCreatorRealmName(authentication)))));
    }

    private QueryBuilder randomSimpleQuery(String name) {
        switch (randomIntBetween(0, 6)) {
            case 0:
                return QueryBuilders.termQuery(name, randomAlphaOfLengthBetween(3, 8));
            case 1:
                return QueryBuilders.termsQuery(name, randomArray(1, 3, String[]::new, () -> randomAlphaOfLengthBetween(3, 8)));
            case 2:
                return QueryBuilders.idsQuery().addIds(randomArray(1, 3, String[]::new, () -> randomAlphaOfLength(22)));
            case 3:
                return QueryBuilders.prefixQuery(name, "prod-");
            case 4:
                return QueryBuilders.wildcardQuery(name, "prod-*-east-*");
            case 5:
                return QueryBuilders.matchAllQuery();
            default:
                return QueryBuilders.rangeQuery(name)
                    .from(Instant.now().minus(1, ChronoUnit.DAYS).toEpochMilli(), randomBoolean())
                    .to(Instant.now().toEpochMilli(), randomBoolean());
        }
    }
}
