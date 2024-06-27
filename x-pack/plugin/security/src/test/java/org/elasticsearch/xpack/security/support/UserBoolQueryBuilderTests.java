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
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MultiTermQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.SpanQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.indices.TermsLookup;
import org.elasticsearch.script.Script;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsIterableContaining.hasItem;
import static org.hamcrest.core.StringContains.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class UserBoolQueryBuilderTests extends ESTestCase {
    private static final String[] allowedIndexFieldNames = new String[] { "username", "roles", "full_name", "email", "enabled" };

    public void testBuildFromSimpleQuery() {
        final QueryBuilder query = randomSimpleQuery();
        final UserBoolQueryBuilder userQueryBuilder = UserBoolQueryBuilder.build(query);
        assertCommonFilterQueries(userQueryBuilder);
        final List<QueryBuilder> mustQueries = userQueryBuilder.must();
        assertThat(mustQueries, hasSize(1));
        assertThat(mustQueries.get(0), equalTo(query));
        assertTrue(userQueryBuilder.should().isEmpty());
        assertTrue(userQueryBuilder.mustNot().isEmpty());
    }

    public void testBuildFromBoolQuery() {
        final BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        if (randomBoolean()) {
            boolQueryBuilder.must(QueryBuilders.prefixQuery(randomAllowedField(), "bar"));
        }
        if (randomBoolean()) {
            boolQueryBuilder.should(QueryBuilders.wildcardQuery(randomAllowedField(), "*ar*"));
        }
        if (randomBoolean()) {
            boolQueryBuilder.filter(QueryBuilders.termsQuery("roles", randomArray(3, 8, String[]::new, () -> "role-" + randomInt())));
        }
        if (randomBoolean()) {
            boolQueryBuilder.minimumShouldMatch(randomIntBetween(1, 2));
        }
        final UserBoolQueryBuilder userBoolQueryBuilder = UserBoolQueryBuilder.build(boolQueryBuilder);
        assertCommonFilterQueries(userBoolQueryBuilder);

        assertThat(userBoolQueryBuilder.must(), hasSize(1));
        assertThat(userBoolQueryBuilder.should(), empty());
        assertThat(userBoolQueryBuilder.mustNot(), empty());
        assertThat(userBoolQueryBuilder.filter(), hasItem(QueryBuilders.termQuery("type", "user")));
        assertThat(userBoolQueryBuilder.must().get(0).getClass(), is(BoolQueryBuilder.class));
        final BoolQueryBuilder translated = (BoolQueryBuilder) userBoolQueryBuilder.must().get(0);
        assertThat(translated.must(), equalTo(boolQueryBuilder.must()));
        assertThat(translated.should(), equalTo(boolQueryBuilder.should()));
        assertThat(translated.mustNot(), equalTo(boolQueryBuilder.mustNot()));
        assertThat(translated.minimumShouldMatch(), equalTo(boolQueryBuilder.minimumShouldMatch()));
        assertThat(translated.filter(), equalTo(boolQueryBuilder.filter()));
    }

    public void testFieldNameTranslation() {
        String field = randomAllowedField();
        final WildcardQueryBuilder wildcardQueryBuilder = QueryBuilders.wildcardQuery(field, "*" + randomAlphaOfLength(3));
        final UserBoolQueryBuilder userBoolQueryBuilder = UserBoolQueryBuilder.build(wildcardQueryBuilder);
        assertCommonFilterQueries(userBoolQueryBuilder);
        assertThat(userBoolQueryBuilder.must().get(0), equalTo(QueryBuilders.wildcardQuery(field, wildcardQueryBuilder.value())));
    }

    public void testAllowListOfFieldNames() {
        final String fieldName = randomValueOtherThanMany(
            v -> Arrays.asList(allowedIndexFieldNames).contains(v),
            () -> randomFrom(randomAlphaOfLengthBetween(3, 20), "type", "password")
        );

        // MatchAllQueryBuilder doesn't do any translation, so skip
        final QueryBuilder q1 = randomValueOtherThanMany(
            q -> q.getClass() == MatchAllQueryBuilder.class,
            () -> randomSimpleQuery(fieldName)
        );
        final IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> UserBoolQueryBuilder.build(q1));

        assertThat(exception.getMessage(), containsString("Field [" + fieldName + "] is not allowed"));
    }

    public void testTermsLookupIsNotAllowed() {
        final TermsQueryBuilder q1 = QueryBuilders.termsLookupQuery("roles", new TermsLookup("lookup", "1", "id"));
        final IllegalArgumentException e1 = expectThrows(IllegalArgumentException.class, () -> UserBoolQueryBuilder.build(q1));
        assertThat(e1.getMessage(), containsString("terms query with terms lookup is not currently supported in this context"));
    }

    public void testDisallowedQueryTypes() {
        final AbstractQueryBuilder<? extends AbstractQueryBuilder<?>> q1 = randomFrom(
            QueryBuilders.constantScoreQuery(mock(QueryBuilder.class)),
            QueryBuilders.boostingQuery(mock(QueryBuilder.class), mock(QueryBuilder.class)),
            QueryBuilders.queryStringQuery("q=a:42"),
            QueryBuilders.combinedFieldsQuery(randomAlphaOfLength(5)),
            QueryBuilders.disMaxQuery(),
            QueryBuilders.distanceFeatureQuery(
                randomAlphaOfLength(5),
                mock(DistanceFeatureQueryBuilder.Origin.class),
                randomAlphaOfLength(5)
            ),
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

        final IllegalArgumentException e1 = expectThrows(IllegalArgumentException.class, () -> UserBoolQueryBuilder.build(q1));
        assertThat(e1.getMessage(), containsString("Query type [" + q1.getName() + "] is not currently supported in this context"));
    }

    public void testWillSetAllowedFields() {
        final UserBoolQueryBuilder userBoolQueryBuilder = UserBoolQueryBuilder.build(randomSimpleQuery());

        final SearchExecutionContext context = mock(SearchExecutionContext.class);
        doAnswer(invocationOnMock -> {
            final Object[] args = invocationOnMock.getArguments();
            @SuppressWarnings("unchecked")
            final Predicate<String> predicate = (Predicate<String>) args[0];
            assertTrue(predicate.getClass().getName().startsWith(UserBoolQueryBuilder.class.getName()));
            testAllowedIndexFieldName(predicate);
            return null;
        }).when(context).setAllowedFields(any());
        try {
            if (randomBoolean()) {
                userBoolQueryBuilder.doToQuery(context);
            } else {
                userBoolQueryBuilder.doRewrite(context);
            }
        } catch (Exception e) {
            // just ignore any exception from superclass since we only need verify the allowedFields are set
        } finally {
            verify(context).setAllowedFields(any());
        }
    }

    private void testAllowedIndexFieldName(Predicate<String> predicate) {
        final String allowedField = randomAllowedField();
        assertTrue(predicate.test(allowedField));

        final String disallowedField = randomBoolean() ? (randomAlphaOfLengthBetween(1, 3) + allowedField) : (allowedField.substring(1));
        assertFalse(predicate.test(disallowedField));
    }

    private void assertCommonFilterQueries(UserBoolQueryBuilder qb) {
        final List<TermQueryBuilder> tqb = qb.filter()
            .stream()
            .filter(q -> q.getClass() == TermQueryBuilder.class)
            .map(q -> (TermQueryBuilder) q)
            .toList();
        assertTrue(tqb.stream().anyMatch(q -> q.equals(QueryBuilders.termQuery("type", "user"))));
    }

    private String randomAllowedField() {
        return randomFrom(allowedIndexFieldNames);
    }

    private QueryBuilder randomSimpleQuery() {
        return randomSimpleQuery(randomAllowedField());
    }

    private QueryBuilder randomSimpleQuery(String fieldName) {
        return randomFrom(
            QueryBuilders.termQuery(fieldName, randomAlphaOfLengthBetween(3, 8)),
            QueryBuilders.termsQuery(fieldName, randomArray(1, 3, String[]::new, () -> randomAlphaOfLengthBetween(3, 8))),
            QueryBuilders.prefixQuery(fieldName, randomAlphaOfLength(randomIntBetween(3, 10))),
            QueryBuilders.wildcardQuery(fieldName, "*" + randomAlphaOfLength(randomIntBetween(3, 10))),
            QueryBuilders.matchAllQuery(),
            QueryBuilders.existsQuery(fieldName)
        );
    }
}
