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
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.MultiTermQueryBuilder;
import org.elasticsearch.index.query.PrefixQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.SimpleQueryStringBuilder;
import org.elasticsearch.index.query.SpanQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.indices.TermsLookup;
import org.elasticsearch.script.Script;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTests;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.action.apikey.TransportQueryApiKeyAction;
import org.elasticsearch.xpack.security.authc.ApiKeyService;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static org.elasticsearch.test.LambdaMatchers.falseWith;
import static org.elasticsearch.test.LambdaMatchers.trueWith;
import static org.elasticsearch.xpack.security.support.ApiKeyFieldNameTranslators.FIELD_NAME_TRANSLATORS;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ApiKeyBoolQueryBuilderTests extends ESTestCase {

    public void testBuildFromSimpleQuery() {
        final Authentication authentication = randomBoolean() ? AuthenticationTests.randomAuthentication(null, null) : null;
        final QueryBuilder q1 = randomSimpleQuery("name");
        final List<String> queryFields = new ArrayList<>();
        final ApiKeyBoolQueryBuilder apiKeyQb1 = ApiKeyBoolQueryBuilder.build(q1, queryFields::add, authentication);
        assertQueryFields(queryFields, q1, authentication);
        assertCommonFilterQueries(apiKeyQb1, authentication);
        final List<QueryBuilder> mustQueries = apiKeyQb1.must();
        assertThat(mustQueries, hasSize(1));
        assertThat(mustQueries.get(0), equalTo(q1));
        assertTrue(apiKeyQb1.should().isEmpty());
        assertTrue(apiKeyQb1.mustNot().isEmpty());
    }

    public void testQueryForDomainAuthentication() {
        final Authentication authentication = AuthenticationTests.randomAuthentication(null, AuthenticationTests.randomRealmRef(true));
        final QueryBuilder query = randomSimpleQuery("name");
        final List<String> queryFields = new ArrayList<>();
        final ApiKeyBoolQueryBuilder apiKeysQuery = ApiKeyBoolQueryBuilder.build(query, queryFields::add, authentication);
        assertQueryFields(queryFields, query, authentication);
        assertThat(apiKeysQuery.filter().get(0), is(QueryBuilders.termQuery("doc_type", "api_key")));
        assertThat(
            apiKeysQuery.filter().get(1),
            is(QueryBuilders.termQuery("creator.principal", authentication.getEffectiveSubject().getUser().principal()))
        );
        if (authentication.getEffectiveSubject().getRealm().getDomain().realms().size() == 1) {
            assertThat(
                apiKeysQuery.filter().get(2),
                is(
                    QueryBuilders.termQuery(
                        "creator.realm",
                        authentication.getEffectiveSubject().getRealm().getDomain().realms().stream().findFirst().get().getName()
                    )
                )
            );
        } else {
            assertThat(apiKeysQuery.filter().get(2), instanceOf(BoolQueryBuilder.class));
            assertThat(((BoolQueryBuilder) apiKeysQuery.filter().get(2)).must().size(), is(0));
            assertThat(((BoolQueryBuilder) apiKeysQuery.filter().get(2)).mustNot().size(), is(0));
            assertThat(((BoolQueryBuilder) apiKeysQuery.filter().get(2)).filter().size(), is(0));
            assertThat(((BoolQueryBuilder) apiKeysQuery.filter().get(2)).minimumShouldMatch(), is("1"));
            for (RealmConfig.RealmIdentifier realmIdentifier : authentication.getEffectiveSubject().getRealm().getDomain().realms()) {
                assertThat(
                    ((BoolQueryBuilder) apiKeysQuery.filter().get(2)).should(),
                    hasItem(QueryBuilders.termQuery("creator.realm", realmIdentifier.getName()))
                );
            }
        }
    }

    public void testBuildFromBoolQuery() {
        final Authentication authentication = randomBoolean() ? AuthenticationTests.randomAuthentication(null, null) : null;
        final List<String> queryFields = new ArrayList<>();
        final BoolQueryBuilder bq1 = QueryBuilders.boolQuery();

        boolean accessesNameField = false;
        if (randomBoolean()) {
            bq1.must(QueryBuilders.prefixQuery("name", "prod-"));
            accessesNameField = true;
        }
        if (randomBoolean()) {
            bq1.should(QueryBuilders.wildcardQuery("name", "*-east-*"));
            accessesNameField = true;
        }
        if (randomBoolean()) {
            bq1.filter(
                QueryBuilders.termsQuery("name", randomArray(3, 8, String[]::new, () -> "prod-" + randomInt() + "-east-" + randomInt()))
            );
            accessesNameField = true;
        }
        if (randomBoolean()) {
            bq1.mustNot(QueryBuilders.idsQuery().addIds(randomArray(1, 3, String[]::new, () -> randomAlphaOfLength(22))));
        }
        if (randomBoolean()) {
            bq1.minimumShouldMatch(randomIntBetween(1, 2));
        }
        final ApiKeyBoolQueryBuilder apiKeyQb1 = ApiKeyBoolQueryBuilder.build(bq1, queryFields::add, authentication);
        assertCommonFilterQueries(apiKeyQb1, authentication);

        assertThat(queryFields, hasItem("doc_type"));
        if (accessesNameField) {
            assertThat(queryFields, hasItem("name"));
        }
        if (authentication != null && authentication.isApiKey() == false) {
            assertThat(queryFields, hasItem("creator.principal"));
            assertThat(queryFields, hasItem("creator.realm"));
        }

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
        {
            List<String> queryFields = new ArrayList<>();
            final String metadataKey = randomAlphaOfLengthBetween(3, 8);
            final TermQueryBuilder q1 = QueryBuilders.termQuery("metadata." + metadataKey, randomAlphaOfLengthBetween(3, 8));
            ApiKeyBoolQueryBuilder apiKeyQb1 = ApiKeyBoolQueryBuilder.build(q1, queryFields::add, authentication);
            assertThat(queryFields, hasItem("doc_type"));
            assertThat(queryFields, hasItem("metadata_flattened." + metadataKey));
            if (authentication != null && authentication.isApiKey() == false) {
                assertThat(queryFields, hasItem("creator.principal"));
                assertThat(queryFields, hasItem("creator.realm"));
            }
            assertCommonFilterQueries(apiKeyQb1, authentication);
            assertThat(apiKeyQb1.must().get(0), equalTo(QueryBuilders.termQuery("metadata_flattened." + metadataKey, q1.value())));

            queryFields = new ArrayList<>();
            String queryStringQuery = randomAlphaOfLength(8);
            SimpleQueryStringBuilder q2 = QueryBuilders.simpleQueryStringQuery(queryStringQuery).field("metadata");
            apiKeyQb1 = ApiKeyBoolQueryBuilder.build(q2, queryFields::add, authentication);
            assertThat(queryFields, hasItem("doc_type"));
            assertThat(queryFields, hasItem("metadata_flattened"));
            if (authentication != null && authentication.isApiKey() == false) {
                assertThat(queryFields, hasItem("creator.principal"));
                assertThat(queryFields, hasItem("creator.realm"));
            }
            assertCommonFilterQueries(apiKeyQb1, authentication);
            assertThat(
                apiKeyQb1.must().get(0),
                equalTo(QueryBuilders.simpleQueryStringQuery(queryStringQuery).field("metadata_flattened"))
            );
        }

        // username
        {
            final List<String> queryFields = new ArrayList<>();
            final PrefixQueryBuilder q2 = QueryBuilders.prefixQuery("username", randomAlphaOfLength(3));
            final ApiKeyBoolQueryBuilder apiKeyQb2 = ApiKeyBoolQueryBuilder.build(q2, queryFields::add, authentication);
            assertThat(queryFields, hasItem("doc_type"));
            assertThat(queryFields, hasItem("creator.principal"));
            if (authentication != null && authentication.isApiKey() == false) {
                assertThat(queryFields, hasItem("creator.realm"));
            }
            assertCommonFilterQueries(apiKeyQb2, authentication);
            assertThat(apiKeyQb2.must().get(0), equalTo(QueryBuilders.prefixQuery("creator.principal", q2.value())));
        }

        // realm name
        {
            final List<String> queryFields = new ArrayList<>();
            final WildcardQueryBuilder q3 = QueryBuilders.wildcardQuery("realm_name", "*" + randomAlphaOfLength(3));
            final ApiKeyBoolQueryBuilder apiKeyQb3 = ApiKeyBoolQueryBuilder.build(q3, queryFields::add, authentication);
            assertThat(queryFields, hasItem("doc_type"));
            assertThat(queryFields, hasItem("creator.realm"));
            if (authentication != null && authentication.isApiKey() == false) {
                assertThat(queryFields, hasItem("creator.principal"));
            }
            assertCommonFilterQueries(apiKeyQb3, authentication);
            assertThat(apiKeyQb3.must().get(0), equalTo(QueryBuilders.wildcardQuery("creator.realm", q3.value())));
        }

        // creation_time
        {
            final List<String> queryFields = new ArrayList<>();
            final TermQueryBuilder q4 = QueryBuilders.termQuery("creation", randomLongBetween(0, Long.MAX_VALUE));
            final ApiKeyBoolQueryBuilder apiKeyQb4 = ApiKeyBoolQueryBuilder.build(q4, queryFields::add, authentication);
            assertThat(queryFields, hasItem("doc_type"));
            assertThat(queryFields, hasItem("creation_time"));
            if (authentication != null && authentication.isApiKey() == false) {
                assertThat(queryFields, hasItem("creator.principal"));
                assertThat(queryFields, hasItem("creator.realm"));
            }
            assertCommonFilterQueries(apiKeyQb4, authentication);
            assertThat(apiKeyQb4.must().get(0), equalTo(QueryBuilders.termQuery("creation_time", q4.value())));
        }

        // expiration_time
        {
            final List<String> queryFields = new ArrayList<>();
            final TermQueryBuilder q5 = QueryBuilders.termQuery("expiration", randomLongBetween(0, Long.MAX_VALUE));
            final ApiKeyBoolQueryBuilder apiKeyQb5 = ApiKeyBoolQueryBuilder.build(q5, queryFields::add, authentication);
            assertThat(queryFields, hasItem("doc_type"));
            assertThat(queryFields, hasItem("expiration_time"));
            if (authentication != null && authentication.isApiKey() == false) {
                assertThat(queryFields, hasItem("creator.principal"));
                assertThat(queryFields, hasItem("creator.realm"));
            }
            assertCommonFilterQueries(apiKeyQb5, authentication);
            assertThat(apiKeyQb5.must().get(0), equalTo(QueryBuilders.termQuery("expiration_time", q5.value())));
        }

        // type
        {
            final List<String> queryFields = new ArrayList<>();
            float fieldBoost = randomFloat();
            final SimpleQueryStringBuilder q5 = QueryBuilders.simpleQueryStringQuery("q=42").field("type", fieldBoost);
            final ApiKeyBoolQueryBuilder apiKeyQb5 = ApiKeyBoolQueryBuilder.build(q5, queryFields::add, authentication);
            assertThat(queryFields, hasItem("doc_type"));
            assertThat(queryFields, hasItem("runtime_key_type")); // "type" translation
            if (authentication != null && authentication.isApiKey() == false) {
                assertThat(queryFields, hasItem("creator.principal"));
                assertThat(queryFields, hasItem("creator.realm"));
            }
            assertCommonFilterQueries(apiKeyQb5, authentication);
            assertThat(
                apiKeyQb5.must().get(0),
                equalTo(QueryBuilders.simpleQueryStringQuery("q=42").field("runtime_key_type", fieldBoost))
            );
        }

        // test them all together
        {
            final List<String> queryFields = new ArrayList<>();
            final SimpleQueryStringBuilder q6 = QueryBuilders.simpleQueryStringQuery("+OK -NOK maybe~3")
                .field("username")
                .field("realm_name")
                .field("name")
                .field("type")
                .field("creation")
                .field("expiration")
                .field("invalidated")
                .field("invalidation")
                .field("metadata")
                .field("metadata.inner");
            final ApiKeyBoolQueryBuilder apiKeyQb6 = ApiKeyBoolQueryBuilder.build(q6, queryFields::add, authentication);
            assertThat(queryFields, hasItem("doc_type"));
            assertThat(queryFields, hasItem("creator.principal"));
            assertThat(queryFields, hasItem("creator.realm"));
            assertThat(queryFields, hasItem("name"));
            assertThat(queryFields, hasItem("runtime_key_type")); // "type" translation
            assertThat(queryFields, hasItem("creation_time"));
            assertThat(queryFields, hasItem("expiration_time"));
            assertThat(queryFields, hasItem("api_key_invalidated"));
            assertThat(queryFields, hasItem("invalidation_time"));
            assertThat(queryFields, hasItem("metadata_flattened"));
            assertThat(queryFields, hasItem("metadata_flattened.inner"));
            assertCommonFilterQueries(apiKeyQb6, authentication);
            assertThat(
                apiKeyQb6.must().get(0),
                equalTo(
                    QueryBuilders.simpleQueryStringQuery("+OK -NOK maybe~3")
                        .field("creator.principal")
                        .field("creator.realm")
                        .field("name")
                        .field("runtime_key_type")
                        .field("creation_time")
                        .field("expiration_time")
                        .field("api_key_invalidated")
                        .field("invalidation_time")
                        .field("metadata_flattened")
                        .field("metadata_flattened.inner")
                )
            );
        }
    }

    public void testAllowListOfFieldNames() {
        final Authentication authentication = randomBoolean() ? AuthenticationTests.randomAuthentication(null, null) : null;

        final String randomFieldName = randomValueOtherThanMany(
            s -> FIELD_NAME_TRANSLATORS.stream().anyMatch(t -> t.supports(s)),
            () -> randomAlphaOfLengthBetween(3, 20)
        );
        final String fieldName = randomFrom(
            randomFieldName,
            "api_key_hash",
            "api_key_invalidated",
            "doc_type",
            "role_descriptors",
            "limited_by_role_descriptors",
            "version",
            "creator",
            "creator.metadata"
        );

        {
            final QueryBuilder q1 = randomValueOtherThanMany(
                q -> q.getClass() == IdsQueryBuilder.class
                    || q.getClass() == MatchAllQueryBuilder.class
                    || q.getClass() == SimpleQueryStringBuilder.class,
                () -> randomSimpleQuery(fieldName)
            );
            final IllegalArgumentException e1 = expectThrows(
                IllegalArgumentException.class,
                () -> ApiKeyBoolQueryBuilder.build(q1, ignored -> {}, authentication)
            );
            assertThat(e1.getMessage(), containsString("Field [" + fieldName + "] is not allowed for API Key query"));
        }

        // also wrapped in a boolean query
        {
            final QueryBuilder q1 = randomValueOtherThanMany(
                q -> q.getClass() == IdsQueryBuilder.class
                    || q.getClass() == MatchAllQueryBuilder.class
                    || q.getClass() == SimpleQueryStringBuilder.class,
                () -> randomSimpleQuery(fieldName)
            );
            final BoolQueryBuilder q2 = QueryBuilders.boolQuery();
            if (randomBoolean()) {
                if (randomBoolean()) {
                    q2.filter(q1);
                } else {
                    q2.must(q1);
                }
            } else {
                if (randomBoolean()) {
                    q2.should(q1);
                } else {
                    q2.mustNot(q1);
                }
            }
            IllegalArgumentException e2 = expectThrows(
                IllegalArgumentException.class,
                () -> ApiKeyBoolQueryBuilder.build(q2, ignored -> {}, authentication)
            );
            assertThat(e2.getMessage(), containsString("Field [" + fieldName + "] is not allowed for API Key query"));
        }
    }

    public void testTermsLookupIsNotAllowed() {
        final Authentication authentication = randomBoolean() ? AuthenticationTests.randomAuthentication(null, null) : null;
        final TermsQueryBuilder q1 = QueryBuilders.termsLookupQuery("name", new TermsLookup("lookup", "1", "names"));
        final IllegalArgumentException e1 = expectThrows(
            IllegalArgumentException.class,
            () -> ApiKeyBoolQueryBuilder.build(q1, ignored -> {}, authentication)
        );
        assertThat(e1.getMessage(), containsString("terms query with terms lookup is not supported for API Key query"));
    }

    public void testRangeQueryWithRelationIsNotAllowed() {
        final Authentication authentication = randomBoolean() ? AuthenticationTests.randomAuthentication(null, null) : null;
        final RangeQueryBuilder q1 = QueryBuilders.rangeQuery("creation").relation("contains");
        final IllegalArgumentException e1 = expectThrows(
            IllegalArgumentException.class,
            () -> ApiKeyBoolQueryBuilder.build(q1, ignored -> {}, authentication)
        );
        assertThat(e1.getMessage(), containsString("range query with relation is not supported for API Key query"));
    }

    public void testDisallowedQueryTypes() {
        final Authentication authentication = randomBoolean() ? AuthenticationTests.randomAuthentication(null, null) : null;

        final AbstractQueryBuilder<? extends AbstractQueryBuilder<?>> q1 = randomFrom(
            QueryBuilders.matchQuery(randomAlphaOfLength(5), randomAlphaOfLength(5)),
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

        final IllegalArgumentException e1 = expectThrows(
            IllegalArgumentException.class,
            () -> ApiKeyBoolQueryBuilder.build(q1, ignored -> {}, authentication)
        );
        assertThat(e1.getMessage(), containsString("Query type [" + q1.getName() + "] is not supported for API Key query"));

        // also wrapped in a boolean query
        {
            final BoolQueryBuilder q2 = QueryBuilders.boolQuery();
            if (randomBoolean()) {
                if (randomBoolean()) {
                    q2.filter(q1);
                } else {
                    q2.must(q1);
                }
            } else {
                if (randomBoolean()) {
                    q2.should(q1);
                } else {
                    q2.mustNot(q1);
                }
            }
            IllegalArgumentException e2 = expectThrows(
                IllegalArgumentException.class,
                () -> ApiKeyBoolQueryBuilder.build(q2, ignored -> {}, authentication)
            );
            assertThat(e2.getMessage(), containsString("Query type [" + q1.getName() + "] is not supported for API Key query"));
        }
    }

    public void testWillSetAllowedFields() throws IOException {
        final ApiKeyBoolQueryBuilder apiKeyQb1 = ApiKeyBoolQueryBuilder.build(
            randomSimpleQuery("name"),
            ignored -> {},
            randomBoolean() ? AuthenticationTests.randomAuthentication(null, null) : null
        );

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

    public void testWillFilterForApiKeyId() {
        final String apiKeyId = randomAlphaOfLength(20);
        final Authentication authentication = AuthenticationTests.randomApiKeyAuthentication(
            new User(randomAlphaOfLengthBetween(5, 8)),
            apiKeyId
        );
        final ApiKeyBoolQueryBuilder apiKeyQb = ApiKeyBoolQueryBuilder.build(
            randomFrom(randomSimpleQuery("name"), null),
            ignored -> {},
            authentication
        );
        assertThat(apiKeyQb.filter(), hasItem(QueryBuilders.termQuery("doc_type", "api_key")));
        assertThat(apiKeyQb.filter(), hasItem(QueryBuilders.idsQuery().addIds(apiKeyId)));
    }

    public void testSimpleQueryStringFieldPatternTranslation() {
        String queryStringQuery = randomAlphaOfLength(8);
        Authentication authentication = randomBoolean() ? AuthenticationTests.randomAuthentication(null, null) : null;
        // no field translates to all the allowed fields
        {
            List<String> queryFields = new ArrayList<>();
            SimpleQueryStringBuilder q = QueryBuilders.simpleQueryStringQuery(queryStringQuery);
            ApiKeyBoolQueryBuilder apiKeyQb = ApiKeyBoolQueryBuilder.build(q, queryFields::add, authentication);
            assertThat(
                queryFields.subList(0, 9),
                containsInAnyOrder(
                    "creator.principal",
                    "creator.realm",
                    "name",
                    "runtime_key_type",
                    "creation_time",
                    "expiration_time",
                    "api_key_invalidated",
                    "invalidation_time",
                    "metadata_flattened"
                )
            );
            assertThat(queryFields.get(9), is("doc_type"));
            assertThat(
                apiKeyQb.must().get(0),
                equalTo(
                    QueryBuilders.simpleQueryStringQuery(queryStringQuery)
                        .field("creator.principal")
                        .field("creator.realm")
                        .field("name")
                        .field("runtime_key_type")
                        .field("creation_time")
                        .field("expiration_time")
                        .field("api_key_invalidated")
                        .field("invalidation_time")
                        .field("metadata_flattened")
                        .lenient(true)
                )
            );
        }
        // * matches all fields
        {
            List<String> queryFields = new ArrayList<>();
            float fieldBoost = Math.abs(randomFloat());
            SimpleQueryStringBuilder q = QueryBuilders.simpleQueryStringQuery(queryStringQuery).field("*", fieldBoost);
            ApiKeyBoolQueryBuilder apiKeyQb = ApiKeyBoolQueryBuilder.build(q, queryFields::add, authentication);
            assertThat(
                queryFields.subList(0, 9),
                containsInAnyOrder(
                    "creator.principal",
                    "creator.realm",
                    "name",
                    "runtime_key_type",
                    "creation_time",
                    "expiration_time",
                    "api_key_invalidated",
                    "invalidation_time",
                    "metadata_flattened"
                )
            );
            assertThat(queryFields.get(9), is("doc_type"));
            assertThat(
                apiKeyQb.must().get(0),
                equalTo(
                    QueryBuilders.simpleQueryStringQuery(queryStringQuery)
                        .field("creator.principal", fieldBoost)
                        .field("creator.realm", fieldBoost)
                        .field("name", fieldBoost)
                        .field("runtime_key_type", fieldBoost)
                        .field("creation_time", fieldBoost)
                        .field("expiration_time", fieldBoost)
                        .field("api_key_invalidated", fieldBoost)
                        .field("invalidation_time", fieldBoost)
                        .field("metadata_flattened", fieldBoost)
                        .lenient(true)
                )
            );
        }
        // pattern that matches a subset of fields
        {
            List<String> queryFields = new ArrayList<>();
            float fieldBoost = Math.abs(randomFloat());
            boolean lenient = randomBoolean();
            SimpleQueryStringBuilder q = QueryBuilders.simpleQueryStringQuery(queryStringQuery).field("i*", fieldBoost).lenient(lenient);
            ApiKeyBoolQueryBuilder apiKeyQb = ApiKeyBoolQueryBuilder.build(q, queryFields::add, authentication);
            assertThat(queryFields.subList(0, 2), containsInAnyOrder("api_key_invalidated", "invalidation_time"));
            assertThat(queryFields.get(2), is("doc_type"));
            assertThat(
                apiKeyQb.must().get(0),
                equalTo(
                    QueryBuilders.simpleQueryStringQuery(queryStringQuery)
                        .field("api_key_invalidated", fieldBoost)
                        .field("invalidation_time", fieldBoost)
                        .lenient(lenient)
                )
            );
        }
        // multi pattern that matches a subset of fields
        {
            List<String> queryFields = new ArrayList<>();
            float boost1 = randomFrom(2.0f, 4.0f, 8.0f);
            float boost2 = randomFrom(2.0f, 4.0f, 8.0f);
            float boost3 = randomFrom(2.0f, 4.0f, 8.0f);
            SimpleQueryStringBuilder q = QueryBuilders.simpleQueryStringQuery(queryStringQuery)
                .field("i*", boost1)
                .field("u*", boost2)
                .field("user*", boost3);
            ApiKeyBoolQueryBuilder apiKeyQb = ApiKeyBoolQueryBuilder.build(q, queryFields::add, authentication);
            assertThat(queryFields.subList(0, 3), containsInAnyOrder("creator.principal", "api_key_invalidated", "invalidation_time"));
            assertThat(queryFields.get(4), is("doc_type"));
            assertThat(
                apiKeyQb.must().get(0),
                equalTo(
                    QueryBuilders.simpleQueryStringQuery(queryStringQuery)
                        .field("api_key_invalidated", boost1)
                        .field("invalidation_time", boost1)
                        .field("creator.principal", boost2 * boost3)
                        .lenient(false)
                )
            );

            // wildcards don't expand under metadata.*
            queryFields = new ArrayList<>();
            q = QueryBuilders.simpleQueryStringQuery(queryStringQuery)
                .field("rea*", boost1)
                .field("t*", boost1)
                .field("ty*", boost2)
                .field("me*", boost2)
                .field("metadata.*", boost3)
                .field("metadata.x*", boost3);
            apiKeyQb = ApiKeyBoolQueryBuilder.build(q, queryFields::add, authentication);
            assertThat(
                queryFields.subList(0, 4),
                containsInAnyOrder("creator.realm", "runtime_key_type", "metadata_flattened", "runtime_key_type")
            );
            assertThat(queryFields.get(4), is("doc_type"));
            assertThat(
                apiKeyQb.must().get(0),
                equalTo(
                    QueryBuilders.simpleQueryStringQuery(queryStringQuery)
                        .field("creator.realm", boost1)
                        .field("runtime_key_type", boost1 * boost2)
                        .field("metadata_flattened", boost2)
                        .lenient(false)
                )
            );
        }
        // patterns that don't match anything
        {
            List<String> queryFields = new ArrayList<>();
            float boost1 = randomFrom(2.0f, 4.0f, 8.0f);
            float boost2 = randomFrom(2.0f, 4.0f, 8.0f);
            float boost3 = randomFrom(2.0f, 4.0f, 8.0f);
            SimpleQueryStringBuilder q = QueryBuilders.simpleQueryStringQuery(queryStringQuery)
                .field("field_that_does_not*", boost1)
                .field("what*", boost2)
                .field("aiaiaiai*", boost3);
            ApiKeyBoolQueryBuilder apiKeyQb = ApiKeyBoolQueryBuilder.build(q, queryFields::add, authentication);
            assertThat(queryFields.get(0), is("doc_type"));
            if (authentication != null) {
                assertThat(queryFields.get(1), is("creator.principal"));
                assertThat(queryFields.get(2), is("creator.realm"));
                assertThat(queryFields.size(), is(3));
            } else {
                assertThat(queryFields.size(), is(1));
            }
            assertThat(apiKeyQb.must().get(0), equalTo(new MatchNoneQueryBuilder()));
        }
        // disallowed or unknown field is silently ignored
        {
            List<String> queryFields = new ArrayList<>();
            float boost1 = randomFrom(2.0f, 4.0f, 8.0f);
            float boost2 = randomFrom(2.0f, 4.0f, 8.0f);
            SimpleQueryStringBuilder q = QueryBuilders.simpleQueryStringQuery(queryStringQuery)
                .field("field_that_does_not*", boost1)
                .field("unknown_field", boost2);
            ApiKeyBoolQueryBuilder apiKeyQb = ApiKeyBoolQueryBuilder.build(q, queryFields::add, authentication);
            assertThat(queryFields.get(0), is("doc_type"));
            if (authentication != null) {
                assertThat(queryFields.get(1), is("creator.principal"));
                assertThat(queryFields.get(2), is("creator.realm"));
                assertThat(queryFields.size(), is(3));
            } else {
                assertThat(queryFields.size(), is(1));
            }
            assertThat(apiKeyQb.must().get(0), equalTo(new MatchNoneQueryBuilder()));

            // translated field
            queryFields = new ArrayList<>();
            String translatedField = randomFrom(
                "creator.principal",
                "creator.realm",
                "runtime_key_type",
                "creation_time",
                "expiration_time",
                "api_key_invalidated",
                "invalidation_time",
                "metadata_flattened"
            );
            SimpleQueryStringBuilder q2 = QueryBuilders.simpleQueryStringQuery(queryStringQuery)
                .field(translatedField, boost1)
                .field("field_that_does_not*", boost2);
            apiKeyQb = ApiKeyBoolQueryBuilder.build(q2, queryFields::add, authentication);
            assertThat(queryFields.get(0), is("doc_type"));
            if (authentication != null) {
                assertThat(queryFields.get(1), is("creator.principal"));
                assertThat(queryFields.get(2), is("creator.realm"));
                assertThat(queryFields.size(), is(3));
            } else {
                assertThat(queryFields.size(), is(1));
            }

            assertThat(apiKeyQb.must().get(0), equalTo(new MatchNoneQueryBuilder()));
        }
    }

    private void testAllowedIndexFieldName(Predicate<String> predicate) {
        final String allowedField = randomFrom(
            "doc_type",
            "name",
            "type",
            TransportQueryApiKeyAction.API_KEY_TYPE_RUNTIME_MAPPING_FIELD,
            "api_key_invalidated",
            "creation_time",
            "expiration_time",
            "metadata_flattened." + randomAlphaOfLengthBetween(1, 10),
            "creator.principal",
            "creator.realm"
        );
        assertThat(predicate, trueWith(allowedField));

        final String disallowedField = randomBoolean() ? (randomAlphaOfLengthBetween(1, 3) + allowedField) : (allowedField.substring(1));
        assertThat(predicate, falseWith(disallowedField));
    }

    private void assertCommonFilterQueries(ApiKeyBoolQueryBuilder qb, Authentication authentication) {
        final List<TermQueryBuilder> tqb = qb.filter()
            .stream()
            .filter(q -> q.getClass() == TermQueryBuilder.class)
            .map(q -> (TermQueryBuilder) q)
            .toList();
        assertTrue(tqb.stream().anyMatch(q -> q.equals(QueryBuilders.termQuery("doc_type", "api_key"))));
        if (authentication == null) {
            return;
        }
        assertTrue(
            tqb.stream()
                .anyMatch(
                    q -> q.equals(QueryBuilders.termQuery("creator.principal", authentication.getEffectiveSubject().getUser().principal()))
                )
        );
        assertTrue(
            tqb.stream()
                .anyMatch(q -> q.equals(QueryBuilders.termQuery("creator.realm", ApiKeyService.getCreatorRealmName(authentication))))
        );
    }

    private QueryBuilder randomSimpleQuery(String fieldName) {
        return switch (randomIntBetween(0, 8)) {
            case 0 -> QueryBuilders.termQuery(fieldName, randomAlphaOfLengthBetween(3, 8));
            case 1 -> QueryBuilders.termsQuery(fieldName, randomArray(1, 3, String[]::new, () -> randomAlphaOfLengthBetween(3, 8)));
            case 2 -> QueryBuilders.idsQuery().addIds(randomArray(1, 3, String[]::new, () -> randomAlphaOfLength(22)));
            case 3 -> QueryBuilders.prefixQuery(fieldName, "prod-");
            case 4 -> QueryBuilders.wildcardQuery(fieldName, "prod-*-east-*");
            case 5 -> QueryBuilders.matchAllQuery();
            case 6 -> QueryBuilders.existsQuery(fieldName);
            case 7 -> QueryBuilders.rangeQuery(fieldName)
                .from(Instant.now().minus(1, ChronoUnit.DAYS).toEpochMilli(), randomBoolean())
                .to(Instant.now().toEpochMilli(), randomBoolean());
            case 8 -> QueryBuilders.simpleQueryStringQuery("+rest key*")
                .field(fieldName)
                .lenient(randomBoolean())
                .analyzeWildcard(randomBoolean());
            default -> throw new IllegalStateException("illegal switch case");
        };
    }

    private void assertQueryFields(List<String> actualQueryFields, QueryBuilder queryBuilder, Authentication authentication) {
        assertThat(actualQueryFields, hasItem("doc_type"));
        if ((queryBuilder instanceof IdsQueryBuilder || queryBuilder instanceof MatchAllQueryBuilder) == false) {
            assertThat(actualQueryFields, hasItem("name"));
        }
        if (authentication != null && authentication.isApiKey() == false) {
            assertThat(actualQueryFields, hasItem("creator.principal"));
            assertThat(actualQueryFields, hasItem("creator.realm"));
        }
    }
}
