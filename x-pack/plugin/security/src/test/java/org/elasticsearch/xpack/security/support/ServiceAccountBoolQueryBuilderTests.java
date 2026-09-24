/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.indices.TermsLookup;
import org.elasticsearch.script.Script;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.security.authc.service.UserManagedServiceAccountStore.SERVICE_ACCOUNT_DOC_TYPE;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ServiceAccountBoolQueryBuilderTests extends ESTestCase {

    /**
     * Query-level names that are also the index-level names. The realm domain is the one field whose two names
     * differ, and is covered separately.
     */
    private static final List<String> ALLOWED_FIELDS = List.of(
        "username",
        "roles",
        "enabled",
        "description",
        "creator.principal",
        "creator.full_name",
        "creator.email",
        "creator.realm",
        "creator.realm_type",
        "creator.api_key.id",
        "creator.api_key.name",
        "created_at",
        "editor.principal",
        "editor.full_name",
        "editor.email",
        "editor.realm",
        "editor.realm_type",
        "editor.api_key.id",
        "editor.api_key.name",
        "edited_at"
    );

    public void testANullQuerySelectsEveryServiceAccountDocument() {
        final ServiceAccountBoolQueryBuilder query = ServiceAccountBoolQueryBuilder.build(null);
        assertThat(query.must(), empty());
        assertThat(query.should(), empty());
        assertThat(query.mustNot(), empty());
        assertThat(query.filter(), contains(QueryBuilders.termQuery("doc_type", SERVICE_ACCOUNT_DOC_TYPE)));
    }

    public void testASimpleQueryIsKeptAndRestrictedToServiceAccountDocuments() {
        final QueryBuilder simpleQuery = randomSimpleQuery(randomFrom(ALLOWED_FIELDS));
        final ServiceAccountBoolQueryBuilder query = ServiceAccountBoolQueryBuilder.build(simpleQuery);
        assertThat(query.must(), contains(simpleQuery));
        assertThat(query.should(), empty());
        assertThat(query.mustNot(), empty());
        assertThat(query.filter(), contains(QueryBuilders.termQuery("doc_type", SERVICE_ACCOUNT_DOC_TYPE)));
    }

    /**
     * A response names the realm domain by its name alone, so the query field follows the response and is translated
     * to the {@code name} of the stored domain object. The index-level name is not accepted as a query field.
     */
    public void testTheRealmDomainIsQueriedByTheNameAResponseReports() {
        final String author = randomFrom("creator", "editor");
        final ServiceAccountBoolQueryBuilder query = ServiceAccountBoolQueryBuilder.build(
            QueryBuilders.termQuery(author + ".realm_domain", "corp")
        );
        assertThat(query.must(), contains(QueryBuilders.termQuery(author + ".realm_domain.name", "corp")));

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ServiceAccountBoolQueryBuilder.build(QueryBuilders.termQuery(author + ".realm_domain.name", "corp"))
        );
        assertThat(e.getMessage(), containsString("Field [" + author + ".realm_domain.name] is not allowed for querying or aggregation"));
    }

    public void testABoolQueryIsTranslatedClauseByClause() {
        final BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        if (randomBoolean()) {
            boolQuery.must(QueryBuilders.prefixQuery("username", "apps/"));
        }
        if (randomBoolean()) {
            boolQuery.should(QueryBuilders.wildcardQuery("username", "*/worker_*"));
        }
        if (randomBoolean()) {
            boolQuery.filter(QueryBuilders.termsQuery("roles", randomArray(1, 4, String[]::new, () -> "role-" + randomInt())));
        }
        if (randomBoolean()) {
            boolQuery.mustNot(QueryBuilders.termQuery("enabled", false));
        }
        if (randomBoolean()) {
            boolQuery.minimumShouldMatch(randomIntBetween(1, 2));
        }

        final ServiceAccountBoolQueryBuilder query = ServiceAccountBoolQueryBuilder.build(boolQuery);
        assertThat(query.must(), hasSize(1));
        assertThat(query.must().get(0), instanceOf(BoolQueryBuilder.class));
        final BoolQueryBuilder translated = (BoolQueryBuilder) query.must().get(0);
        // Every field name is already the index-level name, so the translation is a structural copy.
        assertThat(translated.must(), equalTo(boolQuery.must()));
        assertThat(translated.should(), equalTo(boolQuery.should()));
        assertThat(translated.mustNot(), equalTo(boolQuery.mustNot()));
        assertThat(translated.filter(), equalTo(boolQuery.filter()));
        assertThat(translated.minimumShouldMatch(), equalTo(boolQuery.minimumShouldMatch()));
        assertThat(query.filter(), contains(QueryBuilders.termQuery("doc_type", SERVICE_ACCOUNT_DOC_TYPE)));
    }

    public void testFieldsOutsideTheAllowlistAreRejected() {
        // Fields of other document types in the security index, and fields of the account document itself that the
        // API does not expose, are refused alike.
        // The metadata of an API key's creator, and the realms that make up a domain, are stored under the same
        // "creator" object but are not part of an account's attribution.
        final String fieldName = randomFrom(
            "doc_type",
            "version",
            "password",
            "full_name",
            "metadata_flattened",
            "creator.metadata",
            "creator.metadata.foo",
            "creator.realm_domain.realms.name",
            "editor.realm_domain.realms.type"
        );
        final QueryBuilder query = randomValueOtherThanMany(q -> q instanceof MatchAllQueryBuilder, () -> randomSimpleQuery(fieldName));
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ServiceAccountBoolQueryBuilder.build(query));
        assertThat(e.getMessage(), containsString("Field [" + fieldName + "] is not allowed for querying or aggregation"));
    }

    public void testFieldNamePatternsAreRejected() {
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ServiceAccountBoolQueryBuilder.build(QueryBuilders.termQuery("user*", "apps/worker_1"))
        );
        assertThat(e.getMessage(), containsString("Field name pattern [user*] is not allowed for querying or aggregation"));
    }

    public void testTermsLookupIsRejected() {
        final TermsQueryBuilder query = QueryBuilders.termsLookupQuery("roles", new TermsLookup("lookup", "1", "id"));
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ServiceAccountBoolQueryBuilder.build(query));
        assertThat(e.getMessage(), containsString("terms query with terms lookup is not currently supported in this context"));
    }

    public void testUnsupportedQueryTypesAreRejected() {
        final AbstractQueryBuilder<?> query = randomFrom(
            QueryBuilders.queryStringQuery("username:apps*"),
            QueryBuilders.regexpQuery("username", "apps/.*"),
            QueryBuilders.fuzzyQuery("username", "apps/worker"),
            QueryBuilders.scriptQuery(new Script(randomAlphaOfLength(5))),
            QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("roles", "role-a")),
            QueryBuilders.disMaxQuery(),
            QueryBuilders.wrapperQuery(randomAlphaOfLength(5))
        );
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ServiceAccountBoolQueryBuilder.build(query));
        assertThat(e.getMessage(), containsString("Query type [" + query.getName() + "] is not currently supported in this context"));
    }

    public void testTheSearchContextIsRestrictedToTheAllowedIndexFields() {
        final ServiceAccountBoolQueryBuilder query = ServiceAccountBoolQueryBuilder.build(randomSimpleQuery(randomFrom(ALLOWED_FIELDS)));
        final SearchExecutionContext context = mock(SearchExecutionContext.class);
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final Predicate<String> allowed = (Predicate<String>) invocation.getArguments()[0];
            for (String field : ALLOWED_FIELDS) {
                assertTrue(field, allowed.test(field));
            }
            // The realm domain's index-level name passes; its query-level name is an object and does not.
            assertTrue(allowed.test("creator.realm_domain.name"));
            assertTrue(allowed.test("editor.realm_domain.name"));
            assertFalse(allowed.test("creator.realm_domain"));
            // The filter's own field and the document id have to pass so the query built here can run at all.
            assertTrue(allowed.test("doc_type"));
            assertTrue(allowed.test("_id"));
            for (String field : List.of(
                "version",
                "password",
                "type",
                "full_name",
                "metadata_flattened",
                "creator.metadata",
                "creator.realm_domain.realms.name"
            )) {
                assertFalse(field, allowed.test(field));
            }
            return null;
        }).when(context).setAllowedFields(any());
        try {
            if (randomBoolean()) {
                query.toQuery(context);
            } else {
                query.doRewrite(context);
            }
        } catch (Exception e) {
            // The mocked context cannot build a Lucene query; this test is only about the allowed-fields restriction.
        } finally {
            verify(context).setAllowedFields(any());
        }
    }

    private QueryBuilder randomSimpleQuery(String fieldName) {
        return randomFrom(
            QueryBuilders.termQuery(fieldName, randomAlphaOfLengthBetween(3, 8)),
            QueryBuilders.termsQuery(fieldName, randomArray(1, 3, String[]::new, () -> randomAlphaOfLengthBetween(3, 8))),
            QueryBuilders.prefixQuery(fieldName, randomAlphaOfLength(randomIntBetween(3, 10))),
            QueryBuilders.wildcardQuery(fieldName, "*" + randomAlphaOfLength(randomIntBetween(3, 10))),
            QueryBuilders.matchQuery(fieldName, randomAlphaOfLengthBetween(3, 8)),
            QueryBuilders.matchAllQuery(),
            QueryBuilders.existsQuery(fieldName)
        );
    }
}
