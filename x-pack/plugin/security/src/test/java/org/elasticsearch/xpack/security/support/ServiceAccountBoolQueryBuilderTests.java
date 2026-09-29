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
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Stream;

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
     * Query-level names that are also the index-level names: the account's own fields and those of its updater.
     */
    private static final List<String> IDEM_FIELDS = List.of(
        "username",
        "roles",
        "enabled",
        "description",
        "updated_by.principal",
        "updated_by.full_name",
        "updated_by.email",
        "updated_by.realm",
        "updated_by.realm_type",
        "updated_by.api_key.id",
        "updated_by.api_key.name"
    );

    /**
     * Query-level names whose index-level names differ, by their index-level names. The creator and the creation time
     * are stored in the fields that API keys established, and the realm domain of either author is queried by the
     * name a response reports.
     */
    private static final Map<String, String> TRANSLATED_FIELDS = Map.ofEntries(
        Map.entry("created_by.principal", "creator.principal"),
        Map.entry("created_by.full_name", "creator.full_name"),
        Map.entry("created_by.email", "creator.email"),
        Map.entry("created_by.realm", "creator.realm"),
        Map.entry("created_by.realm_type", "creator.realm_type"),
        Map.entry("created_by.realm_domain", "creator.realm_domain.name"),
        Map.entry("created_by.api_key.id", "creator.api_key.id"),
        Map.entry("created_by.api_key.name", "creator.api_key.name"),
        Map.entry("created_at", "creation_time"),
        Map.entry("updated_by.realm_domain", "updated_by.realm_domain.name"),
        Map.entry("updated_at", "update_time")
    );

    private static final List<String> QUERY_FIELDS = Stream.concat(IDEM_FIELDS.stream(), TRANSLATED_FIELDS.keySet().stream()).toList();

    private static final List<String> INDEX_FIELDS = Stream.concat(IDEM_FIELDS.stream(), TRANSLATED_FIELDS.values().stream()).toList();

    public void testANullQuerySelectsEveryServiceAccountDocument() {
        final ServiceAccountBoolQueryBuilder query = ServiceAccountBoolQueryBuilder.build(null);
        assertThat(query.must(), empty());
        assertThat(query.should(), empty());
        assertThat(query.mustNot(), empty());
        assertThat(query.filter(), contains(QueryBuilders.termQuery("doc_type", SERVICE_ACCOUNT_DOC_TYPE)));
    }

    public void testASimpleQueryIsKeptAndRestrictedToServiceAccountDocuments() {
        final QueryBuilder simpleQuery = randomSimpleQuery(randomFrom(IDEM_FIELDS));
        final ServiceAccountBoolQueryBuilder query = ServiceAccountBoolQueryBuilder.build(simpleQuery);
        assertThat(query.must(), contains(simpleQuery));
        assertThat(query.should(), empty());
        assertThat(query.mustNot(), empty());
        assertThat(query.filter(), contains(QueryBuilders.termQuery("doc_type", SERVICE_ACCOUNT_DOC_TYPE)));
    }

    /**
     * The creator and the creation time are queried by the names a response reports and stored in the fields that API
     * keys established, so a query on them is rewritten to the stored name. The updater's timestamp follows the index's
     * naming of timestamps for the same reason. The stored names are not accepted as query fields.
     */
    public void testFieldsStoredUnderAnotherNameAreTranslated() {
        for (Map.Entry<String, String> field : TRANSLATED_FIELDS.entrySet()) {
            final ServiceAccountBoolQueryBuilder query = ServiceAccountBoolQueryBuilder.build(QueryBuilders.termQuery(field.getKey(), "x"));
            assertThat(field.getKey(), query.must(), contains(QueryBuilders.termQuery(field.getValue(), "x")));
        }
        final String indexField = randomFrom("creator.principal", "creator.realm", "creation_time", "update_time");
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ServiceAccountBoolQueryBuilder.build(QueryBuilders.termQuery(indexField, "x"))
        );
        assertThat(e.getMessage(), containsString("Field [" + indexField + "] is not allowed for querying or aggregation"));
    }

    /**
     * A response names the realm domain by its name alone, so the query field follows the response and is translated
     * to the {@code name} of the stored domain object. The index-level name is not accepted as a query field.
     */
    public void testTheRealmDomainIsQueriedByTheNameAResponseReports() {
        final String author = randomFrom("created_by", "updated_by");
        final String storedAuthor = author.equals("created_by") ? "creator" : author;
        final ServiceAccountBoolQueryBuilder query = ServiceAccountBoolQueryBuilder.build(
            QueryBuilders.termQuery(author + ".realm_domain", "corp")
        );
        assertThat(query.must(), contains(QueryBuilders.termQuery(storedAuthor + ".realm_domain.name", "corp")));

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
        // "creator" object but are not part of an account's attribution, under either of the object's names.
        final String fieldName = randomFrom(
            "doc_type",
            "version",
            "password",
            "full_name",
            "metadata_flattened",
            "creator.metadata",
            "created_by.metadata",
            "created_by.metadata.foo",
            "created_by.realm_domain.realms.name",
            "updated_by.realm_domain.realms.type"
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
        final ServiceAccountBoolQueryBuilder query = ServiceAccountBoolQueryBuilder.build(randomSimpleQuery(randomFrom(QUERY_FIELDS)));
        final SearchExecutionContext context = mock(SearchExecutionContext.class);
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final Predicate<String> allowed = (Predicate<String>) invocation.getArguments()[0];
            for (String field : INDEX_FIELDS) {
                assertTrue(field, allowed.test(field));
            }
            // The filter's own field and the document id have to pass so the query built here can run at all.
            assertTrue(allowed.test("doc_type"));
            assertTrue(allowed.test("_id"));
            // Query-level names that differ from the index-level ones are not index fields.
            for (String field : List.of(
                "version",
                "password",
                "type",
                "full_name",
                "metadata_flattened",
                "creator.metadata",
                "creator.realm_domain",
                "creator.realm_domain.realms.name",
                "created_by.principal",
                "created_at",
                "updated_by.realm_domain",
                "updated_at"
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
