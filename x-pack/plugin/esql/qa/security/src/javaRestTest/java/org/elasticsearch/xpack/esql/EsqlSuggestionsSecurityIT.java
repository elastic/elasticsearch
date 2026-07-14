/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.anything;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

/**
 * Security coverage for {@code POST /_esql/suggestions} (see the suggestions API spec).
 *
 * <p>The transport action performs index resolution (field-caps, which is FLS-enforced) on every
 * request that isn't a remote-qualified fallback, so this is index-scoped from security's point of
 * view. Reuses the {@code roles.yml} fixtures already defined for {@link EsqlSecurityIT}
 * ({@code user1}/{@code user2}, {@code fls_user}, {@code dls_user}) rather than inventing new roles.
 *
 * <p>Suggestions never reads document rows in its baseline (non-{@code includeSampleValues}) path —
 * analysis only resolves mappings, not rows — so DLS has no bearing on that path's output; the
 * hot-tier value-sampling path is the first one DLS/FLS interact with at the document level, covered
 * separately below.
 */
public class EsqlSuggestionsSecurityIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        .rolesFile(Resource.fromClasspath("roles.yml"))
        .user("test-admin", "x-pack-test-password", "test-admin", true)
        .user("user1", "x-pack-test-password", "user1", false)
        .user("user2", "x-pack-test-password", "user2", false)
        .user("fls_user", "x-pack-test-password", "fls_user", false)
        .user("dls_user", "x-pack-test-password", "dls_user", false)
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("test-admin", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Before
    public void indexDocuments() throws IOException {
        // `index` mirrors EsqlSecurityIT's shape: fls_user is granted [value, partial] on it, `org` is FLS-denied.
        String mapping = """
            "properties":{"value": {"type": "double"}, "org": {"type": "keyword"}, "partial": {"type": "text"}}
            """;
        createIndex("index", Settings.EMPTY, mapping);
        indexDoc("index", 1, 10.0, "sales");
        refresh("index");

        createIndex("index-user2", Settings.EMPTY, mapping);
        indexDoc("index-user2", 1, 20.0, "engineering");
        refresh("index-user2");

        // dls_user's role has a non-match-all DLS query on lookup-user2 (org == marketing).
        createIndex("lookup-user2", Settings.EMPTY, mapping);
        indexDoc("lookup-user2", 1, 30.0, "marketing");
        indexDoc("lookup-user2", 2, 40.0, "sales");
        refresh("lookup-user2");
    }

    private void indexDoc(String index, int id, double value, String org) throws IOException {
        Request indexDoc = new Request("PUT", index + "/_doc/" + id);
        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        builder.field("value", value);
        builder.field("org", org);
        builder.field("partial", org + value);
        indexDoc.setJsonEntity(Strings.toString(builder.endObject()));
        client().performRequest(indexDoc);
    }

    private Response runSuggestions(String user, String query, int cursor) throws IOException {
        return runSuggestions(user, query, cursor, false);
    }

    private Response runSuggestions(String user, String query, int cursor, boolean includeSampleValues) throws IOException {
        Request request = new Request("POST", "/_esql/suggestions");
        XContentBuilder json = JsonXContent.contentBuilder();
        json.startObject();
        json.field("query", query);
        json.field("cursor", cursor);
        json.field("include_sample_values", includeSampleValues);
        json.endObject();
        request.setJsonEntity(Strings.toString(json));
        // The analysis path this endpoint shares with real query execution can emit the ordinary "no
        // LIMIT" advisory warning header; this endpoint's own response-body `warnings` field (a closed
        // enum) is what these tests actually assert on, so tolerate any HTTP warning headers here.
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("es-security-runas-user", user).setWarningsHandler(warnings -> false)
        );
        return client().performRequest(request);
    }

    public void testUnauthorizedIndexIsDenied() {
        String query = "FROM index-user2 | KEEP val*";
        ResponseException e = expectThrows(ResponseException.class, () -> runSuggestions("user1", query, query.length()));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(403));
    }

    public void testFieldLevelSecurityRestrictsFieldsMap() throws IOException {
        String query = "FROM index | KEEP *";

        Response adminResp = runSuggestions("test-admin", query, query.length());
        assertOK(adminResp);
        @SuppressWarnings("unchecked")
        Map<String, Object> adminFields = (Map<String, Object>) entityAsMap(adminResp).get("fields");
        assertMap(adminFields, matchesMap().extraOk().entry("value", anything()).entry("org", anything()).entry("partial", anything()));

        Response flsResp = runSuggestions("fls_user", query, query.length());
        assertOK(flsResp);
        @SuppressWarnings("unchecked")
        Map<String, Object> flsFields = (Map<String, Object>) entityAsMap(flsResp).get("fields");
        // Permitted fields still present:
        assertMap(flsFields, matchesMap().extraOk().entry("value", anything()).entry("partial", anything()));
        // Restricted field correctly absent, not merely nulled out:
        assertThat(flsFields, not(hasKey("org")));
    }

    /**
     * DLS filters documents, not mappings. Suggestions' baseline path never reads document rows, so a
     * DLS role query has no bearing on the (identical) fields/types it returns — documented explicitly
     * as correct, not an oversight.
     */
    public void testDocumentLevelSecurityIsNoOpForBaselinePath() throws IOException {
        String query = "FROM lookup-user2 | KEEP *";

        Response adminResp = runSuggestions("test-admin", query, query.length());
        assertOK(adminResp);
        Response dlsResp = runSuggestions("dls_user", query, query.length());
        assertOK(dlsResp);

        @SuppressWarnings("unchecked")
        Map<String, Object> adminFields = (Map<String, Object>) entityAsMap(adminResp).get("fields");
        @SuppressWarnings("unchecked")
        Map<String, Object> dlsFields = (Map<String, Object>) entityAsMap(dlsResp).get("fields");
        assertMap(dlsFields, matchesMap(adminFields));
    }

    /**
     * Privilege-check ordering must not leak schema information via error-message differences: a
     * malformed/oversized query or out-of-range cursor from an authenticated-but-privileged-on-the-index
     * user still returns the same validation error an unauthenticated (post-auth) request would get.
     */
    public void testMalformedQueryReturnsPlainValidationError() throws IOException {
        ResponseException e = expectThrows(ResponseException.class, () -> runSuggestions("user1", "FROM (((", 3));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
    }

    public void testOutOfRangeCursorReturnsPlainValidationError() throws IOException {
        String query = "FROM index | KEEP val*";
        ResponseException e = expectThrows(ResponseException.class, () -> runSuggestions("user1", query, query.length() + 999));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(e.getMessage(), containsString("[cursor] must be within"));
    }

    /**
     * The hot-tier value-sampling path is the first one that reads documents at all, and reading a raw
     * {@code TermsEnum} bypasses normal per-document security filtering entirely — it needs its own
     * explicit FLS gate. {@code org} is FLS-denied for {@code fls_user} on {@code index} (see {@code
     * roles.yml}'s {@code grant: [value, partial]}), so a {@code WHERE org == "..."} equality context must
     * degrade safely: no {@code values}, no error.
     */
    public void testFieldLevelSecurityDegradesHotTierValueSampling() throws IOException {
        String query = "FROM index | WHERE org == \"\"";
        int cursor = query.indexOf("\"\"") + 1;
        // fls_user cannot see `org` at all (it is not in the role's field_security grant list), so
        // analysis of a query referencing it may reasonably fail before the value-sampling path is even
        // reached — that is itself a safe degradation (no leak), just not the same shape as a shard-level
        // FLS skip. Either outcome is acceptable here; what must never happen is `values` coming back.
        try {
            Response resp = runSuggestions("fls_user", query, cursor, true);
            Map<String, Object> body = entityAsMap(resp);
            @SuppressWarnings("unchecked")
            Map<String, Object> fields = (Map<String, Object>) body.get("fields");
            if (fields.containsKey("org")) {
                @SuppressWarnings("unchecked")
                Map<String, Object> org = (Map<String, Object>) fields.get("org");
                assertThat(org, not(hasKey("values")));
            }
        } catch (ResponseException e) {
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        }
    }

    /**
     * {@code dls_user}'s role has a non-match-all DLS query on {@code lookup-user2} ({@code org ==
     * marketing}). Unlike a real search, a raw {@code TermsEnum} read can't be filtered per document, so the
     * gate refuses the read outright rather than serving DLS-inconsistent {@code docFreq} numbers —
     * {@code dls_active} attaches and no {@code values} come back for the field.
     */
    public void testDocumentLevelSecurityDegradesHotTierValueSampling() throws IOException {
        String query = "FROM lookup-user2 | WHERE org == \"\"";
        int cursor = query.indexOf("\"\"") + 1;
        // Unlike fls_user, dls_user's role grants full field visibility on lookup-user2 (only a
        // document-level, non-match-all query restricts it), so analysis succeeds normally and the
        // hot-tier gate is the thing actually under test here.
        Response resp = runSuggestions("dls_user", query, cursor, true);
        assertOK(resp);
        Map<String, Object> body = entityAsMap(resp);
        @SuppressWarnings("unchecked")
        Map<String, Object> fields = (Map<String, Object>) body.get("fields");
        if (fields.containsKey("org")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> org = (Map<String, Object>) fields.get("org");
            assertThat(org, not(hasKey("values")));
        }
        @SuppressWarnings("unchecked")
        List<String> warnings = (List<String>) body.get("warnings");
        assertThat(warnings, hasItem("dls_active"));
    }
}
