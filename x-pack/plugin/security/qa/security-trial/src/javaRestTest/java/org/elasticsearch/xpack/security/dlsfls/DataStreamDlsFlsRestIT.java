/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.dlsfls;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xpack.security.SecurityOnTrialLicenseRestTestCase;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/**
 * Document and field level security on data streams with more than one backing index, exercised through the REST
 * layer. A grant on the data stream name is translated to every backing index, and the entry recorded under the data
 * stream name itself drives the request-level guards (suggest, profile, {@code min_doc_count: 0} terms aggregations,
 * alias creation). The scenarios cover:
 * <ul>
 * <li>DLS and FLS applied to every backing index of a data stream, through search, async search, point in time, field
 * capabilities, mappings and ES|QL, and the request-level guards keyed on the data stream name;</li>
 * <li>an unrestricted direct grant on one backing index: naming that index unions both grants, the data stream entry
 * keeps its DLS so the request-level guards still fire, and when only the data stream is named the outcome for that
 * index depends on where its shard is allocated relative to the coordinating node (see the test for details);</li>
 * <li>data stream aliases over several data streams, where each backing index gets the union of the alias and data
 * stream grants that cover it;</li>
 * <li>adding an alias to a data stream, rejected for a DLS/FLS user;</li>
 * <li>a failure store with more than one failure index, with and without an unrestricted direct grant;</li>
 * <li>an API key whose own role and owner role are intersected per backing index;</li>
 * <li>the request cache, whose key must separate users with different DLS on the same backing indices.</li>
 * </ul>
 */
public class DataStreamDlsFlsRestIT extends SecurityOnTrialLicenseRestTestCase {

    private static final SecureString PASSWORD = new SecureString("test-user-password".toCharArray());
    private static final String DATA_STREAM = "dls-ds-app";
    private static final String OTHER_DATA_STREAM = "dls-ds-web";
    private static final String DATA_STREAM_ALIAS = "dls-ds-all";

    private final List<String> usersToDelete = new ArrayList<>();
    private final List<String> rolesToDelete = new ArrayList<>();

    @After
    public void deleteUsersAndRoles() throws IOException {
        for (String user : usersToDelete) {
            deleteUser(user);
        }
        for (String role : rolesToDelete) {
            deleteRole(role);
        }
    }

    public void testDlsFlsOnDataStreamAppliesToEveryBackingIndex() throws IOException {
        final List<String> backingIndices = createDataStreamWithThreeBackingIndices(DATA_STREAM);
        // view_index_metadata is needed for the mappings API below
        final String user = createUserWithRole("ds_user", "ds_dls_fls_role", Strings.format("""
            {
              "indices": [
                {
                  "names": ["%s"],
                  "privileges": ["read", "view_index_metadata"],
                  "query": {"term": {"tenant": "a"}},
                  "field_security": {"grant": ["@timestamp", "tenant", "message"]}
                }
              ]
            }""", DATA_STREAM));

        // documents from the first and the last backing index match, and the field "secret" is stripped from all of them
        for (String target : List.of(DATA_STREAM, ".ds-" + DATA_STREAM + "-*", String.join(",", backingIndices))) {
            final Map<String, Map<String, Object>> hits = hitsById(search(user, target, MATCH_ALL));
            assertThat("searching [" + target + "]", hits.keySet(), containsInAnyOrder("a1", "a3"));
            assertThat(hits.get("a1").get("_index"), equalTo(backingIndices.get(0)));
            assertThat(hits.get("a3").get("_index"), equalTo(backingIndices.get(2)));
            hits.values().forEach(hit -> assertSourceHasOnly(hit, "@timestamp", "tenant", "message"));
        }

        // async search
        final Request asyncSearch = new Request("POST", "/" + DATA_STREAM + "/_async_search");
        asyncSearch.addParameter("wait_for_completion_timeout", "30s");
        asyncSearch.setJsonEntity(MATCH_ALL);
        final ObjectPath asyncResponse = ObjectPath.createFromResponse(performRequest(user, asyncSearch));
        assertThat(asyncResponse.evaluate("is_running"), is(false));
        assertThat(asyncResponse.evaluate("is_partial"), is(false));
        final List<Map<String, Object>> asyncHits = asyncResponse.evaluate("response.hits.hits");
        assertThat(asyncHits.stream().map(hit -> (String) hit.get("_id")).toList(), containsInAnyOrder("a1", "a3"));

        // point in time: the PIT search is authorized against the concrete indices encoded in the PIT id
        final Request openPit = new Request("POST", "/" + DATA_STREAM + "/_pit");
        openPit.addParameter("keep_alive", "1m");
        final String pitId = ObjectPath.createFromResponse(performRequest(user, openPit)).evaluate("id");
        try {
            final Request pitSearch = new Request("POST", "/_search");
            pitSearch.setJsonEntity(Strings.format("""
                {"size": 10, "pit": {"id": "%s", "keep_alive": "1m"}}""", pitId));
            final Map<String, Map<String, Object>> pitHits = hitsById(performRequest(user, pitSearch));
            assertThat(pitHits.keySet(), containsInAnyOrder("a1", "a3"));
            pitHits.values().forEach(hit -> assertSourceHasOnly(hit, "@timestamp", "tenant", "message"));
        } finally {
            final Request closePit = new Request("DELETE", "/_pit");
            closePit.setJsonEntity(Strings.format("""
                {"id": "%s"}""", pitId));
            assertOK(performRequest(user, closePit));
        }

        // field capabilities and mappings apply the field filter per backing index
        final Request fieldCaps = new Request("GET", "/" + DATA_STREAM + "/_field_caps");
        fieldCaps.addParameter("fields", "*");
        final Map<String, Object> fields = ObjectPath.createFromResponse(performRequest(user, fieldCaps)).evaluate("fields");
        assertThat(fields, hasKey("tenant"));
        assertThat(fields, hasKey("message"));
        assertThat(fields, not(hasKey("secret")));
        final Map<String, Object> mappings = responseAsMap(performRequest(user, new Request("GET", "/" + DATA_STREAM + "/_mapping")));
        assertThat(mappings.keySet(), containsInAnyOrder(backingIndices.toArray()));
        for (String backingIndex : backingIndices) {
            @SuppressWarnings("unchecked")
            final Map<String, Object> properties = (Map<String, Object>) XContentMapValues.extractValue(
                backingIndex + ".mappings.properties",
                mappings
            );
            assertThat("mapping of [" + backingIndex + "]", properties, hasKey("tenant"));
            assertThat("mapping of [" + backingIndex + "]", properties, not(hasKey("secret")));
        }

        // ES|QL
        assertThat(esqlValues(user, "FROM " + DATA_STREAM + " | STATS c = COUNT(*) | LIMIT 10"), equalTo(List.of(List.of(2))));
        final Request esqlColumns = new Request("POST", "/_query");
        esqlColumns.setJsonEntity(Strings.format("""
            {"query": "FROM %s | LIMIT 1"}""", DATA_STREAM));
        final List<Map<String, Object>> columns = ObjectPath.createFromResponse(performRequest(user, esqlColumns)).evaluate("columns");
        final List<String> columnNames = columns.stream().map(column -> (String) column.get("name")).toList();
        assertThat(columnNames, containsInAnyOrder("@timestamp", "tenant", "message"));

        // request-level guards keyed on the data stream name
        assertSuggestIsRejected(user, DATA_STREAM);
        assertProfileIsRejected(user, DATA_STREAM);
    }

    public void testDirectGrantOnWriteIndexRelaxesOnlyThatIndexWhenNamed() throws IOException {
        final List<String> backingIndices = createDataStreamWithThreeBackingIndices(DATA_STREAM);
        final String writeIndex = backingIndices.get(2);
        // two groups with different name sets, so they are not merged at role-build time
        final String user = createUserWithRole("ds_mixed_user", "ds_mixed_role", Strings.format("""
            {
              "indices": [
                {"names": ["%s"], "privileges": ["read"], "query": {"term": {"tenant": "a"}}},
                {"names": ["%s"], "privileges": ["read"]}
              ]
            }""", DATA_STREAM, writeIndex));

        // The write index named next to the data stream or via a wildcard: both grants cover it, so it is unrestricted,
        // while the older backing indices only ever see the data stream grant.
        assertThat(hitIds(search(user, DATA_STREAM + "," + writeIndex, MATCH_ALL)), containsInAnyOrder("a1", "a3", "b3"));
        assertThat(hitIds(search(user, ".ds-" + DATA_STREAM + "-*", MATCH_ALL)), containsInAnyOrder("a1", "a3", "b3"));
        assertThat(hitIds(search(user, writeIndex, MATCH_ALL)), containsInAnyOrder("a3", "b3"));
        assertThat(hitIds(search(user, backingIndices.get(1), MATCH_ALL)), hasSize(0));

        // The data stream alone. At the coordinator the direct grant does not match the data stream, so the write index
        // entry carries the data stream's DLS. Shard-level query requests, however, name the concrete backing index: on
        // the coordinating node they are authorized with the coordinator's entries (DLS applies, "b3" hidden), on any
        // other node they are authorized on their own and the direct grant is unioned in ("b3" visible); the fetch
        // phase is authorized on its own on every node. Which one happens depends on where the write index shard is
        // allocated relative to the coordinating node, so only the invariants are asserted here: "a1" and "a3" are
        // always visible, "b2" never is. The same holds for the terms aggregation below, whose forced exclusion of
        // hidden documents keeps "s-b2" out in either case.
        for (int i = 0; i < 4; i++) {
            final Set<String> ids = hitIds(search(user, DATA_STREAM, MATCH_ALL));
            assertThat(ids, hasItems("a1", "a3"));
            assertThat(ids, not(hasItem("b2")));
            assertThat(ids, everyItem(is(in(Set.of("a1", "a3", "b3")))));
            final List<String> keys = termsAggregationKeys(user, DATA_STREAM);
            assertThat(keys, hasItems("s-a1", "s-a3"));
            assertThat(keys, not(hasItem("s-b2")));
        }
        assertThat(termsAggregationKeys(user, DATA_STREAM + "," + writeIndex), containsInAnyOrder("s-a1", "s-a3", "s-b3"));

        // The request-level guards are decided by the coordinator's entries for the requested names. The data stream
        // entry must keep its DLS even though the write index requested next to it is unrestricted: before this was
        // fixed that entry could be overwritten with the write index's unrestricted access, depending on the iteration
        // order of the requested names.
        assertSuggestIsRejected(user, DATA_STREAM);
        assertSuggestIsRejected(user, DATA_STREAM + "," + writeIndex);
        assertProfileIsRejected(user, DATA_STREAM + "," + writeIndex);
        // whereas the unrestricted write index on its own may use them
        assertOK(search(user, writeIndex, SUGGEST));
        assertOK(search(user, writeIndex, PROFILE));
    }

    public void testDataStreamAliasAndDataStreamGrantsAreUnionedPerBackingIndex() throws IOException {
        createDataStreamWithThreeBackingIndices(DATA_STREAM);
        createOtherDataStream();
        final Request addAlias = new Request("POST", "/_aliases");
        addAlias.setJsonEntity(Strings.format("""
            {
              "actions": [
                {"add": {"index": "%s", "alias": "%s"}},
                {"add": {"index": "%s", "alias": "%s"}}
              ]
            }""", DATA_STREAM, DATA_STREAM_ALIAS, OTHER_DATA_STREAM, DATA_STREAM_ALIAS));
        assertOK(adminClient().performRequest(addAlias));

        final String user = createUserWithRole("ds_alias_user", "ds_alias_role", Strings.format("""
            {
              "indices": [
                {
                  "names": ["%s"],
                  "privileges": ["read"],
                  "query": {"term": {"tenant": "a"}},
                  "field_security": {"grant": ["@timestamp", "tenant"]}
                },
                {
                  "names": ["%s"],
                  "privileges": ["read"],
                  "query": {"term": {"tenant": "b"}},
                  "field_security": {"grant": ["@timestamp", "secret"]}
                }
              ]
            }""", DATA_STREAM_ALIAS, OTHER_DATA_STREAM));

        // through the alias only the alias grant applies, to the backing indices of both data streams
        Map<String, Map<String, Object>> hits = hitsById(search(user, DATA_STREAM_ALIAS, MATCH_ALL));
        assertThat(hits.keySet(), containsInAnyOrder("a1", "a3", "w1"));
        hits.values().forEach(hit -> assertSourceHasOnly(hit, "@timestamp", "tenant"));

        // through the other data stream's name only its own grant applies
        hits = hitsById(search(user, OTHER_DATA_STREAM, MATCH_ALL));
        assertThat(hits.keySet(), containsInAnyOrder("w2"));
        hits.values().forEach(hit -> assertSourceHasOnly(hit, "@timestamp", "secret"));

        // requested together, the backing indices of the other data stream get the union of both grants
        // (either tenant, both field grants) while the first data stream's backing indices keep the alias grant
        hits = hitsById(search(user, DATA_STREAM_ALIAS + "," + OTHER_DATA_STREAM, MATCH_ALL));
        assertThat(hits.keySet(), containsInAnyOrder("a1", "a3", "w1", "w2"));
        assertSourceHasOnly(hits.get("a1"), "@timestamp", "tenant");
        assertSourceHasOnly(hits.get("a3"), "@timestamp", "tenant");
        assertSourceHasOnly(hits.get("w1"), "@timestamp", "tenant", "secret");
        assertSourceHasOnly(hits.get("w2"), "@timestamp", "tenant", "secret");
    }

    public void testAddAliasToDataStreamIsRejectedForDlsFlsUser() throws IOException {
        createDataStreamWithThreeBackingIndices(DATA_STREAM);
        final String aliasName = "dls-ds-new-alias";
        final String dlsUser = createUserWithRole("ds_alias_manage_dls_user", "ds_alias_manage_dls_role", Strings.format("""
            {
              "indices": [
                {"names": ["%s", "%s"], "privileges": ["manage"], "query": {"term": {"tenant": "a"}}}
              ]
            }""", DATA_STREAM, aliasName));
        final String plainUser = createUserWithRole("ds_alias_manage_user", "ds_alias_manage_role", Strings.format("""
            {
              "indices": [
                {"names": ["%s", "%s"], "privileges": ["manage"]}
              ]
            }""", DATA_STREAM, aliasName));
        final String addAliasBody = Strings.format("""
            {"actions": [{"add": {"index": "%s", "alias": "%s"}}]}""", DATA_STREAM, aliasName);

        final Request rejected = new Request("POST", "/_aliases");
        rejected.setJsonEntity(addAliasBody);
        final ResponseException e = expectThrows(ResponseException.class, () -> performRequest(dlsUser, rejected));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(
            e.getMessage(),
            containsString("Alias requests are not allowed for users who have field or document level security enabled")
        );

        final Request allowed = new Request("POST", "/_aliases");
        allowed.setJsonEntity(addAliasBody);
        assertOK(performRequest(plainUser, allowed));
    }

    public void testFailureStoreDlsFlsAppliesToEveryFailureIndex() throws IOException {
        createDataStreamWithThreeBackingIndices(DATA_STREAM);
        final List<String> failureIndices = createFailureStoreWithTwoIndices(DATA_STREAM);
        final String secondFailureIndex = failureIndices.get(1);
        final String user = createUserWithRole("ds_failures_user", "ds_failures_role", Strings.format("""
            {
              "indices": [
                {
                  "names": ["%s"],
                  "privileges": ["read_failure_store"],
                  "query": {"term": {"document.id": "f1"}},
                  "field_security": {"grant": ["@timestamp", "document.id", "error.type"]}
                }
              ]
            }""", DATA_STREAM));

        // only the failure document of "f1", stored in the first failure index, matches; FLS strips the rest
        for (String target : List.of(DATA_STREAM + "::failures", ".fs-" + DATA_STREAM + "-*", String.join(",", failureIndices))) {
            final List<Map<String, Object>> hits = hits(search(user, target, MATCH_ALL));
            assertThat("searching [" + target + "]", hits, hasSize(1));
            assertThat(hits.get(0).get("_index"), equalTo(failureIndices.get(0)));
            assertThat(XContentMapValues.extractValue("_source.document.id", hits.get(0)), equalTo("f1"));
            assertSourceHasOnly(hits.get(0), "@timestamp", "document", "error");
            assertThat(XContentMapValues.extractValue("_source.document.source", hits.get(0)), is(nullValue()));
            assertThat(XContentMapValues.extractValue("_source.error.message", hits.get(0)), is(nullValue()));
        }
        assertSuggestIsRejected(user, DATA_STREAM + "::failures");

        // An unrestricted direct grant on the second failure index: when that index is named it is unrestricted, the
        // first failure index keeps the DLS. When only ::failures is named the outcome for the second failure index
        // depends on shard placement, exactly as for backing indices (see the write index scenario), so only the
        // invariants are asserted: "f1" is always visible and nothing else can appear besides "f2".
        final String mixedUser = createUserWithRole("ds_failures_mixed_user", "ds_failures_mixed_role", Strings.format("""
            {
              "indices": [
                {"names": ["%s"], "privileges": ["read_failure_store"], "query": {"term": {"document.id": "f1"}}},
                {"names": ["%s"], "privileges": ["read"]}
              ]
            }""", DATA_STREAM, secondFailureIndex));
        assertThat(
            documentIds(search(mixedUser, DATA_STREAM + "::failures," + secondFailureIndex, MATCH_ALL)),
            containsInAnyOrder("f1", "f2")
        );
        assertThat(documentIds(search(mixedUser, secondFailureIndex, MATCH_ALL)), containsInAnyOrder("f2"));
        for (int i = 0; i < 4; i++) {
            final Set<String> ids = documentIds(search(mixedUser, DATA_STREAM + "::failures", MATCH_ALL));
            assertThat(ids, hasItem("f1"));
            assertThat(ids, everyItem(is(in(Set.of("f1", "f2")))));
        }
        assertSuggestIsRejected(mixedUser, DATA_STREAM + "::failures");
        assertSuggestIsRejected(mixedUser, DATA_STREAM + "::failures," + secondFailureIndex);
    }

    public void testApiKeyAndOwnerRolesAreIntersectedPerBackingIndex() throws IOException {
        createDataStreamWithThreeBackingIndices(DATA_STREAM);
        final String owner = createUserWithRole("ds_api_key_owner", "ds_api_key_owner_role", Strings.format("""
            {
              "cluster": ["manage_own_api_key"],
              "indices": [
                {"names": ["%s"], "privileges": ["read"], "query": {"term": {"tenant": "a"}}}
              ]
            }""", DATA_STREAM));
        final Request createApiKey = new Request("POST", "/_security/api_key");
        createApiKey.setJsonEntity(Strings.format("""
            {
              "name": "ds-dls-fls-key",
              "role_descriptors": {
                "key_role": {
                  "indices": [
                    {"names": ["%s"], "privileges": ["read"], "field_security": {"grant": ["@timestamp", "tenant"]}}
                  ]
                }
              }
            }""", DATA_STREAM));
        final String encodedApiKey = ObjectPath.createFromResponse(performRequest(owner, createApiKey)).evaluate("encoded");

        // DLS from the owner role and FLS from the API key role both apply, to every backing index
        for (String target : List.of(DATA_STREAM, ".ds-" + DATA_STREAM + "-*")) {
            final Request search = new Request("GET", "/" + target + "/_search");
            search.setJsonEntity(MATCH_ALL);
            search.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "ApiKey " + encodedApiKey));
            final Map<String, Map<String, Object>> hits = hitsById(client().performRequest(search));
            assertThat("searching [" + target + "]", hits.keySet(), containsInAnyOrder("a1", "a3"));
            hits.values().forEach(hit -> assertSourceHasOnly(hit, "@timestamp", "tenant"));
        }
        final Request suggest = new Request("GET", "/" + DATA_STREAM + "/_search");
        suggest.setJsonEntity(SUGGEST);
        suggest.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "ApiKey " + encodedApiKey));
        final ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(suggest));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(e.getMessage(), containsString("Suggest isn't supported if document level security is enabled"));
    }

    public void testRequestCacheSeparatesUsersWithDifferentDlsOnTheSameBackingIndices() throws IOException {
        final List<String> backingIndices = createDataStreamWithThreeBackingIndices(DATA_STREAM);
        final int shards = backingIndices.size(); // one primary shard per backing index, no replicas
        final String userA = createUserWithRole("ds_cache_user_a", "ds_cache_role_a", dlsRole(DATA_STREAM, "a"));
        final String userB = createUserWithRole("ds_cache_user_b", "ds_cache_role_b", dlsRole(DATA_STREAM, "b"));
        assertRequestCacheState(0, 0);

        assertThat(cachedTotalHits(userA), equalTo(2));
        assertRequestCacheState(0, shards);
        assertThat(cachedTotalHits(userA), equalTo(2));
        assertRequestCacheState(shards, shards);

        // a user with a different DLS query on the same shards must not be served from user A's cached entries
        assertThat(cachedTotalHits(userB), equalTo(2));
        assertRequestCacheState(shards, 2 * shards);
        assertThat(cachedTotalHits(userB), equalTo(2));
        assertRequestCacheState(2 * shards, 2 * shards);
    }

    private static final String MATCH_ALL = """
        {"size": 10, "query": {"match_all": {}}}""";
    private static final String SUGGEST = """
        {"suggest": {"message_suggest": {"text": "alpa", "term": {"field": "message"}}}}""";
    private static final String PROFILE = """
        {"profile": true, "query": {"match_all": {}}}""";

    private static String dlsRole(String dataStream, String tenant) {
        return Strings.format("""
            {
              "indices": [
                {"names": ["%s"], "privileges": ["read"], "query": {"term": {"tenant": "%s"}}}
              ]
            }""", dataStream, tenant);
    }

    private String createUserWithRole(String username, String roleName, String roleDescriptor) throws IOException {
        upsertRole(roleDescriptor, roleName);
        rolesToDelete.add(roleName);
        createUser(username, PASSWORD, List.of(roleName));
        usersToDelete.add(username);
        return username;
    }

    private void createTemplates() throws IOException {
        final Request componentTemplate = new Request("PUT", "/_component_template/dls-ds-component");
        componentTemplate.setJsonEntity("""
            {
              "template": {
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {
                  "properties": {
                    "@timestamp": {"type": "date"},
                    "tenant": {"type": "keyword"},
                    "message": {"type": "text"},
                    "secret": {"type": "keyword"},
                    "age": {"type": "integer"}
                  }
                },
                "data_stream_options": {"failure_store": {"enabled": true}}
              }
            }""");
        assertOK(adminClient().performRequest(componentTemplate));
        final Request indexTemplate = new Request("PUT", "/_index_template/dls-ds-template");
        indexTemplate.setJsonEntity("""
            {
              "index_patterns": ["dls-ds-*"],
              "data_stream": {},
              "priority": 500,
              "composed_of": ["dls-ds-component"]
            }""");
        assertOK(adminClient().performRequest(indexTemplate));
    }

    /**
     * Three backing indices: {@code a1} in the first, {@code b2} in the second, {@code a3} and {@code b3} in the third
     * (the write index). Returns the backing index names in rollover order.
     */
    private List<String> createDataStreamWithThreeBackingIndices(String dataStream) throws IOException {
        createTemplates();
        indexDocument(dataStream, "a1", "a", "alpha one", "s-a1");
        rollover(dataStream);
        indexDocument(dataStream, "b2", "b", "bravo two", "s-b2");
        rollover(dataStream);
        indexDocument(dataStream, "a3", "a", "alpha three", "s-a3");
        indexDocument(dataStream, "b3", "b", "bravo three", "s-b3");
        final List<String> backingIndices = backingIndices(dataStream);
        assertThat(backingIndices, hasSize(3));
        return backingIndices;
    }

    /** Two backing indices: {@code w1} (tenant a) in the first, {@code w2} (tenant b) and {@code w3} (tenant c) in the second. */
    private void createOtherDataStream() throws IOException {
        indexDocument(OTHER_DATA_STREAM, "w1", "a", "web one", "s-w1");
        rollover(OTHER_DATA_STREAM);
        indexDocument(OTHER_DATA_STREAM, "w2", "b", "web two", "s-w2");
        indexDocument(OTHER_DATA_STREAM, "w3", "c", "web three", "s-w3");
        assertThat(backingIndices(OTHER_DATA_STREAM), hasSize(2));
    }

    /**
     * Two failure indices: the failed document {@code f1} is redirected into the first, {@code f2} into the second
     * after rolling the failure store over. Returns the failure index names in rollover order.
     */
    private List<String> createFailureStoreWithTwoIndices(String dataStream) throws IOException {
        indexFailingDocument(dataStream, "f1");
        rollover(dataStream + "::failures");
        indexFailingDocument(dataStream, "f2");
        final List<String> failureIndices = failureIndices(dataStream);
        assertThat(failureIndices, hasSize(2));
        return failureIndices;
    }

    private void indexDocument(String dataStream, String id, String tenant, String message, String secret) throws IOException {
        final Request request = new Request("PUT", "/" + dataStream + "/_doc/" + id);
        request.addParameter("op_type", "create");
        request.addParameter("refresh", "true");
        request.setJsonEntity(Strings.format("""
            {"@timestamp": "2026-01-01T00:00:00Z", "tenant": "%s", "message": "%s", "secret": "%s"}""", tenant, message, secret));
        assertThat(adminClient().performRequest(request).getStatusLine().getStatusCode(), equalTo(201));
    }

    private void indexFailingDocument(String dataStream, String id) throws IOException {
        final Request request = new Request("PUT", "/" + dataStream + "/_doc/" + id);
        request.addParameter("op_type", "create");
        request.addParameter("refresh", "true");
        request.setJsonEntity("""
            {"@timestamp": "2026-01-01T00:00:00Z", "tenant": "a", "age": "not an integer"}""");
        final Response response = adminClient().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(201));
        assertThat(ObjectPath.createFromResponse(response).evaluate("failure_store"), equalTo("used"));
    }

    private void rollover(String target) throws IOException {
        assertOK(adminClient().performRequest(new Request("POST", "/" + target + "/_rollover")));
    }

    @SuppressWarnings("unchecked")
    private List<String> backingIndices(String dataStream) throws IOException {
        final Map<String, Object> response = responseAsMap(adminClient().performRequest(new Request("GET", "/_data_stream/" + dataStream)));
        return (List<String>) XContentMapValues.extractValue("data_streams.indices.index_name", response);
    }

    @SuppressWarnings("unchecked")
    private List<String> failureIndices(String dataStream) throws IOException {
        final Map<String, Object> response = responseAsMap(adminClient().performRequest(new Request("GET", "/_data_stream/" + dataStream)));
        return (List<String>) XContentMapValues.extractValue("data_streams.failure_store.indices.index_name", response);
    }

    private Response performRequest(String user, Request request) throws IOException {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue(user, PASSWORD)));
        return client().performRequest(request);
    }

    private Response search(String user, String target, String body) throws IOException {
        final Request request = new Request("GET", "/" + target + "/_search");
        request.setJsonEntity(body);
        return performRequest(user, request);
    }

    private void assertSuggestIsRejected(String user, String target) {
        final ResponseException e = expectThrows(ResponseException.class, () -> search(user, target, SUGGEST));
        assertThat("suggest on [" + target + "]", e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(e.getMessage(), containsString("Suggest isn't supported if document level security is enabled"));
    }

    private void assertProfileIsRejected(String user, String target) {
        final ResponseException e = expectThrows(ResponseException.class, () -> search(user, target, PROFILE));
        assertThat("profile on [" + target + "]", e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(e.getMessage(), containsString("A search request cannot be profiled if document level security is enabled"));
    }

    private List<String> termsAggregationKeys(String user, String target) throws IOException {
        final Response response = search(user, target, """
            {"size": 0, "aggs": {"secrets": {"terms": {"field": "secret", "min_doc_count": 0}}}}""");
        final List<Map<String, Object>> buckets = ObjectPath.createFromResponse(response).evaluate("aggregations.secrets.buckets");
        return buckets.stream().map(bucket -> (String) bucket.get("key")).toList();
    }

    private List<List<Object>> esqlValues(String user, String query) throws IOException {
        final Request request = new Request("POST", "/_query");
        request.setJsonEntity(Strings.format("""
            {"query": "%s"}""", query));
        return ObjectPath.createFromResponse(performRequest(user, request)).evaluate("values");
    }

    private int cachedTotalHits(String user) throws IOException {
        final Request request = new Request("GET", "/" + DATA_STREAM + "/_search");
        request.addParameter("request_cache", "true");
        request.setJsonEntity("""
            {"size": 0, "query": {"match_all": {}}}""");
        return ObjectPath.createFromResponse(performRequest(user, request)).evaluate("hits.total.value");
    }

    private void assertRequestCacheState(int expectedHits, int expectedMisses) throws IOException {
        final Request request = new Request("GET", "/" + DATA_STREAM + "/_stats/request_cache");
        request.addParameter("filter_path", "_all.total.request_cache");
        final ObjectPath stats = ObjectPath.createFromResponse(adminClient().performRequest(request));
        assertThat("request cache hits", stats.evaluate("_all.total.request_cache.hit_count"), equalTo(expectedHits));
        assertThat("request cache misses", stats.evaluate("_all.total.request_cache.miss_count"), equalTo(expectedMisses));
    }

    private static List<Map<String, Object>> hits(Response response) throws IOException {
        return ObjectPath.createFromResponse(response).evaluate("hits.hits");
    }

    private static Set<String> hitIds(Response response) throws IOException {
        return hits(response).stream().map(hit -> (String) hit.get("_id")).collect(Collectors.toSet());
    }

    private static Map<String, Map<String, Object>> hitsById(Response response) throws IOException {
        final Map<String, Map<String, Object>> hitsById = new HashMap<>();
        for (Map<String, Object> hit : hits(response)) {
            hitsById.put((String) hit.get("_id"), hit);
        }
        return hitsById;
    }

    /** The {@code document.id} of each failure store document in the response, i.e. the id of the document that failed. */
    private static Set<String> documentIds(Response response) throws IOException {
        return hits(response).stream()
            .map(hit -> (String) XContentMapValues.extractValue("_source.document.id", hit))
            .collect(Collectors.toSet());
    }

    @SuppressWarnings("unchecked")
    private static void assertSourceHasOnly(Map<String, Object> hit, String... fields) {
        final Map<String, Object> source = (Map<String, Object>) hit.get("_source");
        assertThat("source of [" + hit.get("_id") + "]", source.keySet(), containsInAnyOrder(fields));
    }
}
