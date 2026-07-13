/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.apache.http.HttpHeaders;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.ApplicationResourcePrivileges;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.security.support.SecurityMigrations;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.security.action.UpdateIndexMigrationVersionAction.MIGRATION_VERSION_CUSTOM_DATA_KEY;
import static org.elasticsearch.xpack.core.security.action.UpdateIndexMigrationVersionAction.MIGRATION_VERSION_CUSTOM_KEY;
import static org.elasticsearch.xpack.core.security.test.TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.iterableWithSize;

public final class QueryRoleIT extends SecurityInBasicRestTestCase {

    private static final String READ_SECURITY_USER_AUTH_HEADER = "Basic cmVhZF9zZWN1cml0eV91c2VyOnJlYWQtc2VjdXJpdHktcGFzc3dvcmQ=";

    @Before
    public void initialize() {
        new ReservedRolesStore();
    }

    public void testSimpleQueryAllRoles() throws Exception {
        createRandomRole();
        assertQuery("", 1 + ReservedRolesStore.names().size(), roles -> {
            // default size is 10
            assertThat(roles, iterableWithSize(10));
        });
        assertQuery(
            Strings.format("""
                {"query":{"match_all":{}},"from":%d}""", 1 + ReservedRolesStore.names().size()),
            1 + ReservedRolesStore.names().size(),
            roles -> assertThat(roles, emptyIterable())
        );
    }

    public void testDisallowedFields() throws Exception {
        if (randomBoolean()) {
            createRandomRole();
        }
        // query on some disallowed field
        {
            Request request = new Request(randomFrom("POST", "GET"), "/_security/_query/role");
            request.setOptions(request.getOptions().toBuilder().addHeader(HttpHeaders.AUTHORIZATION, READ_SECURITY_USER_AUTH_HEADER));
            request.setJsonEntity("""
                {"query":{"prefix":{"password":"whatever"}}}""");
            ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
            assertThat(e.getMessage(), containsString("Field [password] is not allowed for querying or aggregation"));
        }
        // query on the _id field
        {
            Request request = new Request(randomFrom("POST", "GET"), "/_security/_query/role");
            request.setOptions(request.getOptions().toBuilder().addHeader(HttpHeaders.AUTHORIZATION, READ_SECURITY_USER_AUTH_HEADER));
            request.setJsonEntity("""
                {"query":{"term":{"_id":"role-test"}}}""");
            ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
            assertThat(e.getMessage(), containsString("Field [_id] is not allowed for querying or aggregation"));
        }
        // sort on disallowed field
        {
            Request request = new Request(randomFrom("POST", "GET"), "/_security/_query/role");
            request.setOptions(request.getOptions().toBuilder().addHeader(HttpHeaders.AUTHORIZATION, READ_SECURITY_USER_AUTH_HEADER));
            request.setJsonEntity("""
                {"query":{"bool":{"must_not":[{"wildcard":{"applications.application":"a*9"}}]}},"sort":["api_key_hash"]}""");
            ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
            assertThat(e.getMessage(), containsString("Field [api_key_hash] is not allowed for querying or aggregation"));
        }
    }

    public void testDisallowedQueryType() throws Exception {
        if (randomBoolean()) {
            createRandomRole();
        }
        // query using some disallowed query type
        {
            Request request = new Request(randomFrom("POST", "GET"), "/_security/_query/role");
            request.setOptions(request.getOptions().toBuilder().addHeader(HttpHeaders.AUTHORIZATION, READ_SECURITY_USER_AUTH_HEADER));
            request.setJsonEntity("""
                {"query":{"match_phrase":{"description":{"query":"whatever"}}}}""");
            ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
            assertThat(e.getMessage(), containsString("Query type [match_phrase] is not currently supported in this context"));
        }
        // query using some disallowed query type inside the (allowed) boolean query type
        {
            Request request = new Request(randomFrom("POST", "GET"), "/_security/_query/role");
            request.setOptions(request.getOptions().toBuilder().addHeader(HttpHeaders.AUTHORIZATION, READ_SECURITY_USER_AUTH_HEADER));
            request.setJsonEntity("""
                {"query":{"bool":{"must_not":[{"more_like_this":{"fields":["description"],"like":"hollywood"}}]}}}""");
            ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
            assertThat(e.getMessage(), containsString("Query type [more_like_this] is not currently supported in this context"));
        }
    }

    public void testSimpleMetadataSearch() throws Exception {
        int nroles = randomIntBetween(1, 3);
        for (int i = 0; i < nroles; i++) {
            createRandomRole();
        }
        RoleDescriptor matchesOnMetadataValue = createRole(
            "matchesOnMetadataValue",
            randomBoolean() ? null : randomAlphaOfLength(8),
            Map.of("matchSimpleKey", "matchSimpleValue"),
            randomApplicationPrivileges()
        );
        RoleDescriptor matchesOnMetadataKey = createRole(
            "matchesOnMetadataKey",
            randomBoolean() ? null : randomAlphaOfLength(8),
            Map.of("matchSimpleKey", "other"),
            randomApplicationPrivileges()
        );
        createRole(
            "other2",
            randomBoolean() ? null : randomAlphaOfLength(8),
            Map.of("other", "matchSimpleValue"),
            randomApplicationPrivileges()
        );
        waitForMigrationCompletion(adminClient(), SecurityMigrations.ROLE_METADATA_FLATTENED_MIGRATION_VERSION);
        assertQuery("""
            {"query":{"term":{"metadata.matchSimpleKey":"matchSimpleValue"}}}""", 1, roles -> {
            assertThat(roles, iterableWithSize(1));
            assertRoleMap(roles.get(0), matchesOnMetadataValue);
        });
        assertQuery("""
            {"query":{"exists":{"field":"metadata.matchSimpleKey"}}}""", 2, roles -> {
            assertThat(roles, iterableWithSize(2));
            roles.sort(Comparator.comparing(o -> ((String) o.get("name"))));
            assertRoleMap(roles.get(0), matchesOnMetadataKey);
            assertRoleMap(roles.get(1), matchesOnMetadataValue);
        });
    }

    public void testSearchMultipleMetadataFields() throws Exception {
        createRole(
            "noMetadataRole",
            randomBoolean() ? null : randomAlphaOfLength(8),
            randomBoolean() ? null : Map.of(),
            randomApplicationPrivileges()
        );
        RoleDescriptor role1 = createRole(
            "1" + randomAlphaOfLength(4),
            randomBoolean() ? null : randomAlphaOfLength(8),
            Map.of("simpleField1", "matchThis", "simpleField2", "butNotThis"),
            randomApplicationPrivileges()
        );
        createRole(
            "2" + randomAlphaOfLength(4),
            randomBoolean() ? null : randomAlphaOfLength(8),
            Map.of("simpleField2", "butNotThis"),
            randomApplicationPrivileges()
        );
        RoleDescriptor role3 = createRole(
            "3" + randomAlphaOfLength(4),
            randomBoolean() ? null : randomAlphaOfLength(8),
            Map.of("listField1", List.of("matchThis", "butNotThis"), "listField2", List.of("butNotThisToo")),
            randomApplicationPrivileges()
        );
        createRole(
            "4" + randomAlphaOfLength(4),
            randomBoolean() ? null : randomAlphaOfLength(8),
            Map.of("listField2", List.of("butNotThisToo", "andAlsoNotThis")),
            randomApplicationPrivileges()
        );
        RoleDescriptor role5 = createRole(
            "5" + randomAlphaOfLength(4),
            randomBoolean() ? null : randomAlphaOfLength(8),
            Map.of("listField1", List.of("maybeThis", List.of("matchThis")), "listField2", List.of("butNotThis")),
            randomApplicationPrivileges()
        );
        RoleDescriptor role6 = createRole(
            "6" + randomAlphaOfLength(4),
            randomBoolean() ? null : randomAlphaOfLength(8),
            Map.of("mapField1", Map.of("innerField", "matchThis")),
            randomApplicationPrivileges()
        );
        createRole(
            "7" + randomAlphaOfLength(4),
            randomBoolean() ? null : randomAlphaOfLength(8),
            Map.of("mapField1", Map.of("innerField", "butNotThis")),
            randomApplicationPrivileges()
        );
        RoleDescriptor role8 = createRole(
            "8" + randomAlphaOfLength(4),
            randomBoolean() ? null : randomAlphaOfLength(8),
            Map.of("mapField1", Map.of("innerField", "butNotThis", "innerField2", Map.of("deeperInnerField", "matchThis"))),
            randomApplicationPrivileges()
        );
        waitForMigrationCompletion(adminClient(), SecurityMigrations.ROLE_METADATA_FLATTENED_MIGRATION_VERSION);
        Consumer<List<Map<String, Object>>> matcher = roles -> {
            assertThat(roles, iterableWithSize(5));
            roles.sort(Comparator.comparing(o -> ((String) o.get("name"))));
            assertRoleMap(roles.get(0), role1);
            assertRoleMap(roles.get(1), role3);
            assertRoleMap(roles.get(2), role5);
            assertRoleMap(roles.get(3), role6);
            assertRoleMap(roles.get(4), role8);
        };
        assertQuery("""
            {"query":{"prefix":{"metadata":"match"}}}""", 5, matcher);
        assertQuery("""
            {"query":{"simple_query_string":{"fields":["meta*"],"query":"matchThis"}}}""", 5, matcher);
    }

    @SuppressWarnings("unchecked")
    public void testSimpleSort() throws IOException {
        // some other non-matching roles
        int nOtherRoles = randomIntBetween(1, 5);
        for (int i = 0; i < nOtherRoles; i++) {
            createRandomRole();
        }
        // some matching roles (at least 2, for sorting)
        int nMatchingRoles = randomIntBetween(2, 5);
        for (int i = 0; i < nMatchingRoles; i++) {
            ApplicationResourcePrivileges[] applicationResourcePrivileges = randomArray(
                1,
                5,
                ApplicationResourcePrivileges[]::new,
                this::randomApplicationResourcePrivileges
            );
            {
                int matchingApplicationIndex = randomIntBetween(0, applicationResourcePrivileges.length - 1);
                // make sure the "application" matches the filter query below ("a*9")
                applicationResourcePrivileges[matchingApplicationIndex] = RoleDescriptor.ApplicationResourcePrivileges.builder()
                    .application("a" + randomAlphaOfLength(4) + "9")
                    .resources(applicationResourcePrivileges[matchingApplicationIndex].getResources())
                    .privileges(applicationResourcePrivileges[matchingApplicationIndex].getPrivileges())
                    .build();
            }
            {
                int matchingApplicationIndex = randomIntBetween(0, applicationResourcePrivileges.length - 1);
                int matchingResourcesIndex = randomIntBetween(
                    0,
                    applicationResourcePrivileges[matchingApplicationIndex].getResources().length - 1
                );
                // make sure the "resources" matches the terms query below ("99")
                applicationResourcePrivileges[matchingApplicationIndex] = RoleDescriptor.ApplicationResourcePrivileges.builder()
                    .application(applicationResourcePrivileges[matchingApplicationIndex].getApplication())
                    .resources(applicationResourcePrivileges[matchingApplicationIndex].getResources()[matchingResourcesIndex] = "99")
                    .privileges(applicationResourcePrivileges[matchingApplicationIndex].getPrivileges())
                    .build();
            }
            createRole(
                randomAlphaOfLength(4) + i,
                randomBoolean() ? null : randomAlphaOfLength(8),
                randomBoolean() ? null : randomMetadata(),
                applicationResourcePrivileges
            );
        }
        assertQuery("""
            {"query":{"bool":{"filter":[{"wildcard":{"applications.application":"a*9"}}]}},"sort":["name"]}""", nMatchingRoles, roles -> {
            assertThat(roles, iterableWithSize(nMatchingRoles));
            // assert sorting on name
            for (int i = 0; i < nMatchingRoles; i++) {
                assertThat(roles.get(i).get("_sort"), instanceOf(List.class));
                assertThat(((List<String>) roles.get(i).get("_sort")), iterableWithSize(1));
                assertThat(((List<String>) roles.get(i).get("_sort")).get(0), equalTo(roles.get(i).get("name")));
            }
            // assert the ascending sort order
            for (int i = 1; i < nMatchingRoles; i++) {
                int compareNames = roles.get(i - 1).get("name").toString().compareTo(roles.get(i).get("name").toString());
                assertThat(compareNames < 0, is(true));
            }
        });
        assertQuery(
            """
                {"query":{"bool":{"must":[{"terms":{"applications.resources":["99"]}}]}},"sort":["applications.privileges"]}""",
            nMatchingRoles,
            roles -> {
                assertThat(roles, iterableWithSize(nMatchingRoles));
                // assert sorting on best "applications.privileges"
                for (int i = 0; i < nMatchingRoles; i++) {
                    assertThat(roles.get(i).get("_sort"), instanceOf(List.class));
                    assertThat(((List<String>) roles.get(i).get("_sort")), iterableWithSize(1));
                    assertThat(((List<String>) roles.get(i).get("_sort")).get(0), equalTo(getPrivilegeNameUsedForSorting(roles.get(i))));
                }
                // assert the ascending sort order
                for (int i = 1; i < nMatchingRoles; i++) {
                    int comparePrivileges = getPrivilegeNameUsedForSorting(roles.get(i - 1)).compareTo(
                        getPrivilegeNameUsedForSorting(roles.get(i))
                    );
                    assertThat(comparePrivileges < 0, is(true));
                }
            }
        );
    }

    @SuppressWarnings("unchecked")
    public void testSortWithPagination() throws IOException {
        int roleIdx = 0;
        // some non-matching roles
        int nOtherRoles = randomIntBetween(0, 5);
        for (int i = 0; i < nOtherRoles; i++) {
            createRole(
                Strings.format("role_%03d", roleIdx++),
                randomBoolean() ? null : randomDescription(),
                randomBoolean() ? null : randomMetadata(),
                randomApplicationPrivileges()
            );
        }
        // first matching role
        RoleDescriptor firstMatchingRole = createRole(
            Strings.format("role_%03d", roleIdx++),
            "some ZZZZmatchZZZZ descr",
            randomBoolean() ? null : randomMetadata(),
            randomApplicationPrivileges()
        );
        nOtherRoles = randomIntBetween(0, 5);
        for (int i = 0; i < nOtherRoles; i++) {
            createRole(
                Strings.format("role_%03d", roleIdx++),
                randomBoolean() ? null : randomDescription(),
                randomBoolean() ? null : randomMetadata(),
                randomApplicationPrivileges()
            );
        }
        // second matching role
        RoleDescriptor secondMatchingRole = createRole(
            Strings.format("role_%03d", roleIdx++),
            "other ZZZZmatchZZZZ meh",
            randomBoolean() ? null : randomMetadata(),
            randomApplicationPrivileges()
        );
        nOtherRoles = randomIntBetween(0, 5);
        for (int i = 0; i < nOtherRoles; i++) {
            createRole(
                Strings.format("role_%03d", roleIdx++),
                randomBoolean() ? null : randomDescription(),
                randomBoolean() ? null : randomMetadata(),
                randomApplicationPrivileges()
            );
        }
        // third matching role
        RoleDescriptor thirdMatchingRole = createRole(
            Strings.format("role_%03d", roleIdx++),
            "me ZZZZmatchZZZZ go",
            randomBoolean() ? null : randomMetadata(),
            randomApplicationPrivileges()
        );
        nOtherRoles = randomIntBetween(0, 5);
        for (int i = 0; i < nOtherRoles; i++) {
            createRole(
                Strings.format("role_%03d", roleIdx++),
                randomBoolean() ? null : randomDescription(),
                randomBoolean() ? null : randomMetadata(),
                randomApplicationPrivileges()
            );
        }
        String queryTemplate = """
            {"query":{"match":{"description":{"query":"ZZZZmatchZZZZ"}}},
             "size":1,
             "sort":[{"name":{"order":"desc"}},{"applications.resources":{"order":"asc"}}]
             %s
            }""";
        AtomicReference<String> searchAfter = new AtomicReference<>("");
        Consumer<Map<String, Object>> searchAfterChain = roleMap -> {
            assertThat(roleMap.get("_sort"), instanceOf(List.class));
            assertThat(((List<String>) roleMap.get("_sort")), iterableWithSize(2));
            String firstSortValue = ((List<String>) roleMap.get("_sort")).get(0);
            assertThat(firstSortValue, equalTo(roleMap.get("name")));
            String secondSortValue = ((List<String>) roleMap.get("_sort")).get(1);
            searchAfter.set(
                ",\"search_after\":[\""
                    + firstSortValue
                    + "\","
                    + (secondSortValue != null ? ("\"" + secondSortValue + "\"") : "null")
                    + "]"
            );
        };
        assertQuery(Strings.format(queryTemplate, searchAfter.get()), 3, roles -> {
            assertThat(roles, iterableWithSize(1));
            assertRoleMap(roles.get(0), thirdMatchingRole);
            searchAfterChain.accept(roles.get(0));
        });
        assertQuery(Strings.format(queryTemplate, searchAfter.get()), 3, roles -> {
            assertThat(roles, iterableWithSize(1));
            assertRoleMap(roles.get(0), secondMatchingRole);
            searchAfterChain.accept(roles.get(0));
        });
        assertQuery(Strings.format(queryTemplate, searchAfter.get()), 3, roles -> {
            assertThat(roles, iterableWithSize(1));
            assertRoleMap(roles.get(0), firstMatchingRole);
            searchAfterChain.accept(roles.get(0));
        });
        // no more results
        assertQuery(Strings.format(queryTemplate, searchAfter.get()), 3, roles -> assertThat(roles, emptyIterable()));
    }

    @SuppressWarnings("unchecked")
    private String getPrivilegeNameUsedForSorting(Map<String, Object> roleMap) {
        String bestPrivilege = null;
        List<Map<String, Object>> applications = (List<Map<String, Object>>) roleMap.get("applications");
        if (applications == null) {
            return bestPrivilege;
        }
        for (Map<String, Object> application : applications) {
            List<String> privileges = (List<String>) application.get("privileges");
            if (privileges != null) {
                for (String privilege : privileges) {
                    if (bestPrivilege == null) {
                        bestPrivilege = privilege;
                    } else if (privilege.compareTo(bestPrivilege) < 0) {
                        bestPrivilege = privilege;
                    }
                }
            }
        }
        return bestPrivilege;
    }

    private RoleDescriptor createRandomRole() throws IOException {
        return createRole(
            randomUUID(),
            randomBoolean() ? null : randomDescription(),
            randomBoolean() ? null : randomMetadata(),
            randomApplicationPrivileges()
        );
    }

    private ApplicationResourcePrivileges[] randomApplicationPrivileges() {
        ApplicationResourcePrivileges[] applicationResourcePrivileges = randomArray(
            0,
            3,
            ApplicationResourcePrivileges[]::new,
            this::randomApplicationResourcePrivileges
        );
        return applicationResourcePrivileges.length == 0 && randomBoolean() ? null : applicationResourcePrivileges;
    }

    @SuppressWarnings("unchecked")
    private RoleDescriptor createRole(
        String roleName,
        String description,
        Map<String, Object> metadata,
        ApplicationResourcePrivileges... applicationResourcePrivileges
    ) throws IOException {
        Request request = new Request("POST", "/_security/role/" + roleName);
        Map<String, Object> requestMap = new HashMap<>();
        if (description != null) {
            requestMap.put(RoleDescriptor.Fields.DESCRIPTION.getPreferredName(), description);
        }
        if (metadata != null) {
            requestMap.put(RoleDescriptor.Fields.METADATA.getPreferredName(), metadata);
        }
        if (applicationResourcePrivileges != null) {
            requestMap.put(RoleDescriptor.Fields.APPLICATIONS.getPreferredName(), applicationResourcePrivileges);
        }
        BytesReference source = BytesReference.bytes(jsonBuilder().map(requestMap));
        request.setJsonEntity(source.utf8ToString());
        Response response = adminClient().performRequest(request);
        assertOK(response);
        Map<String, Object> responseMap = responseAsMap(response);
        assertTrue((Boolean) ((Map<String, Object>) responseMap.get("role")).get("created"));
        return new RoleDescriptor(
            roleName,
            null,
            null,
            applicationResourcePrivileges,
            null,
            null,
            metadata,
            null,
            null,
            null,
            null,
            description
        );
    }

    static void assertQuery(String body, int total, Consumer<List<Map<String, Object>>> roleVerifier) throws IOException {
        assertQuery(client(), body, total, roleVerifier);
    }

    private static Request queryRoleRequestWithAuth() {
        Request request = new Request(randomFrom("POST", "GET"), "/_security/_query/role");
        request.setOptions(request.getOptions().toBuilder().addHeader(HttpHeaders.AUTHORIZATION, READ_SECURITY_USER_AUTH_HEADER));
        return request;
    }

    public static void assertQuery(RestClient client, String body, int total, Consumer<List<Map<String, Object>>> roleVerifier)
        throws IOException {
        Request request = queryRoleRequestWithAuth();
        request.setJsonEntity(body);
        Response response = client.performRequest(request);
        assertOK(response);
        Map<String, Object> responseMap = responseAsMap(response);
        assertThat(responseMap.get("total"), is(total));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> roles = new ArrayList<>((List<Map<String, Object>>) responseMap.get("roles"));
        assertThat(roles.size(), is(responseMap.get("count")));
        roleVerifier.accept(roles);
    }

    @SuppressWarnings("unchecked")
    private void assertRoleMap(Map<String, Object> roleMap, RoleDescriptor roleDescriptor) {
        assertThat(roleMap.get("name"), equalTo(roleDescriptor.getName()));
        if (Strings.isNullOrEmpty(roleDescriptor.getDescription())) {
            assertThat(roleMap.get("description"), nullValue());
        } else {
            assertThat(roleMap.get("description"), equalTo(roleDescriptor.getDescription()));
        }
        // "applications" is always present
        assertThat(roleMap.get("applications"), instanceOf(Iterable.class));
        if (roleDescriptor.getApplicationPrivileges().length == 0) {
            assertThat((Iterable<ApplicationResourcePrivileges>) roleMap.get("applications"), emptyIterable());
        } else {
            assertThat(
                (Iterable<Map<String, Object>>) roleMap.get("applications"),
                iterableWithSize(roleDescriptor.getApplicationPrivileges().length)
            );
            Iterator<Map<String, Object>> responseIterator = ((Iterable<Map<String, Object>>) roleMap.get("applications")).iterator();
            Iterator<ApplicationResourcePrivileges> descriptorIterator = Arrays.asList(roleDescriptor.getApplicationPrivileges())
                .iterator();
            while (responseIterator.hasNext()) {
                assertTrue(descriptorIterator.hasNext());
                Map<String, Object> responsePrivilege = responseIterator.next();
                ApplicationResourcePrivileges descriptorPrivilege = descriptorIterator.next();
                assertThat(responsePrivilege.get("application"), equalTo(descriptorPrivilege.getApplication()));
                assertThat(responsePrivilege.get("privileges"), equalTo(Arrays.asList(descriptorPrivilege.getPrivileges())));
                assertThat(responsePrivilege.get("resources"), equalTo(Arrays.asList(descriptorPrivilege.getResources())));
            }
            assertFalse(descriptorIterator.hasNext());
        }
        // in this test suite all roles are always enabled
        assertTrue(roleMap.containsKey("transient_metadata"));
        assertThat(roleMap.get("transient_metadata"), Matchers.instanceOf(Map.class));
        assertThat(((Map<String, Object>) roleMap.get("transient_metadata")).get("enabled"), equalTo(true));
    }

    private Map<String, Object> randomMetadata() {
        return randomMetadata(3);
    }

    private Map<String, Object> randomMetadata(int maxLevel) {
        int size = randomIntBetween(0, 5);
        Map<String, Object> metadata = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            switch (randomFrom(1, 2, 3, 4, 5)) {
                case 1:
                    metadata.put(randomAlphaOfLength(4), randomAlphaOfLength(4));
                    break;
                case 2:
                    metadata.put(randomAlphaOfLength(4), randomInt());
                    break;
                case 3:
                    metadata.put(randomAlphaOfLength(4), randomList(0, 3, () -> randomAlphaOfLength(4)));
                    break;
                case 4:
                    metadata.put(randomAlphaOfLength(4), randomList(0, 3, () -> randomInt(4)));
                    break;
                case 5:
                    if (maxLevel > 0) {
                        metadata.put(randomAlphaOfLength(4), randomMetadata(maxLevel - 1));
                    }
                    break;
            }
        }
        return metadata;
    }

    private ApplicationResourcePrivileges randomApplicationResourcePrivileges() {
        String applicationName;
        if (randomBoolean()) {
            applicationName = "*";
        } else {
            applicationName = randomAlphaOfLength(1).toLowerCase(Locale.ROOT) + randomAlphaOfLengthBetween(2, 10);
        }
        Supplier<String> privilegeNameSupplier = () -> randomAlphaOfLength(1).toLowerCase(Locale.ROOT) + randomAlphaOfLengthBetween(2, 8);
        int size = randomIntBetween(1, 5);
        List<String> resources = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            if (randomBoolean()) {
                String suffix = randomBoolean() ? "*" : randomAlphaOfLengthBetween(4, 9);
                resources.add(randomAlphaOfLengthBetween(2, 5) + "/" + suffix);
            } else {
                resources.add(randomAlphaOfLength(1).toLowerCase(Locale.ROOT) + randomAlphaOfLengthBetween(2, 8));
            }
        }
        return RoleDescriptor.ApplicationResourcePrivileges.builder()
            .application(applicationName)
            .resources(resources)
            .privileges(randomList(1, 3, privilegeNameSupplier))
            .build();
    }

    private String randomDescription() {
        StringBuilder randomDescriptionBuilder = new StringBuilder();
        int nParts = randomIntBetween(1, 5);
        for (int i = 0; i < nParts; i++) {
            randomDescriptionBuilder.append(randomAlphaOfLengthBetween(1, 5));
        }
        return randomDescriptionBuilder.toString();
    }

    @SuppressWarnings("unchecked")
    public static void waitForMigrationCompletion(RestClient adminClient, Integer migrationVersion) throws Exception {
        final Request request = new Request("GET", "_cluster/state/metadata/" + INTERNAL_SECURITY_MAIN_INDEX_7);
        assertBusy(() -> {
            Response response = adminClient.performRequest(request);
            assertOK(response);
            Map<String, Object> responseMap = responseAsMap(response);
            Map<String, Object> indicesMetadataMap = (Map<String, Object>) ((Map<String, Object>) responseMap.get("metadata")).get(
                "indices"
            );
            assertTrue(indicesMetadataMap.containsKey(INTERNAL_SECURITY_MAIN_INDEX_7));
            assertTrue(
                ((Map<String, Object>) indicesMetadataMap.get(INTERNAL_SECURITY_MAIN_INDEX_7)).containsKey(MIGRATION_VERSION_CUSTOM_KEY)
            );
            if (migrationVersion != null) {
                assertTrue(
                    ((Map<String, Object>) ((Map<String, Object>) indicesMetadataMap.get(INTERNAL_SECURITY_MAIN_INDEX_7)).get(
                        MIGRATION_VERSION_CUSTOM_KEY
                    )).containsKey(MIGRATION_VERSION_CUSTOM_DATA_KEY)
                );
                Integer versionInteger = Integer.parseInt(
                    (String) ((Map<String, Object>) ((Map<String, Object>) indicesMetadataMap.get(INTERNAL_SECURITY_MAIN_INDEX_7)).get(
                        MIGRATION_VERSION_CUSTOM_KEY
                    )).get(MIGRATION_VERSION_CUSTOM_DATA_KEY)
                );
                assertThat(versionInteger, greaterThanOrEqualTo(migrationVersion));
            }
        });
    }
}
