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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.ApplicationResourcePrivileges;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.iterableWithSize;

public class QueryRoleIT extends SecurityInBasicRestTestCase {

    private static final String READ_SECURITY_USER_AUTH_HEADER = "Basic cmVhZF9zZWN1cml0eV91c2VyOnJlYWQtc2VjdXJpdHktcGFzc3dvcmQ=";

    public void testSimpleQueryAllRoles() throws IOException {
        assertQuery("", 0, roles -> assertThat(roles, emptyIterable()));
        RoleDescriptor createdRole = createRandomRole();
        assertQuery("", 1, roles -> {
            assertThat(roles, iterableWithSize(1));
            assertRoleMap(roles.get(0), createdRole);
        });
    }

    public void testSimpleMetadataSearch() throws IOException {
        int nroles = randomIntBetween(1, 3);
        for (int i = 0; i < nroles; i++) {
            createRandomRole();
        }
        RoleDescriptor matchThisRole = createRole(
            "match_this",
            randomBoolean() ? null : randomAlphaOfLength(8),
            Map.of("matchSimpleKey", "matchSimpleValue"),
            randomApplicationPrivileges()
        );
        createRole(
            "other",
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
        assertQuery("""
            {"query":{"term":{"metadata.matchSimpleKey":"matchSimpleValue"}}}""", 1, roles -> {
            assertThat(roles, iterableWithSize(1));
            assertRoleMap(roles.get(0), matchThisRole);
        });
    }

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
            int matchingApplicationIndex = randomIntBetween(0, applicationResourcePrivileges.length - 1);
            // make sure the application matches the filter query below ("a*z")
            applicationResourcePrivileges[matchingApplicationIndex] = RoleDescriptor.ApplicationResourcePrivileges.builder()
                    .application("a" + randomAlphaOfLength(4) + "z")
                    .resources(applicationResourcePrivileges[matchingApplicationIndex].getResources())
                    .privileges(applicationResourcePrivileges[matchingApplicationIndex].getPrivileges())
                    .build();
            createRole(
                    randomAlphaOfLength(4) + i,
                    randomBoolean() ? null : randomAlphaOfLength(8),
                    randomBoolean() ? null : randomMetadata(),
                    applicationResourcePrivileges
            );
        }
        assertQuery("""
            {"query":{"bool":{"filter":[{"wildcard":{"applications.application":"a*z"}}]}},"sort":["name"]}""", nMatchingRoles, roles -> {
            assertThat(roles, iterableWithSize(nMatchingRoles));
            // assert the sort order
            for (int i = 1; i < nMatchingRoles; i++) {
                int compareNames = roles.get(i - 1).get("name").toString().compareTo(roles.get(i).get("name").toString());
                assertThat(compareNames < 0, is(true));
            }
        });
    }

    private RoleDescriptor createRandomRole() throws IOException {
        return createRole(
            randomUUID(),
            randomBoolean() ? null : randomAlphaOfLength(8),
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

    private Request queryRoleRequestWithAuth() {
        Request request = new Request(randomFrom("POST", "GET"), "/_security/_query/role");
        request.setOptions(request.getOptions().toBuilder().addHeader(HttpHeaders.AUTHORIZATION, READ_SECURITY_USER_AUTH_HEADER));
        return request;
    }

    private void assertQuery(String body, int total, Consumer<List<Map<String, Object>>> roleVerifier) throws IOException {
        Request request = queryRoleRequestWithAuth();
        request.setJsonEntity(body);
        Response response = client().performRequest(request);
        assertOK(response);
        Map<String, Object> responseMap = responseAsMap(response);
        assertThat(responseMap.get("total"), is(total));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> roles = (List<Map<String, Object>>) responseMap.get("roles");
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
}
