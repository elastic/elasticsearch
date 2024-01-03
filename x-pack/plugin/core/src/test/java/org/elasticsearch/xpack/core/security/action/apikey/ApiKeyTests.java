/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xpack.core.security.authz.RoleDescriptorTests.randomCrossClusterAccessRoleDescriptor;
import static org.elasticsearch.xpack.core.security.authz.RoleDescriptorTests.randomUniquelyNamedRoleDescriptors;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ApiKeyTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    public void testXContent() throws IOException {
        final ApiKey apiKey = randomApiKeyInstance();
        // The metadata will never be null because the constructor convert it to empty map if a null is passed in
        assertThat(apiKey.getMetadata(), notNullValue());

        XContentBuilder builder = XContentFactory.jsonBuilder();
        apiKey.toXContent(builder, ToXContent.EMPTY_PARAMS);
        final String jsonString = Strings.toString(builder);
        final Map<String, Object> map = XContentHelper.convertToMap(JsonXContent.jsonXContent, jsonString, false);

        try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, jsonString)) {
            assertThat(ApiKey.fromXContent(parser), equalTo(apiKey));
        }

        assertThat(map.get("name"), equalTo(apiKey.getName()));
        assertThat(map.get("id"), equalTo(apiKey.getId()));
        assertThat(map.get("type"), equalTo(apiKey.getType().value()));
        assertThat(Long.valueOf(map.get("creation").toString()), equalTo(apiKey.getCreation().toEpochMilli()));
        if (apiKey.getExpiration() != null) {
            assertThat(Long.valueOf(map.get("expiration").toString()), equalTo(apiKey.getExpiration().toEpochMilli()));
        } else {
            assertThat(map.containsKey("expiration"), is(false));
        }
        assertThat(map.get("invalidated"), is(apiKey.isInvalidated()));
        assertThat(map.get("username"), equalTo(apiKey.getUsername()));
        assertThat(map.get("realm"), equalTo(apiKey.getRealm()));
        assertThat(map.get("metadata"), equalTo(Objects.requireNonNullElseGet(apiKey.getMetadata(), Map::of)));

        if (apiKey.getRoleDescriptors() == null) {
            assertThat(map, not(hasKey("role_descriptors")));
            assertThat(map, not(hasKey("access")));
        } else {
            final var rdMap = (Map<String, Object>) map.get("role_descriptors");
            assertThat(rdMap.size(), equalTo(apiKey.getRoleDescriptors().size()));
            for (var roleDescriptor : apiKey.getRoleDescriptors()) {
                assertThat(rdMap, hasKey(roleDescriptor.getName()));
                assertThat(XContentTestUtils.convertToMap(roleDescriptor), equalTo(rdMap.get(roleDescriptor.getName())));
            }

            if (apiKey.getType() == ApiKey.Type.CROSS_CLUSTER) {
                final var accessMap = (Map<String, Object>) map.get("access");
                final CrossClusterApiKeyRoleDescriptorBuilder roleDescriptorBuilder = CrossClusterApiKeyRoleDescriptorBuilder.parse(
                    XContentTestUtils.convertToXContent(accessMap, XContentType.JSON).utf8ToString()
                );
                assertThat(roleDescriptorBuilder.build(), equalTo(apiKey.getRoleDescriptors().get(0)));
            } else {
                assertThat(map, not(hasKey("access")));
            }
        }

        final var limitedByList = (List<Map<String, Object>>) map.get("limited_by");
        if (apiKey.getType() != ApiKey.Type.CROSS_CLUSTER) {
            assertThat(limitedByList.size(), equalTo(1));
            final Map<String, Object> limitedByMap = limitedByList.get(0);

            int roleDescriptorCount = 0;
            for (Set<RoleDescriptor> roleDescriptors : apiKey.getLimitedBy().roleDescriptorsList()) {
                for (RoleDescriptor roleDescriptor : roleDescriptors) {
                    assertThat(limitedByMap, hasKey(roleDescriptor.getName()));
                    assertThat(XContentTestUtils.convertToMap(roleDescriptor), equalTo(limitedByMap.get(roleDescriptor.getName())));
                    roleDescriptorCount++;
                }
            }
            assertThat(limitedByMap.size(), equalTo(roleDescriptorCount));
        } else {
            assertThat(limitedByList, nullValue());
        }
    }

    public void testParseApiKeyType() throws IOException {
        assertThat(parseTypeString(randomFrom("rest", "REST", "Rest")), is(ApiKey.Type.REST));
        assertThat(parseTypeString(randomFrom("cross_cluster", "CROSS_CLUSTER", "Cross_Cluster")), is(ApiKey.Type.CROSS_CLUSTER));

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> parseTypeString(randomAlphaOfLengthBetween(3, 20))
        );
        assertThat(e.getMessage(), containsString("invalid API key type"));
    }

    private ApiKey.Type parseTypeString(String typeString) throws IOException {
        if (randomBoolean()) {
            return ApiKey.Type.parse(typeString);
        } else {
            return ApiKey.Type.fromXContent(
                JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, "\"" + typeString + "\"")
            );
        }
    }

    public static Map<String, Object> randomMetadata() {
        Map<String, Object> randomMetadata = randomFrom(
            Map.of(
                "application",
                randomAlphaOfLength(5),
                "number",
                1,
                "numbers",
                List.of(1, 3, 5),
                "environment",
                Map.of("os", "linux", "level", 42, "category", "trusted")
            ),
            Map.of(randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8)),
            Map.of(),
            null
        );

        // Make metadata mutable for testing purposes
        return randomMetadata == null ? new HashMap<>() : new HashMap<>(randomMetadata);
    }

    public static ApiKey randomApiKeyInstance() {
        final String name = randomAlphaOfLengthBetween(4, 10);
        final String id = randomAlphaOfLength(20);
        final ApiKey.Type type = randomFrom(ApiKey.Type.values());
        // between 1970 and 2065
        final Instant creation = Instant.ofEpochSecond(randomLongBetween(0, 3000000000L), randomLongBetween(0, 999999999));
        final Instant expiration = randomBoolean()
            ? null
            : Instant.ofEpochSecond(randomLongBetween(0, 3000000000L), randomLongBetween(0, 999999999));
        final boolean invalidated = randomBoolean();
        final Instant invalidation = invalidated
            ? Instant.ofEpochSecond(randomLongBetween(0, 3000000000L), randomLongBetween(0, 999999999))
            : null;
        final String username = randomAlphaOfLengthBetween(4, 10);
        final String realmName = randomAlphaOfLengthBetween(3, 8);
        final Map<String, Object> metadata = randomMetadata();
        final List<RoleDescriptor> roleDescriptors = type == ApiKey.Type.CROSS_CLUSTER
            ? List.of(randomCrossClusterAccessRoleDescriptor())
            : randomFrom(randomUniquelyNamedRoleDescriptors(0, 3), null);
        final List<RoleDescriptor> limitedByRoleDescriptors = type == ApiKey.Type.CROSS_CLUSTER
            ? null
            : randomUniquelyNamedRoleDescriptors(0, 3);

        return new ApiKey(
            name,
            id,
            type,
            creation,
            expiration,
            invalidated,
            invalidation,
            username,
            realmName,
            metadata,
            roleDescriptors,
            limitedByRoleDescriptors
        );
    }
}
