/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.core.security.authz.RoleDescriptorTests.randomUniquelyNamedRoleDescriptors;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class ApiKeyTests extends ESTestCase {

    public void testXContent() throws IOException {
        final String name = randomAlphaOfLengthBetween(4, 10);
        final String id = randomAlphaOfLength(20);
        // between 1970 and 2065
        final Instant creation = Instant.ofEpochSecond(randomLongBetween(0, 3000000000L), randomLongBetween(0, 999999999));
        final Instant expiration = randomBoolean()
            ? null
            : Instant.ofEpochSecond(randomLongBetween(0, 3000000000L), randomLongBetween(0, 999999999));
        final boolean invalidated = randomBoolean();
        final String username = randomAlphaOfLengthBetween(4, 10);
        final String realmName = randomAlphaOfLengthBetween(3, 8);
        final Map<String, Object> metadata = randomMetadata();
        final List<RoleDescriptor> roleDescriptors = randomBoolean() ? null : randomUniquelyNamedRoleDescriptors(0, 3);

        final ApiKey apiKey = new ApiKey(name, id, creation, expiration, invalidated, username, realmName, metadata, roleDescriptors);
        // The metadata will never be null because the constructor convert it to empty map if a null is passed in
        assertThat(apiKey.getMetadata(), notNullValue());

        XContentBuilder builder = XContentFactory.jsonBuilder();
        apiKey.toXContent(builder, ToXContent.EMPTY_PARAMS);
        final Map<String, Object> map = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();

        assertThat(map.get("name"), equalTo(name));
        assertThat(map.get("id"), equalTo(id));
        assertThat(Long.valueOf(map.get("creation").toString()), equalTo(creation.toEpochMilli()));
        if (expiration != null) {
            assertThat(Long.valueOf(map.get("expiration").toString()), equalTo(expiration.toEpochMilli()));
        } else {
            assertThat(map.containsKey("expiration"), is(false));
        }
        assertThat(map.get("invalidated"), is(invalidated));
        assertThat(map.get("username"), equalTo(username));
        assertThat(map.get("realm"), equalTo(realmName));
        assertThat(map.get("metadata"), equalTo(Objects.requireNonNullElseGet(metadata, Map::of)));

        if (roleDescriptors == null) {
            assertThat(map, not(hasKey("role_descriptors")));
        } else {
            @SuppressWarnings("unchecked")
            final Map<String, Object> rdMap = (Map<String, Object>) map.get("role_descriptors");
            assertThat(rdMap.size(), equalTo(roleDescriptors.size()));
            for (var roleDescriptor : roleDescriptors) {
                assertThat(rdMap, hasKey(roleDescriptor.getName()));
                assertThat(XContentTestUtils.convertToMap(roleDescriptor), equalTo(rdMap.get(roleDescriptor.getName())));
            }
        }
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> randomMetadata() {
        return randomFrom(
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
    }
}
