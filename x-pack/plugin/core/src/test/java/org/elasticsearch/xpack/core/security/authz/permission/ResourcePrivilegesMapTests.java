/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ResourcePrivilegesMapTests extends ESTestCase {

    public void testBuilder() {
        ResourcePrivilegesMap instance = ResourcePrivilegesMap.builder()
            .addResourcePrivilege("*", Map.of("read", true, "write", true))
            .build();
        assertThat(instance.getResourceToResourcePrivileges().size(), is(1));
        assertThat(instance.getResourceToResourcePrivileges().get("*").isAllowed("read"), is(true));
        assertThat(instance.getResourceToResourcePrivileges().get("*").isAllowed("write"), is(true));

        instance = ResourcePrivilegesMap.builder().addResourcePrivilege("*", Map.of("read", true, "write", false)).build();
        assertThat(instance.getResourceToResourcePrivileges().size(), is(1));
        assertThat(instance.getResourceToResourcePrivileges().get("*").isAllowed("read"), is(true));
        assertThat(instance.getResourceToResourcePrivileges().get("*").isAllowed("write"), is(false));

        instance = ResourcePrivilegesMap.builder()
            .addResourcePrivilege("some-other", Map.of("index", true, "write", true))
            .addResourcePrivilegesMap(instance)
            .build();
        assertThat(instance.getResourceToResourcePrivileges().size(), is(2));
        assertThat(instance.getResourceToResourcePrivileges().get("*").isAllowed("read"), is(true));
        assertThat(instance.getResourceToResourcePrivileges().get("*").isAllowed("write"), is(false));
        assertThat(instance.getResourceToResourcePrivileges().get("some-other").isAllowed("index"), is(true));
        assertThat(instance.getResourceToResourcePrivileges().get("some-other").isAllowed("write"), is(true));
    }

    public void testIntersection() {
        ResourcePrivilegesMap.Builder builder = ResourcePrivilegesMap.builder();
        ResourcePrivilegesMap instance = ResourcePrivilegesMap.builder()
            .addResourcePrivilege("*", Map.of("read", true, "write", true))
            .addResourcePrivilege("index-*", Map.of("read", true, "write", true))
            .build();
        ResourcePrivilegesMap otherInstance = ResourcePrivilegesMap.builder()
            .addResourcePrivilege("*", Map.of("read", true, "write", false))
            .addResourcePrivilege("index-*", Map.of("read", false, "write", true))
            .build();
        ResourcePrivilegesMap result = builder.addResourcePrivilegesMap(instance).addResourcePrivilegesMap(otherInstance).build();
        assertThat(result.getResourceToResourcePrivileges().size(), is(2));
        assertThat(result.getResourceToResourcePrivileges().get("*").isAllowed("read"), is(true));
        assertThat(result.getResourceToResourcePrivileges().get("*").isAllowed("write"), is(false));
        assertThat(result.getResourceToResourcePrivileges().get("index-*").isAllowed("read"), is(false));
        assertThat(result.getResourceToResourcePrivileges().get("index-*").isAllowed("write"), is(true));
        assertThat(result.getResourceToResourcePrivileges().get("index-uncommon"), is(nullValue()));
    }

    public void testEqualsHashCode() {
        ResourcePrivilegesMap instance = ResourcePrivilegesMap.builder()
            .addResourcePrivilege("*", Map.of("read", true, "write", true))
            .build();

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(instance, (original) -> {
            return ResourcePrivilegesMap.builder().addResourcePrivilegesMap(original).build();
        });
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(instance, (original) -> {
            return ResourcePrivilegesMap.builder().addResourcePrivilegesMap(original).build();
        }, ResourcePrivilegesMapTests::mutateTestItem);
    }

    private static ResourcePrivilegesMap mutateTestItem(ResourcePrivilegesMap original) {
        return switch (randomIntBetween(0, 1)) {
            case 0 -> ResourcePrivilegesMap.builder()
                .addResourcePrivilege(randomAlphaOfLength(6), Map.of("read", true, "write", true))
                .build();
            case 1 -> ResourcePrivilegesMap.builder().addResourcePrivilege("*", Map.of("read", false, "write", false)).build();
            default -> ResourcePrivilegesMap.builder()
                .addResourcePrivilege(randomAlphaOfLength(6), Map.of("read", true, "write", true))
                .build();
        };
    }
}
