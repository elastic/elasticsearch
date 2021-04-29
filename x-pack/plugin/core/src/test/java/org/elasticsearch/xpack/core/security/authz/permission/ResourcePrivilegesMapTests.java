/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ResourcePrivilegesMapTests extends ESTestCase {

    public void testBuilder() {
        ResourcePrivilegesMap instance = ResourcePrivilegesMap.builder()
                .addResourcePrivilege("*", mapBuilder().put("read", true).put("write", true).map()).build();
        assertThat(instance.allAllowed(), is(true));
        assertThat(instance.getResourceToResourcePrivileges().size(), is(1));
        assertThat(instance.getResourceToResourcePrivileges().get("*").isAllowed("read"), is(true));
        assertThat(instance.getResourceToResourcePrivileges().get("*").isAllowed("write"), is(true));

        instance = ResourcePrivilegesMap.builder().addResourcePrivilege("*", mapBuilder().put("read", true).put("write", false).map())
                .build();
        assertThat(instance.allAllowed(), is(false));
        assertThat(instance.getResourceToResourcePrivileges().size(), is(1));
        assertThat(instance.getResourceToResourcePrivileges().get("*").isAllowed("read"), is(true));
        assertThat(instance.getResourceToResourcePrivileges().get("*").isAllowed("write"), is(false));

        instance = ResourcePrivilegesMap.builder()
                .addResourcePrivilege("some-other", mapBuilder().put("index", true).put("write", true).map())
                .addResourcePrivilegesMap(instance).build();
        assertThat(instance.allAllowed(), is(false));
        assertThat(instance.getResourceToResourcePrivileges().size(), is(2));
        assertThat(instance.getResourceToResourcePrivileges().get("*").isAllowed("read"), is(true));
        assertThat(instance.getResourceToResourcePrivileges().get("*").isAllowed("write"), is(false));
        assertThat(instance.getResourceToResourcePrivileges().get("some-other").isAllowed("index"), is(true));
        assertThat(instance.getResourceToResourcePrivileges().get("some-other").isAllowed("write"), is(true));
    }

    public void testIntersection() {
        ResourcePrivilegesMap instance = ResourcePrivilegesMap.builder()
                .addResourcePrivilege("*", mapBuilder().put("read", true).put("write", true).map())
                .addResourcePrivilege("index-*", mapBuilder().put("read", true).put("write", true).map()).build();
        ResourcePrivilegesMap otherInstance = ResourcePrivilegesMap.builder()
                .addResourcePrivilege("*", mapBuilder().put("read", true).put("write", false).map())
                .addResourcePrivilege("index-*", mapBuilder().put("read", false).put("write", true).map())
                .addResourcePrivilege("index-uncommon", mapBuilder().put("read", false).put("write", true).map()).build();
        ResourcePrivilegesMap result = ResourcePrivilegesMap.intersection(instance, otherInstance);
        assertThat(result.allAllowed(), is(false));
        assertThat(result.getResourceToResourcePrivileges().size(), is(2));
        assertThat(result.getResourceToResourcePrivileges().get("*").isAllowed("read"), is(true));
        assertThat(result.getResourceToResourcePrivileges().get("*").isAllowed("write"), is(false));
        assertThat(result.getResourceToResourcePrivileges().get("index-*").isAllowed("read"), is(false));
        assertThat(result.getResourceToResourcePrivileges().get("index-*").isAllowed("write"), is(true));
        assertThat(result.getResourceToResourcePrivileges().get("index-uncommon"), is(nullValue()));
    }

    public void testEqualsHashCode() {
        ResourcePrivilegesMap instance = ResourcePrivilegesMap.builder()
                .addResourcePrivilege("*", mapBuilder().put("read", true).put("write", true).map()).build();

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(instance, (original) -> {
            return ResourcePrivilegesMap.builder().addResourcePrivilegesMap(original).build();
        });
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(instance, (original) -> {
            return ResourcePrivilegesMap.builder().addResourcePrivilegesMap(original).build();
        }, ResourcePrivilegesMapTests::mutateTestItem);
    }

    private static ResourcePrivilegesMap mutateTestItem(ResourcePrivilegesMap original) {
        switch (randomIntBetween(0, 1)) {
        case 0:
            return ResourcePrivilegesMap.builder()
                    .addResourcePrivilege(randomAlphaOfLength(6), mapBuilder().put("read", true).put("write", true).map()).build();
        case 1:
            return ResourcePrivilegesMap.builder().addResourcePrivilege("*", mapBuilder().put("read", false).put("write", false).map())
                    .build();
        default:
            return ResourcePrivilegesMap.builder()
                    .addResourcePrivilege(randomAlphaOfLength(6), mapBuilder().put("read", true).put("write", true).map()).build();
        }
    }

    private static MapBuilder<String, Boolean> mapBuilder() {
        return MapBuilder.newMapBuilder();
    }
}
