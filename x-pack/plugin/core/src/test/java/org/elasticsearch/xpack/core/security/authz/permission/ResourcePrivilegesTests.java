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

import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ResourcePrivilegesTests extends ESTestCase {

    public void testBuilder() {
        ResourcePrivileges instance = createInstance();
        ResourcePrivileges expected = new ResourcePrivileges("*", mapBuilder().put("read", true).put("write", false).map());
        assertThat(instance, equalTo(expected));
    }

    public void testWhenSamePrivilegeExists() {
        ResourcePrivileges.Builder builder = ResourcePrivileges.builder("*").addPrivilege("read", true);

        Map<String, Boolean> mapWhereReadIsAllowed = mapBuilder().put("read", true).map();
        builder.addPrivileges(mapWhereReadIsAllowed);
        assertThat(builder.build().isAllowed("read"), is(true));

        Map<String, Boolean> mapWhereReadIsDenied = mapBuilder().put("read", false).map();
        builder.addPrivileges(mapWhereReadIsDenied);
        assertThat(builder.build().isAllowed("read"), is(false));
    }

    public void testEqualsHashCode() {
        ResourcePrivileges instance = createInstance();

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            instance,
            (original) -> { return ResourcePrivileges.builder(original.getResource()).addPrivileges(original.getPrivileges()).build(); }
        );
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            instance,
            (original) -> { return ResourcePrivileges.builder(original.getResource()).addPrivileges(original.getPrivileges()).build(); },
            ResourcePrivilegesTests::mutateTestItem
        );
    }

    private ResourcePrivileges createInstance() {
        ResourcePrivileges instance = ResourcePrivileges.builder("*")
            .addPrivilege("read", true)
            .addPrivileges(Collections.singletonMap("write", false))
            .build();
        return instance;
    }

    private static ResourcePrivileges mutateTestItem(ResourcePrivileges original) {
        return switch (randomIntBetween(0, 1)) {
            case 0 -> ResourcePrivileges.builder(randomAlphaOfLength(6)).addPrivileges(original.getPrivileges()).build();
            case 1 -> ResourcePrivileges.builder(original.getResource()).addPrivileges(Collections.emptyMap()).build();
            default -> ResourcePrivileges.builder(randomAlphaOfLength(6)).addPrivileges(Collections.emptyMap()).build();
        };
    }

    private static MapBuilder<String, Boolean> mapBuilder() {
        return MapBuilder.newMapBuilder();
    }
}
