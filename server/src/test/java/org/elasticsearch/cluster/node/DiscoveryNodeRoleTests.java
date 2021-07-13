/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.node;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

public class DiscoveryNodeRoleTests extends ESTestCase {

    public void testRolesIsImmutable() {
        expectThrows(UnsupportedOperationException.class, () -> DiscoveryNodeRole.roles().add(DiscoveryNodeRole.DATA_ROLE));
    }

    public void testRoleNamesIsImmutable() {
        expectThrows(
            UnsupportedOperationException.class,
            () -> DiscoveryNodeRole.roleNames().add(DiscoveryNodeRole.DATA_ROLE.roleName())
        );
    }

    public void testDiscoveryNodeRoleEqualsHashCode() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            new DiscoveryNodeRole.UnknownRole(randomAlphaOfLength(10), randomAlphaOfLength(1), randomBoolean()),
            r -> new DiscoveryNodeRole.UnknownRole(r.roleName(), r.roleNameAbbreviation(), r.canContainData()),
            r -> {
                final int value = randomIntBetween(0, 2);
                switch (value) {
                    case 0:
                    return new DiscoveryNodeRole.UnknownRole(
                        randomAlphaOfLength(21 - r.roleName().length()),
                        r.roleNameAbbreviation(),
                        r.canContainData()
                    );
                    case 1:
                    return new DiscoveryNodeRole.UnknownRole(
                        r.roleName(),
                        randomAlphaOfLength(3 - r.roleNameAbbreviation().length()),
                        r.canContainData());
                    case 2:
                        return new DiscoveryNodeRole.UnknownRole(r.roleName(), r.roleNameAbbreviation(), r.canContainData() == false);
                    default:
                        throw new AssertionError("unexpected value [" + value + "] not between 0 and 2");
                }
            });

    }

    public void testUnknownRoleIsDistinctFromKnownRoles() {
        for (DiscoveryNodeRole buildInRole : DiscoveryNodeRole.roles()) {
            final DiscoveryNodeRole.UnknownRole unknownDataRole = new DiscoveryNodeRole.UnknownRole(
                buildInRole.roleName(),
                buildInRole.roleNameAbbreviation(),
                buildInRole.canContainData()
            );
            assertNotEquals(buildInRole, unknownDataRole);
            assertNotEquals(buildInRole.toString(), unknownDataRole.toString());
        }
    }
}
