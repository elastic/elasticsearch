/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.node;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;

public class DiscoveryNodeRoleTests extends ESTestCase {

    public void testDiscoveryNodeSetPossibleRolesRejectsDuplicateRoleNames() {
        final IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> DiscoveryNode.setAdditionalRoles(Set.of(
                        new DiscoveryNodeRole("foo", "f") {

                            @Override
                            public Setting<Boolean> legacySetting() {
                                return null;
                            }

                        },
                        new DiscoveryNodeRole("foo", "f") {

                            @Override
                            public Setting<Boolean> legacySetting() {
                                return null;
                            }

                        })));
        assertThat(e, hasToString(containsString("Duplicate key")));
    }

    public void testDiscoveryNodeSetPossibleRolesRejectsDuplicateRoleNameAbbreviations() {
        final IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> DiscoveryNode.setAdditionalRoles(Set.of(
                        new DiscoveryNodeRole("foo_1", "f") {

                            @Override
                            public Setting<Boolean> legacySetting() {
                                return null;
                            }

                        },
                        new DiscoveryNodeRole("foo_2", "f") {

                            @Override
                            public Setting<Boolean> legacySetting() {
                                return null;
                            }

                        })));
        assertThat(e, hasToString(containsString("Duplicate key")));
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
        for (DiscoveryNodeRole buildInRole : DiscoveryNodeRole.BUILT_IN_ROLES) {
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
