/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.cluster.node.DiscoveryNodeRole.VOTING_ONLY_NODE_ROLE;
import static org.elasticsearch.node.Node.NODE_EXTERNAL_ID_SETTING;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.node.NodeRoleSettings.NODE_ROLES_SETTING;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class DesiredNodeTests extends ESTestCase {
    public void testExternalIdIsRequired() {
        final Settings.Builder settings = Settings.builder();
        if (randomBoolean()) {
            final String key = randomBoolean() ? NODE_NAME_SETTING.getKey() : NODE_EXTERNAL_ID_SETTING.getKey();
            if (randomBoolean()) {
                settings.put(key, "   ");
            } else {
                settings.putNull(key);
            }
        }

        final IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new DesiredNode(settings.build(), 1, ByteSizeValue.ofGb(1), ByteSizeValue.ofGb(1), Version.CURRENT)
        );
        assertThat(exception.getMessage(), is(equalTo("[node.name] or [node.external_id] is missing or empty")));
    }

    public void testExternalIdFallbacksToNodeName() {
        final String nodeName = randomAlphaOfLength(10);
        final Settings settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), nodeName).build();

        DesiredNode desiredNode = new DesiredNode(settings, 1, ByteSizeValue.ofGb(1), ByteSizeValue.ofGb(1), Version.CURRENT);
        assertThat(desiredNode.externalId(), is(notNullValue()));
        assertThat(desiredNode.externalId(), is(equalTo(nodeName)));
    }

    public void testNumberOfProcessorsValidation() {
        final Settings settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), randomAlphaOfLength(10)).build();

        expectThrows(
            IllegalArgumentException.class,
            () -> new DesiredNode(settings, randomInvalidProcessor(), ByteSizeValue.ofGb(1), ByteSizeValue.ofGb(1), Version.CURRENT)
        );

        // Processor ranges
        expectThrows(
            IllegalArgumentException.class,
            () -> new DesiredNode(
                settings,
                new DesiredNode.ProcessorsRange(randomInvalidProcessor(), randomFrom(random(), null, 1.0)),
                ByteSizeValue.ofGb(1),
                ByteSizeValue.ofGb(1),
                Version.CURRENT
            )
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new DesiredNode(
                settings,
                new DesiredNode.ProcessorsRange(randomDouble() + 0.1, randomInvalidProcessor()),
                ByteSizeValue.ofGb(1),
                ByteSizeValue.ofGb(1),
                Version.CURRENT
            )
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new DesiredNode(
                settings,
                new DesiredNode.ProcessorsRange(randomInvalidProcessor(), randomInvalidProcessor()),
                ByteSizeValue.ofGb(1),
                ByteSizeValue.ofGb(1),
                Version.CURRENT
            )
        );

        final var lowerBound = randomDoubleBetween(0.1, 10, true);
        final var upperBound = randomDoubleBetween(0.01, lowerBound - Math.ulp(lowerBound), true);
        expectThrows(
            IllegalArgumentException.class,
            () -> new DesiredNode(
                settings,
                new DesiredNode.ProcessorsRange(lowerBound, upperBound),
                ByteSizeValue.ofGb(1),
                ByteSizeValue.ofGb(1),
                Version.CURRENT
            )
        );
    }

    public void testHasMasterRole() {
        {
            final Settings settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), randomAlphaOfLength(10)).build();

            DesiredNode desiredNode = new DesiredNode(settings, 1, ByteSizeValue.ofGb(1), ByteSizeValue.ofGb(1), Version.CURRENT);
            assertTrue(desiredNode.hasMasterRole());
        }

        {
            final Settings settings = Settings.builder()
                .put(NODE_NAME_SETTING.getKey(), randomAlphaOfLength(10))
                .put(NODE_ROLES_SETTING.getKey(), "master")
                .build();

            DesiredNode desiredNode = new DesiredNode(settings, 1, ByteSizeValue.ofGb(1), ByteSizeValue.ofGb(1), Version.CURRENT);
            assertTrue(desiredNode.hasMasterRole());
        }

        {
            final Settings settings = Settings.builder()
                .put(NODE_NAME_SETTING.getKey(), randomAlphaOfLength(10))
                .put(NODE_ROLES_SETTING.getKey(), "data_hot")
                .build();

            DesiredNode desiredNode = new DesiredNode(settings, 1, ByteSizeValue.ofGb(1), ByteSizeValue.ofGb(1), Version.CURRENT);
            assertFalse(desiredNode.hasMasterRole());
        }
    }

    public void testGetRoles() {
        final var settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), randomAlphaOfLength(10));

        final var role = randomBoolean() ? null : randomValueOtherThan(VOTING_ONLY_NODE_ROLE, () -> randomFrom(DiscoveryNodeRole.roles()));
        if (role != null) {
            settings.put(NODE_ROLES_SETTING.getKey(), role.roleName());
        }

        final var desiredNode = new DesiredNode(settings.build(), 1, ByteSizeValue.ofGb(1), ByteSizeValue.ofGb(1), Version.CURRENT);

        if (role != null) {
            assertThat(desiredNode.getRoles(), hasSize(1));
            assertThat(desiredNode.getRoles(), contains(role));
        } else {
            assertThat(desiredNode.getRoles(), contains(NODE_ROLES_SETTING.get(Settings.EMPTY).toArray()));
        }
    }

    public void testNodeCPUsRoundUp() {
        final var settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), randomAlphaOfLength(10)).build();

        {
            final var desiredNode = new DesiredNode(
                settings,
                new DesiredNode.ProcessorsRange(0.4, 1.2),
                ByteSizeValue.ofGb(1),
                ByteSizeValue.ofGb(1),
                Version.CURRENT
            );

            assertThat(desiredNode.minProcessors().count(), is(equalTo(0.4)));
            assertThat(desiredNode.roundedDownMinProcessors(), is(equalTo(1)));
            assertThat(desiredNode.maxProcessors().count(), is(equalTo(1.2)));
            assertThat(desiredNode.roundedUpMaxProcessors(), is(equalTo(2)));
        }

        {
            final var desiredNode = new DesiredNode(settings, 1.2, ByteSizeValue.ofGb(1), ByteSizeValue.ofGb(1), Version.CURRENT);

            assertThat(desiredNode.minProcessors().count(), is(equalTo(1.2)));
            assertThat(desiredNode.roundedDownMinProcessors(), is(equalTo(1)));
            assertThat(desiredNode.maxProcessors().count(), is(equalTo(1.2)));
            assertThat(desiredNode.roundedUpMaxProcessors(), is(equalTo(2)));
        }

        {
            final var desiredNode = new DesiredNode(settings, 1024, ByteSizeValue.ofGb(1), ByteSizeValue.ofGb(1), Version.CURRENT);

            assertThat(desiredNode.minProcessors().count(), is(equalTo(1024.0)));
            assertThat(desiredNode.roundedDownMinProcessors(), is(equalTo(1024)));
            assertThat(desiredNode.maxProcessors().count(), is(equalTo(1024.0)));
            assertThat(desiredNode.roundedUpMaxProcessors(), is(equalTo(1024)));
        }
    }

    public void testDesiredNodeIsCompatible() {
        final var settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), randomAlphaOfLength(10)).build();

        {
            final var desiredNode = new DesiredNode(
                settings,
                new DesiredNode.ProcessorsRange(0.4, 1.2),
                ByteSizeValue.ofGb(1),
                ByteSizeValue.ofGb(1),
                Version.CURRENT
            );
            assertThat(desiredNode.isCompatibleWithVersion(Version.V_8_2_0), is(equalTo(false)));
            assertThat(desiredNode.isCompatibleWithVersion(Version.V_8_3_0), is(equalTo(true)));
        }

        {
            final var desiredNode = new DesiredNode(
                settings,
                randomIntBetween(0, 10) + randomDoubleBetween(0.00001, 0.99999, true),
                ByteSizeValue.ofGb(1),
                ByteSizeValue.ofGb(1),
                Version.CURRENT
            );
            assertThat(desiredNode.isCompatibleWithVersion(Version.V_8_2_0), is(equalTo(false)));
            assertThat(desiredNode.isCompatibleWithVersion(Version.V_8_3_0), is(equalTo(true)));
        }

        {
            final var desiredNode = new DesiredNode(settings, 2.0f, ByteSizeValue.ofGb(1), ByteSizeValue.ofGb(1), Version.CURRENT);
            assertThat(desiredNode.isCompatibleWithVersion(Version.V_8_2_0), is(equalTo(true)));
            assertThat(desiredNode.isCompatibleWithVersion(Version.V_8_3_0), is(equalTo(true)));
        }
    }

    public void testEqualsOrProcessorsCloseTo() {
        final Settings settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), randomAlphaOfLength(10)).build();
        final double maxDelta = 1E-3;

        final double processorCount = randomNumberOfProcessors();
        final boolean isEqualOrCloseTo = randomBoolean();
        final ByteSizeValue memory = ByteSizeValue.ofGb(randomIntBetween(1, 32));
        final ByteSizeValue storage = ByteSizeValue.ofGb(randomIntBetween(128, 256));

        final DesiredNode desiredNode1;
        final DesiredNode desiredNode2;
        if (randomBoolean()) {
            desiredNode1 = new DesiredNode(settings, processorCount, memory, storage, Version.CURRENT);
            desiredNode2 = new DesiredNode(
                settings,
                isEqualOrCloseTo ? (float) processorCount : processorCount + maxDelta,
                memory,
                storage,
                Version.CURRENT
            );
        } else {
            final double desiredNodes1Min = processorCount;
            final Double desiredNodes1Max = randomBoolean() ? processorCount + randomIntBetween(1, 10) : null;
            final DesiredNode.ProcessorsRange desiredNodes1ProcessorsRange = new DesiredNode.ProcessorsRange(
                desiredNodes1Min,
                desiredNodes1Max
            );

            final double modifiedMinProcessors = isEqualOrCloseTo ? (float) desiredNodes1Min : desiredNodes1Min + maxDelta;

            final double desiredNodes2Min;
            final Double desiredNodes2Max;
            if (desiredNodes1Max != null && randomBoolean()) {
                desiredNodes2Min = randomBoolean() ? desiredNodes1Min : modifiedMinProcessors;
                desiredNodes2Max = isEqualOrCloseTo ? desiredNodes1Max.floatValue() : desiredNodes1Max + maxDelta;
            } else {
                desiredNodes2Min = modifiedMinProcessors;
                desiredNodes2Max = desiredNodes1Max;
            }
            final DesiredNode.ProcessorsRange desiredNodes2ProcessorsRange = new DesiredNode.ProcessorsRange(
                desiredNodes2Min,
                desiredNodes2Max
            );

            desiredNode1 = new DesiredNode(settings, desiredNodes1ProcessorsRange, memory, storage, Version.CURRENT);
            desiredNode2 = new DesiredNode(settings, desiredNodes2ProcessorsRange, memory, storage, Version.CURRENT);
        }

        assertThat(desiredNode1.equalsWithProcessorsCloseTo(desiredNode2), is(isEqualOrCloseTo));
    }

    private double randomNumberOfProcessors() {
        return randomDoubleBetween(Double.MIN_VALUE, 512.99999999, true);
    }

    private Double randomInvalidProcessor() {
        // 1E-7 is rounded to 0 since we only consider up to 5 decimal places
        return randomFrom(0.0, -1.0, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
    }
}
