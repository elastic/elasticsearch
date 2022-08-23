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
import org.elasticsearch.common.unit.Processors;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.cluster.node.DiscoveryNodeRole.VOTING_ONLY_NODE_ROLE;
import static org.elasticsearch.node.Node.NODE_EXTERNAL_ID_SETTING;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.node.NodeRoleSettings.NODE_ROLES_SETTING;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class DesiredNodeTests extends ESTestCase {
    public static final double MAX_ERROR = 7E-5;

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
                randomIntBetween(0, 10) + randomDouble(),
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

    public void testFloatProcessorsConvertedToDoubleAreCloseToEqual() {
        final double processorCount = randomNumberOfProcessors();
        final float processorCountAsFloat = (float) processorCount;
        final Processors bwcProcessors = new Processors(processorCountAsFloat);
        final Processors doubleProcessor = new Processors(processorCount);
        assertThat(DesiredNode.processorsEqualsOrCloseTo(bwcProcessors, doubleProcessor, MAX_ERROR), is(true));
    }

    public void testProcessorsAreConsideredDifferentIfTheDifferenceIsGreaterThanMaxError() {
        // Ensure that (processorCount - MAX_ERROR) is at least the smallest representable processor
        final double processorCount = Math.max(Math.ulp(0.0) + MAX_ERROR, randomNumberOfProcessors());
        final Processors processorsA = new Processors(processorCount + MAX_ERROR);
        final Processors processorsB = new Processors(processorCount - MAX_ERROR);
        assertThat(DesiredNode.processorsEqualsOrCloseTo(processorsA, processorsB, MAX_ERROR), is(false));
        assertThat(processorsA.equals(processorsB), is(false));
    }

    public void testRoundedProcessorsToFloatAreCloseToEqual() {
        double processorCount = randomNumberOfProcessors();
        final Processors doubleProcessor = new Processors(processorCount);
        final Processors floatProcessor = new Processors((float) doubleProcessor.count());
        assertThat(DesiredNode.processorsEqualsOrCloseTo(doubleProcessor, floatProcessor, MAX_ERROR), is(true));
    }

    public void testEqualsOrProcessorsCloseTo() {
        final Settings settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), randomAlphaOfLength(10)).build();

        final double processorCount = randomNumberOfProcessors();
        final boolean shouldBeConsideredEqual = randomBoolean();
        final double maxDifferenceBetweenProcessorCounts = shouldBeConsideredEqual ? MAX_ERROR / 2 : MAX_ERROR * 2;
        final ByteSizeValue memory = ByteSizeValue.ofGb(randomIntBetween(1, 32));
        final ByteSizeValue storage = ByteSizeValue.ofGb(randomIntBetween(128, 256));

        final DesiredNode desiredNode1;
        final DesiredNode desiredNode2;
        if (randomBoolean()) {
            desiredNode1 = new DesiredNode(
                settings,
                processorCount + maxDifferenceBetweenProcessorCounts,
                memory,
                storage,
                Version.CURRENT
            );
            desiredNode2 = new DesiredNode(settings, processorCount, memory, storage, Version.CURRENT);
        } else {
            final Double maxProcessors = randomBoolean() ? processorCount + randomIntBetween(1, 10) : null;

            final Double maxProcessorsDesiredNode1;
            if (maxProcessors != null && randomBoolean()) {
                maxProcessorsDesiredNode1 = maxProcessors + maxDifferenceBetweenProcessorCounts;
            } else {
                maxProcessorsDesiredNode1 = maxProcessors;
            }

            final DesiredNode.ProcessorsRange processorsRange1 = new DesiredNode.ProcessorsRange(
                processorCount + maxDifferenceBetweenProcessorCounts,
                maxProcessorsDesiredNode1
            );

            final DesiredNode.ProcessorsRange processorsRange2 = new DesiredNode.ProcessorsRange(processorCount, maxProcessors);

            desiredNode1 = new DesiredNode(settings, processorsRange1, memory, storage, Version.CURRENT);
            desiredNode2 = new DesiredNode(settings, processorsRange2, memory, storage, Version.CURRENT);
        }

        assertThat(desiredNode1.equalsWithProcessorsCloseTo(desiredNode2, MAX_ERROR), is(shouldBeConsideredEqual));
        assertThat(desiredNode1, is(not(equalTo(desiredNode2))));
    }

    private double randomNumberOfProcessors() {
        return randomDoubleBetween(Math.ulp(0.0), 512.99999999, true);
    }

    private Double randomInvalidProcessor() {
        // 1E-7 is rounded to 0 since we only consider up to 5 decimal places
        return randomFrom(0.0, -1.0, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
    }
}
