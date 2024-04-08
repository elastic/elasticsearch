/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.transform.utils.TransformConfigVersionUtils;
import org.hamcrest.Matchers;

import java.lang.reflect.Modifier;
import java.net.InetAddress;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.sameInstance;

public class TransformConfigVersionTests extends ESTestCase {

    public void testVersionComparison() {
        TransformConfigVersion V_7_2_0 = TransformConfigVersion.V_7_2_0;
        TransformConfigVersion V_8_0_0 = TransformConfigVersion.V_8_0_0;
        TransformConfigVersion V_10 = TransformConfigVersion.V_10;
        assertThat(V_7_2_0.before(V_8_0_0), is(true));
        assertThat(V_7_2_0.before(V_7_2_0), is(false));
        assertThat(V_8_0_0.before(V_7_2_0), is(false));
        assertThat(V_8_0_0.before(V_10), is(true));
        assertThat(V_10.before(V_10), is(false));

        assertThat(V_7_2_0.onOrBefore(V_8_0_0), is(true));
        assertThat(V_7_2_0.onOrBefore(V_7_2_0), is(true));
        assertThat(V_8_0_0.onOrBefore(V_7_2_0), is(false));
        assertThat(V_8_0_0.onOrBefore(V_10), is(true));
        assertThat(V_10.onOrBefore(V_10), is(true));

        assertThat(V_7_2_0.after(V_8_0_0), is(false));
        assertThat(V_7_2_0.after(V_7_2_0), is(false));
        assertThat(V_8_0_0.after(V_7_2_0), is(true));
        assertThat(V_10.after(V_8_0_0), is(true));
        assertThat(V_10.after(V_10), is(false));

        assertThat(V_7_2_0.onOrAfter(V_8_0_0), is(false));
        assertThat(V_7_2_0.onOrAfter(V_7_2_0), is(true));
        assertThat(V_8_0_0.onOrAfter(V_7_2_0), is(true));
        assertThat(V_10.onOrAfter(V_8_0_0), is(true));
        assertThat(V_10.onOrAfter(V_10), is(true));
        assertThat(V_7_2_0.onOrAfter(V_10), is(false));

        assertThat(V_7_2_0, Matchers.is(lessThan(V_8_0_0)));
        assertThat(V_7_2_0.compareTo(V_7_2_0), is(0));
        assertThat(V_8_0_0, Matchers.is(greaterThan(V_7_2_0)));
        assertThat(V_10, Matchers.is(greaterThan(V_8_0_0)));
        assertThat(V_10.compareTo(V_10), is(0));

    }

    public static class CorrectFakeVersion {
        public static final TransformConfigVersion V_0_00_01 = new TransformConfigVersion(199);
        public static final TransformConfigVersion V_0_000_002 = new TransformConfigVersion(2);
        public static final TransformConfigVersion V_0_000_003 = new TransformConfigVersion(3);
        public static final TransformConfigVersion V_0_000_004 = new TransformConfigVersion(4);
    }

    public static class DuplicatedIdFakeVersion {
        public static final TransformConfigVersion V_0_000_001 = new TransformConfigVersion(1);
        public static final TransformConfigVersion V_0_000_002 = new TransformConfigVersion(2);
        public static final TransformConfigVersion V_0_000_003 = new TransformConfigVersion(2);
    }

    public void testStaticTransformConfigVersionChecks() {
        assertThat(
            TransformConfigVersion.getAllVersionIds(CorrectFakeVersion.class),
            equalTo(
                Map.of(
                    199,
                    CorrectFakeVersion.V_0_00_01,
                    2,
                    CorrectFakeVersion.V_0_000_002,
                    3,
                    CorrectFakeVersion.V_0_000_003,
                    4,
                    CorrectFakeVersion.V_0_000_004
                )
            )
        );
        AssertionError e = expectThrows(AssertionError.class, () -> TransformConfigVersion.getAllVersionIds(DuplicatedIdFakeVersion.class));
        assertThat(e.getMessage(), containsString("have the same version number"));
    }

    private static final Set<DiscoveryNodeRole> ROLES_WITH_TRANSFORM = Set.of(DiscoveryNodeRole.TRANSFORM_ROLE);

    public void testGetMinMaxTransformConfigVersion() {
        Map<String, String> nodeAttr1 = Map.of(
            TransformConfigVersion.TRANSFORM_CONFIG_VERSION_NODE_ATTR,
            TransformConfigVersion.V_7_2_0.toString()
        );
        Map<String, String> nodeAttr2 = Map.of(
            TransformConfigVersion.TRANSFORM_CONFIG_VERSION_NODE_ATTR,
            TransformConfigVersion.V_8_2_0.toString()
        );
        Map<String, String> nodeAttr3 = Map.of(
            TransformConfigVersion.TRANSFORM_CONFIG_VERSION_NODE_ATTR,
            TransformConfigVersion.V_10.toString()
        );
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(
                DiscoveryNodeUtils.builder("_node_id1")
                    .name("_node_name1")
                    .address(new TransportAddress(InetAddress.getLoopbackAddress(), 9300))
                    .attributes(nodeAttr1)
                    .roles(ROLES_WITH_TRANSFORM)
                    .version(VersionInformation.inferVersions(Version.fromString("7.2.0")))
                    .build()
            )
            .add(
                DiscoveryNodeUtils.builder("_node_id2")
                    .name("_node_name2")
                    .address(new TransportAddress(InetAddress.getLoopbackAddress(), 9301))
                    .attributes(nodeAttr2)
                    .roles(ROLES_WITH_TRANSFORM)
                    .version(VersionInformation.inferVersions(Version.fromString("7.1.0")))
                    .build()
            )
            .add(
                DiscoveryNodeUtils.builder("_node_id3")
                    .name("_node_name3")
                    .address(new TransportAddress(InetAddress.getLoopbackAddress(), 9302))
                    .attributes(nodeAttr3)
                    .roles(ROLES_WITH_TRANSFORM)
                    .version(VersionInformation.inferVersions(Version.fromString("7.0.0")))
                    .build()
            )
            .build();

        assertEquals(TransformConfigVersion.V_7_2_0, TransformConfigVersion.getMinTransformConfigVersion(nodes));
        assertEquals(TransformConfigVersion.V_10, TransformConfigVersion.getMaxTransformConfigVersion(nodes));
    }

    public void testGetTransformConfigVersionForNode() {
        DiscoveryNode node = DiscoveryNodeUtils.builder("_node_id4")
            .name("_node_name4")
            .address(new TransportAddress(InetAddress.getLoopbackAddress(), 9303))
            .roles(ROLES_WITH_TRANSFORM)
            .version(VersionInformation.inferVersions(Version.fromString("8.7.0")))
            .build();
        TransformConfigVersion transformConfigVersion = TransformConfigVersion.getTransformConfigVersionForNode(node);
        assertEquals(TransformConfigVersion.V_8_7_0, transformConfigVersion);

        DiscoveryNode node1 = DiscoveryNodeUtils.builder("_node_id5")
            .name("_node_name5")
            .address(new TransportAddress(InetAddress.getLoopbackAddress(), 9304))
            .attributes(Map.of(TransformConfigVersion.TRANSFORM_CONFIG_VERSION_NODE_ATTR, TransformConfigVersion.V_8_5_0.toString()))
            .roles(ROLES_WITH_TRANSFORM)
            .version(VersionInformation.inferVersions(Version.fromString("8.7.0")))
            .build();
        TransformConfigVersion TransformConfigVersion1 = TransformConfigVersion.getTransformConfigVersionForNode(node1);
        assertEquals(TransformConfigVersion.V_8_5_0, TransformConfigVersion1);
    }

    public void testDefinedConstants() throws IllegalAccessException {
        Pattern historicalVersion = Pattern.compile("^V_(\\d{1,2})_(\\d{1,2})_(\\d{1,2})$");
        Pattern TransformConfigVersion = Pattern.compile("^V_(\\d+)$");
        Set<String> ignore = Set.of("ZERO", "CURRENT", "MINIMUM_COMPATIBLE");

        for (java.lang.reflect.Field field : TransformConfigVersion.class.getFields()) {
            String fieldName = field.getName();
            if (fieldName.equals("V_8_10_0") == false) {
                continue;
            }
            if (field.getType() == TransformConfigVersion.class && ignore.contains(field.getName()) == false) {

                // check the field modifiers
                assertEquals(
                    "Field " + field.getName() + " should be public static final",
                    Modifier.PUBLIC | Modifier.STATIC | Modifier.FINAL,
                    field.getModifiers()
                );

                Matcher matcher = historicalVersion.matcher(field.getName());
                if (matcher.matches()) {
                    // old-style version constant
                    String idString = matcher.group(1) + "." + matcher.group(2) + "." + matcher.group(3);
                    String fieldStr = field.get(null).toString();
                    assertEquals(
                        "Field " + field.getName() + " does not have expected id " + idString,
                        idString,
                        field.get(null).toString()
                    );
                } else if ((matcher = TransformConfigVersion.matcher(field.getName())).matches()) {
                    String idString = matcher.group(1);
                    assertEquals(
                        "Field " + field.getName() + " does not have expected id " + idString,
                        idString,
                        field.get(null).toString()
                    );
                } else {
                    fail("Field " + field.getName() + " does not have expected format");
                }
            }
        }
    }

    public void testMin() {
        assertEquals(
            TransformConfigVersionUtils.getPreviousVersion(),
            TransformConfigVersion.min(TransformConfigVersion.CURRENT, TransformConfigVersionUtils.getPreviousVersion())
        );
        assertEquals(
            TransformConfigVersion.fromId(TransformConfigVersion.FIRST_TRANSFORM_VERSION.id()),
            TransformConfigVersion.min(
                TransformConfigVersion.fromId(TransformConfigVersion.FIRST_TRANSFORM_VERSION.id()),
                TransformConfigVersion.CURRENT
            )
        );
    }

    public void testMax() {
        assertEquals(
            TransformConfigVersion.CURRENT,
            TransformConfigVersion.max(TransformConfigVersion.CURRENT, TransformConfigVersionUtils.getPreviousVersion())
        );
        assertEquals(
            TransformConfigVersion.CURRENT,
            TransformConfigVersion.max(
                TransformConfigVersion.fromId(TransformConfigVersion.FIRST_TRANSFORM_VERSION.id()),
                TransformConfigVersion.CURRENT
            )
        );
    }

    public void testVersionConstantPresent() {
        Set<TransformConfigVersion> ignore = Set.of(
            TransformConfigVersion.ZERO,
            TransformConfigVersion.CURRENT,
            TransformConfigVersion.FIRST_TRANSFORM_VERSION
        );
        assertThat(TransformConfigVersion.CURRENT, sameInstance(TransformConfigVersion.fromId(TransformConfigVersion.CURRENT.id())));
        final int iters = scaledRandomIntBetween(20, 100);
        for (int i = 0; i < iters; i++) {
            TransformConfigVersion version = TransformConfigVersionUtils.randomVersion(ignore);

            assertThat(version, sameInstance(TransformConfigVersion.fromId(version.id())));
        }
    }

    public void testCurrentIsLatest() {
        assertThat(Collections.max(TransformConfigVersion.getAllVersions()), Matchers.is(TransformConfigVersion.CURRENT));
    }

    public void testToString() {
        TransformConfigVersion transformVersion = TransformConfigVersion.fromId(5_00_00_99);
        String transformVersionStr = transformVersion.toString();
        Version version = Version.fromId(5_00_00_99);
        String versionStr = version.toString();
        assertEquals("5.0.0", TransformConfigVersion.fromId(5_00_00_99).toString());
        assertEquals("2.3.0", TransformConfigVersion.fromId(2_03_00_99).toString());
        assertEquals("1.0.0", TransformConfigVersion.fromId(1_00_00_99).toString());
        assertEquals("2.0.0", TransformConfigVersion.fromId(2_00_00_99).toString());
        assertEquals("5.0.0", TransformConfigVersion.fromId(5_00_00_99).toString());

        String str = TransformConfigVersion.fromId(10_00_00_10).toString();

        assertEquals("10.0.0", TransformConfigVersion.fromId(10_00_00_10).toString());

        assertEquals("7.3.0", TransformConfigVersion.V_7_3_0.toString());
        assertEquals("8.6.1", TransformConfigVersion.V_8_6_1.toString());
        assertEquals("8.0.0", TransformConfigVersion.V_8_0_0.toString());
        assertEquals("7.2.1", TransformConfigVersion.V_7_2_1.toString());
        assertEquals("7.15.1", TransformConfigVersion.V_7_15_1.toString());
        assertEquals("10.0.0", TransformConfigVersion.V_10.toString());
    }

    public void testFromString() {
        assertEquals(TransformConfigVersion.V_7_3_0, TransformConfigVersion.fromString("7.3.0"));
        assertEquals(TransformConfigVersion.V_8_6_1, TransformConfigVersion.fromString("8.6.1"));
        assertEquals(TransformConfigVersion.V_8_0_0, TransformConfigVersion.fromString("8.0.0"));
        assertEquals(TransformConfigVersion.V_10, TransformConfigVersion.fromString("8.10.0"));
        assertEquals(TransformConfigVersion.V_10, TransformConfigVersion.fromString("10.0.0"));

        TransformConfigVersion V_8_0_1 = TransformConfigVersion.fromString("8.0.1");
        assertEquals(false, KnownTransformConfigVersions.ALL_VERSIONS.contains(V_8_0_1));
        assertEquals(8000199, V_8_0_1.id());

        TransformConfigVersion unknownVersion = TransformConfigVersion.fromId(TransformConfigVersion.CURRENT.id() + 1);
        assertEquals(false, KnownTransformConfigVersions.ALL_VERSIONS.contains(unknownVersion));
        assertEquals(TransformConfigVersion.CURRENT.id() + 1, unknownVersion.id());

        for (String version : new String[] { "10.2", "7.17.2.99", "9" }) {
            Exception e = expectThrows(IllegalArgumentException.class, () -> TransformConfigVersion.fromString(version));
            assertEquals("Transform config version [" + version + "] not valid", e.getMessage());
        }
    }
}
