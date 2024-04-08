/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.utils.MlConfigVersionUtils;
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

public class MlConfigVersionTests extends ESTestCase {

    public void testVersionComparison() {
        MlConfigVersion V_7_2_0 = MlConfigVersion.V_7_2_0;
        MlConfigVersion V_8_0_0 = MlConfigVersion.V_8_0_0;
        MlConfigVersion V_10 = MlConfigVersion.V_10;
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
        public static final MlConfigVersion V_0_00_01 = new MlConfigVersion(199);
        public static final MlConfigVersion V_0_000_002 = new MlConfigVersion(2);
        public static final MlConfigVersion V_0_000_003 = new MlConfigVersion(3);
        public static final MlConfigVersion V_0_000_004 = new MlConfigVersion(4);
    }

    public static class DuplicatedIdFakeVersion {
        public static final MlConfigVersion V_0_000_001 = new MlConfigVersion(1);
        public static final MlConfigVersion V_0_000_002 = new MlConfigVersion(2);
        public static final MlConfigVersion V_0_000_003 = new MlConfigVersion(2);
    }

    public void testStaticMlConfigVersionChecks() {
        assertThat(
            MlConfigVersion.getAllVersionIds(CorrectFakeVersion.class),
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
        AssertionError e = expectThrows(AssertionError.class, () -> MlConfigVersion.getAllVersionIds(DuplicatedIdFakeVersion.class));
        assertThat(e.getMessage(), containsString("have the same version number"));
    }

    private static final Set<DiscoveryNodeRole> ROLES_WITH_ML = Set.of(
        DiscoveryNodeRole.MASTER_ROLE,
        DiscoveryNodeRole.ML_ROLE,
        DiscoveryNodeRole.DATA_ROLE
    );

    public void testGetMinMaxMlConfigVersion() {
        Map<String, String> nodeAttr1 = Map.of(MlConfigVersion.ML_CONFIG_VERSION_NODE_ATTR, MlConfigVersion.V_7_1_0.toString());
        Map<String, String> nodeAttr2 = Map.of(MlConfigVersion.ML_CONFIG_VERSION_NODE_ATTR, MlConfigVersion.V_8_2_0.toString());
        Map<String, String> nodeAttr3 = Map.of(MlConfigVersion.ML_CONFIG_VERSION_NODE_ATTR, MlConfigVersion.V_10.toString());
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(
                DiscoveryNodeUtils.builder("_node_id1")
                    .name("_node_name1")
                    .address(new TransportAddress(InetAddress.getLoopbackAddress(), 9300))
                    .attributes(nodeAttr1)
                    .roles(ROLES_WITH_ML)
                    .version(VersionInformation.inferVersions(Version.fromString("7.2.0")))
                    .build()
            )
            .add(
                DiscoveryNodeUtils.builder("_node_id2")
                    .name("_node_name2")
                    .address(new TransportAddress(InetAddress.getLoopbackAddress(), 9301))
                    .attributes(nodeAttr2)
                    .roles(ROLES_WITH_ML)
                    .version(VersionInformation.inferVersions(Version.fromString("7.1.0")))
                    .build()
            )
            .add(
                DiscoveryNodeUtils.builder("_node_id3")
                    .name("_node_name3")
                    .address(new TransportAddress(InetAddress.getLoopbackAddress(), 9302))
                    .attributes(nodeAttr3)
                    .roles(ROLES_WITH_ML)
                    .version(VersionInformation.inferVersions(Version.fromString("7.0.0")))
                    .build()
            )
            .build();

        assertEquals(MlConfigVersion.V_7_1_0, MlConfigVersion.getMinMlConfigVersion(nodes));
        assertEquals(MlConfigVersion.V_10, MlConfigVersion.getMaxMlConfigVersion(nodes));
    }

    public void testGetMinMaxMlConfigVersionWhenMlConfigVersionAttrIsMissing() {
        Map<String, String> nodeAttr1 = Map.of(MlConfigVersion.ML_CONFIG_VERSION_NODE_ATTR, MlConfigVersion.V_7_1_0.toString());
        Map<String, String> nodeAttr2 = Map.of(MlConfigVersion.ML_CONFIG_VERSION_NODE_ATTR, MlConfigVersion.V_8_2_0.toString());
        Map<String, String> nodeAttr3 = Map.of();
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(
                DiscoveryNodeUtils.builder("_node_id1")
                    .name("_node_name1")
                    .address(new TransportAddress(InetAddress.getLoopbackAddress(), 9300))
                    .attributes(nodeAttr1)
                    .roles(ROLES_WITH_ML)
                    .version(VersionInformation.inferVersions(Version.fromString("7.2.0")))
                    .build()
            )
            .add(
                DiscoveryNodeUtils.builder("_node_id2")
                    .name("_node_name2")
                    .address(new TransportAddress(InetAddress.getLoopbackAddress(), 9301))
                    .attributes(nodeAttr2)
                    .roles(ROLES_WITH_ML)
                    .version(VersionInformation.inferVersions(Version.fromString("7.1.0")))
                    .build()
            )
            .add(
                DiscoveryNodeUtils.builder("_node_id3")
                    .name("_node_name3")
                    .address(new TransportAddress(InetAddress.getLoopbackAddress(), 9302))
                    .attributes(nodeAttr3)
                    .roles(ROLES_WITH_ML)
                    .version(
                        new VersionInformation(
                            Version.V_8_11_0,
                            IndexVersion.getMinimumCompatibleIndexVersion(Version.V_8_11_0.id),
                            IndexVersion.fromId(Version.V_8_11_0.id)
                        )
                    )
                    .build()
            )
            .build();

        assertEquals(MlConfigVersion.V_7_1_0, MlConfigVersion.getMinMlConfigVersion(nodes));
        // _node_name3 is ignored
        assertEquals(MlConfigVersion.V_8_2_0, MlConfigVersion.getMaxMlConfigVersion(nodes));
    }

    public void testGetMlConfigVersionForNode() {
        DiscoveryNode node = DiscoveryNodeUtils.builder("_node_id4")
            .name("_node_name4")
            .address(new TransportAddress(InetAddress.getLoopbackAddress(), 9303))
            .roles(ROLES_WITH_ML)
            .version(VersionInformation.inferVersions(Version.fromString("8.7.0")))
            .build();
        MlConfigVersion mlConfigVersion = MlConfigVersion.getMlConfigVersionForNode(node);
        assertEquals(MlConfigVersion.V_8_7_0, mlConfigVersion);

        DiscoveryNode node1 = DiscoveryNodeUtils.builder("_node_id5")
            .name("_node_name5")
            .address(new TransportAddress(InetAddress.getLoopbackAddress(), 9304))
            .attributes(Map.of(MlConfigVersion.ML_CONFIG_VERSION_NODE_ATTR, MlConfigVersion.V_8_5_0.toString()))
            .roles(ROLES_WITH_ML)
            .version(VersionInformation.inferVersions(Version.fromString("8.7.0")))
            .build();
        MlConfigVersion mlConfigVersion1 = MlConfigVersion.getMlConfigVersionForNode(node1);
        assertEquals(MlConfigVersion.V_8_5_0, mlConfigVersion1);
    }

    public void testDefinedConstants() throws IllegalAccessException {
        Pattern historicalVersion = Pattern.compile("^V_(\\d{1,2})_(\\d{1,2})_(\\d{1,2})$");
        Pattern MlConfigVersion = Pattern.compile("^V_(\\d+)$");
        Set<String> ignore = Set.of("ZERO", "CURRENT", "MINIMUM_COMPATIBLE");

        for (java.lang.reflect.Field field : MlConfigVersion.class.getFields()) {
            String fieldName = field.getName();
            if (fieldName.equals("V_8_10_0") == false) {
                continue;
            }
            if (field.getType() == MlConfigVersion.class && ignore.contains(field.getName()) == false) {

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
                } else if ((matcher = MlConfigVersion.matcher(field.getName())).matches()) {
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
            MlConfigVersionUtils.getPreviousVersion(),
            MlConfigVersion.min(MlConfigVersion.CURRENT, MlConfigVersionUtils.getPreviousVersion())
        );
        assertEquals(
            MlConfigVersion.fromId(MlConfigVersion.FIRST_ML_VERSION.id()),
            MlConfigVersion.min(MlConfigVersion.fromId(MlConfigVersion.FIRST_ML_VERSION.id()), MlConfigVersion.CURRENT)
        );
    }

    public void testMax() {
        assertEquals(MlConfigVersion.CURRENT, MlConfigVersion.max(MlConfigVersion.CURRENT, MlConfigVersionUtils.getPreviousVersion()));
        assertEquals(
            MlConfigVersion.CURRENT,
            MlConfigVersion.max(MlConfigVersion.fromId(MlConfigVersion.FIRST_ML_VERSION.id()), MlConfigVersion.CURRENT)
        );
    }

    public void testVersionConstantPresent() {
        Set<MlConfigVersion> ignore = Set.of(MlConfigVersion.ZERO, MlConfigVersion.CURRENT, MlConfigVersion.FIRST_ML_VERSION);
        assertThat(MlConfigVersion.CURRENT, sameInstance(MlConfigVersion.fromId(MlConfigVersion.CURRENT.id())));
        final int iters = scaledRandomIntBetween(20, 100);
        for (int i = 0; i < iters; i++) {
            MlConfigVersion version = MlConfigVersionUtils.randomVersion(ignore);

            assertThat(version, sameInstance(MlConfigVersion.fromId(version.id())));
        }
    }

    public void testCurrentIsLatest() {
        assertThat(Collections.max(MlConfigVersion.getAllVersions()), Matchers.is(MlConfigVersion.CURRENT));
    }

    public void testToString() {
        MlConfigVersion mlVersion = MlConfigVersion.fromId(5_00_00_99);
        String mlVersionStr = mlVersion.toString();
        Version version = Version.fromId(5_00_00_99);
        String versionStr = version.toString();
        assertEquals("5.0.0", MlConfigVersion.fromId(5_00_00_99).toString());
        assertEquals("2.3.0", MlConfigVersion.fromId(2_03_00_99).toString());
        assertEquals("1.0.0", MlConfigVersion.fromId(1_00_00_99).toString());
        assertEquals("2.0.0", MlConfigVersion.fromId(2_00_00_99).toString());
        assertEquals("5.0.0", MlConfigVersion.fromId(5_00_00_99).toString());

        String str = MlConfigVersion.fromId(10_00_00_10).toString();

        assertEquals("10.0.0", MlConfigVersion.fromId(10_00_00_10).toString());

        assertEquals("7.3.0", MlConfigVersion.V_7_3_0.toString());
        assertEquals("8.6.1", MlConfigVersion.V_8_6_1.toString());
        assertEquals("8.0.0", MlConfigVersion.V_8_0_0.toString());
        assertEquals("7.0.1", MlConfigVersion.V_7_0_1.toString());
        assertEquals("7.15.1", MlConfigVersion.V_7_15_1.toString());
        assertEquals("10.0.0", MlConfigVersion.V_10.toString());
    }

    public void testFromString() {
        assertEquals(MlConfigVersion.V_7_3_0, MlConfigVersion.fromString("7.3.0"));
        assertEquals(MlConfigVersion.V_8_6_1, MlConfigVersion.fromString("8.6.1"));
        assertEquals(MlConfigVersion.V_8_0_0, MlConfigVersion.fromString("8.0.0"));
        assertEquals(MlConfigVersion.V_10, MlConfigVersion.fromString("8.10.0"));
        assertEquals(MlConfigVersion.V_10, MlConfigVersion.fromString("10.0.0"));

        MlConfigVersion V_8_0_1 = MlConfigVersion.fromString("8.0.1");
        assertEquals(false, KnownMlConfigVersions.ALL_VERSIONS.contains(V_8_0_1));
        assertEquals(8000199, V_8_0_1.id());

        MlConfigVersion unknownVersion = MlConfigVersion.fromId(MlConfigVersion.CURRENT.id() + 1);
        assertEquals(false, KnownMlConfigVersions.ALL_VERSIONS.contains(unknownVersion));
        assertEquals(MlConfigVersion.CURRENT.id() + 1, unknownVersion.id());

        for (String version : new String[] { "10.2", "7.17.2.99", "9" }) {
            Exception e = expectThrows(IllegalArgumentException.class, () -> MlConfigVersion.fromString(version));
            assertEquals("ML config version [" + version + "] not valid", e.getMessage());
        }
    }
}
