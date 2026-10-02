/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.cluster.local;

import org.elasticsearch.test.cluster.local.LocalClusterSpec.LocalNodeSpec;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.Version;
import org.junit.After;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/** What a node of an upgrade test's cluster is given that a node of any other test's is not. */
public class DefaultSystemPropertyProviderTests {

    private static final String COLUMNAR_CODEC_FLAG = "es.columnar_codec_feature_flag_enabled";
    private static final String OLD_CLUSTER_VERSION = "tests.old_cluster_version";
    private static final String BWC_REFSPEC = "tests.bwc.refspec.main";
    private static final String BWC_MAIN_VERSION = "tests.bwc.main.version";

    @After
    public void clearUpgradeProperties() {
        System.clearProperty(OLD_CLUSTER_VERSION);
        System.clearProperty(BWC_REFSPEC);
        System.clearProperty(BWC_MAIN_VERSION);
    }

    /** The version to upgrade from is named either way round, and a suite naming only the second is an upgrade too. */
    @Test
    public void testTheOtherNameForTheVersionUpgradedFromCountsAsWell() {
        System.setProperty(BWC_MAIN_VERSION, "9.6.0");
        assertThat(propertiesFor(Version.CURRENT).get(COLUMNAR_CODEC_FLAG), is("false"));
    }

    @Test
    public void testAnOrdinaryClusterIsLeftOnWhateverTheBuildDecides() {
        assertThat(propertiesFor(Version.CURRENT), not(hasKey(COLUMNAR_CODEC_FLAG)));
    }

    @Test
    public void testUpgradingFromAReleasedVersionTurnsTheFlagOff() {
        System.setProperty(OLD_CLUSTER_VERSION, "9.5.0");
        assertThat(propertiesFor(Version.CURRENT).get(COLUMNAR_CODEC_FLAG), is("false"));
    }

    /** The build candidate carries the current version, so the ref of main it was built from is what names it. */
    @Test
    public void testUpgradingFromAnotherBuildOfMainTurnsTheFlagOff() {
        System.setProperty(BWC_REFSPEC, "3ad3f5df3b7fa2a9a50dddbb3f7b51120c08bc02");
        assertThat(propertiesFor(Version.CURRENT).get(COLUMNAR_CODEC_FLAG), is("false"));
    }

    /** A node of a version that predates the flag reads the property as nothing, which is why it needs no version check. */
    @Test
    public void testANodeOfAnOlderVersionIsGivenItToo() {
        System.setProperty(OLD_CLUSTER_VERSION, "8.19.0");
        assertThat(propertiesFor(Version.fromString("8.19.0")).get(COLUMNAR_CODEC_FLAG), is("false"));
    }

    /** A cluster of another version says so itself, so a suite that sets neither property is covered by that alone. */
    @Test
    public void testAClusterOfAnotherVersionNeedsNoProperty() {
        assertThat(propertiesFor(Version.fromString("9.5.0")).get(COLUMNAR_CODEC_FLAG), is("false"));
    }

    /** A build candidate carries the current version number, and is told apart by being detached from this build. */
    @Test
    public void testADetachedBuildOfTheCurrentVersionNeedsNoProperty() {
        assertThat(propertiesFor(Version.fromString(Version.CURRENT.toString(), true)).get(COLUMNAR_CODEC_FLAG), is("false"));
    }

    private static Map<String, String> propertiesFor(Version version) {
        final DefaultLocalClusterSpecBuilder builder = new DefaultLocalClusterSpecBuilder();
        // A node of a prior version is only configurable on the default distribution.
        builder.name("test").version(version).distribution(DistributionType.DEFAULT);
        final LocalNodeSpec node = builder.buildClusterSpec().getNodes().get(0);
        return new DefaultSystemPropertyProvider().get(node);
    }
}
