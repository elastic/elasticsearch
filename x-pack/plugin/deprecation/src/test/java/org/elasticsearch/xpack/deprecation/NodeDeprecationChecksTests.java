/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.deprecation.DeprecationChecks.NODE_SETTINGS_CHECKS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;

public class NodeDeprecationChecksTests extends ESTestCase {

    public void testRemovedSettingNotSet() {
        final Settings settings = Settings.EMPTY;
        final Setting<?> removedSetting = Setting.simpleString("node.removed_setting");
        final DeprecationIssue issue =
            NodeDeprecationChecks.checkRemovedSetting(settings, removedSetting, "http://removed-setting.example.com");
        assertThat(issue, nullValue());
    }

    public void testRemovedSetting() {
        final Settings settings = Settings.builder().put("node.removed_setting", "value").build();
        final Setting<?> removedSetting = Setting.simpleString("node.removed_setting");
        final DeprecationIssue issue =
            NodeDeprecationChecks.checkRemovedSetting(settings, removedSetting, "https://removed-setting.example.com");
        assertThat(issue, not(nullValue()));
        assertThat(issue.getLevel(), equalTo(DeprecationIssue.Level.CRITICAL));
        assertThat(
            issue.getMessage(),
            equalTo("setting [node.removed_setting] is deprecated and will be removed in the next major version"));
        assertThat(
            issue.getDetails(),
            equalTo("the setting [node.removed_setting] is currently set to [value], remove this setting"));
        assertThat(issue.getUrl(), equalTo("https://removed-setting.example.com"));
    }

    public void testSharedDataPathSetting() {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .put(Environment.PATH_SHARED_DATA_SETTING.getKey(), createTempDir()).build();

        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(NODE_SETTINGS_CHECKS, c -> c.apply(settings, null));
        final String expectedUrl =
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.13/breaking-changes-7.13.html#deprecate-shared-data-path-setting";
        assertThat(issues, contains(
            new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "setting [path.shared_data] is deprecated and will be removed in a future version",
                expectedUrl,
                "Found shared data path configured. Discontinue use of this setting.",
                null)));
    }

    public void testCheckReservedPrefixedRealmNames() {
        final Settings.Builder builder = Settings.builder();
        final boolean invalidFileRealmName = randomBoolean();
        final boolean invalidNativeRealmName = randomBoolean();
        final boolean invalidOtherRealmName = (false == invalidFileRealmName && false == invalidNativeRealmName) || randomBoolean();

        final List<String> invalidRealmNames = new ArrayList<>();

        final String fileRealmName = randomAlphaOfLengthBetween(4, 12);
        if (invalidFileRealmName) {
            builder.put("xpack.security.authc.realms.file." + "_" + fileRealmName + ".order", -20);
            invalidRealmNames.add("xpack.security.authc.realms.file." + "_" + fileRealmName);
        } else {
            builder.put("xpack.security.authc.realms.file." + fileRealmName + ".order", -20);
        }

        final String nativeRealmName = randomAlphaOfLengthBetween(4, 12);
        if (invalidNativeRealmName) {
            builder.put("xpack.security.authc.realms.native." + "_" + nativeRealmName + ".order", -10);
            invalidRealmNames.add("xpack.security.authc.realms.native." + "_" + nativeRealmName);
        } else {
            builder.put("xpack.security.authc.realms.native." + nativeRealmName + ".order", -10);
        }

        final int otherRealmId = randomIntBetween(0, 9);
        final String otherRealmName = randomAlphaOfLengthBetween(4, 12);
        if (invalidOtherRealmName) {
            builder.put("xpack.security.authc.realms.type_" + otherRealmId + "." + "_" + otherRealmName + ".order", 0);
            invalidRealmNames.add("xpack.security.authc.realms.type_" + otherRealmId + "." + "_" + otherRealmName);
        } else {
            builder.put("xpack.security.authc.realms.type_" + otherRealmId + "." + otherRealmName + ".order", 0);
        }

        final Settings settings = builder.build();
        final List<DeprecationIssue> deprecationIssues = DeprecationChecks.filterChecks(NODE_SETTINGS_CHECKS, c -> c.apply(settings, null));

        assertEquals(1, deprecationIssues.size());

        final DeprecationIssue deprecationIssue = deprecationIssues.get(0);
        assertEquals("Realm that start with [_] will not be permitted in a future major release.", deprecationIssue.getMessage());
        assertEquals(
            "https://www.elastic.co/guide/en/elasticsearch/reference" + "/7.14/deprecated-7.14.html#reserved-prefixed-realm-names",
            deprecationIssue.getUrl());
        assertEquals(
            "Found realm " + (invalidRealmNames.size() == 1 ? "name" : "names")
                + " with reserved prefix [_]: ["
                + Strings.collectionToDelimitedString(invalidRealmNames.stream().sorted().collect(Collectors.toList()), "; ")
                + "]. " + "In a future major release, node will fail to start if any realm names start with reserved prefix.",
            deprecationIssue.getDetails());
    }

    public void testSingleDataNodeWatermarkSetting() {
        Settings settings = Settings.builder()
            .put(DiskThresholdDecider.ENABLE_FOR_SINGLE_DATA_NODE.getKey(), true)
            .build();

        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(NODE_SETTINGS_CHECKS, c -> c.apply(settings, null));

        final String expectedUrl =
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.14/" +
                "breaking-changes-7.14.html#deprecate-single-data-node-watermark";
        assertThat(issues, hasItem(
            new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "setting [cluster.routing.allocation.disk.watermark.enable_for_single_data_node] is deprecated and" +
                    " will not be available in a future version",
                expectedUrl,
                "found [cluster.routing.allocation.disk.watermark.enable_for_single_data_node] configured." +
                    " Discontinue use of this setting.",
                null)));
    }
}
