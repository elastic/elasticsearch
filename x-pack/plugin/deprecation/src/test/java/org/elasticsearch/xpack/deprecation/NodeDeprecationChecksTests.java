/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;

import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

public class NodeDeprecationChecksTests extends ESTestCase {

    public void testCheckDefaults() {
        final Settings settings = Settings.EMPTY;
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final List<DeprecationIssue> issues =
            DeprecationChecks.filterChecks(DeprecationChecks.NODE_SETTINGS_CHECKS, c -> c.apply(settings, pluginsAndModules));
        assertThat(issues, empty());
    }

    public void testCheckPidfile() {
        final String pidfile = randomAlphaOfLength(16);
        final Settings settings = Settings.builder().put(Environment.PIDFILE_SETTING.getKey(), pidfile).build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final List<DeprecationIssue> issues =
            DeprecationChecks.filterChecks(DeprecationChecks.NODE_SETTINGS_CHECKS, c -> c.apply(settings, pluginsAndModules));
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "setting [pidfile] is deprecated in favor of setting [node.pidfile]",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.4/breaking-changes-7.4.html#deprecate-pidfile",
            "the setting [pidfile] is currently set to [" + pidfile + "], instead set [node.pidfile] to [" + pidfile + "]");
        assertThat(issues, contains(expected));
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{Environment.PIDFILE_SETTING});
    }

    public void testCheckProcessors() {
        final int processors = randomIntBetween(1, 4);
        final Settings settings = Settings.builder().put(EsExecutors.PROCESSORS_SETTING.getKey(), processors).build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final List<DeprecationIssue> issues =
            DeprecationChecks.filterChecks(DeprecationChecks.NODE_SETTINGS_CHECKS, c -> c.apply(settings, pluginsAndModules));
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "setting [processors] is deprecated in favor of setting [node.processors]",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.4/breaking-changes-7.4.html#deprecate-processors",
            "the setting [processors] is currently set to [" + processors + "], instead set [node.processors] to [" + processors + "]");
        assertThat(issues, contains(expected));
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{EsExecutors.PROCESSORS_SETTING});
    }

    public void testCheckMissingRealmOrders() {
        final RealmConfig.RealmIdentifier invalidRealm =
            new RealmConfig.RealmIdentifier(randomAlphaOfLengthBetween(4, 12), randomAlphaOfLengthBetween(4, 12));
        final RealmConfig.RealmIdentifier validRealm =
            new RealmConfig.RealmIdentifier(randomAlphaOfLengthBetween(4, 12), randomAlphaOfLengthBetween(4, 12));
        final Settings settings =
            Settings.builder()
                .put("xpack.security.authc.realms." + invalidRealm.getType() + "." + invalidRealm.getName() + ".enabled", "true")
                .put("xpack.security.authc.realms." + validRealm.getType() + "." + validRealm.getName() + ".order", randomInt())
                .build();

        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final List<DeprecationIssue> deprecationIssues =
            DeprecationChecks.filterChecks(DeprecationChecks.NODE_SETTINGS_CHECKS, c -> c.apply(settings, pluginsAndModules));

        assertEquals(1, deprecationIssues.size());
        assertEquals(new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "Realm order will be required in next major release.",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.7/breaking-changes-7.7.html#deprecate-missing-realm-order",
            String.format(
                Locale.ROOT,
                "Found realms without order config: [%s]. In next major release, node will fail to start with missing realm order.",
                RealmSettings.realmSettingPrefix(invalidRealm) + RealmSettings.ORDER_SETTING_KEY
            )
        ), deprecationIssues.get(0));
    }

    public void testCheckUniqueRealmOrders() {
        final int order = randomInt(9999);

        final RealmConfig.RealmIdentifier invalidRealm1 =
            new RealmConfig.RealmIdentifier(randomAlphaOfLengthBetween(4, 12), randomAlphaOfLengthBetween(4, 12));
        final RealmConfig.RealmIdentifier invalidRealm2 =
            new RealmConfig.RealmIdentifier(randomAlphaOfLengthBetween(4, 12), randomAlphaOfLengthBetween(4, 12));
        final RealmConfig.RealmIdentifier validRealm =
            new RealmConfig.RealmIdentifier(randomAlphaOfLengthBetween(4, 12), randomAlphaOfLengthBetween(4, 12));
        final Settings settings = Settings.builder()
            .put("xpack.security.authc.realms."
                + invalidRealm1.getType() + "." + invalidRealm1.getName() + ".order", order)
            .put("xpack.security.authc.realms."
                + invalidRealm2.getType() + "." + invalidRealm2.getName() + ".order", order)
            .put("xpack.security.authc.realms."
                + validRealm.getType() + "." + validRealm.getName() + ".order", order + 1)
            .build();

        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final List<DeprecationIssue> deprecationIssues =
            DeprecationChecks.filterChecks(DeprecationChecks.NODE_SETTINGS_CHECKS, c -> c.apply(settings, pluginsAndModules));

        assertEquals(1, deprecationIssues.size());
        assertEquals(DeprecationIssue.Level.CRITICAL, deprecationIssues.get(0).getLevel());
        assertEquals(
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.7/breaking-changes-7.7.html#deprecate-duplicated-realm-orders",
            deprecationIssues.get(0).getUrl());
        assertEquals("Realm orders must be unique in next major release.", deprecationIssues.get(0).getMessage());
        assertThat(deprecationIssues.get(0).getDetails(), startsWith("Found multiple realms configured with the same order:"));
        assertThat(deprecationIssues.get(0).getDetails(), containsString(invalidRealm1.getType() + "." + invalidRealm1.getName()));
        assertThat(deprecationIssues.get(0).getDetails(), containsString(invalidRealm2.getType() + "." + invalidRealm2.getName()));
        assertThat(deprecationIssues.get(0).getDetails(), not(containsString(validRealm.getType() + "." + validRealm.getName())));
    }

    public void testCorrectRealmOrders() {
        final int order = randomInt(9999);
        final Settings settings = Settings.builder()
            .put("xpack.security.authc.realms."
                + randomAlphaOfLengthBetween(4, 12) + "." + randomAlphaOfLengthBetween(4, 12) + ".order", order)
            .put("xpack.security.authc.realms."
                + randomAlphaOfLengthBetween(4, 12) + "." + randomAlphaOfLengthBetween(4, 12) + ".order", order + 1)
            .build();

        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final List<DeprecationIssue> deprecationIssues =
            DeprecationChecks.filterChecks(DeprecationChecks.NODE_SETTINGS_CHECKS, c -> c.apply(settings, pluginsAndModules));

        assertEquals(0, deprecationIssues.size());
    }
}
