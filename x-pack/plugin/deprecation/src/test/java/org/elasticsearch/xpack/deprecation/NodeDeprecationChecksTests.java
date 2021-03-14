/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.bootstrap.JavaVersion;
import org.elasticsearch.common.collect.Set;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.Node;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class NodeDeprecationChecksTests extends ESTestCase {

    public void testCheckDefaults() {
        final Settings settings = Settings.EMPTY;
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules);
        assertThat(issues, empty());
    }

    public void testJavaVersion() {
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            DeprecationChecks.NODE_SETTINGS_CHECKS,
            c -> c.apply(Settings.EMPTY, pluginsAndModules)
        );

        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "Java 11 is required",
            "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-8.0.html#breaking_80_packaging_changes",
            "Java 11 will be required for future versions of Elasticsearch, this node is running version ["
                + JavaVersion.current().toString() + "]. Consider switching to a distribution of Elasticsearch with a bundled JDK. "
                + "If you are already using a distribution with a bundled JDK, ensure the JAVA_HOME environment variable is not set.");

        if (isJvmEarlierThan11()) {
            assertThat(issues, hasItem(expected));
        } else {
            assertThat(issues, not(hasItem(expected)));
        }
    }

    public void testCheckPidfile() {
        final String pidfile = randomAlphaOfLength(16);
        final Settings settings = Settings.builder().put(Environment.PIDFILE_SETTING.getKey(), pidfile).build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "setting [pidfile] is deprecated in favor of setting [node.pidfile]",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.4/breaking-changes-7.4.html#deprecate-pidfile",
            "the setting [pidfile] is currently set to [" + pidfile + "], instead set [node.pidfile] to [" + pidfile + "]");
        assertThat(issues, hasItem(expected));
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{Environment.PIDFILE_SETTING});
    }

    public void testCheckProcessors() {
        final int processors = randomIntBetween(1, 4);
        final Settings settings = Settings.builder().put(EsExecutors.PROCESSORS_SETTING.getKey(), processors).build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "setting [processors] is deprecated in favor of setting [node.processors]",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.4/breaking-changes-7.4.html#deprecate-processors",
            "the setting [processors] is currently set to [" + processors + "], instead set [node.processors] to [" + processors + "]");
        assertThat(issues, hasItem(expected));
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{EsExecutors.PROCESSORS_SETTING});
    }

    public void testCheckMissingRealmOrders() {
        final RealmConfig.RealmIdentifier invalidRealm =
            new RealmConfig.RealmIdentifier(randomRealmTypeOtherThanFileOrNative(), randomAlphaOfLengthBetween(4, 12));
        final RealmConfig.RealmIdentifier validRealm =
            new RealmConfig.RealmIdentifier(randomRealmTypeOtherThanFileOrNative(), randomAlphaOfLengthBetween(4, 12));
        final Settings settings =
            Settings.builder()
                .put("xpack.security.authc.realms.file.default_file.enabled", false)
                .put("xpack.security.authc.realms.native.default_native.enabled", false)
                .put("xpack.security.authc.realms." + invalidRealm.getType() + "." + invalidRealm.getName() + ".enabled", "true")
                .put("xpack.security.authc.realms." + validRealm.getType() + "." + validRealm.getName() + ".order", randomInt())
                .build();

        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final List<DeprecationIssue> deprecationIssues = getDeprecationIssues(settings, pluginsAndModules);

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

    public void testRealmOrderIsNotRequiredIfRealmIsDisabled() {
        final RealmConfig.RealmIdentifier realmIdentifier =
            new RealmConfig.RealmIdentifier(randomAlphaOfLengthBetween(4, 12), randomAlphaOfLengthBetween(4, 12));
        final Settings settings =
            Settings.builder()
                .put("xpack.security.authc.realms." + realmIdentifier.getType() + "." + realmIdentifier.getName() + ".enabled", "false")
            .build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final List<DeprecationIssue> deprecationIssues = getDeprecationIssues(settings, pluginsAndModules);
        assertTrue(deprecationIssues.isEmpty());
    }

    public void testCheckUniqueRealmOrders() {
        final int order = randomInt(9999);

        final RealmConfig.RealmIdentifier invalidRealm1 =
            new RealmConfig.RealmIdentifier(randomRealmTypeOtherThanFileOrNative(), randomAlphaOfLengthBetween(4, 12));
        final RealmConfig.RealmIdentifier invalidRealm2 =
            new RealmConfig.RealmIdentifier(randomRealmTypeOtherThanFileOrNative(), randomAlphaOfLengthBetween(4, 12));
        final RealmConfig.RealmIdentifier validRealm =
            new RealmConfig.RealmIdentifier(randomRealmTypeOtherThanFileOrNative(), randomAlphaOfLengthBetween(4, 12));
        final Settings settings = Settings.builder()
            .put("xpack.security.authc.realms.file.default_file.enabled", false)
            .put("xpack.security.authc.realms.native.default_native.enabled", false)
            .put("xpack.security.authc.realms."
                + invalidRealm1.getType() + "." + invalidRealm1.getName() + ".order", order)
            .put("xpack.security.authc.realms."
                + invalidRealm2.getType() + "." + invalidRealm2.getName() + ".order", order)
            .put("xpack.security.authc.realms."
                + validRealm.getType() + "." + validRealm.getName() + ".order", order + 1)
            .build();

        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final List<DeprecationIssue> deprecationIssues = getDeprecationIssues(settings, pluginsAndModules);

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
            .put("xpack.security.authc.realms.file.default_file.enabled", false)
            .put("xpack.security.authc.realms.native.default_native.enabled", false)
            .put("xpack.security.authc.realms."
                + randomRealmTypeOtherThanFileOrNative() + "." + randomAlphaOfLengthBetween(4, 12) + ".order", order)
            .put("xpack.security.authc.realms."
                + randomRealmTypeOtherThanFileOrNative() + "." + randomAlphaOfLengthBetween(4, 12) + ".order", order + 1)
            .build();

        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final List<DeprecationIssue> deprecationIssues = getDeprecationIssues(settings, pluginsAndModules);

        assertTrue(deprecationIssues.isEmpty());
    }

    public void testCheckImplicitlyDisabledBasicRealms() {
        final Settings.Builder builder = Settings.builder();

        final boolean otherRealmConfigured = randomBoolean();
        final boolean otherRealmEnabled = randomBoolean();
        if (otherRealmConfigured) {
            final int otherRealmId = randomIntBetween(0, 9);
            final String otherRealmName = randomAlphaOfLengthBetween(4, 12);
            if (otherRealmEnabled) {
                builder.put("xpack.security.authc.realms.type_" + otherRealmId + ".realm_" + otherRealmName + ".order", 1);
            } else {
                builder.put("xpack.security.authc.realms.type_" + otherRealmId + ".realm_" + otherRealmName + ".enabled", false);
            }
        }
        final boolean fileRealmConfigured = randomBoolean();
        final boolean fileRealmEnabled = randomBoolean();
        if (fileRealmConfigured) {
            final String fileRealmName = randomAlphaOfLengthBetween(4, 12);
            // Configure file realm or explicitly disable it
            if (fileRealmEnabled) {
                builder.put("xpack.security.authc.realms.file." + fileRealmName + ".order", 10);
            } else {
                builder.put("xpack.security.authc.realms.file." + fileRealmName + ".enabled", false);
            }
        }
        final boolean nativeRealmConfigured = randomBoolean();
        final boolean nativeRealmEnabled = randomBoolean();
        if (nativeRealmConfigured) {
            final String nativeRealmName = randomAlphaOfLengthBetween(4, 12);
            // Configure native realm or explicitly disable it
            if (nativeRealmEnabled) {
                builder.put("xpack.security.authc.realms.native." + nativeRealmName + ".order", 20);
            } else {
                builder.put("xpack.security.authc.realms.native." + nativeRealmName + ".enabled", false);
            }
        }
        final Settings settings = builder.build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final List<DeprecationIssue> deprecationIssues = getDeprecationIssues(settings, pluginsAndModules);

        if (otherRealmConfigured && otherRealmEnabled) {
            if (false == fileRealmConfigured && false == nativeRealmConfigured) {
                assertCommonImplicitDisabledRealms(deprecationIssues);
                assertEquals("Found implicitly disabled basic realms: [file,native]. " +
                        "They are disabled because there are other explicitly configured realms." +
                        "In next major release, basic realms will always be enabled unless explicitly disabled.",
                    deprecationIssues.get(0).getDetails());
            } else if (false == fileRealmConfigured) {
                assertCommonImplicitDisabledRealms(deprecationIssues);
                assertEquals("Found implicitly disabled basic realm: [file]. " +
                        "It is disabled because there are other explicitly configured realms." +
                        "In next major release, basic realms will always be enabled unless explicitly disabled.",
                    deprecationIssues.get(0).getDetails());
            } else if (false == nativeRealmConfigured) {
                assertCommonImplicitDisabledRealms(deprecationIssues);
                assertEquals("Found implicitly disabled basic realm: [native]. " +
                        "It is disabled because there are other explicitly configured realms." +
                        "In next major release, basic realms will always be enabled unless explicitly disabled.",
                    deprecationIssues.get(0).getDetails());
            } else {
                assertTrue(deprecationIssues.isEmpty());
            }
        } else {
            if (false == fileRealmConfigured && false == nativeRealmConfigured) {
                assertTrue(deprecationIssues.isEmpty());
            } else if (false == fileRealmConfigured) {
                assertCommonImplicitDisabledRealms(deprecationIssues);
                if (nativeRealmEnabled) {
                    assertEquals("Found implicitly disabled basic realm: [file]. " +
                            "It is disabled because there are other explicitly configured realms." +
                            "In next major release, basic realms will always be enabled unless explicitly disabled.",
                        deprecationIssues.get(0).getDetails());
                } else {
                    assertEquals("Found explicitly disabled basic realm: [native]. " +
                            "But it will be enabled because no realm is configured or enabled. " +
                            "In next major release, explicitly disabled basic realms will remain disabled.",
                        deprecationIssues.get(0).getDetails());
                }
            } else if (false == nativeRealmConfigured) {
                assertCommonImplicitDisabledRealms(deprecationIssues);
                if (fileRealmEnabled) {
                    assertEquals("Found implicitly disabled basic realm: [native]. " +
                            "It is disabled because there are other explicitly configured realms." +
                            "In next major release, basic realms will always be enabled unless explicitly disabled.",
                        deprecationIssues.get(0).getDetails());
                } else {
                    assertEquals("Found explicitly disabled basic realm: [file]. " +
                            "But it will be enabled because no realm is configured or enabled. " +
                            "In next major release, explicitly disabled basic realms will remain disabled.",
                        deprecationIssues.get(0).getDetails());
                }
            } else {
                if (false == fileRealmEnabled && false == nativeRealmEnabled) {
                    assertCommonImplicitDisabledRealms(deprecationIssues);
                    assertEquals("Found explicitly disabled basic realms: [file,native]. " +
                            "But they will be enabled because no realm is configured or enabled. " +
                            "In next major release, explicitly disabled basic realms will remain disabled.",
                        deprecationIssues.get(0).getDetails());
                }
            }
        }
    }

    public void testThreadPoolListenerQueueSize() {
        final int size = randomIntBetween(1, 4);
        final Settings settings = Settings.builder().put("thread_pool.listener.queue_size", size).build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "setting [thread_pool.listener.queue_size] is deprecated and will be removed in the next major version",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.x/breaking-changes-7.7.html#deprecate-listener-thread-pool",
            "the setting [thread_pool.listener.queue_size] is currently set to [" + size + "], remove this setting");
        assertThat(issues, hasItem(expected));
        assertSettingDeprecationsAndWarnings(new String[]{"thread_pool.listener.queue_size"});
    }

    public void testThreadPoolListenerSize() {
        final int size = randomIntBetween(1, 4);
        final Settings settings = Settings.builder().put("thread_pool.listener.size", size).build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "setting [thread_pool.listener.size] is deprecated and will be removed in the next major version",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.x/breaking-changes-7.7.html#deprecate-listener-thread-pool",
            "the setting [thread_pool.listener.size] is currently set to [" + size + "], remove this setting");
        assertThat(issues, hasItem(expected));
        assertSettingDeprecationsAndWarnings(new String[]{"thread_pool.listener.size"});
    }

    public void testGeneralScriptSizeSetting() {
        final int size = randomIntBetween(1, 4);
        final Settings settings = Settings.builder().put("script.cache.max_size", size).build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "setting [script.cache.max_size] is deprecated in favor of grouped setting [script.context.*.cache_max_size]",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.9/breaking-changes-7.9.html#deprecate_general_script_cache_size",
            "the setting [script.cache.max_size] is currently set to [" + size + "], instead set [script.context.*.cache_max_size] " +
                "to [" + size + "] where * is a script context");
        assertThat(issues, hasItem(expected));
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{ScriptService.SCRIPT_GENERAL_CACHE_SIZE_SETTING});
    }

    public void testGeneralScriptExpireSetting() {
        final String expire = randomIntBetween(1, 4) + "m";
        final Settings settings = Settings.builder().put("script.cache.expire", expire).build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "setting [script.cache.expire] is deprecated in favor of grouped setting [script.context.*.cache_expire]",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.9/breaking-changes-7.9.html#deprecate_general_script_expire",
            "the setting [script.cache.expire] is currently set to [" + expire + "], instead set [script.context.*.cache_expire] to " +
                "[" + expire + "] where * is a script context");
        assertThat(issues, hasItem(expected));
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{ScriptService.SCRIPT_GENERAL_CACHE_EXPIRE_SETTING});
    }

    public void testGeneralScriptCompileSettings() {
        final String rate = randomIntBetween(1, 100) + "/" + randomIntBetween(1, 200) + "m";
        final Settings settings = Settings.builder().put("script.max_compilations_rate", rate).build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "setting [script.max_compilations_rate] is deprecated in favor of grouped setting [script.context.*.max_compilations_rate]",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.9/breaking-changes-7.9.html#deprecate_general_script_compile_rate",
            "the setting [script.max_compilations_rate] is currently set to [" + rate +
                "], instead set [script.context.*.max_compilations_rate] to [" + rate + "] where * is a script context");
        assertThat(issues, hasItem(expected));
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{ScriptService.SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING});
    }

    public void testClusterRemoteConnectSetting() {
        final boolean value = randomBoolean();
        final Settings settings = Settings.builder().put(RemoteClusterService.ENABLE_REMOTE_CLUSTERS.getKey(), value).build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "setting [cluster.remote.connect] is deprecated in favor of setting [node.remote_cluster_client]",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.7/breaking-changes-7.7.html#deprecate-cluster-remote-connect",
            String.format(
                Locale.ROOT,
                "the setting [%s] is currently set to [%b], instead set [%s] to [%2$b]",
                RemoteClusterService.ENABLE_REMOTE_CLUSTERS.getKey(),
                value,
                "node.remote_cluster_client"
            ));
        assertThat(issues, hasItem(expected));
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{RemoteClusterService.ENABLE_REMOTE_CLUSTERS});
    }

    public void testNodeLocalStorageSetting() {
        final boolean value = randomBoolean();
        final Settings settings = Settings.builder().put(Node.NODE_LOCAL_STORAGE_SETTING.getKey(), value).build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "setting [node.local_storage] is deprecated and will be removed in the next major version",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.8/breaking-changes-7.8.html#deprecate-node-local-storage",
            "the setting [node.local_storage] is currently set to [" + value + "], remove this setting"
        );
        assertThat(issues, hasItem(expected));
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{Node.NODE_LOCAL_STORAGE_SETTING});
    }

    public void testDeprecatedBasicLicenseSettings() {
        Collection<Setting<Boolean>> deprecatedXpackSettings = Set.of(
            XPackSettings.ENRICH_ENABLED_SETTING,
            XPackSettings.FLATTENED_ENABLED,
            XPackSettings.INDEX_LIFECYCLE_ENABLED,
            XPackSettings.MONITORING_ENABLED,
            XPackSettings.ROLLUP_ENABLED,
            XPackSettings.SNAPSHOT_LIFECYCLE_ENABLED,
            XPackSettings.SQL_ENABLED,
            XPackSettings.TRANSFORM_ENABLED,
            XPackSettings.VECTORS_ENABLED
        );

        for (Setting<Boolean> deprecatedSetting : deprecatedXpackSettings) {
            final boolean value = randomBoolean();
            final Settings settings = Settings.builder().put(deprecatedSetting.getKey(), value).build();
            final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
            final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules);
            final DeprecationIssue expected = new DeprecationIssue(
                DeprecationIssue.Level.CRITICAL,
                "setting [" + deprecatedSetting.getKey() + "] is deprecated and will be removed in the next major version",
                "https://www.elastic.co/guide/en/elasticsearch/reference/7.8/breaking-changes-7.8.html" +
                    "#deprecate-basic-license-feature-enabled",
                "the setting [" + deprecatedSetting.getKey() + "] is currently set to [" + value + "], remove this setting"
            );
            assertThat(issues, hasItem(expected));
            assertSettingDeprecationsAndWarnings(new Setting<?>[]{deprecatedSetting});
        }
    }

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

    private static boolean isJvmEarlierThan11() {
        return JavaVersion.current().compareTo(JavaVersion.parse("11")) < 0;
    }

    private List<DeprecationIssue> getDeprecationIssues(Settings settings, PluginsAndModules pluginsAndModules) {
        final List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            DeprecationChecks.NODE_SETTINGS_CHECKS,
            c -> c.apply(settings, pluginsAndModules)
        );

        if (isJvmEarlierThan11()) {
            return issues.stream().filter(i -> i.getMessage().equals("Java 11 is required") == false).collect(Collectors.toList());
        }

        return issues;
    }

    private void assertCommonImplicitDisabledRealms(List<DeprecationIssue> deprecationIssues) {
        assertEquals(1, deprecationIssues.size());
        assertEquals("File and/or native realms are enabled by default in next major release.",
            deprecationIssues.get(0).getMessage());
        assertEquals("https://www.elastic.co/guide/en/elasticsearch/reference" +
                "/7.13/deprecated-7.13.html#implicitly-disabled-basic-realms",
            deprecationIssues.get(0).getUrl());
    }

    private String randomRealmTypeOtherThanFileOrNative() {
        return randomValueOtherThanMany(t -> org.elasticsearch.common.collect.Set.of("file", "native").contains(t),
            () -> randomAlphaOfLengthBetween(4, 12));
    }
}
