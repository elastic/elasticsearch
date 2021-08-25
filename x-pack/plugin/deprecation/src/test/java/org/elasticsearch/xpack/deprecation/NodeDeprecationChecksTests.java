/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.bootstrap.BootstrapSettings;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Set;
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.jdk.JavaVersion;
import org.elasticsearch.node.Node;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.coordination.JoinHelper.JOIN_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_INCLUDE_RELOCATIONS_SETTING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NodeDeprecationChecksTests extends ESTestCase {

    public void testCheckDefaults() {
        final Settings settings = Settings.EMPTY;
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final XPackLicenseState licenseState = new XPackLicenseState(settings, () -> 0);
        final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules, licenseState);

        final DeprecationIssue issue =
            NodeDeprecationChecks.checkImplicitlyDisabledSecurityOnBasicAndTrial(settings, pluginsAndModules, ClusterState.EMPTY_STATE,
                                                                                 licenseState);
        assertThat(issues, hasItem(issue));
    }

    public void testJavaVersion() {
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            DeprecationChecks.NODE_SETTINGS_CHECKS,
            c -> c.apply(Settings.EMPTY, pluginsAndModules, ClusterState.EMPTY_STATE, licenseState)
        );

        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "Java 11 is required",
            "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-8.0.html#breaking_80_packaging_changes",
            "Java 11 will be required for future versions of Elasticsearch, this node is running version ["
                + JavaVersion.current().toString() + "]. Consider switching to a distribution of Elasticsearch with a bundled JDK. "
                + "If you are already using a distribution with a bundled JDK, ensure the JAVA_HOME environment variable is not set.",
            false,
            null);

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
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules, licenseState);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "setting [pidfile] is deprecated in favor of setting [node.pidfile]",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.4/breaking-changes-7.4.html#deprecate-pidfile",
            "the setting [pidfile] is currently set to [" + pidfile + "], instead set [node.pidfile] to [" + pidfile + "]", false, null);
        assertThat(issues, hasItem(expected));
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{Environment.PIDFILE_SETTING});
    }

    public void testCheckProcessors() {
        final int processors = randomIntBetween(1, 4);
        final Settings settings = Settings.builder().put(EsExecutors.PROCESSORS_SETTING.getKey(), processors).build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules, licenseState);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "setting [processors] is deprecated in favor of setting [node.processors]",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.4/breaking-changes-7.4.html#deprecate-processors",
            "the setting [processors] is currently set to [" + processors + "], instead set [node.processors] to [" + processors + "]",
            false, null);
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
                .put("xpack.security.enabled", true)
                .put("xpack.security.authc.realms.file.default_file.enabled", false)
                .put("xpack.security.authc.realms.native.default_native.enabled", false)
                .put("xpack.security.authc.realms." + invalidRealm.getType() + "." + invalidRealm.getName() + ".enabled", "true")
                .put("xpack.security.authc.realms." + validRealm.getType() + "." + validRealm.getName() + ".order", randomInt())
                .build();

        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final XPackLicenseState licenseState = new XPackLicenseState(settings, () -> 0);
        final List<DeprecationIssue> deprecationIssues = getDeprecationIssues(settings, pluginsAndModules, licenseState);

        assertEquals(1, deprecationIssues.size());
        assertEquals(new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "Realm order will be required in next major release.",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.7/breaking-changes-7.7.html#deprecate-missing-realm-order",
            String.format(
                Locale.ROOT,
                "Found realms without order config: [%s]. In next major release, node will fail to start with missing realm order.",
                RealmSettings.realmSettingPrefix(invalidRealm) + RealmSettings.ORDER_SETTING_KEY
            ),
            false,
            null
        ), deprecationIssues.get(0));
    }

    public void testRealmOrderIsNotRequiredIfRealmIsDisabled() {
        final RealmConfig.RealmIdentifier realmIdentifier =
            new RealmConfig.RealmIdentifier(randomAlphaOfLengthBetween(4, 12), randomAlphaOfLengthBetween(4, 12));
        final Settings settings =
            Settings.builder()
                .put("xpack.security.enabled", true)
                .put("xpack.security.authc.realms." + realmIdentifier.getType() + "." + realmIdentifier.getName() + ".enabled", "false")
                .build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final XPackLicenseState licenseState =
            new XPackLicenseState(settings, () -> 0);
        final List<DeprecationIssue> deprecationIssues = getDeprecationIssues(settings, pluginsAndModules, licenseState);
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
            .put("xpack.security.enabled", true)
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
        final XPackLicenseState licenseState = new XPackLicenseState(settings, () -> 0);
        final List<DeprecationIssue> deprecationIssues = getDeprecationIssues(settings, pluginsAndModules, licenseState);

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
            .put("xpack.security.enabled", true)
            .put("xpack.security.authc.realms.file.default_file.enabled", false)
            .put("xpack.security.authc.realms.native.default_native.enabled", false)
            .put("xpack.security.authc.realms."
                + randomRealmTypeOtherThanFileOrNative() + "." + randomAlphaOfLengthBetween(4, 12) + ".order", order)
            .put("xpack.security.authc.realms."
                + randomRealmTypeOtherThanFileOrNative() + "." + randomAlphaOfLengthBetween(4, 12) + ".order", order + 1)
            .build();

        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final XPackLicenseState licenseState =
            new XPackLicenseState(settings, () -> 0);
        final List<DeprecationIssue> deprecationIssues = getDeprecationIssues(settings, pluginsAndModules, licenseState);

        assertTrue(deprecationIssues.isEmpty());
    }

    public void testCheckImplicitlyDisabledBasicRealms() {
        final Settings.Builder builder = Settings.builder();
        builder.put("xpack.security.enabled", true);
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
        final XPackLicenseState licenseState =
            new XPackLicenseState(settings, () -> 0);
        final List<DeprecationIssue> deprecationIssues = getDeprecationIssues(settings, pluginsAndModules, licenseState);

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
                            "But it will be enabled because no other realms are configured or enabled. " +
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
                            "But it will be enabled because no other realms are configured or enabled. " +
                            "In next major release, explicitly disabled basic realms will remain disabled.",
                        deprecationIssues.get(0).getDetails());
                }
            } else {
                if (false == fileRealmEnabled && false == nativeRealmEnabled) {
                    assertCommonImplicitDisabledRealms(deprecationIssues);
                    assertEquals("Found explicitly disabled basic realms: [file,native]. " +
                            "But they will be enabled because no other realms are configured or enabled. " +
                            "In next major release, explicitly disabled basic realms will remain disabled.",
                        deprecationIssues.get(0).getDetails());
                }
            }
        }
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

        final Settings settings = builder.put(XPackSettings.SECURITY_ENABLED.getKey(), true).build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final XPackLicenseState licenseState = new XPackLicenseState(settings, () -> 0);
        final List<DeprecationIssue> deprecationIssues = getDeprecationIssues(settings, pluginsAndModules, licenseState);

        assertEquals(1, deprecationIssues.size());

        final DeprecationIssue deprecationIssue = deprecationIssues.get(0);
        assertEquals("Realm names cannot start with [_] in a future major release.", deprecationIssue.getMessage());
        assertEquals("https://www.elastic.co/guide/en/elasticsearch/reference" +
            "/7.14/deprecated-7.14.html#reserved-prefixed-realm-names", deprecationIssue.getUrl());
        assertEquals("Found realm " + (invalidRealmNames.size() == 1 ? "name" : "names")
                + " with reserved prefix [_]: ["
                + Strings.collectionToDelimitedString(invalidRealmNames.stream().sorted().collect(Collectors.toList()), "; ") + "]. "
                + "In a future major release, node will fail to start if any realm names start with reserved prefix.",
            deprecationIssue.getDetails());
    }

    public void testThreadPoolListenerQueueSize() {
        final int size = randomIntBetween(1, 4);
        final Settings settings = Settings.builder().put("thread_pool.listener.queue_size", size).build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules, licenseState);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "setting [thread_pool.listener.queue_size] is deprecated and will be removed in the next major version",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.x/breaking-changes-7.7.html#deprecate-listener-thread-pool",
            "the setting [thread_pool.listener.queue_size] is currently set to [" + size + "], remove this setting", false, null);
        assertThat(issues, hasItem(expected));
        assertSettingDeprecationsAndWarnings(new String[]{"thread_pool.listener.queue_size"});
    }

    public void testThreadPoolListenerSize() {
        final int size = randomIntBetween(1, 4);
        final Settings settings = Settings.builder().put("thread_pool.listener.size", size).build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules, licenseState);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "setting [thread_pool.listener.size] is deprecated and will be removed in the next major version",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.x/breaking-changes-7.7.html#deprecate-listener-thread-pool",
            "the setting [thread_pool.listener.size] is currently set to [" + size + "], remove this setting", false, null);
        assertThat(issues, hasItem(expected));
        assertSettingDeprecationsAndWarnings(new String[]{"thread_pool.listener.size"});
    }

    public void testGeneralScriptSizeSetting() {
        final int size = randomIntBetween(1, 4);
        final Settings settings = Settings.builder().put("script.cache.max_size", size).build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules, licenseState);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "setting [script.cache.max_size] is deprecated in favor of grouped setting [script.context.*.cache_max_size]",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.9/breaking-changes-7.9.html#deprecate_general_script_cache_size",
            "the setting [script.cache.max_size] is currently set to [" + size + "], instead set [script.context.*.cache_max_size] " +
                "to [" + size + "] where * is a script context", false, null);
        assertThat(issues, hasItem(expected));
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{ScriptService.SCRIPT_GENERAL_CACHE_SIZE_SETTING});
    }

    public void testGeneralScriptExpireSetting() {
        final String expire = randomIntBetween(1, 4) + "m";
        final Settings settings = Settings.builder().put("script.cache.expire", expire).build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules, licenseState);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "setting [script.cache.expire] is deprecated in favor of grouped setting [script.context.*.cache_expire]",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.9/breaking-changes-7.9.html#deprecate_general_script_expire",
            "the setting [script.cache.expire] is currently set to [" + expire + "], instead set [script.context.*.cache_expire] to " +
                "[" + expire + "] where * is a script context", false, null);
        assertThat(issues, hasItem(expected));
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{ScriptService.SCRIPT_GENERAL_CACHE_EXPIRE_SETTING});
    }

    public void testGeneralScriptCompileSettings() {
        final String rate = randomIntBetween(1, 100) + "/" + randomIntBetween(1, 200) + "m";
        final Settings settings = Settings.builder().put("script.max_compilations_rate", rate).build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules, licenseState);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "setting [script.max_compilations_rate] is deprecated in favor of grouped setting [script.context.*.max_compilations_rate]",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.9/breaking-changes-7.9.html#deprecate_general_script_compile_rate",
            "the setting [script.max_compilations_rate] is currently set to [" + rate +
                "], instead set [script.context.*.max_compilations_rate] to [" + rate + "] where * is a script context", false, null);
        assertThat(issues, hasItem(expected));
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{ScriptService.SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING});
    }

    public void testClusterRemoteConnectSetting() {
        final boolean value = randomBoolean();
        final Settings settings = Settings.builder().put(RemoteClusterService.ENABLE_REMOTE_CLUSTERS.getKey(), value).build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules, licenseState);
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
            ), false, null);
        assertThat(issues, hasItem(expected));
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{RemoteClusterService.ENABLE_REMOTE_CLUSTERS});
    }

    public void testNodeLocalStorageSetting() {
        final boolean value = randomBoolean();
        final Settings settings = Settings.builder().put(Node.NODE_LOCAL_STORAGE_SETTING.getKey(), value).build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules, licenseState);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "setting [node.local_storage] is deprecated and will be removed in the next major version",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.8/breaking-changes-7.8.html#deprecate-node-local-storage",
            "the setting [node.local_storage] is currently set to [" + value + "], remove this setting", false, null
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
            final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
            final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules, licenseState);
            final DeprecationIssue expected = new DeprecationIssue(
                DeprecationIssue.Level.CRITICAL,
                "setting [" + deprecatedSetting.getKey() + "] is deprecated and will be removed in the next major version",
                "https://www.elastic.co/guide/en/elasticsearch/reference/7.8/breaking-changes-7.8.html" +
                    "#deprecate-basic-license-feature-enabled",
                "the setting [" + deprecatedSetting.getKey() + "] is currently set to [" + value + "], remove this setting", false, null
            );
            assertThat(issues, hasItem(expected));
            assertSettingDeprecationsAndWarnings(new Setting<?>[]{deprecatedSetting});
        }
    }

    public void testLegacyRoleSettings() {
        final Collection<Setting<Boolean>> legacyRoleSettings = DiscoveryNode.getPossibleRoles()
            .stream()
            .filter(s -> s.legacySetting() != null)
            .map(DiscoveryNodeRole::legacySetting).collect(Collectors.toList());
        for (final Setting<Boolean> legacyRoleSetting : legacyRoleSettings) {
            final boolean value = randomBoolean();
            final Settings settings = Settings.builder().put(legacyRoleSetting.getKey(), value).build();
            final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
            final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
            final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules, licenseState);
            final String roles = DiscoveryNode.getRolesFromSettings(settings)
                .stream()
                .map(DiscoveryNodeRole::roleName)
                .collect(Collectors.joining(","));
            final DeprecationIssue expected = new DeprecationIssue(
                DeprecationIssue.Level.CRITICAL,
                "setting [" + legacyRoleSetting.getKey() + "] is deprecated in favor of setting [node.roles]",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-8.0.html#breaking_80_settings_changes",
                "the setting [" + legacyRoleSetting.getKey() + "] is currently set to ["
                    + value + "], instead set [node.roles] to [" + roles + "]", false, null
            );
            assertThat(issues, hasItem(expected));
            assertSettingDeprecationsAndWarnings(new Setting<?>[]{legacyRoleSetting});
        }
    }

    public void testCheckBootstrapSystemCallFilterSetting() {
        final boolean boostrapSystemCallFilter = randomBoolean();
        final Settings settings =
            Settings.builder().put(BootstrapSettings.SYSTEM_CALL_FILTER_SETTING.getKey(), boostrapSystemCallFilter).build();
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final PluginsAndModules pluginsAndModules =
            new PluginsAndModules(org.elasticsearch.core.List.of(), org.elasticsearch.core.List.of());
        final List<DeprecationIssue> issues =
            DeprecationChecks.filterChecks(DeprecationChecks.NODE_SETTINGS_CHECKS,
                c -> c.apply(settings, pluginsAndModules, ClusterState.EMPTY_STATE, licenseState));
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "setting [bootstrap.system_call_filter] is deprecated and will be removed in the next major version",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.13/breaking-changes-7.13.html#deprecate-system-call-filter-setting",
            "the setting [bootstrap.system_call_filter] is currently set to [" + boostrapSystemCallFilter + "], remove this setting",
            false, null);
        assertThat(issues, hasItem(expected));
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{BootstrapSettings.SYSTEM_CALL_FILTER_SETTING});
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

    private List<DeprecationIssue> getDeprecationIssues(Settings settings, PluginsAndModules pluginsAndModules,
                                                        XPackLicenseState licenseState) {
        final List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            DeprecationChecks.NODE_SETTINGS_CHECKS,
            c -> c.apply(settings, pluginsAndModules, ClusterState.EMPTY_STATE, licenseState)
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
        return randomValueOtherThanMany(t -> Set.of("file", "native").contains(t),
            () -> randomAlphaOfLengthBetween(4, 12));
    }

    public void testMultipleDataPaths() {
        final Settings settings = Settings.builder().putList("path.data", Arrays.asList("d1", "d2")).build();
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final DeprecationIssue issue = NodeDeprecationChecks.checkMultipleDataPaths(settings, null, null, licenseState);
        assertThat(issue, not(nullValue()));
        assertThat(issue.getLevel(), equalTo(DeprecationIssue.Level.CRITICAL));
        assertThat(
            issue.getMessage(),
            equalTo("multiple [path.data] entries are deprecated, use a single data directory"));
        assertThat(
            issue.getDetails(),
            equalTo("Multiple data paths are deprecated. Instead, use RAID or other system level features to utilize multiple disks."));
        String url =
            "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-8.0.html#breaking_80_packaging_changes";
        assertThat(issue.getUrl(), equalTo(url));
    }

    public void testNoMultipleDataPaths() {
        Settings settings = Settings.builder().put("path.data", "data").build();
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final DeprecationIssue issue = NodeDeprecationChecks.checkMultipleDataPaths(settings, null, null, licenseState);
        assertThat(issue, nullValue());
    }

    public void testDataPathsList() {
        final Settings settings = Settings.builder().putList("path.data", "d1").build();
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final DeprecationIssue issue = NodeDeprecationChecks.checkDataPathsList(settings, null, null, licenseState);
        assertThat(issue, not(nullValue()));
        assertThat(issue.getLevel(), equalTo(DeprecationIssue.Level.CRITICAL));
        assertThat(
            issue.getMessage(),
            equalTo("[path.data] in a list is deprecated, use a string value"));
        assertThat(
            issue.getDetails(),
            equalTo("Configuring [path.data] with a list is deprecated. Instead specify as a string value."));
        String url =
            "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-8.0.html#breaking_80_packaging_changes";
        assertThat(issue.getUrl(), equalTo(url));
    }

    public void testNoDataPathsListDefault() {
        final Settings settings = Settings.builder().build();
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final DeprecationIssue issue = NodeDeprecationChecks.checkDataPathsList(settings, null, null, licenseState);
        assertThat(issue, nullValue());
    }

    public void testSharedDataPathSetting() {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .put(Environment.PATH_SHARED_DATA_SETTING.getKey(), createTempDir()).build();
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        DeprecationIssue issue = NodeDeprecationChecks.checkSharedDataPathSetting(settings, null, null, licenseState);
        final String expectedUrl =
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.13/breaking-changes-7.13.html#deprecate-shared-data-path-setting";
        assertThat(issue, equalTo(
            new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "setting [path.shared_data] is deprecated and will be removed in a future version",
                expectedUrl,
                "Found shared data path configured. Discontinue use of this setting.",
                false, null)));
    }

    public void testSingleDataNodeWatermarkSettingExplicit() {
        Settings settings = Settings.builder()
            .put(DiskThresholdDecider.ENABLE_FOR_SINGLE_DATA_NODE.getKey(), false)
            .build();
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(DeprecationChecks.NODE_SETTINGS_CHECKS, c -> c.apply(settings,
            null, ClusterState.EMPTY_STATE, new XPackLicenseState(Settings.EMPTY, () -> 0)));

        final String expectedUrl =
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.14/" +
                "breaking-changes-7.14.html#deprecate-single-data-node-watermark";
        assertThat(issues, hasItem(
            new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "setting [cluster.routing.allocation.disk.watermark.enable_for_single_data_node=false] is deprecated and" +
                    " will not be available in a future version",
                expectedUrl,
                "found [cluster.routing.allocation.disk.watermark.enable_for_single_data_node] configured to false." +
                    " Discontinue use of this setting or set it to true.",
                false,
                null
            )));

        assertWarnings("setting [cluster.routing.allocation.disk.watermark.enable_for_single_data_node=false] is deprecated and" +
            " will not be available in a future version");
    }

    public void testSingleDataNodeWatermarkSettingDefault() {
        DiscoveryNode node1 = new DiscoveryNode(randomAlphaOfLength(5), buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode node2 = new DiscoveryNode(randomAlphaOfLength(5), buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode master = new DiscoveryNode(randomAlphaOfLength(6), buildNewFakeTransportAddress(), Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.MASTER_ROLE),
            Version.CURRENT);
        ClusterStateCreationUtils.state(node1, node1, node1);
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final List<DeprecationIssue> issues = DeprecationChecks.filterChecks(DeprecationChecks.NODE_SETTINGS_CHECKS,
            c -> c.apply(Settings.EMPTY,
                null, ClusterStateCreationUtils.state(node1, node1, node1), licenseState));

        final String expectedUrl =
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.14/" +
                "breaking-changes-7.14.html#deprecate-single-data-node-watermark";
        DeprecationIssue deprecationIssue = new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
            "the default value [false] of setting [cluster.routing.allocation.disk.watermark.enable_for_single_data_node]" +
                " is deprecated and will be changed to true in a future version." +
                " This cluster has only one data node and behavior will therefore change when upgrading",
            expectedUrl,
            "found [cluster.routing.allocation.disk.watermark.enable_for_single_data_node] defaulting to false" +
                " on a single data node cluster. Set it to true to avoid this warning." +
                " Consider using [cluster.routing.allocation.disk.threshold_enabled] to disable disk based allocation", false, null);

        assertThat(issues, hasItem(deprecationIssue));

        assertThat(NodeDeprecationChecks.checkSingleDataNodeWatermarkSetting(Settings.EMPTY, null, ClusterStateCreationUtils.state(master
            , master, master), licenseState),
            nullValue());

        assertThat(NodeDeprecationChecks.checkSingleDataNodeWatermarkSetting(Settings.EMPTY, null, ClusterStateCreationUtils.state(node1,
            node1, node1, node2), licenseState),
            nullValue());

        assertThat(NodeDeprecationChecks.checkSingleDataNodeWatermarkSetting(Settings.EMPTY, null, ClusterStateCreationUtils.state(node1,
            master, node1, master), licenseState),
            equalTo(deprecationIssue));
    }

    public void testMonitoringExporterPassword() {
        // test for presence of deprecated exporter passwords
        final int numExporterPasswords = randomIntBetween(1, 3);
        final String[] exporterNames = new String[numExporterPasswords];
        final Settings.Builder b = Settings.builder();
        for (int k = 0; k < numExporterPasswords; k++) {
            exporterNames[k] = randomAlphaOfLength(5);
            b.put("xpack.monitoring.exporters." + exporterNames[k] + ".auth.password", "_pass");
        }
        final Settings settings = b.build();
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        DeprecationIssue issue = NodeDeprecationChecks.checkMonitoringExporterPassword(settings, null, null , licenseState);
        final String expectedUrl =
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.7/monitoring-settings.html#http-exporter-settings";
        final String joinedNames = Arrays
            .stream(exporterNames)
            .map(s -> "xpack.monitoring.exporters." + s + ".auth.password")
            .sorted()
            .collect(Collectors.joining(","));

        assertThat(issue, equalTo(new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            String.format(
                Locale.ROOT,
                "non-secure passwords for monitoring exporters [%s] are deprecated and will be removed in the next major version",
                joinedNames
            ),
            expectedUrl,
            String.format(
                Locale.ROOT,
                "replace the non-secure monitoring exporter password setting(s) [%s] with their secure 'auth.secure_password' replacement",
                joinedNames
            ), false, null)));

        // test for absence of deprecated exporter passwords
        issue = NodeDeprecationChecks.checkMonitoringExporterPassword(Settings.builder().build(), null, null, licenseState);
        assertThat(issue, nullValue());
    }

    public void testJoinTimeoutSetting() {
        String settingValue = randomTimeValue(1, 1000, new String[]{"d", "h", "ms", "s", "m"});
        String settingKey = JOIN_TIMEOUT_SETTING.getKey();
        final Settings nodeSettings = Settings.builder().put(settingKey, settingValue).build();
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final ClusterState clusterState = ClusterState.EMPTY_STATE;
        final DeprecationIssue expectedIssue = new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
            String.format(Locale.ROOT,
                "setting [%s] is deprecated and will be removed in the next major version",
                settingKey),
            "https://www.elastic.co/guide/en/elasticsearch/reference/master/migrating-8.0.html",
            String.format(Locale.ROOT,
                "the setting [%s] is currently set to [%s], remove this setting",
                settingKey,
                settingValue),
            false,null
        );

        assertThat(
            NodeDeprecationChecks.checkJoinTimeoutSetting(nodeSettings, null, clusterState, licenseState),
            equalTo(expectedIssue)
        );

        final String expectedWarning = String.format(Locale.ROOT,
            "[%s] setting was deprecated in Elasticsearch and will be removed in a future release! " +
                "See the breaking changes documentation for the next major version.",
            settingKey);

        assertWarnings(expectedWarning);
    }

    public void testClusterRoutingAllocationIncludeRelocationsSetting() {
        boolean settingValue = randomBoolean();
        String settingKey = CLUSTER_ROUTING_ALLOCATION_INCLUDE_RELOCATIONS_SETTING.getKey();
        final Settings nodeSettings = Settings.builder().put(settingKey, settingValue).build();
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final ClusterState clusterState = ClusterState.EMPTY_STATE;
        final DeprecationIssue expectedIssue = new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
            String.format(Locale.ROOT,
                "setting [%s] is deprecated and will be removed in the next major version",
                settingKey),
            "https://www.elastic.co/guide/en/elasticsearch/reference/master/migrating-8.0.html#breaking_80_allocation_changes",
            String.format(Locale.ROOT,
                "the setting [%s] is currently set to [%b], remove this setting",
                settingKey,
                settingValue),
            false,null
        );

        assertThat(
            NodeDeprecationChecks.checkClusterRoutingAllocationIncludeRelocationsSetting(nodeSettings, null, clusterState, licenseState),
            equalTo(expectedIssue)
        );

        final String expectedWarning = String.format(Locale.ROOT,
            "[%s] setting was deprecated in Elasticsearch and will be removed in a future release! " +
                "See the breaking changes documentation for the next major version.",
            settingKey);

        assertWarnings(expectedWarning);
    }
    public void testImplicitlyDisabledSecurityWarning() {
        final DeprecationIssue issue =
            NodeDeprecationChecks.checkImplicitlyDisabledSecurityOnBasicAndTrial(Settings.EMPTY,
                null,
                ClusterState.EMPTY_STATE,
                new XPackLicenseState(Settings.EMPTY, () -> 0));
        assertThat(issue.getLevel(), equalTo(DeprecationIssue.Level.CRITICAL));
        assertThat(issue.getMessage(), equalTo("Security is enabled by default for all licenses in the next major version."));
        assertNotNull(issue.getDetails());
        assertThat(issue.getDetails(), containsString("The default behavior of disabling security on "));
        assertThat(issue.getUrl(),
            equalTo("https://www.elastic.co/guide/en/elasticsearch/reference/7.14/migrating-7.14.html#implicitly-disabled-security"));
    }

    public void testExplicitlyConfiguredSecurityOnBasicAndTrial() {
        final boolean enabled = randomBoolean();
        final Settings settings = Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), enabled).build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final XPackLicenseState licenseState = mock(XPackLicenseState.class);
        when(licenseState.getOperationMode()).thenReturn(randomFrom(License.OperationMode.BASIC, License.OperationMode.TRIAL));
        final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules, licenseState);
        assertThat(issues, empty());
    }

    public void testImplicitlyConfiguredSecurityOnGoldPlus() {
        final boolean enabled = randomBoolean();
        final Settings settings = Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), enabled).build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final XPackLicenseState licenseState = mock(XPackLicenseState.class);
        when(licenseState.getOperationMode())
            .thenReturn(randomValueOtherThanMany((m -> m.equals(License.OperationMode.BASIC) || m.equals(License.OperationMode.TRIAL)),
                () -> randomFrom(License.OperationMode.values())));
        final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules, licenseState);
        assertThat(issues, empty());
    }
}
