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
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Set;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.jdk.JavaVersion;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.node.Node;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
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
import static org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider.INDEX_ROUTING_EXCLUDE_SETTING;
import static org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider.INDEX_ROUTING_INCLUDE_SETTING;
import static org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider.INDEX_ROUTING_REQUIRE_SETTING;
import static org.elasticsearch.xpack.deprecation.NodeDeprecationChecks.JAVA_DEPRECATION_MESSAGE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
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

        final DeprecationIssue issue = NodeDeprecationChecks.checkImplicitlyDisabledSecurityOnBasicAndTrial(
            settings,
            pluginsAndModules,
            ClusterState.EMPTY_STATE,
            licenseState
        );
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
            "Java 11 is required in 8.0",
            "https://ela.st/es-deprecation-7-java-version",
            "This node is running Java version ["
                + JavaVersion.current().toString()
                + "]. Consider switching to a distribution of "
                + "Elasticsearch with a bundled JDK or upgrade. If you are already using a distribution with a bundled JDK, ensure the "
                + "JAVA_HOME environment variable is not set.",
            false,
            null
        );

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
            "Setting [pidfile] is deprecated",
            "https://ela.st/es-deprecation-7-pidfile-setting",
            "Remove the [pidfile] setting and set [node.pidfile] to [" + pidfile + "].",
            false,
            null
        );
        assertThat(issues, hasItem(expected));
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { Environment.PIDFILE_SETTING });
    }

    public void testCheckProcessors() {
        final int processors = randomIntBetween(1, 4);
        final Settings settings = Settings.builder().put(EsExecutors.PROCESSORS_SETTING.getKey(), processors).build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules, licenseState);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "Setting [processors] is deprecated",
            "https://ela.st/es-deprecation-7-processors-setting",
            "Remove the [processors] setting and set [node.processors] to [" + processors + "].",
            false,
            null
        );
        assertThat(issues, hasItem(expected));
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { EsExecutors.PROCESSORS_SETTING });
    }

    public void testCheckMissingRealmOrders() {
        final RealmConfig.RealmIdentifier invalidRealm = new RealmConfig.RealmIdentifier(
            randomRealmTypeOtherThanFileOrNative(),
            randomAlphaOfLengthBetween(4, 12)
        );
        final RealmConfig.RealmIdentifier validRealm = new RealmConfig.RealmIdentifier(
            randomRealmTypeOtherThanFileOrNative(),
            randomAlphaOfLengthBetween(4, 12)
        );
        final Settings settings = Settings.builder()
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
        assertEquals(
            new DeprecationIssue(
                DeprecationIssue.Level.CRITICAL,
                "Realm order is required",
                "https://ela.st/es-deprecation-7-realm-orders-required",
                String.format(
                    Locale.ROOT,
                    "Specify the realm order for all realms [%s]. If no realm order is specified, the node will fail to start in 8.0. ",
                    RealmSettings.realmSettingPrefix(invalidRealm) + RealmSettings.ORDER_SETTING_KEY
                ),
                false,
                null
            ),
            deprecationIssues.get(0)
        );
    }

    public void testRealmOrderIsNotRequiredIfRealmIsDisabled() {
        final RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier(
            randomAlphaOfLengthBetween(4, 12),
            randomAlphaOfLengthBetween(4, 12)
        );
        final Settings settings = Settings.builder()
            .put("xpack.security.enabled", true)
            .put("xpack.security.authc.realms." + realmIdentifier.getType() + "." + realmIdentifier.getName() + ".enabled", "false")
            .build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final XPackLicenseState licenseState = new XPackLicenseState(settings, () -> 0);
        final List<DeprecationIssue> deprecationIssues = getDeprecationIssues(settings, pluginsAndModules, licenseState);
        assertTrue(deprecationIssues.isEmpty());
    }

    public void testCheckUniqueRealmOrders() {
        final int order = randomInt(9999);

        final RealmConfig.RealmIdentifier invalidRealm1 = new RealmConfig.RealmIdentifier(
            randomRealmTypeOtherThanFileOrNative(),
            randomAlphaOfLengthBetween(4, 12)
        );
        final RealmConfig.RealmIdentifier invalidRealm2 = new RealmConfig.RealmIdentifier(
            randomRealmTypeOtherThanFileOrNative(),
            randomAlphaOfLengthBetween(4, 12)
        );
        final RealmConfig.RealmIdentifier validRealm = new RealmConfig.RealmIdentifier(
            randomRealmTypeOtherThanFileOrNative(),
            randomAlphaOfLengthBetween(4, 12)
        );
        final Settings settings = Settings.builder()
            .put("xpack.security.enabled", true)
            .put("xpack.security.authc.realms.file.default_file.enabled", false)
            .put("xpack.security.authc.realms.native.default_native.enabled", false)
            .put("xpack.security.authc.realms." + invalidRealm1.getType() + "." + invalidRealm1.getName() + ".order", order)
            .put("xpack.security.authc.realms." + invalidRealm2.getType() + "." + invalidRealm2.getName() + ".order", order)
            .put("xpack.security.authc.realms." + validRealm.getType() + "." + validRealm.getName() + ".order", order + 1)
            .build();

        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final XPackLicenseState licenseState = new XPackLicenseState(settings, () -> 0);
        final List<DeprecationIssue> deprecationIssues = getDeprecationIssues(settings, pluginsAndModules, licenseState);

        assertEquals(1, deprecationIssues.size());
        assertEquals(DeprecationIssue.Level.CRITICAL, deprecationIssues.get(0).getLevel());
        assertEquals("https://ela.st/es-deprecation-7-realm-orders-unique", deprecationIssues.get(0).getUrl());
        assertEquals("Realm orders must be unique", deprecationIssues.get(0).getMessage());
        assertThat(deprecationIssues.get(0).getDetails(), startsWith("The same order is configured for multiple realms:"));
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
            .put(
                "xpack.security.authc.realms."
                    + randomRealmTypeOtherThanFileOrNative()
                    + "."
                    + randomAlphaOfLengthBetween(4, 12)
                    + ".order",
                order
            )
            .put(
                "xpack.security.authc.realms."
                    + randomRealmTypeOtherThanFileOrNative()
                    + "."
                    + randomAlphaOfLengthBetween(4, 12)
                    + ".order",
                order + 1
            )
            .build();

        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final XPackLicenseState licenseState = new XPackLicenseState(settings, () -> 0);
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
        final XPackLicenseState licenseState = new XPackLicenseState(settings, () -> 0);
        final List<DeprecationIssue> deprecationIssues = getDeprecationIssues(settings, pluginsAndModules, licenseState);

        if (otherRealmConfigured && otherRealmEnabled) {
            if (false == fileRealmConfigured && false == nativeRealmConfigured) {
                assertCommonImplicitDisabledRealms(deprecationIssues);
                assertEquals(
                    "Found implicitly disabled basic realms: [file,native]. "
                        + "They are disabled because there are other explicitly configured realms."
                        + "In next major release, basic realms will always be enabled unless explicitly disabled.",
                    deprecationIssues.get(0).getDetails()
                );
            } else if (false == fileRealmConfigured) {
                assertCommonImplicitDisabledRealms(deprecationIssues);
                assertEquals(
                    "Found implicitly disabled basic realm: [file]. "
                        + "It is disabled because there are other explicitly configured realms."
                        + "In next major release, basic realms will always be enabled unless explicitly disabled.",
                    deprecationIssues.get(0).getDetails()
                );
            } else if (false == nativeRealmConfigured) {
                assertCommonImplicitDisabledRealms(deprecationIssues);
                assertEquals(
                    "Found implicitly disabled basic realm: [native]. "
                        + "It is disabled because there are other explicitly configured realms."
                        + "In next major release, basic realms will always be enabled unless explicitly disabled.",
                    deprecationIssues.get(0).getDetails()
                );
            } else {
                assertTrue(deprecationIssues.isEmpty());
            }
        } else {
            if (false == fileRealmConfigured && false == nativeRealmConfigured) {
                assertTrue(deprecationIssues.isEmpty());
            } else if (false == fileRealmConfigured) {
                assertCommonImplicitDisabledRealms(deprecationIssues);
                if (nativeRealmEnabled) {
                    assertEquals(
                        "Found implicitly disabled basic realm: [file]. "
                            + "It is disabled because there are other explicitly configured realms."
                            + "In next major release, basic realms will always be enabled unless explicitly disabled.",
                        deprecationIssues.get(0).getDetails()
                    );
                } else {
                    assertEquals(
                        "Found explicitly disabled basic realm: [native]. "
                            + "But it will be enabled because no other realms are configured or enabled. "
                            + "In next major release, explicitly disabled basic realms will remain disabled.",
                        deprecationIssues.get(0).getDetails()
                    );
                }
            } else if (false == nativeRealmConfigured) {
                assertCommonImplicitDisabledRealms(deprecationIssues);
                if (fileRealmEnabled) {
                    assertEquals(
                        "Found implicitly disabled basic realm: [native]. "
                            + "It is disabled because there are other explicitly configured realms."
                            + "In next major release, basic realms will always be enabled unless explicitly disabled.",
                        deprecationIssues.get(0).getDetails()
                    );
                } else {
                    assertEquals(
                        "Found explicitly disabled basic realm: [file]. "
                            + "But it will be enabled because no other realms are configured or enabled. "
                            + "In next major release, explicitly disabled basic realms will remain disabled.",
                        deprecationIssues.get(0).getDetails()
                    );
                }
            } else {
                if (false == fileRealmEnabled && false == nativeRealmEnabled) {
                    assertCommonImplicitDisabledRealms(deprecationIssues);
                    assertEquals(
                        "Found explicitly disabled basic realms: [file,native]. "
                            + "But they will be enabled because no other realms are configured or enabled. "
                            + "In next major release, explicitly disabled basic realms will remain disabled.",
                        deprecationIssues.get(0).getDetails()
                    );
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
        assertEquals(DeprecationIssue.Level.WARNING, deprecationIssue.getLevel());
        assertEquals("Prefixing realm names with an underscore (_) is deprecated", deprecationIssue.getMessage());
        assertEquals("https://ela.st/es-deprecation-7-realm-names", deprecationIssue.getUrl());
        String expectedDetails = String.format(
            Locale.ROOT,
            "Rename the following realm%s in the realm chain: %s.",
            invalidRealmNames.size() > 1 ? "s" : "",
            Strings.collectionToDelimitedString(invalidRealmNames.stream().sorted().collect(Collectors.toList()), ", ")
        );
        assertEquals(expectedDetails, deprecationIssue.getDetails());
    }

    public void testThreadPoolListenerQueueSize() {
        final int size = randomIntBetween(1, 4);
        final Settings settings = Settings.builder().put("thread_pool.listener.queue_size", size).build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules, licenseState);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "Setting [thread_pool.listener.queue_size] is deprecated",
            "https://ela.st/es-deprecation-7-thread-pool-listener-settings",
            "Remove the [thread_pool.listener.queue_size] setting. The listener pool is no longer used in 8.0.",
            false,
            null
        );
        assertThat(issues, hasItem(expected));
        assertSettingDeprecationsAndWarnings(getDeprecatedSettingsForSettingNames("thread_pool.listener.queue_size"));
    }

    public void testThreadPoolListenerSize() {
        final int size = randomIntBetween(1, 4);
        final Settings settings = Settings.builder().put("thread_pool.listener.size", size).build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules, licenseState);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "Setting [thread_pool.listener.size] is deprecated",
            "https://ela.st/es-deprecation-7-thread-pool-listener-settings",
            "Remove the [thread_pool.listener.size] setting. The listener pool is no longer used in 8.0.",
            false,
            null
        );
        assertThat(issues, hasItem(expected));
        assertSettingDeprecationsAndWarnings(getDeprecatedSettingsForSettingNames("thread_pool.listener.size"));
    }

    private Setting<?>[] getDeprecatedSettingsForSettingNames(String... settingNames) {
        return Arrays.stream(settingNames)
            .map(settingName -> Setting.intSetting(settingName, randomInt(), Setting.Property.Deprecated))
            .toArray(Setting[]::new);
    }

    public void testClusterRemoteConnectSetting() {
        final boolean value = randomBoolean();
        final Settings settings = Settings.builder().put(RemoteClusterService.ENABLE_REMOTE_CLUSTERS.getKey(), value).build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules, licenseState);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "Setting [cluster.remote.connect] is deprecated",
            "https://ela.st/es-deprecation-7-cluster-remote-connect-setting",
            String.format(
                Locale.ROOT,
                "Remove the [%s] setting and set [node.remote_cluster_client] to [%b].",
                RemoteClusterService.ENABLE_REMOTE_CLUSTERS.getKey(),
                value
            ),
            false,
            null
        );
        assertThat(issues, hasItem(expected));
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { RemoteClusterService.ENABLE_REMOTE_CLUSTERS });
    }

    public void testNodeLocalStorageSetting() {
        final boolean value = randomBoolean();
        final Settings settings = Settings.builder().put(Node.NODE_LOCAL_STORAGE_SETTING.getKey(), value).build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules, licenseState);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "Setting [node.local_storage] is deprecated",
            "https://ela.st/es-deprecation-7-node-local-storage-setting",
            "Remove the [node.local_storage] setting. All nodes require local storage in 8.0 and cannot share data paths.",
            false,
            null
        );
        assertThat(issues, hasItem(expected));
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { Node.NODE_LOCAL_STORAGE_SETTING });
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
                "Setting [" + deprecatedSetting.getKey() + "] is deprecated",
                "https://ela.st/es-deprecation-7-xpack-basic-feature-settings",
                "Remove the [" + deprecatedSetting.getKey() + "] setting. Basic features are always enabled in 8.0.",
                false,
                null
            );
            assertThat(issues, hasItem(expected));
            assertSettingDeprecationsAndWarnings(new Setting<?>[] { deprecatedSetting });
        }
    }

    public void testLegacyRoleSettings() {
        final Collection<Setting<Boolean>> legacyRoleSettings = DiscoveryNode.getPossibleRoles()
            .stream()
            .filter(s -> s.legacySetting() != null)
            .map(DiscoveryNodeRole::legacySetting)
            .collect(Collectors.toList());
        for (final Setting<Boolean> legacyRoleSetting : legacyRoleSettings) {
            final boolean value = randomBoolean();
            final Settings settings = Settings.builder().put(legacyRoleSetting.getKey(), value).build();
            final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
            final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
            final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules, licenseState);
            final String role = legacyRoleSetting.getKey().substring(legacyRoleSetting.getKey().indexOf(".") + 1);
            final DeprecationIssue expected = new DeprecationIssue(
                DeprecationIssue.Level.CRITICAL,
                "Setting [" + legacyRoleSetting.getKey() + "] is deprecated",
                "https://ela.st/es-deprecation-7-node-roles",
                "Remove the [" + legacyRoleSetting.getKey() + "] setting. Set [node.roles] and include the [" + role + "] role.",
                false,
                null
            );
            assertThat(issues, hasItem(expected));
        }
    }

    public void testCheckBootstrapSystemCallFilterSetting() {
        final boolean boostrapSystemCallFilter = randomBoolean();
        final Settings settings = Settings.builder()
            .put(BootstrapSettings.SYSTEM_CALL_FILTER_SETTING.getKey(), boostrapSystemCallFilter)
            .build();
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(
            org.elasticsearch.core.List.of(),
            org.elasticsearch.core.List.of()
        );
        final List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            DeprecationChecks.NODE_SETTINGS_CHECKS,
            c -> c.apply(settings, pluginsAndModules, ClusterState.EMPTY_STATE, licenseState)
        );
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "Setting [bootstrap.system_call_filter] is deprecated",
            "https://ela.st/es-deprecation-7-system-call-filter-setting",
            "Remove the [bootstrap.system_call_filter] setting. System call filters are always required in 8.0.",
            false,
            null
        );
        assertThat(issues, hasItem(expected));
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { BootstrapSettings.SYSTEM_CALL_FILTER_SETTING });
    }

    public void testRemovedSettingNotSet() {
        final Settings settings = Settings.EMPTY;
        final Setting<?> removedSetting = Setting.simpleString("node.removed_setting");
        final DeprecationIssue issue = NodeDeprecationChecks.checkRemovedSetting(
            settings,
            removedSetting,
            "http://removed-setting.example.com",
            "Some detail."
        );
        assertThat(issue, nullValue());
    }

    public void testRemovedSetting() {
        final Settings settings = Settings.builder().put("node.removed_setting", "value").build();
        final Setting<?> removedSetting = Setting.simpleString("node.removed_setting");
        final DeprecationIssue issue = NodeDeprecationChecks.checkRemovedSetting(
            settings,
            removedSetting,
            "https://removed-setting.example.com",
            "Some detail."
        );
        assertThat(issue, not(nullValue()));
        assertThat(issue.getLevel(), equalTo(DeprecationIssue.Level.CRITICAL));
        assertThat(issue.getMessage(), equalTo("Setting [node.removed_setting] is deprecated"));
        assertThat(issue.getDetails(), equalTo("Remove the [node.removed_setting] setting. Some detail."));
        assertThat(issue.getUrl(), equalTo("https://removed-setting.example.com"));
    }

    private static boolean isJvmEarlierThan11() {
        return JavaVersion.current().compareTo(JavaVersion.parse("11")) < 0;
    }

    private List<DeprecationIssue> getDeprecationIssues(
        Settings settings,
        PluginsAndModules pluginsAndModules,
        XPackLicenseState licenseState
    ) {
        final List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            DeprecationChecks.NODE_SETTINGS_CHECKS,
            c -> c.apply(settings, pluginsAndModules, ClusterState.EMPTY_STATE, licenseState)
        );

        if (isJvmEarlierThan11()) {
            return issues.stream().filter(i -> i.getMessage().equals(JAVA_DEPRECATION_MESSAGE) == false).collect(Collectors.toList());
        }

        return issues;
    }

    private void assertCommonImplicitDisabledRealms(List<DeprecationIssue> deprecationIssues) {
        assertEquals(1, deprecationIssues.size());
        assertEquals("File and/or native realms are enabled by default in next major release.", deprecationIssues.get(0).getMessage());
        assertEquals("https://ela.st/es-deprecation-7-implicitly-disabled-basic-realms", deprecationIssues.get(0).getUrl());
    }

    private String randomRealmTypeOtherThanFileOrNative() {
        return randomValueOtherThanMany(t -> Set.of("file", "native").contains(t), () -> randomAlphaOfLengthBetween(4, 12));
    }

    public void testMultipleDataPaths() {
        final Settings settings = Settings.builder().putList("path.data", Arrays.asList("d1", "d2")).build();
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final DeprecationIssue issue = NodeDeprecationChecks.checkMultipleDataPaths(settings, null, null, licenseState);
        assertThat(issue, not(nullValue()));
        assertThat(issue.getLevel(), equalTo(DeprecationIssue.Level.CRITICAL));
        assertThat(issue.getMessage(), equalTo("multiple [path.data] entries are deprecated, use a single data directory"));
        assertThat(
            issue.getDetails(),
            equalTo("Multiple data paths are deprecated. Instead, use RAID or other system level features to utilize multiple disks.")
        );
        String url = "https://ela.st/es-deprecation-7-multiple-paths";
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
        assertThat(issue.getMessage(), equalTo("Multiple data paths are not supported"));
        assertThat(
            issue.getDetails(),
            equalTo(
                "The [path.data] setting contains a list of paths. Specify a single path as a string. Use RAID or other system level "
                    + "features to utilize multiple disks. If multiple data paths are configured, the node will fail to start in 8.0. "
            )
        );
        String url = "https://ela.st/es-deprecation-7-multiple-paths";
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
            .put(Environment.PATH_SHARED_DATA_SETTING.getKey(), createTempDir())
            .build();
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        DeprecationIssue issue = NodeDeprecationChecks.checkSharedDataPathSetting(settings, null, null, licenseState);
        final String expectedUrl = "https://ela.st/es-deprecation-7-shared-path-settings";
        assertThat(
            issue,
            equalTo(
                new DeprecationIssue(
                    DeprecationIssue.Level.CRITICAL,
                    "Setting [path.shared_data] is deprecated",
                    expectedUrl,
                    "Remove the [path.shared_data] setting. This setting has had no effect since 6.0.",
                    false,
                    null
                )
            )
        );
    }

    public void testSingleDataNodeWatermarkSettingExplicit() {
        Settings settings = Settings.builder().put(DiskThresholdDecider.ENABLE_FOR_SINGLE_DATA_NODE.getKey(), false).build();
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            DeprecationChecks.NODE_SETTINGS_CHECKS,
            c -> c.apply(settings, null, ClusterState.EMPTY_STATE, new XPackLicenseState(Settings.EMPTY, () -> 0))
        );

        final String expectedUrl = "https://ela.st/es-deprecation-7-disk-watermark-enable-for-single-node-setting";
        assertThat(
            issues,
            hasItem(
                new DeprecationIssue(
                    DeprecationIssue.Level.CRITICAL,
                    "Setting [cluster.routing.allocation.disk.watermark.enable_for_single_data_node=false] is deprecated",
                    expectedUrl,
                    "Remove the [cluster.routing.allocation.disk.watermark.enable_for_single_data_node] setting. Disk watermarks"
                        + " are always enabled for single node clusters in 8.0.",
                    false,
                    null
                )
            )
        );

        assertWarnings(
            "setting [cluster.routing.allocation.disk.watermark.enable_for_single_data_node=false] is deprecated and"
                + " will not be available in a future version"
        );
    }

    public void testSingleDataNodeWatermarkSettingDefault() {
        DiscoveryNode node1 = new DiscoveryNode(randomAlphaOfLength(5), buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode node2 = new DiscoveryNode(randomAlphaOfLength(5), buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode master = new DiscoveryNode(
            randomAlphaOfLength(6),
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.MASTER_ROLE),
            Version.CURRENT
        );
        ClusterStateCreationUtils.state(node1, node1, node1);
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            DeprecationChecks.NODE_SETTINGS_CHECKS,
            c -> c.apply(Settings.EMPTY, null, ClusterStateCreationUtils.state(node1, node1, node1), licenseState)
        );

        final String expectedUrl = "https://ela.st/es-deprecation-7-disk-watermark-enable-for-single-node-setting";
        DeprecationIssue deprecationIssue = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "Disabling disk watermarks for single node clusters is deprecated and no longer the default",
            expectedUrl,
            "Disk watermarks are always enabled in 8.0, which will affect the behavior of this single node cluster when you upgrade. You "
                + "can set \"cluster.routing.allocation.disk.threshold_enabled\" to false to disable disk based allocation.",
            false,
            null
        );

        assertThat(issues, hasItem(deprecationIssue));

        assertThat(
            NodeDeprecationChecks.checkSingleDataNodeWatermarkSetting(
                Settings.EMPTY,
                null,
                ClusterStateCreationUtils.state(master, master, master),
                licenseState
            ),
            nullValue()
        );

        assertThat(
            NodeDeprecationChecks.checkSingleDataNodeWatermarkSetting(
                Settings.EMPTY,
                null,
                ClusterStateCreationUtils.state(node1, node1, node1, node2),
                licenseState
            ),
            nullValue()
        );

        assertThat(
            NodeDeprecationChecks.checkSingleDataNodeWatermarkSetting(
                Settings.EMPTY,
                null,
                ClusterStateCreationUtils.state(node1, master, node1, master),
                licenseState
            ),
            equalTo(deprecationIssue)
        );
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
        DeprecationIssue issue = NodeDeprecationChecks.checkMonitoringExporterPassword(settings, null, null, licenseState);
        final String expectedUrl = "https://ela.st/es-deprecation-7-monitoring-exporter-passwords";
        final String joinedNames = Arrays.stream(exporterNames)
            .map(s -> "xpack.monitoring.exporters." + s + ".auth.password")
            .sorted()
            .collect(Collectors.joining(","));

        assertThat(
            issue,
            equalTo(
                new DeprecationIssue(
                    DeprecationIssue.Level.CRITICAL,
                    String.format(Locale.ROOT, "Monitoring exporters must use secure passwords", joinedNames),
                    expectedUrl,
                    String.format(
                        Locale.ROOT,
                        "Remove the non-secure monitoring exporter password settings: "
                            + "[%s]. Configure secure passwords with [xpack.monitoring.exporters.*.auth.secure_password].",
                        joinedNames
                    ),
                    false,
                    null
                )
            )
        );

        // test for absence of deprecated exporter passwords
        issue = NodeDeprecationChecks.checkMonitoringExporterPassword(Settings.builder().build(), null, null, licenseState);
        assertThat(issue, nullValue());
    }

    public void testJoinTimeoutSetting() {
        String settingValue = randomTimeValue(1, 1000, new String[] { "d", "h", "ms", "s", "m" });
        String settingKey = JOIN_TIMEOUT_SETTING.getKey();
        final Settings nodeSettings = Settings.builder().put(settingKey, settingValue).build();
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final ClusterState clusterState = ClusterState.EMPTY_STATE;
        final DeprecationIssue expectedIssue = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            String.format(Locale.ROOT, "Setting [%s] is deprecated", settingKey),
            "https://ela.st/es-deprecation-7-cluster-join-timeout-setting",
            String.format(Locale.ROOT, "Remove the [%s] setting. Cluster join attempts never time out in 8.0.", settingKey, settingValue),
            false,
            null
        );

        assertThat(NodeDeprecationChecks.checkJoinTimeoutSetting(nodeSettings, null, clusterState, licenseState), equalTo(expectedIssue));

        final String expectedWarning = String.format(
            Locale.ROOT,
            "[%s] setting was deprecated in Elasticsearch and will be removed in a future release! "
                + "See the breaking changes documentation for the next major version.",
            settingKey
        );

        assertWarnings(expectedWarning);
    }

    public void testCheckSearchRemoteSettings() {
        // test for presence of deprecated exporter passwords
        final int numClusters = randomIntBetween(1, 3);
        final String[] clusterNames = new String[numClusters];
        final Settings.Builder settingsBuilder = Settings.builder();
        for (int k = 0; k < numClusters; k++) {
            clusterNames[k] = randomAlphaOfLength(5);
            settingsBuilder.put("search.remote." + clusterNames[k] + ".seeds", randomAlphaOfLength(5));
            settingsBuilder.put("search.remote." + clusterNames[k] + ".proxy", randomAlphaOfLength(5));
            settingsBuilder.put("search.remote." + clusterNames[k] + ".skip_unavailable", randomBoolean());
        }
        settingsBuilder.put("search.remote.connections_per_cluster", randomIntBetween(0, 100));
        settingsBuilder.put("search.remote.initial_connect_timeout", randomIntBetween(30, 60));
        settingsBuilder.put("search.remote.connect", randomBoolean());
        final Settings settings = settingsBuilder.build();
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        DeprecationIssue issue = NodeDeprecationChecks.checkSearchRemoteSettings(settings, null, null, licenseState);

        final String expectedUrl = "https://ela.st/es-deprecation-7-search-remote-settings";
        String joinedNames = Arrays.stream(clusterNames)
            .map(s -> "search.remote." + s + ".seeds")
            .sorted()
            .collect(Collectors.joining(","));
        joinedNames += ",";
        joinedNames += Arrays.stream(clusterNames).map(s -> "search.remote." + s + ".proxy").sorted().collect(Collectors.joining(","));
        joinedNames += ",";
        joinedNames += Arrays.stream(clusterNames)
            .map(s -> "search.remote." + s + ".skip_unavailable")
            .sorted()
            .collect(Collectors.joining(","));
        joinedNames += ",search.remote.connections_per_cluster,search.remote.initial_connect_timeout,search.remote.connect";

        assertThat(
            issue,
            equalTo(
                new DeprecationIssue(
                    DeprecationIssue.Level.CRITICAL,
                    "Remotes for cross cluster search must be configured with cluster remote settings",
                    expectedUrl,
                    String.format(
                        Locale.ROOT,
                        "Replace the search.remote settings [%s] with their secure [cluster.remote] equivalents",
                        joinedNames
                    ),
                    false,
                    null
                )
            )
        );

        // test for absence of deprecated exporter passwords
        issue = NodeDeprecationChecks.checkMonitoringExporterPassword(Settings.builder().build(), null, null, licenseState);
        assertThat(issue, nullValue());
    }

    public void testClusterRoutingAllocationIncludeRelocationsSetting() {
        boolean settingValue = randomBoolean();
        String settingKey = CLUSTER_ROUTING_ALLOCATION_INCLUDE_RELOCATIONS_SETTING.getKey();
        final Settings nodeSettings = Settings.builder().put(settingKey, settingValue).build();
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final ClusterState clusterState = ClusterState.EMPTY_STATE;
        final DeprecationIssue expectedIssue = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            String.format(Locale.ROOT, "Setting [%s] is deprecated", settingKey),
            "https://ela.st/es-deprecation-7-cluster-routing-allocation-disk-include-relocations-setting",
            String.format(Locale.ROOT, "Remove the [%s] setting. Relocating shards are always taken into account in 8.0.", settingKey),
            false,
            null
        );

        assertThat(
            NodeDeprecationChecks.checkClusterRoutingAllocationIncludeRelocationsSetting(nodeSettings, null, clusterState, licenseState),
            equalTo(expectedIssue)
        );

        final String expectedWarning = String.format(
            Locale.ROOT,
            "[%s] setting was deprecated in Elasticsearch and will be removed in a future release! "
                + "See the breaking changes documentation for the next major version.",
            settingKey
        );

        assertWarnings(expectedWarning);
    }

    public void testImplicitlyDisabledSecurityWarning() {
        final DeprecationIssue issue = NodeDeprecationChecks.checkImplicitlyDisabledSecurityOnBasicAndTrial(
            Settings.EMPTY,
            null,
            ClusterState.EMPTY_STATE,
            new XPackLicenseState(Settings.EMPTY, () -> 0)
        );
        assertThat(issue.getLevel(), equalTo(DeprecationIssue.Level.CRITICAL));
        assertThat(issue.getMessage(), equalTo("Security is enabled by default for all licenses"));
        assertNotNull(issue.getDetails());
        assertThat(issue.getDetails(), containsString("Security will no longer be disabled by default"));
        assertThat(issue.getUrl(), equalTo("https://ela.st/es-deprecation-7-implicitly-disabled-security"));
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
        when(licenseState.getOperationMode()).thenReturn(
            randomValueOtherThanMany(
                (m -> m.equals(License.OperationMode.BASIC) || m.equals(License.OperationMode.TRIAL)),
                () -> randomFrom(License.OperationMode.values())
            )
        );
        final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules, licenseState);
        assertThat(issues, empty());
    }

    public void testCheckFractionalByteValueSettings() {
        String settingKey = "network.tcp.send_buffer_size";
        String unit = randomFrom(new String[] { "k", "kb", "m", "mb", "g", "gb", "t", "tb", "p", "pb" });
        float value = Math.abs(randomFloat());
        String settingValue = value + unit;
        String unaffectedSettingKey = "some.other.setting";
        String unaffectedSettingValue = "54.32.43mb"; // Not an actual number, so we don't expect to see a deprecation log about it
        final Settings nodeSettings = Settings.builder()
            .put(settingKey, settingValue)
            .put(unaffectedSettingKey, unaffectedSettingValue)
            .build();
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final ClusterState clusterState = ClusterState.EMPTY_STATE;
        final DeprecationIssue expectedIssue = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "Configuring fractional byte sizes is deprecated",
            "https://ela.st/es-deprecation-7-fractional-byte-settings",
            String.format(Locale.ROOT, "Set the following to whole numbers: [%s].", settingKey),
            false,
            null
        );
        assertThat(
            NodeDeprecationChecks.checkFractionalByteValueSettings(nodeSettings, null, clusterState, licenseState),
            equalTo(expectedIssue)
        );
        assertWarnings(
            String.format(
                Locale.ROOT,
                "Fractional bytes values are deprecated. Use non-fractional bytes values instead: [%s] " + "found for setting [%s]",
                settingValue,
                settingKey
            )
        );
    }

    public void testCheckFrozenCacheLeniency() {
        String cacheSizeSettingValue = "10gb";
        String cacheSizeSettingKey = "xpack.searchable.snapshot.shared_cache.size";
        Settings nodeSettings = Settings.builder().put(cacheSizeSettingKey, cacheSizeSettingValue).put("node.roles", "data_warm").build();
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final ClusterState clusterState = ClusterState.EMPTY_STATE;
        DeprecationIssue expectedIssue = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            String.format(Locale.ROOT, "Only frozen nodes can have a [%s] greater than zero.", cacheSizeSettingKey),
            "https://ela.st/es-deprecation-7-searchable-snapshot-shared-cache-setting",
            String.format(Locale.ROOT, "Set [%s] to zero on any node that doesn't have the [data_frozen] role.", cacheSizeSettingKey),
            false,
            null
        );
        assertThat(NodeDeprecationChecks.checkFrozenCacheLeniency(nodeSettings, null, clusterState, licenseState), equalTo(expectedIssue));

        // If no 'node.roles' is specified, a node gets all roles:
        nodeSettings = Settings.builder().put(cacheSizeSettingKey, cacheSizeSettingValue).build();
        assertThat(NodeDeprecationChecks.checkFrozenCacheLeniency(nodeSettings, null, clusterState, licenseState), equalTo(null));

        // No deprecation warning on a frozen node:
        nodeSettings = Settings.builder().put(cacheSizeSettingKey, cacheSizeSettingValue).put("node.roles", "data_frozen").build();
        assertThat(NodeDeprecationChecks.checkFrozenCacheLeniency(nodeSettings, null, clusterState, licenseState), equalTo(null));

        // No cache size specified, so no deprecation warning:
        nodeSettings = Settings.builder().put("node.roles", "data_warm").build();
        assertThat(NodeDeprecationChecks.checkFrozenCacheLeniency(nodeSettings, null, clusterState, licenseState), equalTo(null));

        // Cache size is not positive, so no deprecation wawrning:
        nodeSettings = Settings.builder().put(cacheSizeSettingKey, "0b").put("node.roles", "data_warm").build();
        assertThat(NodeDeprecationChecks.checkFrozenCacheLeniency(nodeSettings, null, clusterState, licenseState), equalTo(null));
    }

    public void testCheckSslServerEnabled() {
        String httpSslEnabledKey = "xpack.security.http.ssl.enabled";
        String transportSslEnabledKey = "xpack.security.transport.ssl.enabled";
        String problemSettingKey1 = "xpack.security.http.ssl.keystore.path";
        String problemSettingValue1 = "some/fake/path";
        String problemSettingKey2 = "xpack.security.http.ssl.truststore.path";
        String problemSettingValue2 = "some/other/fake/path";
        final Settings nodeSettings = Settings.builder()
            .put(transportSslEnabledKey, "true")
            .put(problemSettingKey1, problemSettingValue1)
            .put(problemSettingKey2, problemSettingValue2)
            .build();
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final ClusterState clusterState = ClusterState.EMPTY_STATE;
        final DeprecationIssue expectedIssue1 = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "Must explicitly enable or disable SSL to configure SSL settings",
            "https://ela.st/es-deprecation-7-explicit-ssl-required",
            String.format(
                Locale.ROOT,
                "The [%s] setting is not configured, but the following SSL settings are: [%s,%s]. To configure SSL, set [%s] or the node "
                    + "will fail to start in 8.0.",
                httpSslEnabledKey,
                problemSettingKey1,
                problemSettingKey2,
                httpSslEnabledKey
            ),
            false,
            null
        );
        final DeprecationIssue expectedIssue2 = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "Must explicitly enable or disable SSL to configure SSL settings",
            "https://ela.st/es-deprecation-7-explicit-ssl-required",
            String.format(
                Locale.ROOT,
                "The [%s] setting is not configured, but the following SSL settings are: [%s,%s]. To configure SSL, set [%s] or the node "
                    + "will fail to start in 8.0.",
                httpSslEnabledKey,
                problemSettingKey2,
                problemSettingKey1,
                httpSslEnabledKey
            ),
            false,
            null
        );

        assertThat(
            NodeDeprecationChecks.checkSslServerEnabled(nodeSettings, null, clusterState, licenseState),
            either(equalTo(expectedIssue1)).or(equalTo(expectedIssue2))
        );
    }

    public void testCheckSslCertConfiguration() {
        // SSL enabled, but no keystore/key/cert properties
        Settings nodeSettings = Settings.builder().put("xpack.security.transport.ssl.enabled", "true").build();
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final ClusterState clusterState = ClusterState.EMPTY_STATE;
        DeprecationIssue expectedIssue = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "Must either configure a keystore or set the key path and certificate path when SSL is enabled",
            "https://ela.st/es-deprecation-7-ssl-settings",
            "None of [xpack.security.transport.ssl.keystore.path], [xpack.security.transport.ssl.key], or [xpack.security.transport.ssl"
                + ".certificate] are set. If [xpack.security.transport.ssl.enabled] is true either use a keystore, or configure"
                + " [xpack.security.transport.ssl.key] and [xpack.security.transport.ssl.certificate].",
            false,
            null
        );
        assertThat(NodeDeprecationChecks.checkSslCertConfiguration(nodeSettings, null, clusterState, licenseState), equalTo(expectedIssue));

        // SSL enabled, and keystore path give, expect no issue
        nodeSettings = Settings.builder()
            .put("xpack.security.transport.ssl.enabled", "true")
            .put("xpack.security.transport.ssl.keystore.path", randomAlphaOfLength(10))
            .build();
        assertThat(NodeDeprecationChecks.checkSslCertConfiguration(nodeSettings, null, clusterState, licenseState), equalTo(null));

        // SSL enabled, and key and certificate path give, expect no issue
        nodeSettings = Settings.builder()
            .put("xpack.security.transport.ssl.enabled", "true")
            .put("xpack.security.transport.ssl.key", randomAlphaOfLength(10))
            .put("xpack.security.transport.ssl.certificate", randomAlphaOfLength(10))
            .build();
        assertThat(NodeDeprecationChecks.checkSslCertConfiguration(nodeSettings, null, clusterState, licenseState), equalTo(null));

        // SSL enabled, specify both keystore and key and certificate path
        nodeSettings = Settings.builder()
            .put("xpack.security.transport.ssl.enabled", "true")
            .put("xpack.security.transport.ssl.keystore.path", randomAlphaOfLength(10))
            .put("xpack.security.transport.ssl.key", randomAlphaOfLength(10))
            .put("xpack.security.transport.ssl.certificate", randomAlphaOfLength(10))
            .build();
        expectedIssue = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "Must either configure a keystore or set the key path and certificate path when SSL is enabled",
            "https://ela.st/es-deprecation-7-ssl-settings",
            "All of [xpack.security.transport.ssl.keystore.path], [xpack.security.transport.ssl.key], and [xpack.security.transport.ssl"
                + ".certificate] are set. Either use a keystore, or configure [xpack.security.transport.ssl.key] and "
                + "[xpack.security.transport.ssl.certificate].",
            false,
            null
        );
        assertThat(NodeDeprecationChecks.checkSslCertConfiguration(nodeSettings, null, clusterState, licenseState), equalTo(expectedIssue));

        // SSL enabled, specify keystore and key
        nodeSettings = Settings.builder()
            .put("xpack.security.transport.ssl.enabled", "true")
            .put("xpack.security.transport.ssl.keystore.path", randomAlphaOfLength(10))
            .put("xpack.security.transport.ssl.key", randomAlphaOfLength(10))
            .build();
        expectedIssue = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "Must either configure a keystore or set the key path and certificate path when SSL is enabled",
            "https://ela.st/es-deprecation-7-ssl-settings",
            "Do not configure both [xpack.security.transport.ssl.keystore.path] and [xpack.security.transport.ssl.key]. Either use a "
                + "keystore, or configure [xpack.security.transport.ssl.key] and [xpack.security.transport.ssl.certificate].",
            false,
            null
        );
        assertThat(NodeDeprecationChecks.checkSslCertConfiguration(nodeSettings, null, clusterState, licenseState), equalTo(expectedIssue));

        // Sanity check that it also works for http:
        nodeSettings = Settings.builder().put("xpack.security.http.ssl.enabled", "true").build();
        expectedIssue = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "Must either configure a keystore or set the key path and certificate path when SSL is enabled",
            "https://ela.st/es-deprecation-7-ssl-settings",
            "None of [xpack.security.http.ssl.keystore.path], [xpack.security.http.ssl.key], or [xpack.security.http.ssl.certificate] are"
                + " set. If [xpack.security.http.ssl.enabled] is true either use a keystore, or configure [xpack.security.http.ssl.key]"
                + " and [xpack.security.http.ssl.certificate].",
            false,
            null
        );
        assertThat(NodeDeprecationChecks.checkSslCertConfiguration(nodeSettings, null, clusterState, licenseState), equalTo(expectedIssue));
    }

    @SuppressForbidden(reason = "sets and unsets es.unsafely_permit_handshake_from_incompatible_builds")
    public void testCheckNoPermitHandshakeFromIncompatibleBuilds() {
        final DeprecationIssue expectedNullIssue = NodeDeprecationChecks.checkNoPermitHandshakeFromIncompatibleBuilds(
            Settings.EMPTY,
            null,
            ClusterState.EMPTY_STATE,
            new XPackLicenseState(Settings.EMPTY, () -> 0),
            () -> null
        );
        assertEquals(null, expectedNullIssue);
        final DeprecationIssue issue = NodeDeprecationChecks.checkNoPermitHandshakeFromIncompatibleBuilds(
            Settings.EMPTY,
            null,
            ClusterState.EMPTY_STATE,
            new XPackLicenseState(Settings.EMPTY, () -> 0),
            () -> randomAlphaOfLengthBetween(1, 10)
        );
        assertNotNull(issue.getDetails());
        assertEquals(
            issue.getDetails(),
            "Remove the [es.unsafely_permit_handshake_from_incompatible_builds] system property. Handshakes "
                + "from incompatible builds are not allowed in 8.0."
        );
        assertThat(issue.getUrl(), equalTo("https://ela.st/es-deprecation-7-permit-handshake-from-incompatible-builds-setting"));
    }

    public void testCheckTransportClientProfilesFilterSetting() {
        final int numProfiles = randomIntBetween(1, 3);
        final String[] profileNames = new String[numProfiles];
        final Settings.Builder b = Settings.builder();
        for (int k = 0; k < numProfiles; k++) {
            profileNames[k] = randomAlphaOfLength(5);
            b.put("transport.profiles." + profileNames[k] + ".xpack.security.type", randomAlphaOfLengthBetween(3, 10));
        }
        final Settings settings = b.build();
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        DeprecationIssue issue = NodeDeprecationChecks.checkTransportClientProfilesFilterSetting(settings, null, null, licenseState);
        final String expectedUrl = "https://ela.st/es-deprecation-7-transport-profiles-settings";
        final String joinedNames = Arrays.stream(profileNames)
            .map(s -> "transport.profiles." + s + ".xpack.security.type")
            .sorted()
            .collect(Collectors.joining(","));

        assertThat(
            issue,
            equalTo(
                new DeprecationIssue(
                    DeprecationIssue.Level.CRITICAL,
                    String.format(Locale.ROOT, "Settings [%s] for the Transport client are deprecated", joinedNames),
                    expectedUrl,
                    "Remove all [transport.profiles] settings. The Transport client no longer exists in 8.0.",
                    false,
                    null
                )
            )
        );

        // test for absence of deprecated exporter passwords
        issue = NodeDeprecationChecks.checkTransportClientProfilesFilterSetting(Settings.builder().build(), null, null, licenseState);
        assertThat(issue, nullValue());
    }

    public void testCheckDelayClusterStateRecoverySettings() {
        Settings settings = Settings.builder()
            .put(GatewayService.EXPECTED_NODES_SETTING.getKey(), randomIntBetween(2, 10))
            .put(GatewayService.EXPECTED_MASTER_NODES_SETTING.getKey(), randomIntBetween(2, 10))
            .put(GatewayService.RECOVER_AFTER_NODES_SETTING.getKey(), randomIntBetween(2, 10))
            .put(GatewayService.RECOVER_AFTER_MASTER_NODES_SETTING.getKey(), randomIntBetween(2, 10))
            .build();
        final ClusterState clusterState = ClusterState.EMPTY_STATE;
        final DeprecationIssue expectedIssue = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "Delaying cluster state recovery based on the number of available master nodes is not supported",
            "https://ela.st/es-deprecation-7-deferred-cluster-state-recovery",
            "Use gateway.expected_data_nodes to wait for a certain number of data nodes. Remove the following settings or the node will "
                + "fail to start in 8.0: "
                + "[gateway.expected_nodes,gateway.expected_master_nodes,gateway.recover_after_nodes,gateway.recover_after_master_nodes]",
            false,
            null
        );
        final XPackLicenseState licenseState = mock(XPackLicenseState.class);
        when(licenseState.getOperationMode()).thenReturn(
            randomValueOtherThanMany(
                (m -> m.equals(License.OperationMode.BASIC) || m.equals(License.OperationMode.TRIAL)),
                () -> randomFrom(License.OperationMode.values())
            )
        );
        assertThat(
            NodeDeprecationChecks.checkDelayClusterStateRecoverySettings(settings, null, clusterState, licenseState),
            equalTo(expectedIssue)
        );
    }

    public void testCheckFixedAutoQueueSizeThreadpool() {
        Settings settings = Settings.builder()
            .put("thread_pool.search.min_queue_size", randomIntBetween(30, 100))
            .put("thread_pool.search.max_queue_size", randomIntBetween(1, 25))
            .put("thread_pool.search.auto_queue_frame_size", randomIntBetween(1, 25))
            .put("thread_pool.search.target_response_time", randomIntBetween(1, 25))
            .put("thread_pool.search_throttled.min_queue_size", randomIntBetween(30, 100))
            .put("thread_pool.search_throttled.max_queue_size", randomIntBetween(1, 25))
            .put("thread_pool.search_throttled.auto_queue_frame_size", randomIntBetween(1, 25))
            .put("thread_pool.search_throttled.target_response_time", randomIntBetween(1, 25))
            .build();
        final ClusterState clusterState = ClusterState.EMPTY_STATE;
        final DeprecationIssue expectedIssue = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "The fixed_auto_queue_size threadpool type is not supported",
            "https://ela.st/es-deprecation-7-fixed-auto-queue-size-settings",
            "Remove the following settings or the node will fail to start in 8.0: "
                + "[thread_pool.search.min_queue_size,thread_pool.search.max_queue_size,"
                + "thread_pool.search.auto_queue_frame_size,thread_pool.search.target_response_time,"
                + "thread_pool.search_throttled.min_queue_size,thread_pool.search_throttled.max_queue_size,"
                + "thread_pool.search_throttled.auto_queue_frame_size,thread_pool.search_throttled.target_response_time].",
            false,
            null
        );
        final XPackLicenseState licenseState = mock(XPackLicenseState.class);
        when(licenseState.getOperationMode()).thenReturn(
            randomValueOtherThanMany(
                (m -> m.equals(License.OperationMode.BASIC) || m.equals(License.OperationMode.TRIAL)),
                () -> randomFrom(License.OperationMode.values())
            )
        );
        assertThat(
            NodeDeprecationChecks.checkFixedAutoQueueSizeThreadpool(settings, null, clusterState, licenseState),
            equalTo(expectedIssue)
        );
    }

    public void testTierAllocationSettings() {
        String settingValue = DataTier.DATA_HOT;
        final Settings settings = settings(Version.CURRENT).put(INDEX_ROUTING_REQUIRE_SETTING.getKey(), DataTier.DATA_HOT)
            .put(INDEX_ROUTING_INCLUDE_SETTING.getKey(), DataTier.DATA_HOT)
            .put(INDEX_ROUTING_EXCLUDE_SETTING.getKey(), DataTier.DATA_HOT)
            .build();
        final DeprecationIssue expectedRequireIssue = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            String.format(Locale.ROOT, "Setting [%s] is deprecated", INDEX_ROUTING_REQUIRE_SETTING.getKey()),
            "https://ela.st/es-deprecation-7-tier-filtering-settings",
            String.format(
                Locale.ROOT,
                "Remove the [%s] setting. Use [index.routing.allocation.include._tier_preference] to control allocation to data tiers.",
                INDEX_ROUTING_REQUIRE_SETTING.getKey()
            ),
            false,
            null
        );
        final DeprecationIssue expectedIncludeIssue = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            String.format(Locale.ROOT, "Setting [%s] is deprecated", INDEX_ROUTING_INCLUDE_SETTING.getKey()),
            "https://ela.st/es-deprecation-7-tier-filtering-settings",
            String.format(
                Locale.ROOT,
                "Remove the [%s] setting. Use [index.routing.allocation.include._tier_preference] to control allocation to data tiers.",
                INDEX_ROUTING_INCLUDE_SETTING.getKey()
            ),
            false,
            null
        );
        final DeprecationIssue expectedExcludeIssue = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            String.format(Locale.ROOT, "Setting [%s] is deprecated", INDEX_ROUTING_EXCLUDE_SETTING.getKey()),
            "https://ela.st/es-deprecation-7-tier-filtering-settings",
            String.format(
                Locale.ROOT,
                "Remove the [%s] setting. Use [index.routing.allocation.include._tier_preference] to control allocation to data tiers.",
                INDEX_ROUTING_EXCLUDE_SETTING.getKey()
            ),
            false,
            null
        );

        IndexMetadata indexMetadata = IndexMetadata.builder("test").settings(settings).numberOfShards(1).numberOfReplicas(0).build();
        assertThat(IndexDeprecationChecks.checkIndexRoutingRequireSetting(indexMetadata), equalTo(expectedRequireIssue));
        assertThat(IndexDeprecationChecks.checkIndexRoutingIncludeSetting(indexMetadata), equalTo(expectedIncludeIssue));
        assertThat(IndexDeprecationChecks.checkIndexRoutingExcludeSetting(indexMetadata), equalTo(expectedExcludeIssue));

        final String warningTemplate = "[%s] setting was deprecated in Elasticsearch and will be removed in a future release! "
            + "See the breaking changes documentation for the next major version.";
        final String[] expectedWarnings = {
            String.format(Locale.ROOT, warningTemplate, INDEX_ROUTING_REQUIRE_SETTING.getKey()),
            String.format(Locale.ROOT, warningTemplate, INDEX_ROUTING_INCLUDE_SETTING.getKey()),
            String.format(Locale.ROOT, warningTemplate, INDEX_ROUTING_EXCLUDE_SETTING.getKey()), };

        assertWarnings(expectedWarnings);
    }

    private void checkSimpleSetting(
        String settingKey,
        String settingValue,
        String url,
        DeprecationChecks.NodeDeprecationCheck<
            Settings,
            PluginsAndModules,
            ClusterState,
            XPackLicenseState,
            DeprecationIssue> checkFunction,
        String additionalDetail
    ) {
        final Settings nodeSettings = Settings.builder().put(settingKey, settingValue).build();
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final ClusterState clusterState = ClusterState.EMPTY_STATE;
        final DeprecationIssue expectedIssue = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            String.format(Locale.ROOT, "Setting [%s] is deprecated", settingKey),
            url,
            String.format(Locale.ROOT, "Remove the [%s] setting. %s", settingKey, additionalDetail),
            false,
            null
        );

        assertThat(checkFunction.apply(nodeSettings, null, clusterState, licenseState), equalTo(expectedIssue));

        final String expectedWarning = String.format(
            Locale.ROOT,
            "[%s] setting was deprecated in Elasticsearch and will be removed in a future release! "
                + "See the breaking changes documentation for the next major version.",
            settingKey
        );

        assertWarnings(expectedWarning);
    }

    public void testCheckAcceptDefaultPasswordSetting() {
        String settingKey = "xpack.security.authc.accept_default_password";
        String settingValue = String.valueOf(randomBoolean());
        String url = "https://ela.st/es-deprecation-7-accept-default-password-setting";
        checkSimpleSetting(
            settingKey,
            settingValue,
            url,
            NodeDeprecationChecks::checkAcceptDefaultPasswordSetting,
            "This setting has not had any effect since 6.0."
        );
    }

    public void testCheckAcceptRolesCacheMaxSizeSetting() {
        String settingKey = "xpack.security.authz.store.roles.index.cache.max_size";
        String settingValue = String.valueOf(randomIntBetween(1, 10000));
        String url = "https://ela.st/es-deprecation-7-roles-index-cache-settings";
        checkSimpleSetting(
            settingKey,
            settingValue,
            url,
            NodeDeprecationChecks::checkAcceptRolesCacheMaxSizeSetting,
            "Native role cache settings have had no effect since 5.2."
        );
    }

    public void testCheckRolesCacheTTLSizeSetting() {
        String settingKey = "xpack.security.authz.store.roles.index.cache.ttl";
        String settingValue = randomPositiveTimeValue();
        String url = "https://ela.st/es-deprecation-7-roles-index-cache-settings";
        checkSimpleSetting(
            settingKey,
            settingValue,
            url,
            NodeDeprecationChecks::checkRolesCacheTTLSizeSetting,
            "Native role cache settings have had no effect since 5.2."
        );
    }

    public void testCheckMaxLocalStorageNodesSetting() {
        String settingKey = NodeEnvironment.MAX_LOCAL_STORAGE_NODES_SETTING.getKey();
        String settingValue = Integer.toString(randomIntBetween(1, 100));
        String url = "https://ela.st/es-deprecation-7-node-local-storage-setting";
        checkSimpleSetting(
            settingKey,
            settingValue,
            url,
            NodeDeprecationChecks::checkMaxLocalStorageNodesSetting,
            "All nodes require local storage in 8.0 and cannot share data paths."
        );
    }

    public void testCheckSamlNameIdFormatSetting() {
        Settings settings = Settings.builder()
            .put("xpack.security.authc.realms.saml.saml1.attributes.principal", randomIntBetween(30, 100))
            .put("xpack.security.authc.realms.saml.saml1.nameid_format", randomIntBetween(1, 25))
            .build();
        final ClusterState clusterState = ClusterState.EMPTY_STATE;
        final XPackLicenseState licenseState = mock(XPackLicenseState.class);
        when(licenseState.getOperationMode()).thenReturn(
            randomValueOtherThanMany(
                (m -> m.equals(License.OperationMode.BASIC) || m.equals(License.OperationMode.TRIAL)),
                () -> randomFrom(License.OperationMode.values())
            )
        );
        assertThat(NodeDeprecationChecks.checkSamlNameIdFormatSetting(settings, null, clusterState, licenseState), equalTo(null));

        settings = Settings.builder().put("xpack.security.authc.realms.saml.saml1.attributes.principal", randomIntBetween(30, 100)).build();
        DeprecationIssue expectedIssue = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "The SAML nameid_format is not set and no longer defaults to \"urn:oasis:names:tc:SAML:2.0:nameid-format:transient\"",
            "https://ela.st/es-deprecation-7-saml-nameid-format",
            "Configure \"xpack.security.authc.realms.saml.saml1.nameid_format\" for SAML realms: "
                + "\"xpack.security.authc.realms.saml.saml1\".",
            false,
            null
        );
        assertThat(NodeDeprecationChecks.checkSamlNameIdFormatSetting(settings, null, clusterState, licenseState), equalTo(expectedIssue));

        settings = Settings.builder()
            .put("xpack.security.authc.realms.saml.saml1.attributes.principal", randomIntBetween(30, 100))
            .put("xpack.security.authc.realms.saml.saml2.attributes.principal", randomIntBetween(30, 100))
            .put("xpack.security.authc.realms.saml.saml2.nameid_format", randomIntBetween(1, 25))
            .build();
        expectedIssue = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "The SAML nameid_format is not set and no longer defaults to \"urn:oasis:names:tc:SAML:2.0:nameid-format:transient\"",
            "https://ela.st/es-deprecation-7-saml-nameid-format",
            "Configure \"xpack.security.authc.realms.saml.saml1.nameid_format\" for SAML realms: "
                + "\"xpack.security.authc.realms.saml.saml1\".",
            false,
            null
        );
        assertThat(NodeDeprecationChecks.checkSamlNameIdFormatSetting(settings, null, clusterState, licenseState), equalTo(expectedIssue));

        settings = Settings.builder()
            .put("xpack.security.authc.realms.saml.saml1.attributes.principal", randomIntBetween(30, 100))
            .put("xpack.security.authc.realms.saml.saml2.attributes.principal", randomIntBetween(30, 100))
            .build();
        expectedIssue = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "The SAML nameid_format is not set and no longer defaults to \"urn:oasis:names:tc:SAML:2.0:nameid-format:transient\"",
            "https://ela.st/es-deprecation-7-saml-nameid-format",
            "Configure \"xpack.security.authc.realms.saml.saml1.nameid_format\" for SAML realms: "
                + "\"xpack.security.authc.realms.saml.saml1\". Configure \"xpack.security.authc.realms.saml.saml2.nameid_format\" for SAML "
                + "realms: \"xpack.security.authc.realms.saml.saml2\".",
            false,
            null
        );
        assertThat(NodeDeprecationChecks.checkSamlNameIdFormatSetting(settings, null, clusterState, licenseState), equalTo(expectedIssue));
    }

    public void testScriptContextCacheSetting() {
        Settings settings = Settings.builder()
            .put(ScriptService.SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), "use-context")
            .build();

        assertThat(
            NodeDeprecationChecks.checkScriptContextCache(settings, null, null, null),
            equalTo(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    ScriptService.USE_CONTEXT_RATE_KEY_DEPRECATION_MESSAGE,
                    "https://ela.st/es-deprecation-7-script-context-cache",
                    "found deprecated script context caches in use, change setting to compilation rate or remove "
                        + "setting to use the default",
                    false,
                    null
                )
            )
        );
    }

    public void testScriptContextCompilationsRateLimitSetting() {
        List<String> contexts = org.elasticsearch.core.List.of("field", "score");
        Settings settings = Settings.builder()
            .put(ScriptService.SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), "use-context")
            .put(ScriptService.SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace(contexts.get(0)).getKey(), "123/5m")
            .put(ScriptService.SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace(contexts.get(1)).getKey(), "456/7m")
            .build();

        assertThat(
            NodeDeprecationChecks.checkScriptContextCompilationsRateLimitSetting(settings, null, null, null),
            equalTo(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "Setting context-specific rate limits"
                        + " [script.context.field.max_compilations_rate,script.context.score.max_compilations_rate] is deprecated."
                        + " Use [script.max_compilations_rate] to rate limit the compilation of user scripts."
                        + " Context-specific caches are no longer needed to prevent system scripts from triggering rate limits.",
                    "https://ela.st/es-deprecation-7-script-context-cache",
                    "[script.context.field.max_compilations_rate,script.context.score.max_compilations_rate] is deprecated and"
                        + " will be removed in a future release",
                    false,
                    null
                )
            )
        );

        assertWarnings(
            "[script.context.field.max_compilations_rate] setting was deprecated in Elasticsearch and will be"
                + " removed in a future release! See the breaking changes documentation for the next major version.",
            "[script.context.score.max_compilations_rate] setting was deprecated in Elasticsearch and will be removed in a future"
                + " release! See the breaking changes documentation for the next major version."
        );
    }

    public void testScriptContextCacheSizeSetting() {
        List<String> contexts = org.elasticsearch.core.List.of("filter", "update");
        Settings settings = Settings.builder()
            .put(ScriptService.SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), "use-context")
            .put(ScriptService.SCRIPT_CACHE_SIZE_SETTING.getConcreteSettingForNamespace(contexts.get(0)).getKey(), 80)
            .put(ScriptService.SCRIPT_CACHE_SIZE_SETTING.getConcreteSettingForNamespace(contexts.get(1)).getKey(), 200)
            .build();

        assertThat(
            NodeDeprecationChecks.checkScriptContextCacheSizeSetting(settings, null, null, null),
            equalTo(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "Setting a context-specific cache size"
                        + " [script.context.filter.cache_max_size,script.context.update.cache_max_size] is deprecated."
                        + " Use [script.cache.max_size] to configure the size of the general cache for scripts."
                        + " Context-specific caches are no longer needed to prevent system scripts from triggering rate limits.",
                    "https://ela.st/es-deprecation-7-script-context-cache",
                    "[script.context.filter.cache_max_size,script.context.update.cache_max_size] is deprecated and will be"
                        + " removed in a future release",
                    false,
                    null
                )
            )
        );

        assertWarnings(
            "[script.context.update.cache_max_size] setting was deprecated in Elasticsearch and will be"
                + " removed in a future release! See the breaking changes documentation for the next major version.",
            "[script.context.filter.cache_max_size] setting was deprecated in Elasticsearch and will be removed in a future"
                + " release! See the breaking changes documentation for the next major version."
        );
    }

    public void testScriptContextCacheExpirationSetting() {
        List<String> contexts = org.elasticsearch.core.List.of("interval", "moving-function");
        Settings settings = Settings.builder()
            .put(ScriptService.SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), "use-context")
            .put(ScriptService.SCRIPT_CACHE_EXPIRE_SETTING.getConcreteSettingForNamespace(contexts.get(0)).getKey(), "100m")
            .put(ScriptService.SCRIPT_CACHE_EXPIRE_SETTING.getConcreteSettingForNamespace(contexts.get(1)).getKey(), "2d")
            .build();

        assertThat(
            NodeDeprecationChecks.checkScriptContextCacheExpirationSetting(settings, null, null, null),
            equalTo(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "Setting a context-specific cache expiration"
                        + " [script.context.interval.cache_expire,script.context.moving-function.cache_expire] is deprecated."
                        + " Use [script.cache.expire] to configure the expiration of the general cache."
                        + " Context-specific caches are no longer needed to prevent system scripts from triggering rate limits.",
                    "https://ela.st/es-deprecation-7-script-context-cache",
                    "[script.context.interval.cache_expire,script.context.moving-function.cache_expire] is deprecated and will be"
                        + " removed in a future release",
                    false,
                    null
                )
            )
        );

        assertWarnings(
            "[script.context.interval.cache_expire] setting was deprecated in Elasticsearch and will be"
                + " removed in a future release! See the breaking changes documentation for the next major version.",
            "[script.context.moving-function.cache_expire] setting was deprecated in Elasticsearch and will be removed in a future"
                + " release! See the breaking changes documentation for the next major version."
        );
    }

    static List<
        DeprecationChecks.NodeDeprecationCheck<
            Settings,
            PluginsAndModules,
            ClusterState,
            XPackLicenseState,
            DeprecationIssue>> MONITORING_SETTINGS_CHECKS = Arrays.asList(
                NodeDeprecationChecks::checkMonitoringSettingHistoryDuration,
                NodeDeprecationChecks::checkMonitoringSettingCollectIndexRecovery,
                NodeDeprecationChecks::checkMonitoringSettingCollectIndices,
                NodeDeprecationChecks::checkMonitoringSettingCollectCcrTimeout,
                NodeDeprecationChecks::checkMonitoringSettingCollectEnrichStatsTimeout,
                NodeDeprecationChecks::checkMonitoringSettingCollectIndexRecoveryStatsTimeout,
                NodeDeprecationChecks::checkMonitoringSettingCollectIndexStatsTimeout,
                NodeDeprecationChecks::checkMonitoringSettingCollectMlJobStatsTimeout,
                NodeDeprecationChecks::checkMonitoringSettingCollectNodeStatsTimeout,
                NodeDeprecationChecks::checkMonitoringSettingCollectClusterStatsTimeout,
                NodeDeprecationChecks::checkMonitoringSettingExportersHost,
                NodeDeprecationChecks::checkMonitoringSettingExportersBulkTimeout,
                NodeDeprecationChecks::checkMonitoringSettingExportersConnectionTimeout,
                NodeDeprecationChecks::checkMonitoringSettingExportersConnectionReadTimeout,
                NodeDeprecationChecks::checkMonitoringSettingExportersAuthUsername,
                NodeDeprecationChecks::checkMonitoringSettingExportersAuthPass,
                NodeDeprecationChecks::checkMonitoringSettingExportersSSL,
                NodeDeprecationChecks::checkMonitoringSettingExportersProxyBase,
                NodeDeprecationChecks::checkMonitoringSettingExportersSniffEnabled,
                NodeDeprecationChecks::checkMonitoringSettingExportersHeaders,
                NodeDeprecationChecks::checkMonitoringSettingExportersTemplateTimeout,
                NodeDeprecationChecks::checkMonitoringSettingExportersMasterTimeout,
                NodeDeprecationChecks::checkMonitoringSettingExportersEnabled,
                NodeDeprecationChecks::checkMonitoringSettingExportersType,
                NodeDeprecationChecks::checkMonitoringSettingExportersAlertsEnabled,
                NodeDeprecationChecks::checkMonitoringSettingExportersAlertsBlacklist,
                NodeDeprecationChecks::checkMonitoringSettingExportersIndexNameTimeFormat,
                NodeDeprecationChecks::checkMonitoringSettingDecommissionAlerts,
                NodeDeprecationChecks::checkMonitoringSettingEsCollectionEnabled,
                NodeDeprecationChecks::checkMonitoringSettingCollectionEnabled,
                NodeDeprecationChecks::checkMonitoringSettingCollectionInterval
            );

    void monitoringSetting(String settingKey, String value) {
        Settings settings = Settings.builder().put(settingKey, value).build();
        final XPackLicenseState licenseState = new XPackLicenseState(settings, () -> 0);
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            MONITORING_SETTINGS_CHECKS,
            c -> c.apply(settings, null, ClusterState.EMPTY_STATE, licenseState)
        );
        final String expectedUrl = "https://ela.st/es-deprecation-7-monitoring-settings";
        assertThat(
            issues,
            hasItem(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "setting [" + settingKey + "] is deprecated and will be removed after 8.0",
                    expectedUrl,
                    "the setting [" + settingKey + "] is currently set to [" + value + "], remove this setting",
                    false,
                    null
                )
            )
        );
    }

    void monitoringExporterSetting(String suffix, String value) {
        String settingKey = "xpack.monitoring.exporters.test." + suffix;
        Settings settings = Settings.builder().put(settingKey, value).build();
        final XPackLicenseState licenseState = new XPackLicenseState(settings, () -> 0);
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            MONITORING_SETTINGS_CHECKS,
            c -> c.apply(settings, null, ClusterState.EMPTY_STATE, licenseState)
        );
        final String expectedUrl = "https://ela.st/es-deprecation-7-monitoring-settings";
        assertThat(
            issues,
            hasItem(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "The [" + settingKey + "] settings are deprecated and will be removed after 8.0",
                    expectedUrl,
                    "Remove the following settings from elasticsearch.yml: [" + settingKey + "]",
                    false,
                    null
                )
            )
        );
    }

    void monitoringExporterGroupedSetting(String suffix, String value) {
        String settingKey = "xpack.monitoring.exporters.test." + suffix;
        String subSettingKey = settingKey + ".subsetting";
        Settings settings = Settings.builder().put(subSettingKey, value).build();
        final XPackLicenseState licenseState = new XPackLicenseState(settings, () -> 0);
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            MONITORING_SETTINGS_CHECKS,
            c -> c.apply(settings, null, ClusterState.EMPTY_STATE, licenseState)
        );
        final String expectedUrl = "https://ela.st/es-deprecation-7-monitoring-settings";
        assertThat(
            issues,
            hasItem(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "The [" + settingKey + ".*] settings are deprecated and will be removed after 8.0",
                    expectedUrl,
                    "Remove the following settings from elasticsearch.yml: [" + subSettingKey + "]",
                    false,
                    null
                )
            )
        );
    }

    void monitoringExporterSecureSetting(String suffix, String value) {
        String settingKey = "xpack.monitoring.exporters.test." + suffix;
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(settingKey, value);
        Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
        final XPackLicenseState licenseState = new XPackLicenseState(settings, () -> 0);
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            MONITORING_SETTINGS_CHECKS,
            c -> c.apply(settings, null, ClusterState.EMPTY_STATE, licenseState)
        );
        final String expectedUrl = "https://ela.st/es-deprecation-7-monitoring-settings";
        assertThat(
            issues,
            hasItem(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "The [" + settingKey + "] settings are deprecated and will be removed after 8.0",
                    expectedUrl,
                    "Remove the following settings from the keystore: [" + settingKey + "]",
                    false,
                    null
                )
            )
        );
    }

    public void testCheckMonitoringSettingHistoryDuration() {
        monitoringSetting("xpack.monitoring.history.duration", "7d");
    }

    public void testCheckMonitoringSettingCollectIndexRecovery() {
        monitoringSetting("xpack.monitoring.collection.index.recovery.active_only", "true");
    }

    public void testCheckMonitoringSettingCollectIndices() {
        monitoringSetting("xpack.monitoring.collection.indices", "[test1,test2]");
    }

    public void testCheckMonitoringSettingCollectCcrTimeout() {
        monitoringSetting("xpack.monitoring.collection.ccr.stats.timeout", "10s");
    }

    public void testCheckMonitoringSettingCollectEnrichStatsTimeout() {
        monitoringSetting("xpack.monitoring.collection.enrich.stats.timeout", "10s");
    }

    public void testCheckMonitoringSettingCollectIndexRecoveryStatsTimeout() {
        monitoringSetting("xpack.monitoring.collection.index.recovery.timeout", "10s");
    }

    public void testCheckMonitoringSettingCollectIndexStatsTimeout() {
        monitoringSetting("xpack.monitoring.collection.index.stats.timeout", "10s");
    }

    public void testCheckMonitoringSettingCollectMlJobStatsTimeout() {
        monitoringSetting("xpack.monitoring.collection.ml.job.stats.timeout", "10s");
    }

    public void testCheckMonitoringSettingCollectNodeStatsTimeout() {
        monitoringSetting("xpack.monitoring.collection.node.stats.timeout", "10s");
    }

    public void testCheckMonitoringSettingCollectClusterStatsTimeout() {
        monitoringSetting("xpack.monitoring.collection.cluster.stats.timeout", "10s");
    }

    public void testCheckMonitoringSettingExportersHost() {
        monitoringExporterSetting("host", "abcdef");
    }

    public void testCheckMonitoringSettingExportersBulkTimeout() {
        monitoringExporterSetting("bulk.timeout", "10s");
    }

    public void testCheckMonitoringSettingExportersConnectionTimeout() {
        monitoringExporterSetting("connection.timeout", "10s");
    }

    public void testCheckMonitoringSettingExportersConnectionReadTimeout() {
        monitoringExporterSetting("connection.read_timeout", "10s");
    }

    public void testCheckMonitoringSettingExportersAuthUsername() {
        monitoringExporterSetting("auth.username", "abcdef");
    }

    public void testCheckMonitoringSettingExportersAuthPass() {
        monitoringExporterSecureSetting("auth.secure_password", "abcdef");
    }

    public void testCheckMonitoringSettingExportersSSL() {
        monitoringExporterGroupedSetting("ssl", "abcdef");
    }

    public void testCheckMonitoringSettingExportersProxyBase() {
        monitoringExporterSetting("proxy.base_path", "abcdef");
    }

    public void testCheckMonitoringSettingExportersSniffEnabled() {
        monitoringExporterSetting("sniff.enabled", "true");
    }

    public void testCheckMonitoringSettingExportersHeaders() {
        monitoringExporterGroupedSetting("headers", "abcdef");
    }

    public void testCheckMonitoringSettingExportersTemplateTimeout() {
        monitoringExporterSetting("index.template.master_timeout", "10s");
    }

    public void testCheckMonitoringSettingExportersMasterTimeout() {
        monitoringExporterSetting("wait_master.timeout", "10s");
    }

    public void testCheckMonitoringSettingExportersEnabled() {
        monitoringExporterSetting("enabled", "true");
    }

    public void testCheckMonitoringSettingExportersType() {
        monitoringExporterSetting("type", "local");
    }

    public void testCheckMonitoringSettingExportersAlertsEnabled() {
        monitoringExporterSetting("cluster_alerts.management.enabled", "true");
    }

    public void testCheckMonitoringSettingExportersAlertsBlacklist() {
        monitoringExporterSetting("cluster_alerts.management.blacklist", "[abcdef,ghijkl]");
    }

    public void testCheckMonitoringSettingExportersIndexNameTimeFormat() {
        monitoringExporterSetting("index.name.time_format", "yyyy-mm-dd");
    }

    public void testCheckMonitoringSettingDecomissionAlerts() {
        monitoringSetting("xpack.monitoring.migration.decommission_alerts", "true");
    }

    public void testCheckMonitoringSettingEsCollectionEnabled() {
        monitoringSetting("xpack.monitoring.elasticsearch.collection.enabled", "true");
    }

    public void testCheckMonitoringSettingCollectionEnabled() {
        monitoringSetting("xpack.monitoring.collection.enabled", "true");
    }

    public void testCheckMonitoringSettingCollectionInterval() {
        monitoringSetting("xpack.monitoring.collection.interval", "10s");
    }

    public void testExporterUseIngestPipelineSettings() {
        Settings settings = Settings.builder().put("xpack.monitoring.exporters.test.use_ingest", true).build();

        List<DeprecationIssue> issues = Collections.singletonList(
            NodeDeprecationChecks.checkExporterUseIngestPipelineSettings(settings, null, null, null)
        );

        final String expectedUrl = "https://ela.st/es-deprecation-7-monitoring-exporter-use-ingest-setting";
        assertThat(
            issues,
            hasItem(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "The [xpack.monitoring.exporters.test.use_ingest] settings are deprecated and will be removed after 8.0",
                    expectedUrl,
                    "Remove the following settings from elasticsearch.yml: [xpack.monitoring.exporters.test.use_ingest]",
                    false,
                    null
                )
            )
        );
    }

    public void testExporterPipelineMasterTimeoutSetting() {
        Settings settings = Settings.builder()
            .put("xpack.monitoring.exporters.test.index.pipeline.master_timeout", TimeValue.timeValueSeconds(10))
            .build();

        List<DeprecationIssue> issues = Collections.singletonList(
            NodeDeprecationChecks.checkExporterPipelineMasterTimeoutSetting(settings, null, null, null)
        );

        final String expectedUrl = "https://ela.st/es-deprecation-7-monitoring-exporter-pipeline-timeout-setting";
        assertThat(
            issues,
            hasItem(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "The [xpack.monitoring.exporters.test.index.pipeline.master_timeout] settings are"
                        + " deprecated and will be removed after 8.0",
                    expectedUrl,
                    "Remove the following settings from elasticsearch.yml: [xpack.monitoring.exporters.test.index.pipeline.master_timeout]",
                    false,
                    null
                )
            )
        );
    }

    public void testExporterCreateLegacyTemplateSetting() {
        Settings settings = Settings.builder().put("xpack.monitoring.exporters.test.index.template.create_legacy_templates", true).build();

        List<DeprecationIssue> issues = Collections.singletonList(
            NodeDeprecationChecks.checkExporterCreateLegacyTemplateSetting(settings, null, null, null)
        );

        final String expectedUrl = "https://ela.st/es-deprecation-7-monitoring-exporter-create-legacy-template-setting";
        assertThat(
            issues,
            hasItem(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "The [xpack.monitoring.exporters.test.index.template.create_legacy_templates] settings are deprecated and will be "
                        + "removed after 8.0",
                    expectedUrl,
                    "Remove the following settings from elasticsearch.yml: "
                        + "[xpack.monitoring.exporters.test.index.template.create_legacy_templates]",
                    false,
                    null
                )
            )
        );
    }
}
