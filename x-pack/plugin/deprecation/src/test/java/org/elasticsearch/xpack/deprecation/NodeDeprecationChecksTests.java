/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.logging.log4j.Level;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.bootstrap.BootstrapSettings;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.coordination.DiscoveryUpgradeService;
import org.elasticsearch.cluster.coordination.NoMasterBlockService;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Set;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.SeedHostsResolver;
import org.elasticsearch.discovery.SettingsBasedSeedHostsProvider;
import org.elasticsearch.discovery.zen.ElectMasterService;
import org.elasticsearch.discovery.zen.FaultDetection;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.DanglingIndicesState;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.http.HttpTransportSettings;
import org.elasticsearch.jdk.JavaVersion;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.node.Node;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.Compression;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.monitoring.Monitoring;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.coordination.JoinHelper.JOIN_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_INCLUDE_RELOCATIONS_SETTING;
import static org.elasticsearch.common.settings.Setting.Property;
import static org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider.CLUSTER_ROUTING_EXCLUDE_SETTING;
import static org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider.CLUSTER_ROUTING_INCLUDE_SETTING;
import static org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider.CLUSTER_ROUTING_REQUIRE_SETTING;
import static org.elasticsearch.xpack.core.ilm.LifecycleSettings.LIFECYCLE_POLL_INTERVAL_SETTING;
import static org.elasticsearch.xpack.deprecation.DeprecationChecks.CLUSTER_SETTINGS_CHECKS;
import static org.elasticsearch.xpack.deprecation.NodeDeprecationChecks.JAVA_DEPRECATION_MESSAGE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
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
                "Remove the [%s] setting. Use the [remote_cluster_client] role instead. Set [node.roles] to [data_frozen,master,"
                    + "remote_cluster_client,data,data_content,data_hot,data_warm,data_cold,ingest] on eligible cross-cluster client "
                    + "nodes.",
                RemoteClusterService.ENABLE_REMOTE_CLUSTERS.getKey()
            ),
            false,
            null
        );
        assertThat(issues, hasItem(expected));
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
        legacyRoleSettings.add(Setting.boolSetting("node.voting_only", false, Property.Deprecated, Property.NodeScope));
        legacyRoleSettings.add(Setting.boolSetting("node.ml", true, Property.Deprecated, Property.NodeScope));
        legacyRoleSettings.add(Setting.boolSetting("node.transform", true, Property.Deprecated, Property.NodeScope));
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
        final Settings clusterSettings = Settings.EMPTY;
        final Settings nodeSettings = Settings.EMPTY;
        final Setting<?> removedSetting = Setting.simpleString("node.removed_setting");
        final DeprecationIssue issue = NodeDeprecationChecks.checkRemovedSetting(
            clusterSettings,
            nodeSettings,
            removedSetting,
            "http://removed-setting.example.com",
            "Some detail."
        );
        assertThat(issue, nullValue());
    }

    public void testRemovedSetting() {
        final Settings clusterSettings = Settings.EMPTY;
        final Settings nodeSettings = Settings.builder().put("node.removed_setting", "value").build();
        final Setting<?> removedSetting = Setting.simpleString("node.removed_setting");
        final DeprecationIssue issue = NodeDeprecationChecks.checkRemovedSetting(
            clusterSettings,
            nodeSettings,
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

    public void testRemovedDynamicSetting() {
        final Settings clusterSettings = Settings.builder().put("node.removed_setting", "value").build();
        final Settings nodeSettings = Settings.EMPTY;
        final Setting<?> removedSetting = Setting.simpleString("node.removed_setting");
        final DeprecationIssue issue = NodeDeprecationChecks.checkRemovedSetting(
            clusterSettings,
            nodeSettings,
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
        assertThat(issue.getLevel(), equalTo(DeprecationIssue.Level.WARNING));
        assertThat(issue.getMessage(), equalTo("Specifying multiple data paths is deprecated"));
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
        assertThat(issue.getLevel(), equalTo(DeprecationIssue.Level.WARNING));
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
                    DeprecationIssue.Level.WARNING,
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
                    "Disk watermarks do not treat single-node clusters differently in versions 8.0 and later, and "
                        + "[cluster.routing.allocation.disk.watermark.enable_for_single_data_node] may not be set to [false] in these "
                        + "versions. Set [cluster.routing.allocation.disk.watermark.enable_for_single_data_node] to [true] to adopt the "
                        + "future behavior before upgrading.",
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
            "Disk watermarks do not treat single-node clusters differently in versions 8.0 and later.",
            expectedUrl,
            "Disk watermarks do not treat single-node clusters differently in versions 8.0 and later, which will affect the behavior of "
                + "this cluster. Set [cluster.routing.allocation.disk.watermark.enable_for_single_data_node] to [true] to adopt the future "
                + "behavior before upgrading.",
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
        DiscoveryNode node1 = new DiscoveryNode(randomAlphaOfLength(5), buildNewFakeTransportAddress(), Version.CURRENT);
        ClusterState state = ClusterStateCreationUtils.state(node1, node1, node1);

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
        DeprecationIssue issue = NodeDeprecationChecks.checkMonitoringExporterPassword(settings, null, state, licenseState);
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
        issue = NodeDeprecationChecks.checkMonitoringExporterPassword(Settings.builder().build(), null, state, licenseState);
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
        DiscoveryNode node1 = new DiscoveryNode(randomAlphaOfLength(5), buildNewFakeTransportAddress(), Version.CURRENT);
        ClusterState state = ClusterStateCreationUtils.state(node1, node1, node1);

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
        issue = NodeDeprecationChecks.checkMonitoringExporterPassword(Settings.builder().build(), null, state, licenseState);
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
        assertThat(issue.getMessage(), equalTo("Security features are enabled by default for all licenses in versions 8.0 and later"));
        assertNotNull(issue.getDetails());
        assertThat(issue.getDetails(), containsString("In Elasticsearch 8.0 the [xpack.security.enabled] setting will always "));
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
            Level.WARN,
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
        final Settings settings = settings(Version.CURRENT).put(CLUSTER_ROUTING_REQUIRE_SETTING.getKey(), DataTier.DATA_HOT)
            .put(CLUSTER_ROUTING_INCLUDE_SETTING.getKey(), DataTier.DATA_HOT)
            .put(CLUSTER_ROUTING_EXCLUDE_SETTING.getKey(), DataTier.DATA_HOT)
            .build();
        final DeprecationIssue expectedRequireIssue = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            String.format(Locale.ROOT, "Setting [%s] is deprecated", CLUSTER_ROUTING_REQUIRE_SETTING.getKey()),
            "https://ela.st/es-deprecation-7-tier-filtering-settings",
            String.format(
                Locale.ROOT,
                "Remove the [%s] setting. Use [index.routing.allocation.include._tier_preference] to control allocation to data tiers.",
                CLUSTER_ROUTING_REQUIRE_SETTING.getKey()
            ),
            false,
            null
        );
        final DeprecationIssue expectedIncludeIssue = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            String.format(Locale.ROOT, "Setting [%s] is deprecated", CLUSTER_ROUTING_INCLUDE_SETTING.getKey()),
            "https://ela.st/es-deprecation-7-tier-filtering-settings",
            String.format(
                Locale.ROOT,
                "Remove the [%s] setting. Use [index.routing.allocation.include._tier_preference] to control allocation to data tiers.",
                CLUSTER_ROUTING_INCLUDE_SETTING.getKey()
            ),
            false,
            null
        );
        final DeprecationIssue expectedExcludeIssue = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            String.format(Locale.ROOT, "Setting [%s] is deprecated", CLUSTER_ROUTING_EXCLUDE_SETTING.getKey()),
            "https://ela.st/es-deprecation-7-tier-filtering-settings",
            String.format(
                Locale.ROOT,
                "Remove the [%s] setting. Use [index.routing.allocation.include._tier_preference] to control allocation to data tiers.",
                CLUSTER_ROUTING_EXCLUDE_SETTING.getKey()
            ),
            false,
            null
        );

        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        ClusterState clusterState = ClusterState.EMPTY_STATE;
        assertThat(
            NodeDeprecationChecks.checkClusterRoutingRequireSetting(settings, pluginsAndModules, clusterState, licenseState),
            equalTo(expectedRequireIssue)
        );
        assertThat(
            NodeDeprecationChecks.checkClusterRoutingIncludeSetting(settings, pluginsAndModules, clusterState, licenseState),
            equalTo(expectedIncludeIssue)
        );
        assertThat(
            NodeDeprecationChecks.checkClusterRoutingExcludeSetting(settings, pluginsAndModules, clusterState, licenseState),
            equalTo(expectedExcludeIssue)
        );

        final String warningTemplate = "[%s] setting was deprecated in Elasticsearch and will be removed in a future release! "
            + "See the breaking changes documentation for the next major version.";
        final String[] expectedWarnings = {
            String.format(Locale.ROOT, warningTemplate, CLUSTER_ROUTING_REQUIRE_SETTING.getKey()),
            String.format(Locale.ROOT, warningTemplate, CLUSTER_ROUTING_INCLUDE_SETTING.getKey()),
            String.format(Locale.ROOT, warningTemplate, CLUSTER_ROUTING_EXCLUDE_SETTING.getKey()), };

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
        String url = "https://ela.st/es-deprecation-7-max-local-storage-nodes";
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
                    "Setting [script.max_compilations_rate] to [use-context] is deprecated",
                    "https://ela.st/es-deprecation-7-script-context-cache",
                    "Remove the context-specific cache settings and set [script.max_compilations_rate] to configure the rate limit for "
                        + "the general cache. If no limit is set, the rate defaults to 150 compilations per five minutes: 150/5m. "
                        + "Context-specific caches are no longer needed to prevent system scripts from triggering rate limits.",
                    false,
                    null
                )
            )
        );
    }

    public void testImplicitScriptContextCacheSetting() {
        List<String> contexts = org.elasticsearch.core.List.of("update", "filter");
        Settings settings = Settings.builder()
            .put(ScriptService.SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace(contexts.get(0)).getKey(), "123/5m")
            .put(ScriptService.SCRIPT_CACHE_SIZE_SETTING.getConcreteSettingForNamespace(contexts.get(1)).getKey(), "2453")
            .build();

        assertThat(
            NodeDeprecationChecks.checkScriptContextCache(settings, null, null, null),
            equalTo(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "Implicitly using the script context cache is deprecated, remove settings "
                        + "[script.context.filter.cache_max_size, script.context.update.max_compilations_rate] "
                        + "to use the script general cache.",
                    "https://ela.st/es-deprecation-7-script-context-cache",
                    "Remove the context-specific cache settings and set [script.max_compilations_rate] to configure the rate limit for "
                        + "the general cache. If no limit is set, the rate defaults to 150 compilations per five minutes: 150/5m. "
                        + "Context-specific caches are no longer needed to prevent system scripts from triggering rate limits.",
                    false,
                    null
                )
            )
        );

        assertWarnings(
            "[script.context.filter.cache_max_size] setting was deprecated in Elasticsearch and will be"
                + " removed in a future release! See the breaking changes documentation for the next major version.",
            "[script.context.update.max_compilations_rate] setting was deprecated in Elasticsearch and will be removed in a future"
                + " release! See the breaking changes documentation for the next major version."
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
                    "Setting a context-specific rate limit is deprecated",
                    "https://ela.st/es-deprecation-7-script-context-cache",
                    "Remove the context-specific rate limit settings: [script.context.field.max_compilations_rate,"
                        + "script.context.score.max_compilations_rate]. Instead, set [script.max_compilations_rate] to configure the rate "
                        + "limit for the general cache.  If no limit is set, the rate defaults to 150 compilations per five minutes: 150/5m"
                        + ". Context-specific caches are no longer needed to prevent system scripts from triggering rate limits.",
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
                    "Setting a context-specific cache size is deprecated",
                    "https://ela.st/es-deprecation-7-script-context-cache",
                    "Remove the context-specific cache size settings: [script.context.filter.cache_max_size,script.context.update"
                        + ".cache_max_size]. Instead, set [script.cache_max_size] to configure the size of the general cache. "
                        + "Context-specific caches are no longer needed to prevent system scripts from triggering rate limits.",
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
                    "Setting a context-specific cache expiration is deprecated",
                    "https://ela.st/es-deprecation-7-script-context-cache",
                    "Remove the context-specific cache expiration settings: [script.context.interval.cache_expire,"
                        + "script.context.moving-function.cache_expire]. Instead, set [script.cache.expire] to configure the expiration of"
                        + " the general cache. Context-specific caches are no longer needed to prevent system scripts from triggering rate "
                        + "limits.",
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
                    "Setting [" + settingKey + "] is deprecated",
                    expectedUrl,
                    "Remove the [" + settingKey + "] setting.",
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
                    "Remove the following settings: [" + settingKey + "]",
                    false,
                    null
                )
            )
        );
    }

    void monitoringExporterGroupedSetting(String suffix, String value) throws JsonProcessingException {
        String settingKey1 = "xpack.monitoring.exporters.test1." + suffix;
        String settingKey2 = "xpack.monitoring.exporters.test2." + suffix;
        String subSettingKey1 = settingKey1 + ".subsetting1";
        String subSetting1Key2 = settingKey2 + ".subsetting1";
        String subSetting2Key2 = settingKey2 + ".subsetting2";
        Settings nodeSettings = Settings.builder().put(subSettingKey1, value).build();
        Settings dynamicSettings = Settings.builder().put(subSetting1Key2, value).put(subSetting2Key2, value).build();
        Metadata.Builder metadataBuilder = Metadata.builder();
        if (randomBoolean()) {
            metadataBuilder.persistentSettings(dynamicSettings);
        } else {
            metadataBuilder.transientSettings(dynamicSettings);
        }
        Metadata metadata = metadataBuilder.build();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .build();
        final XPackLicenseState licenseState = new XPackLicenseState(nodeSettings, () -> 0);
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            MONITORING_SETTINGS_CHECKS,
            c -> c.apply(nodeSettings, null, clusterState, licenseState)
        );
        final String expectedUrl = "https://ela.st/es-deprecation-7-monitoring-settings";
        Map<String, Object> meta = null;
        assertThat(
            issues,
            hasItem(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "The [" + settingKey1 + ".*," + settingKey2 + ".*] settings are deprecated and will be removed after 8.0",
                    expectedUrl,
                    "Remove the following settings: [" + subSettingKey1 + "," + subSetting1Key2 + "," + subSetting2Key2 + "]",
                    false,
                    meta
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

    public void testCheckMonitoringSettingExportersSSL() throws JsonProcessingException {
        monitoringExporterGroupedSetting("ssl", "abcdef");
    }

    public void testCheckMonitoringSettingExportersProxyBase() {
        monitoringExporterSetting("proxy.base_path", "abcdef");
    }

    public void testCheckMonitoringSettingExportersSniffEnabled() {
        monitoringExporterSetting("sniff.enabled", "true");
    }

    public void testCheckMonitoringSettingExportersHeaders() throws JsonProcessingException {
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
            NodeDeprecationChecks.checkExporterUseIngestPipelineSettings(settings, null, ClusterState.EMPTY_STATE, null)
        );

        final String expectedUrl = "https://ela.st/es-deprecation-7-monitoring-exporter-use-ingest-setting";
        assertThat(
            issues,
            hasItem(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "The [xpack.monitoring.exporters.test.use_ingest] settings are deprecated and will be removed after 8.0",
                    expectedUrl,
                    "Remove the following settings: [xpack.monitoring.exporters.test.use_ingest]",
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
            NodeDeprecationChecks.checkExporterPipelineMasterTimeoutSetting(settings, null, ClusterState.EMPTY_STATE, null)
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
                    "Remove the following settings: [xpack.monitoring.exporters.test.index.pipeline.master_timeout]",
                    false,
                    null
                )
            )
        );
    }

    public void testExporterCreateLegacyTemplateSetting() {
        Settings settings = Settings.builder().put("xpack.monitoring.exporters.test.index.template.create_legacy_templates", true).build();

        List<DeprecationIssue> issues = Collections.singletonList(
            NodeDeprecationChecks.checkExporterCreateLegacyTemplateSetting(settings, null, ClusterState.EMPTY_STATE, null)
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
                    "Remove the following settings: " + "[xpack.monitoring.exporters.test.index.template.create_legacy_templates]",
                    false,
                    null
                )
            )
        );
    }

    public void testShardReroutePriority() {
        Settings settings = Settings.builder()
            .put(
                ShardStateAction.FOLLOW_UP_REROUTE_PRIORITY_SETTING.getKey(),
                randomFrom(Priority.HIGH, Priority.NORMAL, Priority.URGENT).toString()
            )
            .build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules, licenseState);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "Setting [cluster.routing.allocation.shard_state.reroute.priority] is deprecated",
            "https://ela.st/es-deprecation-7-reroute-priority-setting",
            "Remove the [cluster.routing.allocation.shard_state.reroute.priority] setting. In a future release this setting will have no "
                + "effect and the priority will always be NORMAL.",
            false,
            null
        );
        assertThat(issues, hasItem(expected));
        assertWarnings(
            "[cluster.routing.allocation.shard_state.reroute.priority] setting was deprecated in Elasticsearch and will be "
                + "removed in a future release! See the breaking changes documentation for the next major version."
        );
    }

    public void testZenDiscoverySettings() {
        Settings settings = Settings.builder()
            .put(DiscoveryUpgradeService.BWC_PING_TIMEOUT_SETTING.getKey(), randomTimeValue())
            .put(DiscoveryUpgradeService.ENABLE_UNSAFE_BOOTSTRAPPING_ON_UPGRADE_SETTING.getKey(), randomTimeValue())
            .put(DiscoverySettings.COMMIT_TIMEOUT_SETTING.getKey(), randomTimeValue())
            .put(DiscoverySettings.PUBLISH_DIFF_ENABLE_SETTING.getKey(), randomBoolean())
            .put(FaultDetection.CONNECT_ON_NETWORK_DISCONNECT_SETTING.getKey(), randomBoolean())
            .put(FaultDetection.PING_INTERVAL_SETTING.getKey(), randomPositiveTimeValue())
            .put(FaultDetection.PING_TIMEOUT_SETTING.getKey(), randomTimeValue())
            .put(FaultDetection.PING_RETRIES_SETTING.getKey(), randomInt())
            .put(FaultDetection.REGISTER_CONNECTION_LISTENER_SETTING.getKey(), randomBoolean())
            .put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey(), randomInt())
            .putList(
                DiscoveryModule.LEGACY_DISCOVERY_HOSTS_PROVIDER_SETTING.getKey(),
                Arrays.asList(generateRandomStringArray(10, 20, false))
            )
            .put(ZenDiscovery.JOIN_RETRY_ATTEMPTS_SETTING.getKey(), randomInt())
            .put(ZenDiscovery.JOIN_RETRY_DELAY_SETTING.getKey(), randomTimeValue())
            .put(ZenDiscovery.JOIN_TIMEOUT_SETTING.getKey(), randomTimeValue())
            .put(ZenDiscovery.MASTER_ELECTION_IGNORE_NON_MASTER_PINGS_SETTING.getKey(), randomBoolean())
            .put(ZenDiscovery.MASTER_ELECTION_WAIT_FOR_JOINS_TIMEOUT_SETTING.getKey(), randomTimeValue())
            .put(ZenDiscovery.MAX_PINGS_FROM_ANOTHER_MASTER_SETTING.getKey(), randomInt())
            .put(NoMasterBlockService.LEGACY_NO_MASTER_BLOCK_SETTING.getKey(), "metadata_write")
            .put(SeedHostsResolver.LEGACY_DISCOVERY_ZEN_PING_UNICAST_CONCURRENT_CONNECTS_SETTING.getKey(), randomInt())
            .putList(
                SettingsBasedSeedHostsProvider.LEGACY_DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING.getKey(),
                Arrays.asList(generateRandomStringArray(10, 20, false))
            )
            .put(SeedHostsResolver.LEGACY_DISCOVERY_ZEN_PING_UNICAST_HOSTS_RESOLVE_TIMEOUT.getKey(), randomPositiveTimeValue())
            .put(ZenDiscovery.PING_TIMEOUT_SETTING.getKey(), randomPositiveTimeValue())
            .put(ZenDiscovery.MAX_PENDING_CLUSTER_STATES_SETTING.getKey(), randomInt())
            .put(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey(), randomPositiveTimeValue())
            .build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules, licenseState);
        Collection<Setting<?>> deprecatedSettings = Set.of(
            DiscoveryUpgradeService.BWC_PING_TIMEOUT_SETTING,
            DiscoveryUpgradeService.ENABLE_UNSAFE_BOOTSTRAPPING_ON_UPGRADE_SETTING,
            DiscoverySettings.COMMIT_TIMEOUT_SETTING,
            DiscoverySettings.PUBLISH_DIFF_ENABLE_SETTING,
            FaultDetection.CONNECT_ON_NETWORK_DISCONNECT_SETTING,
            FaultDetection.PING_INTERVAL_SETTING,
            FaultDetection.PING_TIMEOUT_SETTING,
            FaultDetection.PING_RETRIES_SETTING,
            FaultDetection.REGISTER_CONNECTION_LISTENER_SETTING,
            ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING,
            DiscoveryModule.LEGACY_DISCOVERY_HOSTS_PROVIDER_SETTING,
            ZenDiscovery.JOIN_RETRY_ATTEMPTS_SETTING,
            ZenDiscovery.JOIN_RETRY_DELAY_SETTING,
            ZenDiscovery.JOIN_TIMEOUT_SETTING,
            ZenDiscovery.MASTER_ELECTION_IGNORE_NON_MASTER_PINGS_SETTING,
            ZenDiscovery.MASTER_ELECTION_WAIT_FOR_JOINS_TIMEOUT_SETTING,
            ZenDiscovery.MAX_PINGS_FROM_ANOTHER_MASTER_SETTING,
            NoMasterBlockService.LEGACY_NO_MASTER_BLOCK_SETTING,
            SeedHostsResolver.LEGACY_DISCOVERY_ZEN_PING_UNICAST_CONCURRENT_CONNECTS_SETTING,
            SettingsBasedSeedHostsProvider.LEGACY_DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING,
            SeedHostsResolver.LEGACY_DISCOVERY_ZEN_PING_UNICAST_HOSTS_RESOLVE_TIMEOUT,
            ZenDiscovery.PING_TIMEOUT_SETTING,
            ZenDiscovery.MAX_PENDING_CLUSTER_STATES_SETTING,
            DiscoverySettings.PUBLISH_TIMEOUT_SETTING
        );
        for (Setting<?> deprecatedSetting : deprecatedSettings) {
            final DeprecationIssue expected = new DeprecationIssue(
                DeprecationIssue.Level.CRITICAL,
                "Setting [" + deprecatedSetting.getKey() + "] is deprecated",
                "https://ela.st/es-deprecation-7-unused_zen_settings",
                "Remove the [" + deprecatedSetting.getKey() + "] setting.",
                false,
                null
            );
            assertThat(issues, hasItem(expected));
        }
    }

    public void testDynamicSettings() throws JsonProcessingException {
        Settings clusterSettings = Settings.builder()
            .put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey(), randomInt())
            .build();
        Settings nodettings = Settings.builder().build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        Metadata.Builder metadataBuilder = Metadata.builder();
        if (randomBoolean()) {
            metadataBuilder.persistentSettings(clusterSettings);
        } else {
            metadataBuilder.transientSettings(clusterSettings);
        }
        Metadata metadata = metadataBuilder.build();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .build();
        final List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
            DeprecationChecks.NODE_SETTINGS_CHECKS,
            c -> c.apply(nodettings, pluginsAndModules, clusterState, licenseState)
        );

        Collection<Setting<?>> deprecatedSettings = Set.of(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING);
        Map<String, Object> meta = buildMetaObjectForRemovableSettings("discovery.zen.minimum_master_nodes");
        for (Setting<?> deprecatedSetting : deprecatedSettings) {
            final DeprecationIssue expected = new DeprecationIssue(
                DeprecationIssue.Level.CRITICAL,
                "Setting [" + deprecatedSetting.getKey() + "] is deprecated",
                "https://ela.st/es-deprecation-7-unused_zen_settings",
                "Remove the [" + deprecatedSetting.getKey() + "] setting.",
                false,
                meta
            );
            assertThat(issues, hasItem(expected));
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> buildMetaObjectForRemovableSettings(String... settingNames) throws JsonProcessingException {
        String settingNamesString = Arrays.stream(settingNames)
            .map(settingName -> "\"" + settingName + "\"")
            .collect(Collectors.joining(","));
        String metaString = "{\"actions\": [{\"action_type\": \"remove_settings\", \"objects\":[" + settingNamesString + "]}]}";
        return new ObjectMapper().readValue(metaString, Map.class);
    }

    public void testAutoImportDanglingIndicesSetting() {
        Settings settings = Settings.builder()
            .put(DanglingIndicesState.AUTO_IMPORT_DANGLING_INDICES_SETTING.getKey(), randomBoolean())
            .build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules, licenseState);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "Setting [gateway.auto_import_dangling_indices] is deprecated",
            "https://ela.st/es-deprecation-7-auto-import-dangling-indices-setting",
            "Remove the [gateway.auto_import_dangling_indices] setting. Use of this setting is unsafe.",
            false,
            null
        );
        assertThat(issues, hasItem(expected));
        assertWarnings(
            "[gateway.auto_import_dangling_indices] setting was deprecated in Elasticsearch and will be removed in a future "
                + "release! See the breaking changes documentation for the next major version."
        );
    }

    public void testHttpContentTypeRequiredSetting() {
        Settings settings = Settings.builder().put(HttpTransportSettings.SETTING_HTTP_CONTENT_TYPE_REQUIRED.getKey(), true).build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules, licenseState);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "Setting [http.content_type.required] is deprecated",
            "https://ela.st/es-deprecation-7-http-content-type-required-setting",
            "Remove the [http.content_type.required] setting. This setting no longer has any effect.",
            false,
            null
        );
        assertThat(issues, hasItem(expected));
        assertWarnings(
            "[http.content_type.required] setting was deprecated in Elasticsearch and will be removed in a future release! See"
                + " the breaking changes documentation for the next major version."
        );
    }

    public void testFsRepositoryCompressionSetting() {
        boolean settingValue = randomBoolean();
        Settings settings = Settings.builder().put(FsRepository.REPOSITORIES_COMPRESS_SETTING.getKey(), settingValue).build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules, licenseState);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "Setting [repositories.fs.compress] is deprecated",
            "https://ela.st/es-deprecation-7-filesystem-repository-compression-setting",
            "Remove the [repositories.fs.compress] setting and set [compress] to [" + settingValue + "].",
            false,
            null
        );
        assertThat(issues, hasItem(expected));
        assertWarnings(
            "[repositories.fs.compress] setting was deprecated in Elasticsearch and will be removed in a future release! See the breaking"
                + " changes documentation for the next major version."
        );
    }

    public void testTransportSettings() {
        Map<Setting<?>, Setting<?>> settingToReplacementMap = new HashMap<>();
        settingToReplacementMap.put(HttpTransportSettings.OLD_SETTING_HTTP_TCP_NO_DELAY, HttpTransportSettings.SETTING_HTTP_TCP_NO_DELAY);
        settingToReplacementMap.put(NetworkService.TCP_CONNECT_TIMEOUT, TransportSettings.CONNECT_TIMEOUT);
        settingToReplacementMap.put(TransportSettings.TCP_CONNECT_TIMEOUT, TransportSettings.CONNECT_TIMEOUT);
        settingToReplacementMap.put(TransportSettings.OLD_PORT, TransportSettings.PORT);
        settingToReplacementMap.put(TransportSettings.OLD_TCP_NO_DELAY, TransportSettings.TCP_NO_DELAY);
        settingToReplacementMap.put(TransportSettings.OLD_TRANSPORT_COMPRESS, TransportSettings.TRANSPORT_COMPRESS);
        Map<Setting<?>, Object> settingToTestValueMap = new HashMap<>();
        settingToTestValueMap.put(HttpTransportSettings.OLD_SETTING_HTTP_TCP_NO_DELAY, randomBoolean());
        /*
         * Note: Limiting time values to between 1 and 23 because the downstream code will normalize them so that 100 hours becomes 4.2
         * days, so the expected values would not match the actual.
         */
        settingToTestValueMap.put(NetworkService.TCP_CONNECT_TIMEOUT, randomTimeValue(1, 23));
        settingToTestValueMap.put(TransportSettings.TCP_CONNECT_TIMEOUT, randomTimeValue(1, 23));
        settingToTestValueMap.put(TransportSettings.OLD_PORT, randomAlphaOfLength(10));
        settingToTestValueMap.put(TransportSettings.OLD_TCP_NO_DELAY, randomBoolean());
        settingToTestValueMap.put(
            TransportSettings.OLD_TRANSPORT_COMPRESS,
            randomFrom(Compression.Enabled.TRUE, Compression.Enabled.FALSE, Compression.Enabled.INDEXING_DATA)
        );
        Settings.Builder settingsBuilder = Settings.builder();
        settingToTestValueMap.entrySet()
            .stream()
            .forEach(settingAndValue -> settingsBuilder.put(settingAndValue.getKey().getKey(), settingAndValue.getValue().toString()));
        Settings settings = settingsBuilder.build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules, licenseState);
        java.util.Set<String> warnings = new HashSet<>();
        for (Map.Entry<Setting<?>, Setting<?>> deprecatedSettingToReplacement : settingToReplacementMap.entrySet()) {
            final DeprecationIssue expected = new DeprecationIssue(
                DeprecationIssue.Level.CRITICAL,
                "Setting [" + deprecatedSettingToReplacement.getKey().getKey() + "] is deprecated",
                "https://ela.st/es-deprecation-7-transport-settings",
                String.format(
                    Locale.ROOT,
                    "Remove the [%s] setting and set [%s] to [%s].",
                    deprecatedSettingToReplacement.getKey().getKey(),
                    deprecatedSettingToReplacement.getValue().getKey(),
                    settingToTestValueMap.get(deprecatedSettingToReplacement.getKey())
                ),
                false,
                null
            );
            assertThat(issues, hasItem(expected));
            warnings.add(
                String.format(
                    Locale.ROOT,
                    "[%s] setting was deprecated in Elasticsearch and will be removed in a future "
                        + "release! See the breaking changes documentation for the next major version.",
                    deprecatedSettingToReplacement.getKey().getKey()
                )
            );
        }
        assertWarnings(warnings.toArray(new String[0]));
    }

    public void testXpackDataFrameEnabledSetting() {
        boolean settingValue = randomBoolean();
        Settings settings = Settings.builder().put("xpack.data_frame.enabled", settingValue).build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules, licenseState);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "Setting [xpack.data_frame.enabled] is deprecated",
            "https://ela.st/es-deprecation-7-xpack-dataframe-setting",
            "Remove the [xpack.data_frame.enabled] setting. As of 7.9.2 basic license level features are always enabled.",
            false,
            null
        );
        assertThat(issues, hasItem(expected));
        assertWarnings(
            "[xpack.data_frame.enabled] setting was deprecated in Elasticsearch and will be removed in a "
                + "future release! See the breaking changes documentation for the next major version."
        );
    }

    public void testWatcherHistoryCleanerServiceSetting() {
        boolean settingValue = randomBoolean();
        Settings settings = Settings.builder().put(Monitoring.CLEAN_WATCHER_HISTORY.getKey(), settingValue).build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules, licenseState);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "Setting [xpack.watcher.history.cleaner_service.enabled] is deprecated",
            "https://ela.st/es-deprecation-7-watcher-history-cleaner-setting",
            "Remove the [xpack.watcher.history.cleaner_service.enabled] setting. "
                + "Watcher history indices are now managed by the watch-history-ilm-policy ILM policy.",
            false,
            null
        );
        assertThat(issues, hasItem(expected));
        assertWarnings(
            "[xpack.watcher.history.cleaner_service.enabled] setting was deprecated in Elasticsearch and will be removed in a "
                + "future release! See the breaking changes documentation for the next major version."
        );
    }

    public void testLifecyleStepMasterTimeoutSetting() {
        Settings settings = Settings.builder()
            .put(LifecycleSettings.LIFECYCLE_STEP_MASTER_TIMEOUT_SETTING.getKey(), randomTimeValue())
            .build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules, licenseState);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "Setting [indices.lifecycle.step.master_timeout] is deprecated",
            "https://ela.st/es-deprecation-7-lifecycle-master-timeout-setting",
            "Remove the [indices.lifecycle.step.master_timeout] setting. As of 7.16 the timeout is always infinite.",
            false,
            null
        );
        assertThat(issues, hasItem(expected));
        assertWarnings(
            true,
            new DeprecationWarning(
                Level.WARN,
                "[indices.lifecycle.step.master_timeout] setting was deprecated in Elasticsearch and will be removed in a future release!"
                    + " See the breaking changes documentation for the next major version."
            )
        );
    }

    public void testEqlEnabledSetting() {
        Settings settings = Settings.builder().put("xpack.eql.enabled", randomBoolean()).build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules, licenseState);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "Setting [xpack.eql.enabled] is deprecated",
            "https://ela.st/es-deprecation-7-eql-enabled-setting",
            "Remove the [xpack.eql.enabled] setting. As of 7.9.2 basic license level features are always enabled.",
            false,
            null
        );
        assertThat(issues, hasItem(expected));
        assertWarnings(
            true,
            new DeprecationWarning(
                Level.WARN,
                "[xpack.eql.enabled] setting was deprecated in Elasticsearch and will be removed in a future release!"
                    + " See the breaking changes documentation for the next major version."
            )
        );
    }

    public void testCheckNodeAttrData() {
        Settings settings = Settings.builder().put("node.attr.data", randomAlphaOfLength(randomIntBetween(4, 20))).build();
        final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
        final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
        final List<DeprecationIssue> issues = getDeprecationIssues(settings, pluginsAndModules, licenseState);
        final DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "Setting node.attributes.data is not recommended",
            "https://ela.st/es-deprecation-7-node-attr-data-setting",
            "One or more of your nodes is configured with node.attributes.data settings. This is typically used to create a "
                + "hot/warm or tiered architecture, based on legacy guidelines. Data tiers are a recommended replacement for tiered "
                + "architecture clusters.",
            false,
            null
        );
        assertThat(issues, hasItem(expected));
    }

    public void testPollIntervalTooLow() throws JsonProcessingException {
        {
            Settings settings = Settings.builder()
                .put(LifecycleSettings.LIFECYCLE_STEP_MASTER_TIMEOUT_SETTING.getKey(), randomTimeValue())
                .build();
            final PluginsAndModules pluginsAndModules = new PluginsAndModules(Collections.emptyList(), Collections.emptyList());
            final XPackLicenseState licenseState = new XPackLicenseState(Settings.EMPTY, () -> 0);
            final String tooLowInterval = randomTimeValue(1, 999, "ms", "micros", "nanos");
            Metadata badMetaDtata = Metadata.builder()
                .persistentSettings(Settings.builder().put(LIFECYCLE_POLL_INTERVAL_SETTING.getKey(), tooLowInterval).build())
                .build();
            ClusterState badState = ClusterState.builder(new ClusterName("test")).metadata(badMetaDtata).build();
            final List<DeprecationIssue> issues = DeprecationChecks.filterChecks(
                DeprecationChecks.NODE_SETTINGS_CHECKS,
                c -> c.apply(settings, pluginsAndModules, badState, licenseState)
            );

            DeprecationIssue expected = new DeprecationIssue(
                DeprecationIssue.Level.CRITICAL,
                "Index Lifecycle Management poll interval is set too low",
                "https://ela.st/es-deprecation-7-indices-lifecycle-poll-interval-setting",
                "The ILM ["
                    + LIFECYCLE_POLL_INTERVAL_SETTING.getKey()
                    + "] setting is set to ["
                    + tooLowInterval
                    + "]. "
                    + "Set the interval to at least 1s.",
                false,
                buildMetaObjectForRemovableSettings("indices.lifecycle.poll_interval")
            );
            assertThat(issues, hasItem(expected));
            assertWarnings(
                "[indices.lifecycle.step.master_timeout] setting was deprecated in Elasticsearch and will be removed "
                    + "in a future release! See the breaking changes documentation for the next major version."
            );
        }

        // Test that other values are ok
        {
            final String okInterval = randomTimeValue(1, 9999, "d", "h", "s");
            Metadata okMetadata = Metadata.builder()
                .persistentSettings(Settings.builder().put(LIFECYCLE_POLL_INTERVAL_SETTING.getKey(), okInterval).build())
                .build();
            ClusterState okState = ClusterState.builder(new ClusterName("test")).metadata(okMetadata).build();
            List<DeprecationIssue> noIssues = DeprecationChecks.filterChecks(CLUSTER_SETTINGS_CHECKS, c -> c.apply(okState));
            assertThat(noIssues, hasSize(0));
        }
    }
}
