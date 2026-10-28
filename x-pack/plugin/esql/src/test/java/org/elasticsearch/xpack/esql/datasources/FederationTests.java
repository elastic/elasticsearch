/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.logging.log4j.Level;
import org.apache.lucene.util.Constants;
import org.elasticsearch.Build;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalSourceCacheSettings;
import org.elasticsearch.xpack.esql.datasources.dataset.DatasetService;
import org.elasticsearch.xpack.esql.datasources.datasource.DataSourceService;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;

/**
 * Unit tests for {@link Federation}, the availability gate for external data sources and datasets. The registered state
 * comes from a system property read into a {@code static final} at class load and cannot be flipped in-JVM, so property
 * parsing, the enforcement branch, the set of registered settings, and the startup logging are exercised through the
 * package-private {@link Federation#readRegistered}, {@link Federation#ensureEnabled(boolean)},
 * {@link Federation#settings(boolean)} and {@link Federation#logEffectiveState(boolean, boolean, boolean, boolean, boolean)} seams,
 * which take their inputs as parameters. The end-to-end behavior of both levers at the REST and transport surface is
 * covered by the federation REST ITs.
 */
public class FederationTests extends ESTestCase {

    private static Function<String, String> property(String value) {
        return Map.of(Federation.REGISTER_PROPERTY, value)::get;
    }

    private static Settings enabled(boolean enabled) {
        return Settings.builder().put(Federation.FEDERATION_ENABLED.getKey(), enabled).build();
    }

    /** Node settings of a stateless node that leaves {@link Federation#FEDERATION_ENABLED} to its default. */
    private static Settings stateless() {
        return Settings.builder().put(DiscoveryNode.STATELESS_ENABLED_SETTING_NAME, true).build();
    }

    public void testRegisteredByDefaultWhenPropertyAbsent() {
        assertTrue(Federation.readRegistered(key -> null));
    }

    public void testRegisteredByDefaultWhenBlank() {
        assertTrue(Federation.readRegistered(property("   ")));
    }

    public void testRegisteredWhenTrue() {
        assertTrue(Federation.readRegistered(property("true")));
    }

    public void testNotRegisteredWhenFalse() {
        assertFalse(Federation.readRegistered(property("false")));
    }

    public void testInvalidValueFailsFast() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> Federation.readRegistered(property("maybe")));
        assertTrue(e.getMessage().contains(Federation.REGISTER_PROPERTY));
    }

    /**
     * The wiring of {@link Federation#defaultEnabled} to the node settings: a stateful node (no {@code stateless.enabled})
     * is on out of the box whatever the build, and a stateless node follows the build.
     */
    public void testDefaultFollowsNodeTypeAndBuild() {
        assertTrue("stateful nodes are on by default", Federation.FEDERATION_ENABLED.get(Settings.EMPTY));
        assertEquals(Build.current().isSnapshot(), Federation.FEDERATION_ENABLED.get(stateless()));
        assertFalse("the default is not an explicit value", Federation.FEDERATION_ENABLED.exists(Settings.EMPTY));
    }

    /** The platform plays no part in the default; only the node type and, for a stateless node, the build do. */
    public void testDefaultEnabledIsOffOnlyForStatelessRelease() {
        assertTrue("stateful snapshot", Federation.defaultEnabled(false, true));
        assertTrue("stateful release", Federation.defaultEnabled(false, false));
        assertTrue("stateless snapshot", Federation.defaultEnabled(true, true));
        assertFalse("stateless release", Federation.defaultEnabled(true, false));
    }

    /**
     * A Windows release build cannot be made to serve external data sources; every other combination honours the
     * setting. Both branches are exercised on any host because the platform and build are parameters.
     */
    public void testSettingCannotEnableOnWindowsRelease() {
        assertFalse("windows release ignores the opt-in", Federation.enabled(true, true, false));
        assertTrue("windows snapshot honours it", Federation.enabled(true, true, true));
        assertTrue("other release honours it", Federation.enabled(true, false, false));
        assertTrue("other snapshot honours it", Federation.enabled(true, false, true));
    }

    public void testSettingStaysOffWhereNotAskedFor() {
        for (boolean windows : new boolean[] { true, false }) {
            for (boolean snapshot : new boolean[] { true, false }) {
                assertFalse(Federation.enabled(false, windows, snapshot));
            }
        }
    }

    /**
     * Without an explicit value a stateful node has the feature, except on a Windows release build, where nothing can
     * turn it on; a stateless node has it only in a snapshot build (which is never a Windows release, so no extra
     * term is needed there).
     */
    public void testAvailableWithoutSettingFollowsTheDefault() {
        assumeTrue("the test JVM must not unregister the feature", Federation.isRegistered());
        assertEquals(Federation.enabled(true, Constants.WINDOWS, Build.current().isSnapshot()), Federation.isAvailable(Settings.EMPTY));
        assertEquals(Build.current().isSnapshot(), Federation.isAvailable(stateless()));
    }

    public void testNotAvailableWhenSettingFalse() {
        assertFalse(Federation.isAvailable(enabled(false)));
    }

    public void testAvailableWhenRegisteredAndSettingTrue() {
        assumeTrue(
            "the test JVM must not unregister the feature",
            System.getProperty(Federation.REGISTER_PROPERTY) == null || Federation.readRegistered(System::getProperty)
        );
        assertEquals(Federation.enabled(true, Constants.WINDOWS, Build.current().isSnapshot()), Federation.isAvailable(enabled(true)));
    }

    public void testEnsureEnabledIsNoopWhenEnabled() {
        Federation.ensureEnabled(true); // must not throw
    }

    public void testEnsureEnabledThrowsBadRequestWhenDisabled() {
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> Federation.ensureEnabled(false));
        assertEquals(RestStatus.BAD_REQUEST, e.status());
        assertTrue(e.getMessage().contains("external data sources are not available"));
    }

    public void testNotAvailableExceptionIsBadRequest() {
        ElasticsearchStatusException e = Federation.notAvailableException();
        assertEquals(RestStatus.BAD_REQUEST, e.status());
        assertEquals("external data sources are not available", e.getMessage());
    }

    public void testNoSettingsWhenNotRegistered() {
        assertThat(Federation.settings(false), empty());
    }

    /**
     * The whole configuration surface of the feature travels with the registration lever, not just the gate: an
     * operator who unregistered the feature must not be able to configure any part of it.
     */
    public void testSettingsCoverTheGateAndEveryExternalSourceKnob() {
        List<String> keys = Federation.settings(true).stream().map(Setting::getKey).toList();
        assertThat(keys, hasItem(Federation.FEDERATION_ENABLED.getKey()));
        for (Setting<?> setting : ExternalSourceSettings.settings()) {
            assertThat(keys, hasItem(setting.getKey()));
        }
        for (Setting<?> setting : ExternalSourceCacheSettings.settings()) {
            assertThat(keys, hasItem(setting.getKey()));
        }
        assertThat(keys, hasItem(DataSourceService.MAX_DATA_SOURCES_COUNT_SETTING.getKey()));
        assertThat(keys, hasItem(DatasetService.MAX_DATASETS_COUNT_SETTING.getKey()));
        assertThat(keys, hasSize(3 + ExternalSourceSettings.settings().size() + ExternalSourceCacheSettings.settings().size()));
    }

    /**
     * Registration follows the operator property alone, never {@link Federation#FEDERATION_ENABLED}: with the feature
     * registered but not enabled, the rest of the settings are still registered and still updatable, so a deployment can
     * ship its federation configuration before turning it on, or without ever turning it on. Exercised against a real
     * {@link ClusterSettings}, which is what the cluster settings API validates an update against, so this covers the
     * dynamic keys reaching their consumers as well as the update being accepted at all.
     */
    public void testSettingsStayUpdatableWhileFederationIsNotEnabled() {
        Settings nodeSettings = enabled(false);
        ClusterSettings clusterSettings = new ClusterSettings(nodeSettings, Set.copyOf(Federation.settings(true)));

        AtomicBoolean federatedIdentity = new AtomicBoolean();
        clusterSettings.addSettingsUpdateConsumer(ExternalSourceSettings.FEDERATED_IDENTITY_ENABLED, federatedIdentity::set);
        AtomicInteger maxDiscoveredFiles = new AtomicInteger();
        clusterSettings.addSettingsUpdateConsumer(ExternalSourceSettings.MAX_DISCOVERED_FILES, maxDiscoveredFiles::set);
        AtomicInteger maxListedObjects = new AtomicInteger();
        clusterSettings.addSettingsUpdateConsumer(ExternalSourceSettings.MAX_LISTED_OBJECTS, maxListedObjects::set);
        AtomicBoolean cacheEnabled = new AtomicBoolean(true);
        clusterSettings.addSettingsUpdateConsumer(ExternalSourceCacheSettings.CACHE_ENABLED, cacheEnabled::set);
        AtomicInteger maxDataSources = new AtomicInteger();
        clusterSettings.addSettingsUpdateConsumer(DataSourceService.MAX_DATA_SOURCES_COUNT_SETTING, maxDataSources::set);

        Settings update = Settings.builder()
            .put(ExternalSourceSettings.FEDERATED_IDENTITY_ENABLED.getKey(), true)
            .put(ExternalSourceSettings.MAX_DISCOVERED_FILES.getKey(), 42)
            .put(ExternalSourceSettings.MAX_LISTED_OBJECTS.getKey(), 2_000_000)
            .put(ExternalSourceCacheSettings.CACHE_ENABLED.getKey(), false)
            .put(DataSourceService.MAX_DATA_SOURCES_COUNT_SETTING.getKey(), 7)
            .build();
        clusterSettings.validate(update, true);
        clusterSettings.applySettings(update);

        assertTrue("federated identity must be settable while federation is off", federatedIdentity.get());
        assertEquals(42, maxDiscoveredFiles.get());
        assertEquals(2_000_000, maxListedObjects.get());
        assertFalse(cacheEnabled.get());
        assertEquals(7, maxDataSources.get());
    }

    /**
     * An unregistered node never has the setting in its configuration, so what it feeds in as {@code configured} is the
     * default, which is {@code true} on a stateful node; the value must not change what is logged.
     */
    public void testLogsNotRegisteredAtInfo() {
        MockLog.assertThatLogger(
            () -> Federation.logEffectiveState(false, randomBoolean(), false, false, true),
            Federation.class,
            new MockLog.SeenEventExpectation(
                "not registered",
                Federation.class.getCanonicalName(),
                Level.INFO,
                "*not registered*" + Federation.REGISTER_PROPERTY + "*"
            ),
            new MockLog.UnseenEventExpectation("no warning", Federation.class.getCanonicalName(), Level.WARN, "*")
        );
    }

    /**
     * On a Windows release build the default is vetoed by the platform, but on an unregistered node the operator removed
     * the feature on purpose and that is what must be reported: the registration line is logged, and the veto is not.
     */
    @TestLogging(
        value = "org.elasticsearch.xpack.esql.datasources.Federation:DEBUG",
        reason = "asserts that the DEBUG platform veto is not logged, which takes the level being enabled"
    )
    public void testLogsNotRegisteredAtInfoEvenWhereTheDefaultIsVetoed() {
        MockLog.assertThatLogger(
            () -> Federation.logEffectiveState(false, true, false, true, false),
            Federation.class,
            new MockLog.SeenEventExpectation(
                "not registered",
                Federation.class.getCanonicalName(),
                Level.INFO,
                "*not registered*" + Federation.REGISTER_PROPERTY + "*"
            ),
            new MockLog.UnseenEventExpectation("no veto", Federation.class.getCanonicalName(), Level.DEBUG, "*not supported on Windows*"),
            new MockLog.UnseenEventExpectation("no error", Federation.class.getCanonicalName(), Level.ERROR, "*")
        );
    }

    public void testLogsEnabledAtInfo() {
        MockLog.assertThatLogger(
            () -> Federation.logEffectiveState(true, true, randomBoolean(), false, true),
            Federation.class,
            new MockLog.SeenEventExpectation(
                "enabled",
                Federation.class.getCanonicalName(),
                Level.INFO,
                "*is enabled*" + Federation.FEDERATION_ENABLED.getKey() + "*"
            ),
            new MockLog.UnseenEventExpectation("no warning", Federation.class.getCanonicalName(), Level.WARN, "*")
        );
    }

    @TestLogging(
        value = "org.elasticsearch.xpack.esql.datasources.Federation:DEBUG",
        reason = "a registered node with federation off is inert, so it is logged at DEBUG"
    )
    public void testLogsDisabledAtDebug() {
        MockLog.assertThatLogger(
            () -> Federation.logEffectiveState(true, false, randomBoolean(), false, true),
            Federation.class,
            new MockLog.SeenEventExpectation(
                "disabled",
                Federation.class.getCanonicalName(),
                Level.DEBUG,
                "*is disabled*" + Federation.FEDERATION_ENABLED.getKey() + "*"
            ),
            new MockLog.UnseenEventExpectation("no warning", Federation.class.getCanonicalName(), Level.WARN, "*")
        );
    }

    /**
     * Explicitly enabling the feature where it cannot be served is reported, not silently dropped — the one case where
     * the node deliberately does something other than what it was configured to do.
     */
    public void testLogsUnhonourableEnableAtError() {
        MockLog.assertThatLogger(
            () -> Federation.logEffectiveState(true, true, true, true, false),
            Federation.class,
            new MockLog.SeenEventExpectation(
                "ignored opt-in",
                Federation.class.getCanonicalName(),
                Level.ERROR,
                "*" + Federation.FEDERATION_ENABLED.getKey() + "*not supported on Windows*ignoring*"
            )
        );
    }

    /**
     * The default is on, but a Windows release build cannot serve the feature. Nobody asked for anything, so this is
     * not the misconfiguration above: it is logged at DEBUG like any other node without the feature, so that every
     * Windows release node does not start with an ERROR in its log.
     */
    @TestLogging(
        value = "org.elasticsearch.xpack.esql.datasources.Federation:DEBUG",
        reason = "the platform veto of the default is not a misconfiguration, so it is logged at DEBUG"
    )
    public void testLogsVetoedDefaultAtDebugOnWindowsRelease() {
        MockLog.assertThatLogger(
            () -> Federation.logEffectiveState(true, true, false, true, false),
            Federation.class,
            new MockLog.SeenEventExpectation(
                "vetoed default",
                Federation.class.getCanonicalName(),
                Level.DEBUG,
                "*not supported on Windows*default of [" + Federation.FEDERATION_ENABLED.getKey() + "]=[true]*leaving the feature off*"
            ),
            new MockLog.UnseenEventExpectation("no error", Federation.class.getCanonicalName(), Level.ERROR, "*"),
            new MockLog.UnseenEventExpectation("no warning", Federation.class.getCanonicalName(), Level.WARN, "*")
        );
    }

    /** Explicitly turning it off on that platform is not a misconfiguration, so nothing is reported. */
    public void testLogsNoErrorWhenNotAskedForOnWindows() {
        MockLog.assertThatLogger(
            () -> Federation.logEffectiveState(true, false, true, true, false),
            Federation.class,
            new MockLog.UnseenEventExpectation("no error", Federation.class.getCanonicalName(), Level.ERROR, "*")
        );
    }

    /**
     * The wiring of the default and of {@link Federation#enabled} to the real platform. The parameterised tests above
     * prove the rules on any host; this proves the constants are actually fed to them, which only means anything where
     * it runs: the default reads {@code true} on Windows like everywhere else, and whether that makes the feature
     * available is decided by the build.
     */
    public void testDefaultIsOnButVetoedOnWindowsRelease() {
        assumeTrue("verifies the Windows wiring; runs only on Windows", Constants.WINDOWS);
        assumeTrue("the test JVM must not unregister the feature", Federation.isRegistered());
        assertTrue("the default is on, whatever the platform", Federation.FEDERATION_ENABLED.get(Settings.EMPTY));
        assertEquals(Build.current().isSnapshot(), Federation.isAvailable(Settings.EMPTY));
    }

    /**
     * On a Windows release build the feature cannot be reached by configuration. The setting still reads back the
     * value it was given — it is accepted, not rejected, so a node configured this way still starts — but it no
     * longer decides anything.
     */
    public void testCannotBeEnabledOnWindowsRelease() {
        assumeTrue("runs only on a Windows release build", Constants.WINDOWS && Build.current().isSnapshot() == false);
        assertTrue("the setting accepts the value", Federation.FEDERATION_ENABLED.get(enabled(true)));
        assertFalse("but it cannot make the feature available", Federation.isAvailable(enabled(true)));
    }
}
