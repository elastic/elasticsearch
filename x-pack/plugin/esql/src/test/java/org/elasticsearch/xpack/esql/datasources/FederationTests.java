/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.logging.log4j.Level;
import org.elasticsearch.Build;
import org.elasticsearch.ElasticsearchStatusException;
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
 * {@link Federation#settings(boolean)} and {@link Federation#logEffectiveState(boolean, boolean)} seams, which take
 * their inputs as parameters. The end-to-end behavior of both levers at the REST and transport surface is covered by the
 * federation REST ITs.
 */
public class FederationTests extends ESTestCase {

    private static Function<String, String> property(String value) {
        return Map.of(Federation.REGISTER_PROPERTY, value)::get;
    }

    private static Settings enabled(boolean enabled) {
        return Settings.builder().put(Federation.FEDERATION_ENABLED.getKey(), enabled).build();
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

    public void testEnabledByDefaultOnlyInSnapshotBuilds() {
        assertEquals(Build.current().isSnapshot(), Federation.FEDERATION_ENABLED.get(Settings.EMPTY));
    }

    public void testAvailableWithoutSettingOnlyInSnapshotBuilds() {
        assumeTrue("the test JVM must not unregister the feature", Federation.isRegistered());
        assertEquals(Build.current().isSnapshot(), Federation.isAvailable(Settings.EMPTY));
    }

    public void testNotAvailableWhenSettingFalse() {
        assertFalse(Federation.isAvailable(enabled(false)));
    }

    public void testAvailableWhenRegisteredAndSettingTrue() {
        assumeTrue(
            "the test JVM must not unregister the feature",
            System.getProperty(Federation.REGISTER_PROPERTY) == null || Federation.readRegistered(System::getProperty)
        );
        assertTrue(Federation.isAvailable(enabled(true)));
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
        AtomicBoolean cacheEnabled = new AtomicBoolean(true);
        clusterSettings.addSettingsUpdateConsumer(ExternalSourceCacheSettings.CACHE_ENABLED, cacheEnabled::set);
        AtomicInteger maxDataSources = new AtomicInteger();
        clusterSettings.addSettingsUpdateConsumer(DataSourceService.MAX_DATA_SOURCES_COUNT_SETTING, maxDataSources::set);

        Settings update = Settings.builder()
            .put(ExternalSourceSettings.FEDERATED_IDENTITY_ENABLED.getKey(), true)
            .put(ExternalSourceSettings.MAX_DISCOVERED_FILES.getKey(), 42)
            .put(ExternalSourceCacheSettings.CACHE_ENABLED.getKey(), false)
            .put(DataSourceService.MAX_DATA_SOURCES_COUNT_SETTING.getKey(), 7)
            .build();
        clusterSettings.validate(update, true);
        clusterSettings.applySettings(update);

        assertTrue("federated identity must be settable while federation is off", federatedIdentity.get());
        assertEquals(42, maxDiscoveredFiles.get());
        assertFalse(cacheEnabled.get());
        assertEquals(7, maxDataSources.get());
    }

    public void testLogsNotRegisteredAtInfo() {
        MockLog.assertThatLogger(
            () -> Federation.logEffectiveState(false, false),
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

    public void testLogsEnabledAtInfo() {
        MockLog.assertThatLogger(
            () -> Federation.logEffectiveState(true, true),
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
            () -> Federation.logEffectiveState(true, false),
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
}
