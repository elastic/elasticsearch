/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.logging.log4j.Level;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;

import java.util.Map;
import java.util.function.Function;

/**
 * Unit tests for {@link Federation}, the availability gate for external data sources and datasets. The registered state
 * comes from a system property read into a {@code static final} at class load and cannot be flipped in-JVM, so property
 * parsing, the enforcement branch, and the startup logging are exercised through the package-private
 * {@link Federation#readRegistered}, {@link Federation#ensureEnabled(boolean)} and
 * {@link Federation#logEffectiveState(boolean, boolean)} seams, which take their inputs as parameters. The end-to-end
 * behavior of both levers at the REST and transport surface is covered by the federation REST ITs.
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

    public void testDisabledByDefault() {
        assertFalse(Federation.FEDERATION_ENABLED.get(Settings.EMPTY));
    }

    public void testNotAvailableWhenSettingAbsent() {
        assertFalse(Federation.isAvailable(Settings.EMPTY));
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

    public void testLogsWarningWhenSettingCannotTakeEffect() {
        MockLog.assertThatLogger(
            () -> Federation.logEffectiveState(false, true),
            Federation.class,
            new MockLog.SeenEventExpectation(
                "ineffective setting warning naming both levers",
                Federation.class.getCanonicalName(),
                Level.WARN,
                "*" + Federation.FEDERATION_ENABLED.getKey() + "*" + Federation.REGISTER_PROPERTY + "*"
            )
        );
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

    public void testLogsDefaultOffAtInfo() {
        MockLog.assertThatLogger(
            () -> Federation.logEffectiveState(true, false),
            Federation.class,
            new MockLog.SeenEventExpectation(
                "disabled",
                Federation.class.getCanonicalName(),
                Level.INFO,
                "*is disabled*" + Federation.FEDERATION_ENABLED.getKey() + "*"
            ),
            new MockLog.UnseenEventExpectation("no warning", Federation.class.getCanonicalName(), Level.WARN, "*")
        );
    }
}
