/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.license;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.core.License.OperationMode;
import org.elasticsearch.license.plugin.core.AbstractLicenseeTestCase;
import org.elasticsearch.license.plugin.core.LicenseState;
import org.elasticsearch.license.plugin.core.Licensee.Status;
import org.elasticsearch.license.plugin.core.LicenseeRegistry;

import java.util.function.Predicate;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Tests {@link MarvelLicensee}.
 * <p>
 * If you change the behavior of these tests, then it means that licensing changes for Monitoring!
 */
public class MarvelLicenseeTests extends AbstractLicenseeTestCase {
    private final LicenseeRegistry registry = mock(LicenseeRegistry.class);
    private final MarvelLicensee licensee = new MarvelLicensee(Settings.EMPTY, registry);

    public void testAcknowledgementMessagesToAnyFromFreeIsNoOp() {
        assertEmptyAck(OperationMode.BASIC, randomMode(), licensee);
    }

    public void testAcknowledgementMessagesToTrialGoldOrPlatinumFromAnyIsNoOp() {
        assertEmptyAck(randomMode(), randomTrialStandardGoldOrPlatinumMode(), licensee);
    }

    public void testAcknowledgementMessagesToBasicFromNotBasicNotesLimits() {
        String[] messages = ackLicenseChange(randomModeExcept(OperationMode.BASIC), OperationMode.BASIC, licensee);

        // leaving messages up to inspection
        assertThat(messages.length, equalTo(2));
    }

    public void testCollectionEnabledIsTrueForActiveState() {
        assertEnabled(randomEnabledOrGracePeriodState(), MarvelLicensee::collectionEnabled, true);
    }

    public void testCollectionEnabledIsFalseForInactiveState() {
        assertEnabled(LicenseState.DISABLED, MarvelLicensee::collectionEnabled, false);
    }

    public void testCleaningEnabledIsTrueForActiveState() {
        assertEnabled(randomEnabledOrGracePeriodState(), MarvelLicensee::cleaningEnabled, true);
    }

    public void testCleaningEnabledIsFalseForInactiveState() {
        assertEnabled(LicenseState.DISABLED, MarvelLicensee::cleaningEnabled, false);
    }

    public void testAllowUpdateRetentionIsTrueForNotBasic() {
        assertEnabled(randomModeExcept(OperationMode.BASIC), MarvelLicensee::allowUpdateRetention, true);
    }

    public void testAllowUpdateRetentionIsFalseForBasic() {
        assertEnabled(OperationMode.BASIC, MarvelLicensee::allowUpdateRetention, false);
    }

    /**
     * Assert that the {@link #licensee} is {@code predicate}d as {@code expected} when setting the {@code state}.
     *
     * @param state The state that should cause the {@code expected} {@code predicate}.
     * @param predicate The method to invoke (expected to be an instance method).
     * @param expected The expected outcome given the {@code state} and {@code predicate}.
     */
    private void assertEnabled(LicenseState state, Predicate<MarvelLicensee> predicate, boolean expected) {
        Status status = mock(Status.class);
        when(status.getLicenseState()).thenReturn(state);

        licensee.onChange(status);

        assertThat(predicate.test(licensee), equalTo(expected));

        verify(status).getLicenseState();
        verifyNoMoreInteractions(registry);
    }

    /**
     * Assert that the {@link #licensee} is {@code predicate}d as {@code expected} when setting the {@code mode}.
     *
     * @param mode The mode that should cause the {@code expected} {@code predicate}.
     * @param predicate The method to invoke (expected to be an instance method).
     * @param expected The expected outcome given the {@code mode} and {@code predicate}.
     */
    private void assertEnabled(OperationMode mode, Predicate<MarvelLicensee> predicate, boolean expected) {
        Status status = mock(Status.class);
        when(status.getMode()).thenReturn(mode);

        licensee.onChange(status);

        assertThat(predicate.test(licensee), equalTo(expected));

        verify(status).getMode();
        verifyNoMoreInteractions(registry);
    }
}
