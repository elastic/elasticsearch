/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.license;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.core.License.OperationMode;
import org.elasticsearch.license.plugin.core.AbstractLicenseeTestCase;
import org.elasticsearch.license.plugin.core.Licensee.Status;
import org.elasticsearch.xpack.monitoring.MonitoringLicensee;

import java.util.function.Predicate;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests {@link MonitoringLicensee}.
 * <p>
 * If you change the behavior of these tests, then it means that licensing changes for Monitoring!
 */
public class MonitoringLicenseeTests extends AbstractLicenseeTestCase {
    private final MonitoringLicensee licensee = new MonitoringLicensee(Settings.EMPTY);

    public void testAcknowledgementMessagesToAnyFromFreeIsNoOp() {
        assertEmptyAck(OperationMode.BASIC, randomMode(), licensee);
    }

    public void testAcknowledgementMessagesToTrialGoldOrPlatinumFromAnyIsNoOp() {
        assertEmptyAck(randomMode(), randomTrialStandardGoldOrPlatinumMode(), licensee);
    }

    public void testAcknowledgementMessagesToBasicFromNotBasicNotesLimits() {
        OperationMode from = randomFrom(OperationMode.STANDARD, OperationMode.GOLD, OperationMode.PLATINUM, OperationMode.TRIAL);
        OperationMode to = OperationMode.BASIC;

        String[] messages = ackLicenseChange(from, to, licensee);

        // leaving messages up to inspection
        assertThat(fromToMessage(from, to), messages.length, equalTo(2));
    }

    public void testCollectionEnabledIsTrueForActiveState() {
        assertEnabled(true, MonitoringLicensee::collectionEnabled, true);
    }

    public void testCollectionEnabledIsFalseForInactiveState() {
        assertEnabled(false, MonitoringLicensee::collectionEnabled, false);
    }

    public void testCleaningEnabledIsTrueForActiveState() {
        assertEnabled(true, MonitoringLicensee::cleaningEnabled, true);
    }

    public void testCleaningEnabledIsFalseForInactiveState() {
        assertEnabled(false, MonitoringLicensee::cleaningEnabled, false);
    }

    public void testAllowUpdateRetentionIsTrueForNotBasic() {
        OperationMode mode = randomFrom(OperationMode.STANDARD, OperationMode.GOLD, OperationMode.PLATINUM, OperationMode.TRIAL);
        assertEnabled(mode, MonitoringLicensee::allowUpdateRetention, true);
    }

    public void testAllowUpdateRetentionIsFalseForBasic() {
        assertEnabled(OperationMode.BASIC, MonitoringLicensee::allowUpdateRetention, false);
    }

    public void testAllowUpdateRetentionIsFalseForMissing() {
        assertEnabled(OperationMode.MISSING, MonitoringLicensee::allowUpdateRetention, false);
    }

    /**
     * Assert that the {@link #licensee} is {@code predicate}d as {@code expected} when setting the {@code state}.
     *
     * @param active The state that should cause the {@code expected} {@code predicate}.
     * @param predicate The method to invoke (expected to be an instance method).
     * @param expected The expected outcome given the {@code state} and {@code predicate}.
     */
    private void assertEnabled(boolean active, Predicate<MonitoringLicensee> predicate, boolean expected) {
        Status status = mock(Status.class);
        when(status.isActive()).thenReturn(active);

        licensee.onChange(status);

        assertThat(predicate.test(licensee), equalTo(expected));

        verify(status).isActive();
    }

    /**
     * Assert that the {@link #licensee} is {@code predicate}d as {@code expected} when setting the {@code mode}.
     *
     * @param mode The mode that should cause the {@code expected} {@code predicate}.
     * @param predicate The method to invoke (expected to be an instance method).
     * @param expected The expected outcome given the {@code mode} and {@code predicate}.
     */
    private void assertEnabled(OperationMode mode, Predicate<MonitoringLicensee> predicate, boolean expected) {
        Status status = mock(Status.class);
        when(status.getMode()).thenReturn(mode);

        licensee.onChange(status);

        assertThat(predicate.test(licensee), equalTo(expected));

        verify(status).getMode();
    }
}
