/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.core.License.OperationMode;
import org.elasticsearch.license.plugin.core.AbstractLicenseeTestCase;
import org.elasticsearch.license.plugin.core.Licensee.Status;
import org.elasticsearch.license.plugin.core.LicenseeRegistry;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * Tests {@link SecurityLicensee}.
 * <p>
 * If you change the behavior of these tests, then it means that licensing changes for Security!
 */
public class ShieldLicenseeTests extends AbstractLicenseeTestCase {
    private final SecurityLicenseState shieldState = mock(SecurityLicenseState.class);
    private final LicenseeRegistry registry = mock(LicenseeRegistry.class);

    public void testStartsWithoutTribeNode() {
        SecurityLicensee licensee = new SecurityLicensee(Settings.EMPTY, registry, shieldState);

        // starting the Licensee start trigger it being registered
        licensee.start();

        verify(registry).register(licensee);
        verifyNoMoreInteractions(registry, shieldState);
    }

    public void testDoesNotStartWithTribeNode() {
        Settings settings = Settings.builder().put("tribe.fake.cluster.name", "notchecked").build();
        SecurityLicensee licensee = new SecurityLicensee(settings, registry, shieldState);

        // starting the Licensee as a tribe node should not trigger it being registered
        licensee.start();

        verifyNoMoreInteractions(registry, shieldState);
    }

    public void testOnChangeModifiesShieldLicenseState() {
        Status status = mock(Status.class);

        SecurityLicensee licensee = new SecurityLicensee(Settings.EMPTY, registry, shieldState);

        licensee.onChange(status);

        assertSame(status, licensee.getStatus());

        verify(shieldState).updateStatus(status);
        verifyNoMoreInteractions(registry, shieldState);
    }

    public void testAcknowledgementMessagesFromBasicToAnyNotGoldOrStandardIsNoOp() {
        assertEmptyAck(OperationMode.BASIC,
                randomFrom(OperationMode.values(), mode -> mode != OperationMode.GOLD && mode != OperationMode.STANDARD),
                this::buildLicensee);
    }

    public void testAcknowledgementMessagesFromAnyToTrialOrPlatinumIsNoOp() {
        assertEmptyAck(randomMode(), randomTrialOrPlatinumMode(), this::buildLicensee);
    }

    public void testAcknowledgementMessagesFromTrialStandardGoldOrPlatinumToBasicNotesLimits() {
        OperationMode from = randomTrialStandardGoldOrPlatinumMode();
        OperationMode to = OperationMode.BASIC;

        String[] messages = ackLicenseChange(from, to, this::buildLicensee);

        // leaving messages up to inspection
        assertThat(fromToMessage(from, to), messages.length, equalTo(3));
    }

    public void testAcknowlegmentMessagesFromAnyToStandardNotesLimits() {
        OperationMode from = randomFrom(OperationMode.BASIC, OperationMode.GOLD, OperationMode.PLATINUM, OperationMode.TRIAL);
        OperationMode to = OperationMode.STANDARD;

        String[] messages = ackLicenseChange(from, to, this::buildLicensee);

        // leaving messages up to inspection
        assertThat(fromToMessage(from, to), messages.length, equalTo(4));
    }

    public void testAcknowledgementMessagesFromBasicStandardTrialOrPlatinumToGoldNotesLimits() {
        String[] messages = ackLicenseChange(randomModeExcept(OperationMode.GOLD), OperationMode.GOLD, this::buildLicensee);

        // leaving messages up to inspection
        assertThat(messages.length, equalTo(2));
    }

    private SecurityLicensee buildLicensee() {
        return new SecurityLicensee(Settings.EMPTY, registry, shieldState);
    }
}
