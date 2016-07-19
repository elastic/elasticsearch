/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.core.License.OperationMode;
import org.elasticsearch.license.plugin.core.AbstractLicenseeTestCase;
import org.elasticsearch.license.plugin.core.Licensee.Status;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * Tests {@link SecurityLicensee}.
 * <p>
 * If you change the behavior of these tests, then it means that licensing changes for Security!
 */
public class SecurityLicenseeTests extends AbstractLicenseeTestCase {
    private final SecurityLicenseState securityLicenseState = mock(SecurityLicenseState.class);

    public void testOnChangeModifiesSecurityLicenseState() {
        Status status = mock(Status.class);

        SecurityLicensee licensee = new SecurityLicensee(Settings.EMPTY, securityLicenseState);

        licensee.onChange(status);

        assertSame(status, licensee.getStatus());

        verify(securityLicenseState).updateStatus(status);
        verifyNoMoreInteractions(securityLicenseState);
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
        OperationMode from = randomFrom(OperationMode.BASIC, OperationMode.PLATINUM, OperationMode.TRIAL, OperationMode.STANDARD);
        String[] messages = ackLicenseChange(from, OperationMode.GOLD, this::buildLicensee);

        // leaving messages up to inspection
        assertThat(messages.length, equalTo(2));
    }

    private SecurityLicensee buildLicensee() {
        return new SecurityLicensee(Settings.EMPTY, securityLicenseState);
    }
}
