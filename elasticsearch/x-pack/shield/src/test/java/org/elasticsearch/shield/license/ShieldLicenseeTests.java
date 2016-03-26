/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.license;

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
 * Tests {@link ShieldLicensee}.
 * <p>
 * If you change the behavior of these tests, then it means that licensing changes for Security!
 */
public class ShieldLicenseeTests extends AbstractLicenseeTestCase {
    private final ShieldLicenseState shieldState = mock(ShieldLicenseState.class);
    private final LicenseeRegistry registry = mock(LicenseeRegistry.class);

    public void testStartsWithoutTribeNode() {
        ShieldLicensee licensee = new ShieldLicensee(Settings.EMPTY, registry, shieldState);

        // starting the Licensee start trigger it being registered
        licensee.start();

        verify(registry).register(licensee);
        verifyNoMoreInteractions(registry, shieldState);
    }

    public void testDoesNotStartWithTribeNode() {
        Settings settings = Settings.settingsBuilder().put("tribe.fake.cluster.name", "notchecked").build();
        ShieldLicensee licensee = new ShieldLicensee(settings, registry, shieldState);

        // starting the Licensee as a tribe node should not trigger it being registered
        licensee.start();

        verifyNoMoreInteractions(registry, shieldState);
    }

    public void testOnChangeModifiesShieldLicenseState() {
        Status status = mock(Status.class);

        ShieldLicensee licensee = new ShieldLicensee(Settings.EMPTY, registry, shieldState);

        licensee.onChange(status);

        assertSame(status, licensee.getStatus());

        verify(shieldState).updateStatus(status);
        verifyNoMoreInteractions(registry, shieldState);
    }

    public void testAcknowledgementMessagesFromBasicToAnyIsNoOp() {
        assertEmptyAck(OperationMode.BASIC, randomMode(), this::buildLicensee);
    }

    public void testAcknowledgementMessagesFromAnyToTrialOrPlatinumIsNoOp() {
        assertEmptyAck(randomMode(), randomTrialOrPlatinumMode(), this::buildLicensee);
    }

    public void testAcknowledgementMessagesFromTrialGoldOrPlatinumToBasicNotesLimits() {
        String[] messages = ackLicenseChange(randomTrialGoldOrPlatinumMode(), OperationMode.BASIC, this::buildLicensee);

        // leaving messages up to inspection
        assertThat(messages.length, equalTo(3));
    }

    public void testAcknowledgementMessagesFromTrialOrPlatinumToGoldNotesLimits() {
        String[] messages = ackLicenseChange(randomTrialOrPlatinumMode(), OperationMode.GOLD, this::buildLicensee);

        // leaving messages up to inspection
        assertThat(messages.length, equalTo(2));
    }

    private ShieldLicensee buildLicensee() {
        return new ShieldLicensee(Settings.EMPTY, registry, shieldState);
    }
}
