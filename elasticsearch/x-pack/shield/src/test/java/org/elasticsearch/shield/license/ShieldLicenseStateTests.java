/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.license;

import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.core.LicenseState;
import org.elasticsearch.license.plugin.core.Licensee;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;

/**
 * Unit tests for the {@link ShieldLicenseState}
 */
public class ShieldLicenseStateTests extends ESTestCase {

    public void testDefaults() {
        ShieldLicenseState licenseState = new ShieldLicenseState();
        assertThat(licenseState.securityEnabled(), is(true));
        assertThat(licenseState.statsAndHealthEnabled(), is(true));
        assertThat(licenseState.documentAndFieldLevelSecurityEnabled(), is(true));
        assertThat(licenseState.customRealmsEnabled(), is(true));
    }

    public void testBasic() {
        ShieldLicenseState licenseState = new ShieldLicenseState();
        licenseState.updateStatus(new Licensee.Status(License.OperationMode.BASIC, randomBoolean() ? LicenseState.ENABLED : LicenseState.GRACE_PERIOD));

        assertThat(licenseState.securityEnabled(), is(false));
        assertThat(licenseState.statsAndHealthEnabled(), is(true));
        assertThat(licenseState.documentAndFieldLevelSecurityEnabled(), is(false));
        assertThat(licenseState.customRealmsEnabled(), is(false));
    }

    public void testBasicExpired() {
        ShieldLicenseState licenseState = new ShieldLicenseState();
        licenseState.updateStatus(new Licensee.Status(License.OperationMode.BASIC, LicenseState.DISABLED));

        assertThat(licenseState.securityEnabled(), is(false));
        assertThat(licenseState.statsAndHealthEnabled(), is(false));
        assertThat(licenseState.documentAndFieldLevelSecurityEnabled(), is(false));
        assertThat(licenseState.customRealmsEnabled(), is(false));
    }

    public void testGold() {
        ShieldLicenseState licenseState = new ShieldLicenseState();
        licenseState.updateStatus(new Licensee.Status(License.OperationMode.GOLD, randomBoolean() ? LicenseState.ENABLED : LicenseState.GRACE_PERIOD));

        assertThat(licenseState.securityEnabled(), is(true));
        assertThat(licenseState.statsAndHealthEnabled(), is(true));
        assertThat(licenseState.documentAndFieldLevelSecurityEnabled(), is(false));
        assertThat(licenseState.customRealmsEnabled(), is(false));
    }

    public void testGoldExpired() {
        ShieldLicenseState licenseState = new ShieldLicenseState();
        licenseState.updateStatus(new Licensee.Status(License.OperationMode.GOLD, LicenseState.DISABLED));

        assertThat(licenseState.securityEnabled(), is(true));
        assertThat(licenseState.statsAndHealthEnabled(), is(false));
        assertThat(licenseState.documentAndFieldLevelSecurityEnabled(), is(false));
        assertThat(licenseState.customRealmsEnabled(), is(false));
    }

    public void testPlatinum() {
        ShieldLicenseState licenseState = new ShieldLicenseState();
        licenseState.updateStatus(new Licensee.Status(License.OperationMode.PLATINUM, randomBoolean() ? LicenseState.ENABLED : LicenseState.GRACE_PERIOD));

        assertThat(licenseState.securityEnabled(), is(true));
        assertThat(licenseState.statsAndHealthEnabled(), is(true));
        assertThat(licenseState.documentAndFieldLevelSecurityEnabled(), is(true));
        assertThat(licenseState.customRealmsEnabled(), is(true));
    }

    public void testPlatinumExpired() {
        ShieldLicenseState licenseState = new ShieldLicenseState();
        licenseState.updateStatus(new Licensee.Status(License.OperationMode.PLATINUM, LicenseState.DISABLED));

        assertThat(licenseState.securityEnabled(), is(true));
        assertThat(licenseState.statsAndHealthEnabled(), is(false));
        assertThat(licenseState.documentAndFieldLevelSecurityEnabled(), is(true));
        assertThat(licenseState.customRealmsEnabled(), is(true));
    }
}
