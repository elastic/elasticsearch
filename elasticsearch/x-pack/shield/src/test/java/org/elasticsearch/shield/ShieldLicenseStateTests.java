/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.core.LicenseState;
import org.elasticsearch.license.plugin.core.Licensee;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;

/**
 * Unit tests for the {@link SecurityLicenseState}
 */
public class ShieldLicenseStateTests extends ESTestCase {

    public void testDefaults() {
        SecurityLicenseState licenseState = new SecurityLicenseState();
        assertThat(licenseState.securityEnabled(), is(true));
        assertThat(licenseState.statsAndHealthEnabled(), is(true));
        assertThat(licenseState.documentAndFieldLevelSecurityEnabled(), is(true));
        assertThat(licenseState.customRealmsEnabled(), is(true));
    }

    public void testBasic() {
        SecurityLicenseState licenseState = new SecurityLicenseState();
        licenseState.updateStatus(new Licensee.Status(License.OperationMode.BASIC,
                randomBoolean() ? LicenseState.ENABLED : LicenseState.GRACE_PERIOD));

        assertThat(licenseState.securityEnabled(), is(false));
        assertThat(licenseState.statsAndHealthEnabled(), is(true));
        assertThat(licenseState.documentAndFieldLevelSecurityEnabled(), is(false));
        assertThat(licenseState.customRealmsEnabled(), is(false));
    }

    public void testBasicExpired() {
        SecurityLicenseState licenseState = new SecurityLicenseState();
        licenseState.updateStatus(new Licensee.Status(License.OperationMode.BASIC, LicenseState.DISABLED));

        assertThat(licenseState.securityEnabled(), is(false));
        assertThat(licenseState.statsAndHealthEnabled(), is(false));
        assertThat(licenseState.documentAndFieldLevelSecurityEnabled(), is(false));
        assertThat(licenseState.customRealmsEnabled(), is(false));
    }

    public void testGold() {
        SecurityLicenseState licenseState = new SecurityLicenseState();
        licenseState.updateStatus(new Licensee.Status(License.OperationMode.GOLD,
                randomBoolean() ? LicenseState.ENABLED : LicenseState.GRACE_PERIOD));

        assertThat(licenseState.securityEnabled(), is(true));
        assertThat(licenseState.statsAndHealthEnabled(), is(true));
        assertThat(licenseState.documentAndFieldLevelSecurityEnabled(), is(false));
        assertThat(licenseState.customRealmsEnabled(), is(false));
    }

    public void testGoldExpired() {
        SecurityLicenseState licenseState = new SecurityLicenseState();
        licenseState.updateStatus(new Licensee.Status(License.OperationMode.GOLD, LicenseState.DISABLED));

        assertThat(licenseState.securityEnabled(), is(true));
        assertThat(licenseState.statsAndHealthEnabled(), is(false));
        assertThat(licenseState.documentAndFieldLevelSecurityEnabled(), is(false));
        assertThat(licenseState.customRealmsEnabled(), is(false));
    }

    public void testPlatinum() {
        SecurityLicenseState licenseState = new SecurityLicenseState();
        licenseState.updateStatus(new Licensee.Status(License.OperationMode.PLATINUM,
                randomBoolean() ? LicenseState.ENABLED : LicenseState.GRACE_PERIOD));

        assertThat(licenseState.securityEnabled(), is(true));
        assertThat(licenseState.statsAndHealthEnabled(), is(true));
        assertThat(licenseState.documentAndFieldLevelSecurityEnabled(), is(true));
        assertThat(licenseState.customRealmsEnabled(), is(true));
    }

    public void testPlatinumExpired() {
        SecurityLicenseState licenseState = new SecurityLicenseState();
        licenseState.updateStatus(new Licensee.Status(License.OperationMode.PLATINUM, LicenseState.DISABLED));

        assertThat(licenseState.securityEnabled(), is(true));
        assertThat(licenseState.statsAndHealthEnabled(), is(false));
        assertThat(licenseState.documentAndFieldLevelSecurityEnabled(), is(true));
        assertThat(licenseState.customRealmsEnabled(), is(true));
    }
}
