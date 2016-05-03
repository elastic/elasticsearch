/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.license.core.License;
import org.elasticsearch.license.core.License.OperationMode;
import org.elasticsearch.license.plugin.core.LicenseState;
import org.elasticsearch.license.plugin.core.Licensee;
import org.elasticsearch.shield.SecurityLicenseState.EnabledRealmType;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;

/**
 * Unit tests for the {@link SecurityLicenseState}
 */
public class ShieldLicenseStateTests extends ESTestCase {

    public void testDefaults() {
        SecurityLicenseState licenseState = new SecurityLicenseState();
        assertThat(licenseState.authenticationAndAuthorizationEnabled(), is(true));
        assertThat(licenseState.ipFilteringEnabled(), is(true));
        assertThat(licenseState.auditingEnabled(), is(true));
        assertThat(licenseState.statsAndHealthEnabled(), is(true));
        assertThat(licenseState.documentAndFieldLevelSecurityEnabled(), is(true));
        assertThat(licenseState.enabledRealmType(), is(EnabledRealmType.ALL));
    }

    public void testBasic() {
        SecurityLicenseState licenseState = new SecurityLicenseState();
        licenseState.updateStatus(new Licensee.Status(License.OperationMode.BASIC,
                randomBoolean() ? LicenseState.ENABLED : LicenseState.GRACE_PERIOD));

        assertThat(licenseState.authenticationAndAuthorizationEnabled(), is(false));
        assertThat(licenseState.ipFilteringEnabled(), is(false));
        assertThat(licenseState.auditingEnabled(), is(false));
        assertThat(licenseState.statsAndHealthEnabled(), is(true));
        assertThat(licenseState.documentAndFieldLevelSecurityEnabled(), is(false));
        assertThat(licenseState.enabledRealmType(), is(EnabledRealmType.NONE));
    }

    public void testBasicExpired() {
        SecurityLicenseState licenseState = new SecurityLicenseState();
        licenseState.updateStatus(new Licensee.Status(License.OperationMode.BASIC, LicenseState.DISABLED));

        assertThat(licenseState.authenticationAndAuthorizationEnabled(), is(false));
        assertThat(licenseState.ipFilteringEnabled(), is(false));
        assertThat(licenseState.auditingEnabled(), is(false));
        assertThat(licenseState.statsAndHealthEnabled(), is(false));
        assertThat(licenseState.documentAndFieldLevelSecurityEnabled(), is(false));
        assertThat(licenseState.enabledRealmType(), is(EnabledRealmType.NONE));
    }

    public void testStandard() {
        SecurityLicenseState licenseState = new SecurityLicenseState();
        licenseState.updateStatus(new Licensee.Status(OperationMode.STANDARD,
                randomBoolean() ? LicenseState.ENABLED : LicenseState.GRACE_PERIOD));

        assertThat(licenseState.authenticationAndAuthorizationEnabled(), is(true));
        assertThat(licenseState.ipFilteringEnabled(), is(false));
        assertThat(licenseState.auditingEnabled(), is(false));
        assertThat(licenseState.statsAndHealthEnabled(), is(true));
        assertThat(licenseState.documentAndFieldLevelSecurityEnabled(), is(false));
        assertThat(licenseState.enabledRealmType(), is(EnabledRealmType.NATIVE));
    }

    public void testStandardExpired() {
        SecurityLicenseState licenseState = new SecurityLicenseState();
        licenseState.updateStatus(new Licensee.Status(OperationMode.STANDARD, LicenseState.DISABLED));

        assertThat(licenseState.authenticationAndAuthorizationEnabled(), is(true));
        assertThat(licenseState.ipFilteringEnabled(), is(false));
        assertThat(licenseState.auditingEnabled(), is(false));
        assertThat(licenseState.statsAndHealthEnabled(), is(false));
        assertThat(licenseState.documentAndFieldLevelSecurityEnabled(), is(false));
        assertThat(licenseState.enabledRealmType(), is(EnabledRealmType.NATIVE));
    }

    public void testGold() {
        SecurityLicenseState licenseState = new SecurityLicenseState();
        licenseState.updateStatus(new Licensee.Status(License.OperationMode.GOLD,
                randomBoolean() ? LicenseState.ENABLED : LicenseState.GRACE_PERIOD));

        assertThat(licenseState.authenticationAndAuthorizationEnabled(), is(true));
        assertThat(licenseState.ipFilteringEnabled(), is(true));
        assertThat(licenseState.auditingEnabled(), is(true));
        assertThat(licenseState.statsAndHealthEnabled(), is(true));
        assertThat(licenseState.documentAndFieldLevelSecurityEnabled(), is(false));
        assertThat(licenseState.enabledRealmType(), is(EnabledRealmType.DEFAULT));
    }

    public void testGoldExpired() {
        SecurityLicenseState licenseState = new SecurityLicenseState();
        licenseState.updateStatus(new Licensee.Status(License.OperationMode.GOLD, LicenseState.DISABLED));

        assertThat(licenseState.authenticationAndAuthorizationEnabled(), is(true));
        assertThat(licenseState.ipFilteringEnabled(), is(true));
        assertThat(licenseState.auditingEnabled(), is(true));
        assertThat(licenseState.statsAndHealthEnabled(), is(false));
        assertThat(licenseState.documentAndFieldLevelSecurityEnabled(), is(false));
        assertThat(licenseState.enabledRealmType(), is(EnabledRealmType.DEFAULT));
    }

    public void testPlatinum() {
        SecurityLicenseState licenseState = new SecurityLicenseState();
        licenseState.updateStatus(new Licensee.Status(License.OperationMode.PLATINUM,
                randomBoolean() ? LicenseState.ENABLED : LicenseState.GRACE_PERIOD));

        assertThat(licenseState.authenticationAndAuthorizationEnabled(), is(true));
        assertThat(licenseState.ipFilteringEnabled(), is(true));
        assertThat(licenseState.auditingEnabled(), is(true));
        assertThat(licenseState.statsAndHealthEnabled(), is(true));
        assertThat(licenseState.documentAndFieldLevelSecurityEnabled(), is(true));
        assertThat(licenseState.enabledRealmType(), is(EnabledRealmType.ALL));
    }

    public void testPlatinumExpired() {
        SecurityLicenseState licenseState = new SecurityLicenseState();
        licenseState.updateStatus(new Licensee.Status(License.OperationMode.PLATINUM, LicenseState.DISABLED));

        assertThat(licenseState.authenticationAndAuthorizationEnabled(), is(true));
        assertThat(licenseState.ipFilteringEnabled(), is(true));
        assertThat(licenseState.auditingEnabled(), is(true));
        assertThat(licenseState.statsAndHealthEnabled(), is(false));
        assertThat(licenseState.documentAndFieldLevelSecurityEnabled(), is(true));
        assertThat(licenseState.enabledRealmType(), is(EnabledRealmType.ALL));
    }
}
