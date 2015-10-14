/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.license;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.SysGlobals;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.core.LicenseState;
import org.elasticsearch.license.plugin.core.Licensee;
import org.elasticsearch.license.plugin.core.LicenseeRegistry;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.is;

public class LicenseTests extends ESTestCase {

    private SimpleLicenseeRegistry licenseeRegistry = new SimpleLicenseeRegistry();

    @Test
    public void testPlatinumGoldTrialLicenseCanDoEverything() throws Exception {
        License license = licenseBuilder()
                .type(randomFrom("platinum", "gold", "trial"))
                .build();

        licenseeRegistry.setLicense(license);
        WatcherLicensee watcherLicensee = new WatcherLicensee(Settings.EMPTY, licenseeRegistry);
        licenseeRegistry.register(watcherLicensee);

        assertLicenseGoldPlatinumTrialBehaviour(watcherLicensee);
    }

    @Test
    public void testBasicLicenseIsDisabled() throws Exception {
        License license = licenseBuilder()
                .type("basic")
                .build();

        licenseeRegistry.setLicense(license);
        WatcherLicensee watcherLicensee = new WatcherLicensee(Settings.EMPTY, licenseeRegistry);
        licenseeRegistry.register(watcherLicensee);

        assertLicenseBasicOrNoneBehaviour(watcherLicensee);
    }

    @Test
    public void testNoLicenseDoesNotWork() {
        License license = licenseBuilder()
                .type("basic")
                .build();

        licenseeRegistry.setLicense(license);
        WatcherLicensee watcherLicensee = new WatcherLicensee(Settings.EMPTY, licenseeRegistry);
        licenseeRegistry.register(watcherLicensee);
        licenseeRegistry.disable();

        assertLicenseBasicOrNoneBehaviour(watcherLicensee);
    }

    @Test
    public void testExpiredPlatinumGoldTrialLicenseIsRestricted() throws Exception {
        License license = expiredLicenseBuilder()
                .type(randomFrom("platinum", "gold", "trial"))
                .build();

        licenseeRegistry.setLicense(license);
        WatcherLicensee watcherLicensee = new WatcherLicensee(Settings.EMPTY, licenseeRegistry);
        licenseeRegistry.register(watcherLicensee);

        assertLicenseExpiredBehaviour(watcherLicensee);
    }

    @Test
    public void testUpgradingFromBasicLicenseWorks() {
        License basicLicense = licenseBuilder()
                .type("basic")
                .build();

        licenseeRegistry.setLicense(basicLicense);
        WatcherLicensee watcherLicensee = new WatcherLicensee(Settings.EMPTY, licenseeRegistry);
        licenseeRegistry.register(watcherLicensee);

        assertLicenseBasicOrNoneBehaviour(watcherLicensee);

        License fancyLicense = licenseBuilder()
                .type(randomFrom("platinum", "gold", "trial"))
                .build();

        licenseeRegistry.setLicense(fancyLicense);
        assertLicenseGoldPlatinumTrialBehaviour(watcherLicensee);
    }

    @Test
    public void testDowngradingToBasicLicenseWorks() {
        License basicLicense = licenseBuilder()
                .type(randomFrom("platinum", "gold", "trial"))
                .build();

        licenseeRegistry.setLicense(basicLicense);
        WatcherLicensee watcherLicensee = new WatcherLicensee(Settings.EMPTY, licenseeRegistry);
        licenseeRegistry.register(watcherLicensee);

        assertLicenseGoldPlatinumTrialBehaviour(watcherLicensee);

        License fancyLicense = licenseBuilder()
                .type("basic")
                .build();

        licenseeRegistry.setLicense(fancyLicense);
        assertLicenseBasicOrNoneBehaviour(watcherLicensee);
    }

    @Test
    public void testUpgradingExpiredLicenseWorks() {
        License expiredLicense = expiredLicenseBuilder()
                .type(randomFrom("platinum", "gold", "trial"))
                .build();

        licenseeRegistry.setLicense(expiredLicense);
        WatcherLicensee watcherLicensee = new WatcherLicensee(Settings.EMPTY, licenseeRegistry);
        licenseeRegistry.register(watcherLicensee);

        assertLicenseExpiredBehaviour(watcherLicensee);

        License fancyLicense = licenseBuilder()
                .type(randomFrom("platinum", "gold", "trial"))
                .build();

        licenseeRegistry.setLicense(fancyLicense);
        assertLicenseGoldPlatinumTrialBehaviour(watcherLicensee);
    }

    private void assertLicenseGoldPlatinumTrialBehaviour(WatcherLicensee watcherLicensee) {
        assertThat("Expected putting a watch to be allowed", watcherLicensee.isPutWatchAllowed(), is(true));
        assertThat("Expected getting a watch to be allowed", watcherLicensee.isGetWatchAllowed(), is(true));
        assertThat("Expected watcher transport actions to be allowed", watcherLicensee.isWatcherTransportActionAllowed(), is(true));
        assertThat("Expected actions of a watch to be executed", watcherLicensee.isExecutingActionsAllowed(), is(true));
    }

    private void assertLicenseBasicOrNoneBehaviour(WatcherLicensee watcherLicensee) {
        assertThat("Expected putting a watch not to be allowed", watcherLicensee.isPutWatchAllowed(), is(false));
        assertThat("Expected getting a watch not to be allowed", watcherLicensee.isGetWatchAllowed(), is(false));
        assertThat("Expected watcher transport actions not to be allowed", watcherLicensee.isWatcherTransportActionAllowed(), is(false));
        assertThat("Expected actions of a watch not to be executed", watcherLicensee.isExecutingActionsAllowed(), is(false));
    }

    private void assertLicenseExpiredBehaviour(WatcherLicensee watcherLicensee) {
        assertThat("Expected putting a watch not to be allowed", watcherLicensee.isPutWatchAllowed(), is(false));
        assertThat("Expected getting a watch not to be allowed", watcherLicensee.isGetWatchAllowed(), is(false));
        assertThat("Expected actions of a watch not to be executed", watcherLicensee.isExecutingActionsAllowed(), is(false));
        assertThat("Expected watcher transport actions to be allowed", watcherLicensee.isWatcherTransportActionAllowed(), is(true));
    }

    private License.Builder expiredLicenseBuilder() {
        return licenseBuilder()
                .issueDate(System.currentTimeMillis() - 86400)
                .expiryDate(System.currentTimeMillis() - 1);
    }

    private License.Builder licenseBuilder() {
        return License.builder()
                .issueDate(System.currentTimeMillis())
                .expiryDate(System.currentTimeMillis() + (86400 * 1000))
                .issuedTo("LicensingTests")
                .issuer("test")
                .maxNodes(Integer.MAX_VALUE)
                .signature("_signature")
                .uid(String.valueOf(RandomizedTest.systemPropertyAsInt(SysGlobals.CHILDVM_SYSPROP_JVM_ID, 0)) + System.identityHashCode(LicenseTests.class));
    }

    public static class SimpleLicenseeRegistry extends AbstractComponent implements LicenseeRegistry {

        private final List<Licensee> licensees = new ArrayList<>();
        private License license;

        public SimpleLicenseeRegistry() {
            super(Settings.EMPTY);
        }

        @Override
        public void register(Licensee licensee) {
            licensees.add(licensee);
            enable();
        }

        public void enable() {
            for (Licensee licensee : licensees) {
                licensee.onChange(license, randomBoolean() ? LicenseState.GRACE_PERIOD : LicenseState.ENABLED);
            }
        }

        public void disable() {
            for (Licensee licensee : licensees) {
                licensee.onChange(license, LicenseState.DISABLED);
            }
        }

        public void setLicense(License newLicense) {
            license = newLicense;
            enable();
        }
    }
}
