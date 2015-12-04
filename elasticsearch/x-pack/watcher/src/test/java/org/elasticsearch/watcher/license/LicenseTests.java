/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.license;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.core.LicenseState;
import org.elasticsearch.license.plugin.core.Licensee;
import org.elasticsearch.license.plugin.core.LicenseeRegistry;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.is;

public class LicenseTests extends ESTestCase {

    private SimpleLicenseeRegistry licenseeRegistry = new SimpleLicenseeRegistry();

    public void testPlatinumGoldTrialLicenseCanDoEverything() throws Exception {
        licenseeRegistry.setOperationMode(randomFrom(License.OperationMode.PLATINUM, License.OperationMode.GOLD, License.OperationMode.TRIAL));
        WatcherLicensee watcherLicensee = new WatcherLicensee(Settings.EMPTY, licenseeRegistry);
        licenseeRegistry.register(watcherLicensee);

        assertLicenseGoldPlatinumTrialBehaviour(watcherLicensee);
    }

    public void testBasicLicenseIsDisabled() throws Exception {
        licenseeRegistry.setOperationMode(License.OperationMode.BASIC);
        WatcherLicensee watcherLicensee = new WatcherLicensee(Settings.EMPTY, licenseeRegistry);
        licenseeRegistry.register(watcherLicensee);

        assertLicenseBasicOrNoneOrExpiredBehaviour(watcherLicensee);
    }

    public void testNoLicenseDoesNotWork() {
        licenseeRegistry.setOperationMode(License.OperationMode.BASIC);
        WatcherLicensee watcherLicensee = new WatcherLicensee(Settings.EMPTY, licenseeRegistry);
        licenseeRegistry.register(watcherLicensee);
        licenseeRegistry.disable();

        assertLicenseBasicOrNoneOrExpiredBehaviour(watcherLicensee);
    }

    public void testExpiredPlatinumGoldTrialLicenseIsRestricted() throws Exception {
        licenseeRegistry.setOperationMode(randomFrom(License.OperationMode.PLATINUM, License.OperationMode.GOLD, License.OperationMode.TRIAL));
        WatcherLicensee watcherLicensee = new WatcherLicensee(Settings.EMPTY, licenseeRegistry);
        licenseeRegistry.register(watcherLicensee);
        licenseeRegistry.disable();

        assertLicenseBasicOrNoneOrExpiredBehaviour(watcherLicensee);
    }

    public void testUpgradingFromBasicLicenseWorks() {
        licenseeRegistry.setOperationMode(License.OperationMode.BASIC);
        WatcherLicensee watcherLicensee = new WatcherLicensee(Settings.EMPTY, licenseeRegistry);
        licenseeRegistry.register(watcherLicensee);

        assertLicenseBasicOrNoneOrExpiredBehaviour(watcherLicensee);

        licenseeRegistry.setOperationMode(randomFrom(License.OperationMode.PLATINUM, License.OperationMode.GOLD, License.OperationMode.TRIAL));
        assertLicenseGoldPlatinumTrialBehaviour(watcherLicensee);
    }

    public void testDowngradingToBasicLicenseWorks() {
        licenseeRegistry.setOperationMode(randomFrom(License.OperationMode.PLATINUM, License.OperationMode.GOLD, License.OperationMode.TRIAL));
        WatcherLicensee watcherLicensee = new WatcherLicensee(Settings.EMPTY, licenseeRegistry);
        licenseeRegistry.register(watcherLicensee);

        assertLicenseGoldPlatinumTrialBehaviour(watcherLicensee);

        licenseeRegistry.setOperationMode(License.OperationMode.BASIC);
        assertLicenseBasicOrNoneOrExpiredBehaviour(watcherLicensee);
    }

    public void testUpgradingExpiredLicenseWorks() {
        licenseeRegistry.setOperationMode(randomFrom(License.OperationMode.PLATINUM, License.OperationMode.GOLD, License.OperationMode.TRIAL));
        WatcherLicensee watcherLicensee = new WatcherLicensee(Settings.EMPTY, licenseeRegistry);
        licenseeRegistry.register(watcherLicensee);
        licenseeRegistry.disable();

        assertLicenseBasicOrNoneOrExpiredBehaviour(watcherLicensee);

        licenseeRegistry.setOperationMode(randomFrom(License.OperationMode.PLATINUM, License.OperationMode.GOLD, License.OperationMode.TRIAL));
        assertLicenseGoldPlatinumTrialBehaviour(watcherLicensee);
    }

    private void assertLicenseGoldPlatinumTrialBehaviour(WatcherLicensee watcherLicensee) {
        assertThat("Expected putting a watch to be allowed", watcherLicensee.isPutWatchAllowed(), is(true));
        assertThat("Expected getting a watch to be allowed", watcherLicensee.isGetWatchAllowed(), is(true));
        assertThat("Expected watcher transport actions to be allowed", watcherLicensee.isWatcherTransportActionAllowed(), is(true));
        assertThat("Expected actions of a watch to be executed", watcherLicensee.isExecutingActionsAllowed(), is(true));
    }

    private void assertLicenseBasicOrNoneOrExpiredBehaviour(WatcherLicensee watcherLicensee) {
        assertThat("Expected putting a watch not to be allowed", watcherLicensee.isPutWatchAllowed(), is(false));
        assertThat("Expected getting a watch not to be allowed", watcherLicensee.isGetWatchAllowed(), is(false));
        assertThat("Expected watcher transport actions not to be allowed", watcherLicensee.isWatcherTransportActionAllowed(), is(false));
        assertThat("Expected actions of a watch not to be executed", watcherLicensee.isExecutingActionsAllowed(), is(false));
    }

    public static class SimpleLicenseeRegistry extends AbstractComponent implements LicenseeRegistry {
        private final List<Licensee> licensees = new ArrayList<>();
        private License.OperationMode operationMode;

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
                licensee.onChange(new Licensee.Status(operationMode, randomBoolean() ? LicenseState.ENABLED : LicenseState.GRACE_PERIOD));
            }
        }

        public void disable() {
            for (Licensee licensee : licensees) {
                licensee.onChange(new Licensee.Status(operationMode, LicenseState.DISABLED));
            }
        }

        public void setOperationMode(License.OperationMode operationMode) {
            this.operationMode = operationMode;
            enable();
        }
    }
}
