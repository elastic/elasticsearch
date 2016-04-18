/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.license;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.core.AbstractLicenseeTestCase;
import org.elasticsearch.watcher.WatcherLicensee;

import static org.elasticsearch.license.core.License.OperationMode.BASIC;
import static org.elasticsearch.license.core.License.OperationMode.GOLD;
import static org.elasticsearch.license.core.License.OperationMode.PLATINUM;
import static org.elasticsearch.license.core.License.OperationMode.STANDARD;
import static org.elasticsearch.license.core.License.OperationMode.TRIAL;
import static org.hamcrest.Matchers.is;

public class LicenseTests extends AbstractLicenseeTestCase {

    private final SimpleLicenseeRegistry licenseeRegistry = new SimpleLicenseeRegistry();
    private WatcherLicensee watcherLicensee;

    public void testPlatinumGoldTrialLicenseCanDoEverything() throws Exception {
        initLicense(TRIAL, GOLD, PLATINUM);
        assertWatcherActionsAllowed(watcherLicensee);
    }

    public void testBasicStandardLicenseDisablesWatcher() throws Exception {
        initLicense(BASIC, STANDARD);
        assertWatcherActionsNotAllowed(watcherLicensee);
    }

    public void testNoLicenseDisablesWatcher() {
        initLicense(BASIC, STANDARD);
        licenseeRegistry.disable();

        assertWatcherActionsNotAllowed(watcherLicensee);
    }

    public void testExpiredPlatinumGoldTrialLicenseDisablesWatcher() throws Exception {
        initLicense(TRIAL, GOLD, PLATINUM);
        licenseeRegistry.disable();

        assertWatcherActionsNotAllowed(watcherLicensee);
    }

    public void testUpgradingFromBasicOrStandardLicenseWorks() {
        initLicense(BASIC, STANDARD);
        assertWatcherActionsNotAllowed(watcherLicensee);

        licenseeRegistry.setOperationMode(randomFrom(TRIAL, GOLD, PLATINUM));
        assertWatcherActionsAllowed(watcherLicensee);
    }

    public void testDowngradingToBasicOrStandardLicenseWorks() {
        initLicense(TRIAL, GOLD, PLATINUM);
        assertWatcherActionsAllowed(watcherLicensee);

        licenseeRegistry.setOperationMode(randomFrom(BASIC, STANDARD));
        assertWatcherActionsNotAllowed(watcherLicensee);
    }

    public void testUpgradingExpiredLicenseWorks() {
        initLicense(TRIAL, GOLD, PLATINUM);
        licenseeRegistry.disable();

        assertWatcherActionsNotAllowed(watcherLicensee);

        licenseeRegistry.setOperationMode(randomFrom(TRIAL, GOLD, PLATINUM));
        assertWatcherActionsAllowed(watcherLicensee);
    }

    private void assertWatcherActionsAllowed(WatcherLicensee watcherLicensee) {
        assertThat("Expected putting a watch to be allowed", watcherLicensee.isPutWatchAllowed(), is(true));
        assertThat("Expected getting a watch to be allowed", watcherLicensee.isGetWatchAllowed(), is(true));
        assertThat("Expected watcher transport actions to be allowed", watcherLicensee.isWatcherTransportActionAllowed(), is(true));
        assertThat("Expected actions of a watch to be executed", watcherLicensee.isExecutingActionsAllowed(), is(true));
    }

    private void assertWatcherActionsNotAllowed(WatcherLicensee watcherLicensee) {
        assertThat("Expected putting a watch not to be allowed", watcherLicensee.isPutWatchAllowed(), is(false));
        assertThat("Expected getting a watch not to be allowed", watcherLicensee.isGetWatchAllowed(), is(false));
        assertThat("Expected watcher transport actions not to be allowed", watcherLicensee.isWatcherTransportActionAllowed(), is(false));
        assertThat("Expected actions of a watch not to be executed", watcherLicensee.isExecutingActionsAllowed(), is(false));
    }

    private void initLicense(License.OperationMode ... allowedLicenses) {
        licenseeRegistry.setOperationMode(randomFrom(allowedLicenses));
        watcherLicensee = new WatcherLicensee(Settings.EMPTY, licenseeRegistry);
        licenseeRegistry.register(watcherLicensee);
    }
}
