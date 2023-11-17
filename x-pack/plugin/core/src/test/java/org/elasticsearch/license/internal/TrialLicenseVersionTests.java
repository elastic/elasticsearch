/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license.internal;

import org.elasticsearch.Version;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.license.internal.TrialLicenseVersion.TRIAL_VERSION_CUTOVER;
import static org.hamcrest.Matchers.equalTo;

public class TrialLicenseVersionTests extends ESTestCase {

    public void testCanParseAllVersions() {
        for (var version : Version.getDeclaredVersions(Version.class)) {
            TrialLicenseVersion parsedVersion = TrialLicenseVersion.fromXContent(version.toString());
            // TODO clean up
            if (version.onOrBefore(Version.V_8_12_0)) {
                assertFalse(parsedVersion.ableToStartNewTrial());
            }
        }
    }

    public void testRoundTripParsing() {
        var randomVersion = new TrialLicenseVersion(randomNonNegativeInt());
        assertThat(TrialLicenseVersion.fromXContent(randomVersion.toString()), equalTo(randomVersion));
    }

    public void testNewTrialAllowed() {
        // explicit boundary case
        assertFalse(new TrialLicenseVersion(TRIAL_VERSION_CUTOVER).ableToStartNewTrial());

        assertFalse(new TrialLicenseVersion(randomIntBetween(0, TRIAL_VERSION_CUTOVER)).ableToStartNewTrial());
        assertTrue(new TrialLicenseVersion(randomIntBetween(TRIAL_VERSION_CUTOVER + 1, Integer.MAX_VALUE)).ableToStartNewTrial());
    }
}
