/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license.internal;

import org.elasticsearch.Version;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.license.internal.TrialLicenseVersion.CURRENT;
import static org.elasticsearch.license.internal.TrialLicenseVersion.TRIAL_VERSION_CUTOVER;
import static org.elasticsearch.license.internal.TrialLicenseVersion.TRIAL_VERSION_CUTOVER_MAJOR;
import static org.hamcrest.Matchers.equalTo;

public class TrialLicenseVersionTests extends ESTestCase {

    public void testCanParseAllVersions() {
        for (var version : Version.getDeclaredVersions(Version.class)) {
            // Only consider versions before the cut-over; the comparison becomes meaningless after the cut-over point
            if (version.onOrBefore(Version.fromId(TRIAL_VERSION_CUTOVER))) {
                TrialLicenseVersion parsedVersion = TrialLicenseVersion.fromXContent(version.toString());
                if (version.major < TRIAL_VERSION_CUTOVER_MAJOR) {
                    assertTrue(parsedVersion.ableToStartNewTrial());
                } else {
                    assertFalse(parsedVersion.ableToStartNewTrial());
                }
            }
        }
    }

    public void testRoundTripParsing() {
        var randomVersion = new TrialLicenseVersion(randomNonNegativeInt());
        assertThat(TrialLicenseVersion.fromXContent(randomVersion.toString()), equalTo(randomVersion));
    }

    public void testNewTrialAllowed() {
        assertTrue(new TrialLicenseVersion(randomIntBetween(7_00_00_00, 7_99_99_99)).ableToStartNewTrial());
        assertFalse(new TrialLicenseVersion(CURRENT.asInt()).ableToStartNewTrial());
        assertFalse(new TrialLicenseVersion(randomIntBetween(8_00_00_00, TRIAL_VERSION_CUTOVER)).ableToStartNewTrial());
        final int trialVersion = randomIntBetween(TRIAL_VERSION_CUTOVER, CURRENT.asInt());
        if (trialVersion < CURRENT.asInt()) {
            assertTrue(new TrialLicenseVersion(trialVersion).ableToStartNewTrial());
        } else {
            assertFalse(new TrialLicenseVersion(trialVersion).ableToStartNewTrial());
        }
    }
}
