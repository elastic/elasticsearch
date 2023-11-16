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
import static org.elasticsearch.license.internal.TrialLicenseVersion.TRIAL_VERSION_CUTOVER_MAJOR;
import static org.hamcrest.Matchers.equalTo;

public class TrialLicenseVersionTests extends ESTestCase {

    public void testCanParseAllVersions() {
        for (var version : Version.getDeclaredVersions(Version.class)) {
            TrialLicenseVersion parsedVersion = TrialLicenseVersion.fromXContent(version.toString());
            if (version.major < TRIAL_VERSION_CUTOVER_MAJOR) {
                assertTrue(new TrialLicenseVersion(TRIAL_VERSION_CUTOVER).ableToStartNewTrialSince(parsedVersion));
            } else {
                assertFalse(new TrialLicenseVersion(TRIAL_VERSION_CUTOVER).ableToStartNewTrialSince(parsedVersion));
            }
        }
    }

    public void testRoundTripParsing() {
        var randomVersion = new TrialLicenseVersion(randomNonNegativeInt());
        assertThat(TrialLicenseVersion.fromXContent(randomVersion.toString()), equalTo(randomVersion));
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/102286")
    public void testNewTrialAllowed() {
        var randomVersion = new TrialLicenseVersion(randomNonNegativeInt());
        var subsequentVersion = new TrialLicenseVersion(
            randomVersion.asInt() + randomIntBetween(0, Integer.MAX_VALUE - randomVersion.asInt())
        );
        assertFalse(randomVersion.ableToStartNewTrialSince(randomVersion));
        assertTrue(subsequentVersion.ableToStartNewTrialSince(randomVersion));
    }
}
