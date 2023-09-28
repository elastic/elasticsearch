/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license.internal;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class TrialLicenseEraTests extends ESTestCase {

    public void testCanParseAllVersions() {
        for (var version : Version.getDeclaredVersions(Version.class)) {
            TrialLicenseEra era = TrialLicenseEra.fromString(version.toString());
            assertThat((byte) era.asInt(), equalTo(version.major));
        }
    }

    public void testVersionWireCompatibility() throws IOException {
        for (var version : Version.getDeclaredVersions(Version.class)) {
            versionToEraSerialization(version);
        }

        for (int i = 2; i <= 1_000; i++) { // 1000 chosen arbitrarily; we really just want to make sure there's room to grow
            eraToVersionSerialization(new TrialLicenseEra(i));
        }
    }

    // This simulates a node of a version that supports transport eras receiving a message which is still using old-school Versions
    private TrialLicenseEra versionToEraSerialization(Version version) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setTransportVersion(TransportVersion.current());
            Version.writeVersion(version, output);
            try (
                StreamInput in = new NamedWriteableAwareStreamInput(
                    output.bytes().streamInput(),
                    new NamedWriteableRegistry(Collections.emptyList())
                )
            ) {
                in.setTransportVersion(TransportVersion.current());
                TrialLicenseEra readEra = new TrialLicenseEra(in);
                assertEquals(readEra.asInt(), version.major);
                return readEra;
            }
        }
    }

    // This simulates (imperfectly) a node of a version that does not support transport eras receiving a message which uses trial version
    // eras
    private Version eraToVersionSerialization(TrialLicenseEra era) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setTransportVersion(TransportVersion.current());
            era.writeTo(output);
            try (
                StreamInput in = new NamedWriteableAwareStreamInput(
                    output.bytes().streamInput(),
                    new NamedWriteableRegistry(Collections.emptyList())
                )
            ) {
                in.setTransportVersion(TransportVersion.current());
                Version readVersion = Version.readVersion(in);
                assertEquals(readVersion.major, era.asInt());
                return readVersion;
            }
        }
    }

    public void testRoundTripParsing() {
        var randomEra = new TrialLicenseEra(randomNonNegativeInt());
        assertThat(TrialLicenseEra.fromString(randomEra.toString()), equalTo(randomEra));
    }

    public void testVersionCanParseAllEras() {
        for (int i = 2; i <= TrialLicenseEra.CURRENT.asInt(); i++) {
            Version.fromString(new TrialLicenseEra(i).asVersionString());
        }
    }

    public void testNewTrialAllowed() {
        var randomEra = new TrialLicenseEra(randomNonNegativeInt());
        var subsequentEra = new TrialLicenseEra(randomEra.asInt() + randomIntBetween(0, Integer.MAX_VALUE - randomEra.asInt()));
        assertFalse(randomEra.ableToStartNewTrialSince(randomEra));
        assertTrue(subsequentEra.ableToStartNewTrialSince(randomEra));
    }
}
