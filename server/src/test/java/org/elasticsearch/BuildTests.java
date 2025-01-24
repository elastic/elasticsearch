/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch;

import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Map;

import static org.elasticsearch.test.BuildUtils.mutateBuild;
import static org.elasticsearch.test.BuildUtils.newBuild;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.sameInstance;

public class BuildTests extends ESTestCase {

    /** Asking for the jar metadata should not throw exception in tests, no matter how configured */
    public void testJarMetadata() throws IOException {
        URL url = Build.getElasticsearchCodeSourceLocation();
        // throws exception if does not exist, or we cannot access it
        try (InputStream ignored = FileSystemUtils.openFileURLStream(url)) {}
        // these should never be null
        assertNotNull(Build.current().date());
        assertNotNull(Build.current().hash());
    }

    public void testIsProduction() {
        String version = Math.abs(randomInt()) + "." + Math.abs(randomInt()) + "." + Math.abs(randomInt());
        Build build = Build.current();
        assertTrue(newBuild(build, Map.of("version", version, "isSnapshot", false)).isProductionRelease());
        assertFalse(newBuild(build, Map.of("version", "7.0.0", "isSnapshot", true)).isProductionRelease());
        assertFalse(newBuild(build, Map.of("version", "7.0.0", "qualifier", "alpha1", "isSnapshot", false)).isProductionRelease());
    }

    private record WriteableBuild(Build build) implements Writeable {

        WriteableBuild(StreamInput in) throws IOException {
            this(Build.readBuild(in));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            Build.writeBuild(build, out);
        }
    }

    public void testSerialization() {
        var randomBuild = new WriteableBuild(randomBuild());
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            randomBuild,
            // Note: the cast of the Copy- and MutateFunction is needed for some IDE (specifically Eclipse 4.10.0) to infer the right type
            (WriteableBuild b) -> copyWriteable(b, writableRegistry(), WriteableBuild::new, TransportVersion.current()),
            (WriteableBuild b) -> new WriteableBuild(mutateBuild(b.build))
        );
    }

    public void testTypeParsing() {
        for (final Build.Type type : Build.Type.values()) {
            // strict or not should not impact parsing at all here
            assertThat(Build.Type.fromDisplayName(type.displayName(), randomBoolean()), sameInstance(type));
        }
    }

    public void testLenientTypeParsing() {
        final String displayName = randomAlphaOfLength(8);
        assertThat(Build.Type.fromDisplayName(displayName, false), equalTo(Build.Type.UNKNOWN));
    }

    public void testStrictTypeParsing() {
        final String displayName = randomAlphaOfLength(8);
        @SuppressWarnings("ResultOfMethodCallIgnored")
        final IllegalStateException e = expectThrows(IllegalStateException.class, () -> Build.Type.fromDisplayName(displayName, true));
        assertThat(e, hasToString(containsString("unexpected distribution type [" + displayName + "]; your distribution is broken")));
    }

    public void testSerializationQualifierBwc() throws IOException {
        var randomBuild = new WriteableBuild(randomBuild());
        var serializationVersion = TransportVersionUtils.getPreviousVersion(TransportVersions.V_8_12_0);
        var roundtrip = copyWriteable(randomBuild, writableRegistry(), WriteableBuild::new, serializationVersion);
        assertThat(roundtrip.build.version(), equalTo(randomBuild.build.version()));
        assertThat(roundtrip.build.qualifier(), equalTo(randomBuild.build.qualifier()));
        assertThat(roundtrip.build.isSnapshot(), equalTo(randomBuild.build.isSnapshot()));
        assertThat(roundtrip.build.displayString(), equalTo(randomBuild.build.displayString()));
    }

    private static Build randomBuild() {
        return new Build(
            randomAlphaOfLength(6),
            randomFrom(Build.Type.values()),
            randomAlphaOfLength(6),
            randomAlphaOfLength(6),
            randomAlphaOfLength(6),
            randomFrom(random(), null, "alpha1", "beta1", "rc2"),
            randomBoolean(),
            randomAlphaOfLength(6),
            randomAlphaOfLength(6),
            randomAlphaOfLength(6)
        );
    }
}
