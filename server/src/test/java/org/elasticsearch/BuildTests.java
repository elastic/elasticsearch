/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch;

import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Map;
import java.util.Objects;

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
        assertFalse(newBuild(build, Map.of("version", "7.0.0-SNAPSHOT", "isSnapshot", true)).isProductionRelease());
        assertFalse(newBuild(build, Map.of("version", "7.0.0-alpha1", "isSnapshot", false)).isProductionRelease());
    }

    private static class WriteableBuild implements Writeable {
        private final Build build;

        WriteableBuild(StreamInput in) throws IOException {
            build = Build.readBuild(in);
        }

        WriteableBuild(Build build) {
            this.build = build;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            Build.writeBuild(build, out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            WriteableBuild that = (WriteableBuild) o;
            return build.equals(that.build);
        }

        @Override
        public int hashCode() {
            return Objects.hash(build);
        }
    }

    public void testSerialization() {
        var randomBuild = new WriteableBuild(
            new Build(
                randomAlphaOfLength(6),
                randomFrom(Build.Type.values()),
                randomAlphaOfLength(6),
                randomAlphaOfLength(6),
                randomBoolean(),
                randomAlphaOfLength(6),
                randomAlphaOfLength(6),
                randomAlphaOfLength(6),
                randomAlphaOfLength(6)
            )
        );
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

    public void testIsWireCompatible() {
        String compatibleLegacyVersion = VersionUtils.randomCompatibleVersion(random(), Version.CURRENT).toString();
        IllegalArgumentException e1 = expectThrows(
            IllegalArgumentException.class,
            () -> Build.isWireCompatibleWithCurrent(compatibleLegacyVersion)
        );
        assertThat(e1.getMessage(), equalTo("Cannot parse [" + compatibleLegacyVersion + "] as a transport version identifier"));
        String nonCompatibleLegacyVersion = VersionUtils.getPreviousVersion(Version.CURRENT.minimumCompatibilityVersion()).toString();
        IllegalArgumentException e2 = expectThrows(
            IllegalArgumentException.class,
            () -> Build.isWireCompatibleWithCurrent(nonCompatibleLegacyVersion)
        );
        assertThat(e2.getMessage(), equalTo("Cannot parse [" + nonCompatibleLegacyVersion + "] as a transport version identifier"));

        String transportVersion = TransportVersionUtils.randomCompatibleVersion(random()).toString();
        assertTrue(Build.isWireCompatibleWithCurrent(transportVersion));
        transportVersion = TransportVersion.fromId(TransportVersions.MINIMUM_COMPATIBLE.id() - 1).toString();
        assertFalse(Build.isWireCompatibleWithCurrent(transportVersion));

        IllegalArgumentException e3 = expectThrows(
            IllegalArgumentException.class,
            () -> Build.isWireCompatibleWithCurrentAllowingLegacy("x.y.z")
        );
        assertThat(e3.getMessage(), equalTo("Cannot parse [x.y.z] as a transport version identifier"));
    }

    public void testIsWireCompatibleAllowingLegacy() {
        String legacyVersion = VersionUtils.randomCompatibleVersion(random(), Version.CURRENT).toString();
        assertTrue(Build.isWireCompatibleWithCurrentAllowingLegacy(legacyVersion));
        legacyVersion = VersionUtils.getPreviousVersion(Version.CURRENT.minimumCompatibilityVersion()).toString();
        assertFalse(Build.isWireCompatibleWithCurrentAllowingLegacy(legacyVersion));

        String transportVersion = TransportVersionUtils.randomCompatibleVersion(random()).toString();
        assertTrue(Build.isWireCompatibleWithCurrentAllowingLegacy(transportVersion));
        transportVersion = TransportVersion.fromId(TransportVersions.MINIMUM_COMPATIBLE.id() - 1).toString();
        assertFalse(Build.isWireCompatibleWithCurrentAllowingLegacy(transportVersion));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> Build.isWireCompatibleWithCurrentAllowingLegacy("x.y.z")
        );
        assertThat(e.getMessage(), equalTo("Cannot parse [x.y.z] as a transport version identifier"));
    }

    public void testIsIndexCompatible() {
        String compatibleLegacyVersion = VersionUtils.randomCompatibleVersion(random(), Version.CURRENT).toString();
        IllegalArgumentException e1 = expectThrows(
            IllegalArgumentException.class,
            () -> Build.isIndexCompatibleWithCurrent(compatibleLegacyVersion)
        );
        assertThat(e1.getMessage(), equalTo("Cannot parse [" + compatibleLegacyVersion + "] as an index version identifier"));
        String nonCompatibleLegacyVersion = VersionUtils.getPreviousVersion(Version.CURRENT.minimumCompatibilityVersion()).toString();
        IllegalArgumentException e2 = expectThrows(
            IllegalArgumentException.class,
            () -> Build.isIndexCompatibleWithCurrent(nonCompatibleLegacyVersion)
        );
        assertThat(e2.getMessage(), equalTo("Cannot parse [" + nonCompatibleLegacyVersion + "] as an index version identifier"));

        String indexVersion = IndexVersionUtils.randomCompatibleVersion(random()).toString();
        assertTrue(Build.isIndexCompatibleWithCurrentAllowingLegacy(indexVersion));
        indexVersion = IndexVersion.fromId(IndexVersion.MINIMUM_COMPATIBLE.id() - 1).toString();
        assertFalse(Build.isIndexCompatibleWithCurrentAllowingLegacy(indexVersion));

        IllegalArgumentException e3 = expectThrows(
            IllegalArgumentException.class,
            () -> Build.isIndexCompatibleWithCurrentAllowingLegacy("x.y.z")
        );
        assertThat(e3.getMessage(), equalTo("Cannot parse [x.y.z] as an index version identifier"));
    }

    public void testIsIndexCompatibleAllowingLegacy() {
        String legacyVersion = VersionUtils.randomCompatibleVersion(random(), Version.CURRENT).toString();
        assertTrue(Build.isIndexCompatibleWithCurrentAllowingLegacy(legacyVersion));
        legacyVersion = VersionUtils.getPreviousVersion(Version.CURRENT.minimumCompatibilityVersion()).toString();
        assertFalse(Build.isIndexCompatibleWithCurrentAllowingLegacy(legacyVersion));

        String indexVersion = IndexVersionUtils.randomCompatibleVersion(random()).toString();
        assertTrue(Build.isIndexCompatibleWithCurrentAllowingLegacy(indexVersion));
        indexVersion = IndexVersion.fromId(IndexVersion.MINIMUM_COMPATIBLE.id() - 1).toString();
        assertFalse(Build.isIndexCompatibleWithCurrentAllowingLegacy(indexVersion));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> Build.isIndexCompatibleWithCurrentAllowingLegacy("x.y.z")
        );
        assertThat(e.getMessage(), equalTo("Cannot parse [x.y.z] as an index version identifier"));
    }

}
