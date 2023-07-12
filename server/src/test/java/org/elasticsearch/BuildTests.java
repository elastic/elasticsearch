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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

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
        Build build = new Build(
            Build.current().type(),
            Build.current().hash(),
            Build.current().date(),
            Build.current().isSnapshot(),
            Math.abs(randomInt()) + "." + Math.abs(randomInt()) + "." + Math.abs(randomInt())
        );
        assertTrue(build.qualifiedVersion(), build.isProductionRelease());

        assertFalse(
            new Build(
                Build.current().type(),
                Build.current().hash(),
                Build.current().date(),
                Build.current().isSnapshot(),
                "7.0.0-SNAPSHOT"
            ).isProductionRelease()
        );

        assertFalse(
            new Build(Build.current().type(), Build.current().hash(), Build.current().date(), Build.current().isSnapshot(), "Unknown")
                .isProductionRelease()
        );
    }

    public void testEqualsAndHashCode() {
        Build build = Build.current();

        Build another = new Build(build.type(), build.hash(), build.date(), build.isSnapshot(), build.qualifiedVersion());
        assertEquals(build, another);
        assertEquals(build.hashCode(), another.hashCode());

        final Set<Build.Type> otherTypes = Arrays.stream(Build.Type.values())
            .filter(f -> f.equals(build.type()) == false)
            .collect(Collectors.toSet());
        final Build.Type otherType = randomFrom(otherTypes);
        Build differentType = new Build(otherType, build.hash(), build.date(), build.isSnapshot(), build.qualifiedVersion());
        assertNotEquals(build, differentType);

        Build differentHash = new Build(
            build.type(),
            randomAlphaOfLengthBetween(3, 10),
            build.date(),
            build.isSnapshot(),
            build.qualifiedVersion()
        );
        assertNotEquals(build, differentHash);

        Build differentDate = new Build(build.type(), build.hash(), "1970-01-01", build.isSnapshot(), build.qualifiedVersion());
        assertNotEquals(build, differentDate);

        Build differentSnapshot = new Build(
            build.type(),
            build.hash(),
            build.date(),
            build.isSnapshot() == false,
            build.qualifiedVersion()
        );
        assertNotEquals(build, differentSnapshot);

        Build differentVersion = new Build(build.type(), build.hash(), build.date(), build.isSnapshot(), "1.2.3");
        assertNotEquals(build, differentVersion);
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

    private static String randomStringExcept(final String s) {
        return randomAlphaOfLength(13 - s.length());
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/85800")
    public void testSerialization() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            new WriteableBuild(
                new Build(
                    randomFrom(Build.Type.values()),
                    randomAlphaOfLength(6),
                    randomAlphaOfLength(6),
                    randomBoolean(),
                    randomAlphaOfLength(6)
                )
            ),
            // Note: the cast of the Copy- and MutateFunction is needed for some IDE (specifically Eclipse 4.10.0) to infer the right type
            (WriteableBuild b) -> copyWriteable(b, writableRegistry(), WriteableBuild::new, TransportVersion.current()),
            (WriteableBuild b) -> {
                switch (randomIntBetween(1, 5)) {
                    case 1:
                        return new WriteableBuild(
                            new Build(
                                randomValueOtherThan(b.build.type(), () -> randomFrom(Build.Type.values())),
                                b.build.hash(),
                                b.build.date(),
                                b.build.isSnapshot(),
                                b.build.qualifiedVersion()
                            )
                        );
                    case 2:
                        return new WriteableBuild(
                            new Build(
                                b.build.type(),
                                randomStringExcept(b.build.hash()),
                                b.build.date(),
                                b.build.isSnapshot(),
                                b.build.qualifiedVersion()
                            )
                        );
                    case 3:
                        return new WriteableBuild(
                            new Build(
                                b.build.type(),
                                b.build.hash(),
                                randomStringExcept(b.build.date()),
                                b.build.isSnapshot(),
                                b.build.qualifiedVersion()
                            )
                        );
                    case 4:
                        return new WriteableBuild(
                            new Build(
                                b.build.type(),
                                b.build.hash(),
                                b.build.date(),
                                b.build.isSnapshot() == false,
                                b.build.qualifiedVersion()
                            )
                        );
                    case 5:
                        return new WriteableBuild(
                            new Build(
                                b.build.type(),
                                b.build.hash(),
                                b.build.date(),
                                b.build.isSnapshot(),
                                randomStringExcept(b.build.qualifiedVersion())
                            )
                        );
                }
                throw new AssertionError();
            }
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

}
