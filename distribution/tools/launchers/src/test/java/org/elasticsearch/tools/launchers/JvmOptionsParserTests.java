/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.tools.launchers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JvmOptionsParserTests extends LaunchersTestCase {

    public void testSubstitution() {
        final List<String> jvmOptions = JvmOptionsParser.substitutePlaceholders(
            List.of("-Djava.io.tmpdir=${ES_TMPDIR}"),
            Map.of("ES_TMPDIR", "/tmp/elasticsearch")
        );
        assertThat(jvmOptions, contains("-Djava.io.tmpdir=/tmp/elasticsearch"));
    }

    public void testUnversionedOptions() throws IOException {
        try (StringReader sr = new StringReader("-Xms1g\n-Xmx1g"); BufferedReader br = new BufferedReader(sr)) {
            assertExpectedJvmOptions(randomIntBetween(8, Integer.MAX_VALUE), br, Arrays.asList("-Xms1g", "-Xmx1g"));
        }

    }

    public void testSingleVersionOption() throws IOException {
        final int javaMajorVersion = randomIntBetween(8, Integer.MAX_VALUE - 1);
        final int smallerJavaMajorVersion = randomIntBetween(7, javaMajorVersion);
        final int largerJavaMajorVersion = randomIntBetween(javaMajorVersion + 1, Integer.MAX_VALUE);
        try (
            StringReader sr = new StringReader(
                String.format(
                    Locale.ROOT,
                    "-Xms1g\n%d:-Xmx1g\n%d:-XX:+UseG1GC\n%d:-Xlog:gc",
                    javaMajorVersion,
                    smallerJavaMajorVersion,
                    largerJavaMajorVersion
                )
            );
            BufferedReader br = new BufferedReader(sr)
        ) {
            assertExpectedJvmOptions(javaMajorVersion, br, Arrays.asList("-Xms1g", "-Xmx1g"));
        }
    }

    public void testUnboundedVersionOption() throws IOException {
        final int javaMajorVersion = randomIntBetween(8, Integer.MAX_VALUE - 1);
        final int smallerJavaMajorVersion = randomIntBetween(7, javaMajorVersion);
        final int largerJavaMajorVersion = randomIntBetween(javaMajorVersion + 1, Integer.MAX_VALUE);
        try (
            StringReader sr = new StringReader(
                String.format(
                    Locale.ROOT,
                    "-Xms1g\n%d-:-Xmx1g\n%d-:-XX:+UseG1GC\n%d-:-Xlog:gc",
                    javaMajorVersion,
                    smallerJavaMajorVersion,
                    largerJavaMajorVersion
                )
            );
            BufferedReader br = new BufferedReader(sr)
        ) {
            assertExpectedJvmOptions(javaMajorVersion, br, Arrays.asList("-Xms1g", "-Xmx1g", "-XX:+UseG1GC"));
        }
    }

    public void testBoundedVersionOption() throws IOException {
        final int javaMajorVersion = randomIntBetween(8, Integer.MAX_VALUE - 1);
        final int javaMajorVersionUpperBound = randomIntBetween(javaMajorVersion, Integer.MAX_VALUE - 1);
        final int smallerJavaMajorVersionLowerBound = randomIntBetween(7, javaMajorVersion);
        final int smallerJavaMajorVersionUpperBound = randomIntBetween(smallerJavaMajorVersionLowerBound, javaMajorVersion);
        final int largerJavaMajorVersionLowerBound = randomIntBetween(javaMajorVersion + 1, Integer.MAX_VALUE);
        final int largerJavaMajorVersionUpperBound = randomIntBetween(largerJavaMajorVersionLowerBound, Integer.MAX_VALUE);
        try (
            StringReader sr = new StringReader(
                String.format(
                    Locale.ROOT,
                    "-Xms1g\n%d-%d:-Xmx1g\n%d-%d:-XX:+UseG1GC\n%d-%d:-Xlog:gc",
                    javaMajorVersion,
                    javaMajorVersionUpperBound,
                    smallerJavaMajorVersionLowerBound,
                    smallerJavaMajorVersionUpperBound,
                    largerJavaMajorVersionLowerBound,
                    largerJavaMajorVersionUpperBound
                )
            );
            BufferedReader br = new BufferedReader(sr)
        ) {
            assertExpectedJvmOptions(javaMajorVersion, br, Arrays.asList("-Xms1g", "-Xmx1g"));
        }
    }

    public void testComplexOptions() throws IOException {
        final int javaMajorVersion = randomIntBetween(8, Integer.MAX_VALUE - 1);
        final int javaMajorVersionUpperBound = randomIntBetween(javaMajorVersion, Integer.MAX_VALUE - 1);
        final int smallerJavaMajorVersionLowerBound = randomIntBetween(7, javaMajorVersion);
        final int smallerJavaMajorVersionUpperBound = randomIntBetween(smallerJavaMajorVersionLowerBound, javaMajorVersion);
        final int largerJavaMajorVersionLowerBound = randomIntBetween(javaMajorVersion + 1, Integer.MAX_VALUE);
        final int largerJavaMajorVersionUpperBound = randomIntBetween(largerJavaMajorVersionLowerBound, Integer.MAX_VALUE);
        try (
            StringReader sr = new StringReader(
                String.format(
                    Locale.ROOT,
                    "-Xms1g\n%d:-Xmx1g\n%d-:-XX:+UseG1GC\n%d-%d:-Xlog:gc\n%d-%d:-XX:+PrintFlagsFinal\n%d-%d:-XX+AggressiveOpts",
                    javaMajorVersion,
                    javaMajorVersion,
                    javaMajorVersion,
                    javaMajorVersionUpperBound,
                    smallerJavaMajorVersionLowerBound,
                    smallerJavaMajorVersionUpperBound,
                    largerJavaMajorVersionLowerBound,
                    largerJavaMajorVersionUpperBound
                )
            );
            BufferedReader br = new BufferedReader(sr)
        ) {
            assertExpectedJvmOptions(javaMajorVersion, br, Arrays.asList("-Xms1g", "-Xmx1g", "-XX:+UseG1GC", "-Xlog:gc"));
        }
    }

    private void assertExpectedJvmOptions(final int javaMajorVersion, final BufferedReader br, final List<String> expectedJvmOptions)
        throws IOException {
        final Map<String, AtomicBoolean> seenJvmOptions = new HashMap<>();
        for (final String expectedJvmOption : expectedJvmOptions) {
            assertNull(seenJvmOptions.put(expectedJvmOption, new AtomicBoolean()));
        }
        JvmOptionsParser.parse(javaMajorVersion, br, new JvmOptionsParser.JvmOptionConsumer() {
            @Override
            public void accept(final String jvmOption) {
                final AtomicBoolean seen = seenJvmOptions.get(jvmOption);
                if (seen == null) {
                    fail("unexpected JVM option [" + jvmOption + "]");
                }
                assertFalse("saw JVM option [" + jvmOption + "] more than once", seen.get());
                seen.set(true);
            }
        }, new JvmOptionsParser.InvalidLineConsumer() {
            @Override
            public void accept(final int lineNumber, final String line) {
                fail("unexpected invalid line [" + line + "] on line number [" + lineNumber + "]");
            }
        });
        for (final Map.Entry<String, AtomicBoolean> seenJvmOption : seenJvmOptions.entrySet()) {
            assertTrue("expected JVM option [" + seenJvmOption.getKey() + "]", seenJvmOption.getValue().get());
        }
    }

    public void testInvalidLines() throws IOException {
        try (StringReader sr = new StringReader("XX:+UseG1GC"); BufferedReader br = new BufferedReader(sr)) {
            JvmOptionsParser.parse(randomIntBetween(8, Integer.MAX_VALUE), br, new JvmOptionsParser.JvmOptionConsumer() {
                @Override
                public void accept(final String jvmOption) {
                    fail("unexpected valid JVM option [" + jvmOption + "]");
                }
            }, new JvmOptionsParser.InvalidLineConsumer() {
                @Override
                public void accept(final int lineNumber, final String line) {
                    assertThat(lineNumber, equalTo(1));
                    assertThat(line, equalTo("XX:+UseG1GC"));
                }
            });
        }

        final int javaMajorVersion = randomIntBetween(8, Integer.MAX_VALUE);
        final int smallerJavaMajorVersion = randomIntBetween(7, javaMajorVersion - 1);
        final String invalidRangeLine = String.format(Locale.ROOT, "%d:%d-XX:+UseG1GC", javaMajorVersion, smallerJavaMajorVersion);
        try (StringReader sr = new StringReader(invalidRangeLine); BufferedReader br = new BufferedReader(sr)) {
            assertInvalidLines(br, Collections.singletonMap(1, invalidRangeLine));
        }

        final long invalidLowerJavaMajorVersion = (long) randomIntBetween(1, 16) + Integer.MAX_VALUE;
        final long invalidUpperJavaMajorVersion = (long) randomIntBetween(1, 16) + Integer.MAX_VALUE;
        final String numberFormatExceptionsLine = String.format(
            Locale.ROOT,
            "%d:-XX:+UseG1GC\n8-%d:-XX:+AggressiveOpts",
            invalidLowerJavaMajorVersion,
            invalidUpperJavaMajorVersion
        );
        try (StringReader sr = new StringReader(numberFormatExceptionsLine); BufferedReader br = new BufferedReader(sr)) {
            final Map<Integer, String> invalidLines = new HashMap<>(2);
            invalidLines.put(1, String.format(Locale.ROOT, "%d:-XX:+UseG1GC", invalidLowerJavaMajorVersion));
            invalidLines.put(2, String.format(Locale.ROOT, "8-%d:-XX:+AggressiveOpts", invalidUpperJavaMajorVersion));
            assertInvalidLines(br, invalidLines);
        }

        final String multipleInvalidLines = "XX:+UseG1GC\nXX:+AggressiveOpts";
        try (StringReader sr = new StringReader(multipleInvalidLines); BufferedReader br = new BufferedReader(sr)) {
            final Map<Integer, String> invalidLines = new HashMap<>(2);
            invalidLines.put(1, "XX:+UseG1GC");
            invalidLines.put(2, "XX:+AggressiveOpts");
            assertInvalidLines(br, invalidLines);
        }

        final int lowerBound = randomIntBetween(9, 16);
        final int upperBound = randomIntBetween(8, lowerBound - 1);
        final String upperBoundGreaterThanLowerBound = String.format(Locale.ROOT, "%d-%d-XX:+UseG1GC", lowerBound, upperBound);
        try (StringReader sr = new StringReader(upperBoundGreaterThanLowerBound); BufferedReader br = new BufferedReader(sr)) {
            assertInvalidLines(br, Collections.singletonMap(1, upperBoundGreaterThanLowerBound));
        }
    }

    private void assertInvalidLines(final BufferedReader br, final Map<Integer, String> invalidLines) throws IOException {
        final Map<Integer, String> seenInvalidLines = new HashMap<>(invalidLines.size());
        JvmOptionsParser.parse(randomIntBetween(8, Integer.MAX_VALUE), br, new JvmOptionsParser.JvmOptionConsumer() {
            @Override
            public void accept(final String jvmOption) {
                fail("unexpected valid JVM options [" + jvmOption + "]");
            }
        }, new JvmOptionsParser.InvalidLineConsumer() {
            @Override
            public void accept(final int lineNumber, final String line) {
                seenInvalidLines.put(lineNumber, line);
            }
        });
        assertThat(seenInvalidLines, equalTo(invalidLines));
    }

    public void testSpaceDelimitedJvmOptions() {
        assertThat(JvmOptionsParser.spaceDelimitJvmOptions(Collections.singletonList("-Xms1g")), equalTo("-Xms1g"));
        assertThat(JvmOptionsParser.spaceDelimitJvmOptions(Arrays.asList("-Xms1g", "-Xmx1g")), equalTo("-Xms1g -Xmx1g"));
        assertThat(
            JvmOptionsParser.spaceDelimitJvmOptions(Arrays.asList("-Xms1g", "-Xmx1g", "-XX:+UseG1GC")),
            equalTo("-Xms1g -Xmx1g -XX:+UseG1GC")
        );
    }

}
