/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.server.cli;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class JvmOptionsParserTests extends ESTestCase {

    private static final Map<String, String> TEST_SYSPROPS = Map.of("os.name", "Linux", "os.arch", "aarch64");

    private static final Path ENTITLEMENTS_LIB_DIR = Path.of("lib", "entitlement-bridge");

    @BeforeClass
    public static void beforeClass() throws IOException {
        Files.createDirectories(ENTITLEMENTS_LIB_DIR);
        Files.createTempFile(ENTITLEMENTS_LIB_DIR, "mock-entitlements-bridge", ".jar");
    }

    @AfterClass
    public static void afterClass() throws IOException {
        IOUtils.rm(Path.of("lib"));
    }

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

    public void testMissingRootJvmOptions() throws IOException, JvmOptionsParser.JvmOptionsFileParserException {
        final Path config = createTempDir();
        try {
            final JvmOptionsParser parser = new JvmOptionsParser();
            parser.readJvmOptionsFiles(config);
            fail("expected no such file exception, the root jvm.options file does not exist");
        } catch (final NoSuchFileException expected) {
            // this is expected, the root JVM options file must exist
        }
    }

    public void testReadRootJvmOptions() throws IOException, JvmOptionsParser.JvmOptionsFileParserException {
        final Path config = createTempDir();
        final Path rootJvmOptions = config.resolve("jvm.options");
        Files.write(rootJvmOptions, List.of("# comment", "-Xms256m", "-Xmx256m"), StandardOpenOption.CREATE_NEW, StandardOpenOption.APPEND);
        if (randomBoolean()) {
            // an empty jvm.options.d directory should be irrelevant
            Files.createDirectory(config.resolve("jvm.options.d"));
        }
        final JvmOptionsParser parser = new JvmOptionsParser();
        final List<String> jvmOptions = parser.readJvmOptionsFiles(config);
        assertThat(jvmOptions, contains("-Xms256m", "-Xmx256m"));
    }

    public void testReadJvmOptionsDirectory() throws IOException, JvmOptionsParser.JvmOptionsFileParserException {
        final Path config = createTempDir();
        Files.createDirectory(config.resolve("jvm.options.d"));
        Files.write(
            config.resolve("jvm.options"),
            List.of("# comment", "-Xms256m", "-Xmx256m"),
            StandardOpenOption.CREATE_NEW,
            StandardOpenOption.APPEND
        );
        Files.write(
            config.resolve("jvm.options.d").resolve("heap.options"),
            List.of("# comment", "-Xms384m", "-Xmx384m"),
            StandardOpenOption.CREATE_NEW,
            StandardOpenOption.APPEND
        );
        final JvmOptionsParser parser = new JvmOptionsParser();
        final List<String> jvmOptions = parser.readJvmOptionsFiles(config);
        assertThat(jvmOptions, contains("-Xms256m", "-Xmx256m", "-Xms384m", "-Xmx384m"));
    }

    public void testReadJvmOptionsDirectoryInOrder() throws IOException, JvmOptionsParser.JvmOptionsFileParserException {
        final Path config = createTempDir();
        Files.createDirectory(config.resolve("jvm.options.d"));
        Files.write(
            config.resolve("jvm.options"),
            List.of("# comment", "-Xms256m", "-Xmx256m"),
            StandardOpenOption.CREATE_NEW,
            StandardOpenOption.APPEND
        );
        Files.write(
            config.resolve("jvm.options.d").resolve("first.options"),
            List.of("# comment", "-Xms384m", "-Xmx384m"),
            StandardOpenOption.CREATE_NEW,
            StandardOpenOption.APPEND
        );
        Files.write(
            config.resolve("jvm.options.d").resolve("second.options"),
            List.of("# comment", "-Xms512m", "-Xmx512m"),
            StandardOpenOption.CREATE_NEW,
            StandardOpenOption.APPEND
        );
        final JvmOptionsParser parser = new JvmOptionsParser();
        final List<String> jvmOptions = parser.readJvmOptionsFiles(config);
        assertThat(jvmOptions, contains("-Xms256m", "-Xmx256m", "-Xms384m", "-Xmx384m", "-Xms512m", "-Xmx512m"));
    }

    public void testReadJvmOptionsDirectoryIgnoresFilesNotNamedOptions() throws IOException,
        JvmOptionsParser.JvmOptionsFileParserException {
        final Path config = createTempDir();
        Files.createFile(config.resolve("jvm.options"));
        Files.createDirectory(config.resolve("jvm.options.d"));
        Files.write(
            config.resolve("jvm.options.d").resolve("heap.not-named-options"),
            List.of("# comment", "-Xms256m", "-Xmx256m"),
            StandardOpenOption.CREATE_NEW,
            StandardOpenOption.APPEND
        );
        final JvmOptionsParser parser = new JvmOptionsParser();
        final List<String> jvmOptions = parser.readJvmOptionsFiles(config);
        assertThat(jvmOptions, empty());
    }

    public void testFileContainsInvalidLinesThrowsParserException() throws IOException {
        final Path config = createTempDir();
        final Path rootJvmOptions = config.resolve("jvm.options");
        Files.write(rootJvmOptions, List.of("XX:+UseG1GC"), StandardOpenOption.CREATE_NEW, StandardOpenOption.APPEND);
        try {
            final JvmOptionsParser parser = new JvmOptionsParser();
            parser.readJvmOptionsFiles(config);
            fail("expected JVM options file parser exception, XX:+UseG1GC is improperly formatted");
        } catch (final JvmOptionsParser.JvmOptionsFileParserException expected) {
            assertThat(expected.jvmOptionsFile(), equalTo(rootJvmOptions));
            assertThat(expected.invalidLines().entrySet(), hasSize(1));
            assertThat(expected.invalidLines(), hasKey(1));
            assertThat(expected.invalidLines().get(1), equalTo("XX:+UseG1GC"));
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
        final String invalidRangeLine = Strings.format("%d:%d-XX:+UseG1GC", javaMajorVersion, smallerJavaMajorVersion);
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
            invalidLines.put(1, Strings.format("%d:-XX:+UseG1GC", invalidLowerJavaMajorVersion));
            invalidLines.put(2, Strings.format("8-%d:-XX:+AggressiveOpts", invalidUpperJavaMajorVersion));
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
        final String upperBoundGreaterThanLowerBound = Strings.format("%d-%d-XX:+UseG1GC", lowerBound, upperBound);
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

    public void testNodeProcessorsActiveCount() {
        {
            final List<String> jvmOptions = SystemJvmOptions.systemJvmOptions(Settings.EMPTY, TEST_SYSPROPS);
            assertThat(jvmOptions, not(hasItem(containsString("-XX:ActiveProcessorCount="))));
        }
        {
            Settings nodeSettings = Settings.builder().put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), 1).build();
            final List<String> jvmOptions = SystemJvmOptions.systemJvmOptions(nodeSettings, TEST_SYSPROPS);
            assertThat(jvmOptions, hasItem("-XX:ActiveProcessorCount=1"));
        }
        {
            // check rounding
            Settings nodeSettings = Settings.builder().put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), 0.2).build();
            final List<String> jvmOptions = SystemJvmOptions.systemJvmOptions(nodeSettings, TEST_SYSPROPS);
            assertThat(jvmOptions, hasItem("-XX:ActiveProcessorCount=1"));
        }
        {
            // check validation
            Settings nodeSettings = Settings.builder().put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), 10000).build();
            var e = expectThrows(IllegalArgumentException.class, () -> SystemJvmOptions.systemJvmOptions(nodeSettings, TEST_SYSPROPS));
            assertThat(e.getMessage(), containsString("setting [node.processors] must be <="));
        }
    }

    public void testCommandLineDistributionType() {
        var sysprops = new HashMap<>(TEST_SYSPROPS);
        sysprops.put("es.distribution.type", "testdistro");
        final List<String> jvmOptions = SystemJvmOptions.systemJvmOptions(Settings.EMPTY, sysprops);
        assertThat(jvmOptions, hasItem("-Des.distribution.type=testdistro"));
    }
}
