/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.elasticsearch.codec.profiler.CounterMetric;
import org.elasticsearch.codec.profiler.Metric;
import org.elasticsearch.codec.profiler.TimerMetric;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class ColumnarMetricCatalogueTests extends ESTestCase {

    private static final String SOURCE_ROOT = "libs/columnar/src/main/java";
    // A declaration names its group, its component and its name; the reference tables name a metric component.name.
    private static final Pattern DECLARATION = Pattern.compile(
        "(?:CounterMetric|TimerMetric)\\.of\\(\\s*\"[A-Za-z0-9_]+\",\\s*\"([a-z0-9_]+)\",\\s*\"([a-z0-9_]+)\""
    );
    // A CPU time companion takes the component of the timer it is declared on.
    private static final Pattern COMPANION = Pattern.compile(
        "(?s)TimerMetric\\.of\\(\\s*\"[A-Za-z0-9_]+\",\\s*\"([a-z0-9_]+)\",(?:(?!static final).)*?\\.withCpuTime\\(\\s*\"([a-z0-9_]+)\""
    );
    // A metric cell of the reference tables holds nothing but metric names: one {@code component.name}, or several separated
    // by commas.
    private static final Pattern METRIC_CELL = Pattern.compile("<td>((?:\\{@code [a-z0-9_]+\\.[a-z0-9_]+\\}(?:,\\s*)?)+)</td>");
    private static final Pattern METRIC_NAME = Pattern.compile("\\{@code ([a-z0-9_]+\\.[a-z0-9_]+)\\}");

    public void testEveryDeclaredMetricHasARowInThePackageReference() throws IOException {
        final Path sources = findSources();
        assumeTrue("the sources of the library are not reachable from the test working directory", sources != null);
        final Set<String> declared = declaredMetrics(sources);
        assertThat("the scan found the metrics of the library", declared.size(), greaterThanOrEqualTo(20));

        final String reference = Files.readString(sources.resolve("org/elasticsearch/columnar/package-info.java"), StandardCharsets.UTF_8);
        final Set<String> missing = new TreeSet<>();
        for (String name : declared) {
            if (reference.contains("{@code " + name + "}") == false) {
                missing.add(name);
            }
        }
        assertTrue("metrics declared in the code and missing from the package reference: " + missing, missing.isEmpty());
    }

    public void testTheReferenceTablesNameExactlyTheDeclaredMetrics() throws IOException {
        final Path sources = findSources();
        assumeTrue("the sources of the library are not reachable from the test working directory", sources != null);
        final Set<String> declared = declaredMetrics(sources);
        final String reference = Files.readString(sources.resolve("org/elasticsearch/columnar/package-info.java"), StandardCharsets.UTF_8);
        final Set<String> documented = new TreeSet<>();
        final Matcher cells = METRIC_CELL.matcher(reference);
        while (cells.find()) {
            final Matcher names = METRIC_NAME.matcher(cells.group(1));
            while (names.find()) {
                documented.add(names.group(1));
            }
        }
        assertEquals(
            "the metric cells of the package reference and the declarations of the code must name the same set",
            declared,
            documented
        );
        assertEquals("a metric was added or removed; update the reference tables and this pin together", 33, declared.size());
    }

    public void testTheDeclaredMetricsAreTheAgreedCatalogue() throws IOException {
        final Path sources = findSources();
        assumeTrue("the sources of the library are not reachable from the test working directory", sources != null);
        assertEquals(
            "a metric name is part of the profile response; rename one only together with this list",
            new TreeSet<>(
                Set.of(
                    "readers.opened",
                    "readers.plain",
                    "readers.dictionary",
                    "readers.numeric",
                    "readers.open_time_nanos",
                    "blocks.fetched",
                    "blocks.bytes_fetched",
                    "blocks.fetch_time_nanos",
                    "blocks.decompressed",
                    "blocks.bytes_inflated",
                    "blocks.decompress_time_nanos",
                    "blocks.decode_time_nanos",
                    "blocks.decode_cpu_time_nanos",
                    "blocks.values_decoded",
                    "chunks.decompressed",
                    "chunks.bytes_compressed",
                    "chunks.bytes_inflated",
                    "chunks.decompress_time_nanos",
                    "chunks.cache_hits",
                    "uncompressed.reads",
                    "uncompressed.bytes_read",
                    "uncompressed.read_time_nanos",
                    "dictionary.term_lookups",
                    "dictionary.resolve_time_nanos",
                    "ordinal_skip.intervals_rejected",
                    "ordinal_skip.docid_span",
                    "ordinal_skip.navigation_time_nanos",
                    "pages.as_ordinals",
                    "pages.as_values",
                    "pages.values_in_ordinal_pages",
                    "pages.values_in_value_pages",
                    "pages.call_time_nanos",
                    "pages.byte_lengths_call_time_nanos"
                )
            ),
            declaredMetrics(sources)
        );
    }

    @SuppressForbidden(reason = "reads the codec's package-private metric constants to compare every definition with the golden file")
    public void testEveryDefinitionIsTheGoldenOne() throws Exception {
        final Set<String> lines = new TreeSet<>();
        for (String name : List.of(
            "org.elasticsearch.columnar.substrate.ProfiledChunkDecompressor",
            "org.elasticsearch.columnar.substrate.ChunkedBytesReader",
            "org.elasticsearch.columnar.substrate.ProfiledBlockBytesCodec",
            "org.elasticsearch.columnar.numeric.ProfiledBlockDecoder",
            "org.elasticsearch.columnar.string.StringColumnReader",
            "org.elasticsearch.columnar.string.ProfiledStringBlockSink",
            "org.elasticsearch.columnar.string.DictionaryStringColumnReader",
            "org.elasticsearch.columnar.string.ProfiledIntervalSkipObserver",
            "org.elasticsearch.columnar.ColumNARDocValuesProducer"
        )) {
            for (Field field : Class.forName(name).getDeclaredFields()) {
                if (Modifier.isStatic(field.getModifiers()) && Metric.class.isAssignableFrom(field.getType())) {
                    field.setAccessible(true);
                    switch ((Metric) field.get(null)) {
                        case CounterMetric counter -> lines.add(
                            counter.id().key() + "|" + counter.unit() + "|null|null|" + counter.description()
                        );
                        case TimerMetric timer -> {
                            lines.add(timer.id().key() + "|TIME_NANOS|" + timer.policy() + "|null|" + timer.description());
                            if (timer.cpuTime() != null) {
                                lines.add(
                                    timer.cpuTime().id().key()
                                        + "|TIME_NANOS|"
                                        + timer.policy()
                                        + "|"
                                        + timer.id().key()
                                        + "|"
                                        + timer.cpuTime().description()
                                );
                            }
                        }
                    }
                }
            }
        }
        final Set<String> golden = new TreeSet<>();
        try (InputStream in = getClass().getResourceAsStream("metric-catalogue.golden")) {
            for (String line : new String(in.readAllBytes(), StandardCharsets.UTF_8).split("\n")) {
                if (line.isBlank() == false) {
                    golden.add(line);
                }
            }
        }
        // NOTE: A metric definition is part of the profile response. A change to the golden file changes what a metric means.
        assertEquals("every identity, unit, policy, CPU pairing and description", golden, lines);
    }

    private static Set<String> declaredMetrics(Path sources) throws IOException {
        final Set<String> names = new TreeSet<>();
        try (Stream<Path> files = Files.walk(sources)) {
            for (Path file : (Iterable<Path>) files.filter(f -> f.toString().endsWith(".java"))::iterator) {
                final String source = Files.readString(file, StandardCharsets.UTF_8);
                final Matcher matcher = DECLARATION.matcher(source);
                while (matcher.find()) {
                    names.add(matcher.group(1) + "." + matcher.group(2));
                }
                final Matcher companion = COMPANION.matcher(source);
                while (companion.find()) {
                    names.add(companion.group(1) + "." + companion.group(2));
                }
            }
        }
        return names;
    }

    private static Path findSources() {
        Path directory = Path.of("").toAbsolutePath();
        for (int up = 0; up < 10 && directory != null; up++) {
            final Path fromRoot = directory.resolve(SOURCE_ROOT);
            if (Files.isDirectory(fromRoot)) {
                return fromRoot;
            }
            final Path fromModule = directory.resolve("src/main/java");
            if (Files.isDirectory(fromModule.resolve("org/elasticsearch/columnar"))) {
                return fromModule;
            }
            directory = directory.getParent();
        }
        return null;
    }
}
