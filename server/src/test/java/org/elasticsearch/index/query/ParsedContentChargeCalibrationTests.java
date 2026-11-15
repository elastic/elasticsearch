/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.common.xcontent.ParsedContentReservation;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.PlatformManagedObject;
import java.lang.ref.Reference;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

/**
 * Checks that what a request is charged for parsing, see {@link ParsedContentReservation}, covers the heap that its parsed form retains.
 * Each shape stresses a different way of turning tokens into objects: lean query builders, long and non-ASCII strings, unique names,
 * generic maps and lists, and the other parts of a search request that can hold many elements.
 *
 * <p>The retained heap is read from the heap itself. {@link #COPIES} parsed copies of a request are kept alive, garbage is collected, and
 * the growth of the heap in use is divided between them. {@code RamUsageTester} only approximates JDK collections and strings, which is
 * not precise enough to check the costs against.
 *
 * <p>Field names are not measured. Parsers share the string of a field name through a table that outlives them, and the warm-up puts the
 * names there, so the copies share them. A shape with distinct names therefore reads as charged more than it retains.
 *
 * <p>Each shape is measured at two sizes and compared per element, so that the rest of the request, which does not grow with them, cancels
 * out. Other work in the JVM can add to a reading, so a shape that reads as charged less than it retains is measured again before it fails.
 *
 * <p>The costs of {@code ChargingXContentParser} are measured with compressed object pointers and compact object headers, the layout of
 * the bundled JDK. On older runtimes, which lack compact headers, these tests are skipped, and on the bundled JDK's version or later they
 * fail if compact headers are off. With larger headers, every shape here still reads at least 1 except sibling {@code terms}
 * aggregations, at about 0.95.
 */
public class ParsedContentChargeCalibrationTests extends ESTestCase {

    private static final int SMALL = 1_000;
    private static final int LARGE = 3_000;
    private static final int COPIES = 20;
    private static final double MEASUREMENT_TOLERANCE = 0.01;
    private static final int MEASUREMENT_ATTEMPTS = 3;
    /** The bundled JDK, which uses compact object headers by default. */
    private static final int BUNDLED_JDK_FEATURE_VERSION = 27;
    private static final MemoryMXBean MEMORY = ManagementFactory.getMemoryMXBean();

    private record Shape(String name, IntFunction<String> body) {}

    private List<Shape> shapes() {
        return List.of(
            new Shape("bool of term, 8 char values", n -> query(should(n, i -> termClause(value(i, 8))))),
            new Shape("bool of term, 63 char values", n -> query(should(n, i -> termClause(value(i, 63))))),
            new Shape("bool of term, non-ASCII values", n -> query(should(n, i -> termClause(nonAscii(i))))),
            new Shape("bool of term, distinct fields", n -> query(should(n, i -> clause("term", name(i), value(i, 8))))),
            new Shape("bool of match, 8 char values", n -> query(should(n, i -> clause("match", "k", value(i, 8))))),
            // Kept as a string, which holds two bytes a character once any is outside Latin-1.
            new Shape(
                "bool of match, long mixed-script values",
                n -> query(should(n, i -> clause("match", "k", "a".repeat(500) + "\u65e5" + i)))
            ),
            new Shape("bool of empty bool", n -> query(should(n, i -> "{\"bool\":{}}"))),
            new Shape("bool of match_all", n -> query(should(n, i -> "{\"match_all\":{}}"))),
            new Shape("bool of range", n -> query(should(n, i -> "{\"range\":{\"n\":{\"gte\":" + i + ",\"lte\":" + (i + 1) + "}}}"))),
            new Shape(
                "filters agg, anonymous",
                n -> "{\"aggs\":{\"f\":{\"filters\":{\"filters\":[" + elements(n, i -> termClause(value(i, 8))) + "]}}}}"
            ),
            new Shape(
                "filters agg, keyed",
                n -> "{\"aggs\":{\"f\":{\"filters\":{\"filters\":{"
                    + elements(n, i -> quote(name(i)) + ":" + termClause(value(i, 8)))
                    + "}}}}}"
            ),
            new Shape("terms, 8 char values", n -> query("{\"terms\":{\"k\":[" + elements(n, i -> quote(value(i, 8))) + "]}}")),
            new Shape("terms, numbers", n -> query("{\"terms\":{\"n\":[" + elements(n, i -> Integer.toString(1_000_000 + i)) + "]}}")),
            new Shape("ids", n -> query("{\"ids\":{\"values\":[" + elements(n, i -> quote(value(i, 8))) + "]}}")),
            new Shape(
                "runtime_mappings",
                n -> "{\"runtime_mappings\":{" + elements(n, i -> quote(name(i)) + ":{\"type\":\"keyword\"}") + "}}"
            ),
            new Shape(
                "script params",
                n -> "{\"script_fields\":{\"s\":{\"script\":{\"source\":\"1\",\"params\":{"
                    + elements(n, i -> quote(name(i)) + ":" + quote(value(i, 8)))
                    + "}}}}}"
            ),
            new Shape(
                "script params, integers",
                n -> "{\"script_fields\":{\"s\":{\"script\":{\"source\":\"1\",\"params\":{\"p\":["
                    + elements(n, i -> Integer.toString(1_000_000 + i))
                    + "]}}}}}"
            ),
            new Shape(
                "script params, doubles",
                n -> "{\"script_fields\":{\"s\":{\"script\":{\"source\":\"1\",\"params\":{\"p\":["
                    + elements(n, i -> Double.toString(0.5 + i / 7.0))
                    + "]}}}}}"
            ),
            new Shape(
                "script params, big integers",
                n -> "{\"script_fields\":{\"s\":{\"script\":{\"source\":\"1\",\"params\":{\"p\":["
                    + elements(n, i -> "1234567890123456789" + (10_000 + i))
                    + "]}}}}}"
            ),
            new Shape("_source includes", n -> "{\"_source\":{\"includes\":[" + elements(n, i -> quote(name(i))) + "]}}"),
            new Shape("docvalue_fields", n -> "{\"docvalue_fields\":[" + elements(n, i -> quote(name(i))) + "]}"),
            new Shape("sort", n -> "{\"sort\":[" + elements(n, i -> "{" + quote(name(i)) + ":\"asc\"}") + "]}"),
            new Shape("highlight fields", n -> "{\"highlight\":{\"fields\":{" + elements(n, i -> quote(name(i)) + ":{}") + "}}}"),
            new Shape("sibling max aggs", n -> "{\"aggs\":{" + elements(n, i -> quote(name(i)) + ":{\"max\":{\"field\":\"n\"}}") + "}}"),
            new Shape(
                "sibling terms aggs",
                n -> "{\"aggs\":{" + elements(n, i -> quote(name(i)) + ":{\"terms\":{\"field\":\"k\"}}") + "}}"
            ),
            new Shape(
                "sibling date_histogram aggs",
                n -> "{\"aggs\":{"
                    + elements(n, i -> quote(name(i)) + ":{\"date_histogram\":{\"field\":\"d\",\"calendar_interval\":\"day\"}}")
                    + "}}"
            ),
            new Shape(
                "terms aggs with a max sub-aggregation",
                n -> "{\"aggs\":{"
                    + elements(n, i -> quote(name(i)) + ":{\"terms\":{\"field\":\"k\"},\"aggs\":{\"m\":{\"max\":{\"field\":\"n\"}}}}")
                    + "}}"
            ),
            new Shape(
                "sibling composite aggs",
                n -> "{\"aggs\":{"
                    + elements(n, i -> quote(name(i)) + ":{\"composite\":{\"sources\":[{\"s\":{\"terms\":{\"field\":\"k\"}}}]}}")
                    + "}}"
            ),
            new Shape(
                "bool of nested with inner_hits",
                n -> query(should(n, i -> "{\"nested\":{\"path\":\"p\",\"query\":{\"match_all\":{}},\"inner_hits\":{}}}"))
            ),
            new Shape(
                "term suggesters",
                n -> "{\"suggest\":{" + elements(n, i -> quote(name(i)) + ":{\"text\":\"x\",\"term\":{\"field\":\"f\"}}") + "}}"
            ),
            new Shape(
                "sibling top_hits aggs",
                n -> "{\"aggs\":{" + elements(n, i -> quote(name(i)) + ":{\"top_hits\":{\"size\":1}}") + "}}"
            ),
            new Shape(
                "sibling significant_terms aggs",
                n -> "{\"aggs\":{" + elements(n, i -> quote(name(i)) + ":{\"significant_terms\":{\"field\":\"k\"}}") + "}}"
            ),
            new Shape(
                "sibling histogram aggs",
                n -> "{\"aggs\":{" + elements(n, i -> quote(name(i)) + ":{\"histogram\":{\"field\":\"n\",\"interval\":1}}") + "}}"
            ),
            new Shape("sort, plain names", n -> "{\"sort\":[" + elements(n, i -> quote(name(i))) + "]}"),
            // The shortest names are the tightest case, since the objects are as large and the characters are charged least.
            new Shape("sort, one char names", n -> "{\"sort\":[" + elements(n, i -> "\"a\"") + "]}"),
            new Shape("fields", n -> "{\"fields\":[" + elements(n, i -> quote(name(i))) + "]}"),
            new Shape("stored_fields", n -> "{\"stored_fields\":[" + elements(n, i -> quote(name(i))) + "]}"),
            new Shape("stats", n -> "{\"stats\":[" + elements(n, i -> quote(name(i))) + "]}"),
            new Shape("indices_boost", n -> "{\"indices_boost\":[" + elements(n, i -> "{" + quote(name(i)) + ":1.5}") + "]}"),
            new Shape(
                "terms agg include",
                n -> "{\"aggs\":{\"t\":{\"terms\":{\"field\":\"k\",\"include\":[" + elements(n, i -> quote(value(i, 8))) + "]}}}}"
            ),
            new Shape(
                "multi_match fields",
                n -> query("{\"multi_match\":{\"query\":\"x\",\"fields\":[" + elements(n, i -> quote(name(i))) + "]}}")
            ),
            new Shape(
                "query_string fields",
                n -> query("{\"query_string\":{\"query\":\"x\",\"fields\":[" + elements(n, i -> quote(name(i))) + "]}}")
            ),
            new Shape(
                "more_like_this like",
                n -> query("{\"more_like_this\":{\"fields\":[\"k\"],\"like\":[" + elements(n, i -> quote(value(i, 8))) + "]}}")
            ),
            new Shape(
                "terms_set",
                n -> query(
                    "{\"terms_set\":{\"k\":{\"minimum_should_match_field\":\"n\",\"terms\":["
                        + elements(n, i -> quote(value(i, 8)))
                        + "]}}}"
                )
            ),
            new Shape(
                "knn query vector",
                n -> query(
                    "{\"knn\":{\"field\":\"v\",\"num_candidates\":10,\"query_vector\":["
                        + elements(n, i -> Float.toString(0.5f + i / 7f))
                        + "]}}"
                )
            ),
            new Shape(
                "knn query hex vector",
                n -> query("{\"knn\":{\"field\":\"v\",\"num_candidates\":10,\"query_vector\":" + quote(hex(n)) + "}}")
            ),
            new Shape(
                "knn search vector",
                n -> "{\"knn\":{\"field\":\"v\",\"k\":10,\"num_candidates\":10,\"query_vector\":["
                    + elements(n, i -> Float.toString(0.5f + i / 7f))
                    + "]}}"
            ),
            // Queries with the optional parts they keep.
            new Shape(
                "bool of term with _name",
                n -> query(should(n, i -> "{\"term\":{\"k\":{\"value\":" + quote(value(i, 8)) + ",\"_name\":" + quote(name(i)) + "}}}"))
            ),
            new Shape(
                "bool of terms with _name",
                n -> query(should(n, i -> "{\"terms\":{\"k\":[" + quote(value(i, 8)) + "],\"_name\":" + quote(name(i)) + "}}"))
            ),
            new Shape(
                "bool of ids with _name",
                n -> query(should(n, i -> "{\"ids\":{\"values\":[" + quote(value(i, 8)) + "],\"_name\":" + quote(name(i)) + "}}"))
            ),
            new Shape(
                "bool of terms_set with minimum_should_match and _name",
                n -> query(
                    should(
                        n,
                        i -> "{\"terms_set\":{\"k\":{\"terms\":["
                            + quote(value(i, 8))
                            + "],\"minimum_should_match\":\"1\",\"_name\":"
                            + quote(name(i))
                            + "}}}"
                    )
                )
            ),
            new Shape(
                "bool of wrapper",
                n -> query(should(n, i -> "{\"wrapper\":{\"query\":" + quote(base64(termClause(value(i, 8)))) + "}}"))
            ),
            new Shape(
                "bool of bool with minimum_should_match and _name",
                n -> query(should(n, i -> "{\"bool\":{\"minimum_should_match\":\"1\",\"_name\":" + quote(name(i)) + "}}"))
            ),
            new Shape(
                "bool of knn with filter, similarity, rescore_vector and _name",
                n -> query(should(n, i -> "{\"knn\":" + knn(i, "\"num_candidates\":10") + "}"))
            ),
            new Shape(
                "knn searches with filter, similarity, rescore_vector and _name",
                n -> "{\"knn\":[" + elements(n, i -> knn(i, "\"k\":5,\"num_candidates\":10")) + "]}"
            )
        );
    }

    private NamedXContentRegistry registry;

    @Before
    public void createRegistry() {
        assumeTrue("the costs are measured with compressed object pointers", RamUsageEstimator.COMPRESSED_REFS_ENABLED);
        Boolean compactHeaders = compactObjectHeaders();
        if (compactHeaders != null && Runtime.version().feature() >= BUNDLED_JDK_FEATURE_VERSION) {
            // Fails rather than skips, so that a JDK or flag change that drops the layout the costs were measured with is noticed.
            assertTrue("the costs are measured with compact object headers, which this JDK should use", compactHeaders);
        }
        assumeTrue("the costs are measured with compact object headers", Boolean.TRUE.equals(compactHeaders));
        registry = new NamedXContentRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedXContents());
    }

    /** Whether the JVM uses compact object headers, as the bundled JDK does, or {@code null} if it cannot tell, as older JVMs. */
    private static Boolean compactObjectHeaders() {
        try {
            Class<? extends PlatformManagedObject> beanClass = Class.forName("com.sun.management.HotSpotDiagnosticMXBean")
                .asSubclass(PlatformManagedObject.class);
            Object option = beanClass.getMethod("getVMOption", String.class)
                .invoke(ManagementFactory.getPlatformMXBean(beanClass), "UseCompactObjectHeaders");
            return "true".equals(Class.forName("com.sun.management.VMOption").getMethod("getValue").invoke(option));
        } catch (ReflectiveOperationException e) {
            return null;
        }
    }

    public void testChargeCoversRetainedHeap() throws Exception {
        List<String> undercharged = new ArrayList<>();
        for (Shape shape : shapes()) {
            Row row = measuredRow(shape.name(), shape.body(), this::parseSearchSource);
            if (undercharged(row)) {
                undercharged.add(row.toString());
            }
        }
        assertTrue("shapes charged less than they retain:\n" + String.join("\n", undercharged), undercharged.isEmpty());
    }

    public void testChargeCoversTermsValuesWhileParsed() throws Exception {
        // While a terms query reads its values, the per-token estimate has to cover them, with a BytesRef per string sized for the worst
        // case of UTF-8.
        Row strings = measuredRow(
            "terms values while parsed, 8 char",
            n -> "[" + elements(n, i -> quote(value(i, 8))) + "]",
            ParsedContentChargeCalibrationTests::parseTermsValues
        );
        Row numbers = measuredRow(
            "terms values while parsed, numbers",
            n -> "[" + elements(n, i -> Integer.toString(1_000_000 + i)) + "]",
            ParsedContentChargeCalibrationTests::parseTermsValues
        );
        assertFalse(strings.toString(), undercharged(strings));
        assertFalse(numbers.toString(), undercharged(numbers));
    }

    /**
     * Measures and logs a shape, up to {@link #MEASUREMENT_ATTEMPTS} times while it reads as charged less than it retains.
     */
    private Row measuredRow(String shape, IntFunction<String> body, CheckedFunction<XContentParser, Object, IOException> parse)
        throws Exception {
        Row row = row(shape, body, parse);
        for (int attempt = 1; attempt < MEASUREMENT_ATTEMPTS && undercharged(row); attempt++) {
            logger.info("{} measured as charged less than it retains, measuring again", row);
            row = row(shape, body, parse);
        }
        logger.info("{}", row);
        return row;
    }

    /**
     * Whether a shape is charged less than it retains by more than {@link #MEASUREMENT_TOLERANCE}, since the heap is read to within about
     * half a percent, or the reading is broken, with the larger size read as retaining no more than the smaller.
     */
    private static boolean undercharged(Row row) {
        return row.retained() <= 0 || row.charge() < row.retained() * (1 - MEASUREMENT_TOLERANCE);
    }

    /**
     * Per element of a shape: the tokens by type and the characters it reads, what its parsed form retains, and what it is charged.
     */
    private record Row(
        String shape,
        Map<XContentParser.Token, Double> tokens,
        double stringChars,
        double nameChars,
        double retained,
        double charge
    ) {
        @Override
        public String toString() {
            String byType = tokens.entrySet().stream().map(e -> e.getKey() + "=" + format(e.getValue())).collect(Collectors.joining(", "));
            return String.format(
                Locale.ROOT,
                "[%s] per element: tokens [%s] string chars [%s] name chars [%s] retained [%s] charged [%s] ratio [%.2f]",
                shape,
                byType,
                format(stringChars),
                format(nameChars),
                format(retained),
                format(charge),
                charge / retained
            );
        }

        private static String format(double value) {
            return String.format(Locale.ROOT, "%.3f", value);
        }
    }

    private Row row(String shape, IntFunction<String> body, CheckedFunction<XContentParser, Object, IOException> parse) throws Exception {
        // One-time costs, such as loading classes and filling parser caches, must not count towards what the measured parses retain.
        // Parsing twice puts the field names in the table of names even if the first parse found it full and emptied it.
        warmUp(body.apply(LARGE), parse);
        warmUp(body.apply(LARGE), parse);
        Measurement small = measure(body, SMALL, parse);
        Measurement large = measure(body, LARGE, parse);
        double elements = LARGE - SMALL;
        Map<XContentParser.Token, Double> tokens = new EnumMap<>(XContentParser.Token.class);
        for (Map.Entry<XContentParser.Token, Long> e : large.tokens().entrySet()) {
            double perElement = (e.getValue() - small.tokens().getOrDefault(e.getKey(), 0L)) / elements;
            if (perElement != 0) {
                tokens.put(e.getKey(), perElement);
            }
        }
        return new Row(
            shape,
            tokens,
            (large.stringChars() - small.stringChars()) / elements,
            (large.nameChars() - small.nameChars()) / elements,
            (large.retained() - small.retained()) / elements,
            (large.charge() - small.charge()) / elements
        );
    }

    private record Measurement(Map<XContentParser.Token, Long> tokens, long stringChars, long nameChars, long retained, long charge) {}

    private Measurement measure(IntFunction<String> body, int n, CheckedFunction<XContentParser, Object, IOException> parse)
        throws Exception {
        BytesReference content = new BytesArray(body.apply(n));
        Map<XContentParser.Token, Long> tokens = new EnumMap<>(XContentParser.Token.class);
        long stringChars = 0;
        long nameChars = 0;
        try (XContentParser parser = parser(content)) {
            for (XContentParser.Token token = parser.nextToken(); token != null; token = parser.nextToken()) {
                tokens.merge(token, 1L, Long::sum);
                if (token == XContentParser.Token.VALUE_STRING) {
                    stringChars += parser.text().length();
                } else if (token == XContentParser.Token.FIELD_NAME) {
                    nameChars += parser.currentName().length();
                }
            }
        }
        long charge;
        try (
            ParsedContentReservation reservation = new ParsedContentReservation(
                new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofGb(10)),
                true
            )
        ) {
            try (XContentParser parser = reservation.track(parser(content))) {
                parse.apply(parser);
            }
            charge = reservation.reservedBytes();
        }
        return new Measurement(tokens, stringChars, nameChars, retainedPerCopy(content, parse), charge);
    }

    /**
     * The heap that one parsed copy of a request retains, averaged over {@link #COPIES} copies kept alive at once.
     */
    private long retainedPerCopy(BytesReference content, CheckedFunction<XContentParser, Object, IOException> parse) throws Exception {
        Object[] parsed = new Object[COPIES];
        long before = usedHeapAfterGc();
        for (int c = 0; c < COPIES; c++) {
            try (XContentParser parser = parser(content)) {
                parsed[c] = parse.apply(parser);
            }
        }
        long after = usedHeapAfterGc();
        Reference.reachabilityFence(parsed);
        Reference.reachabilityFence(content);
        return (after - before) / COPIES;
    }

    private void warmUp(String body, CheckedFunction<XContentParser, Object, IOException> parse) throws IOException {
        try (XContentParser parser = parser(new BytesArray(body))) {
            parse.apply(parser);
        }
    }

    /** The heap in use once garbage is collected, read until a collection no longer lowers it. */
    private static long usedHeapAfterGc() {
        long used = Long.MAX_VALUE;
        for (int attempt = 0; attempt < 5; attempt++) {
            System.gc();
            long now = MEMORY.getHeapMemoryUsage().getUsed();
            if (now >= used) {
                break;
            }
            used = now;
        }
        return used;
    }

    private XContentParser parser(BytesReference content) throws IOException {
        return XContentHelper.createParserNotCompressed(
            XContentParserConfiguration.EMPTY.withRegistry(registry),
            content,
            XContentType.JSON
        );
    }

    private Object parseSearchSource(XContentParser parser) throws IOException {
        return new SearchSourceBuilder().parseXContent(parser, true, feature -> true);
    }

    private static Object parseTermsValues(XContentParser parser) throws IOException {
        assertEquals(XContentParser.Token.START_ARRAY, parser.nextToken());
        return TermsQueryBuilder.parseValues(parser);
    }

    private static String query(String query) {
        return "{\"query\":" + query + "}";
    }

    private static String should(int n, IntFunction<String> clause) {
        return "{\"bool\":{\"should\":[" + elements(n, clause) + "]}}";
    }

    private static String clause(String type, String field, String value) {
        return "{\"" + type + "\":{\"" + field + "\":" + quote(value) + "}}";
    }

    /** {@code bytes} bytes as hex, two characters each. */
    private static String hex(int bytes) {
        StringBuilder hex = new StringBuilder(bytes * 2);
        for (int i = 0; i < bytes; i++) {
            hex.append(String.format(Locale.ROOT, "%02x", i & 0xff));
        }
        return hex.toString();
    }

    private static String base64(String s) {
        return Base64.getEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    private static String termClause(String value) {
        return clause("term", "k", value);
    }

    /**
     * The body of a kNN query or search on a two-dimensional vector, with the optional parts it keeps: a filter, a similarity, a rescore
     * and a name.
     */
    private static String knn(int i, String candidates) {
        return "{\"field\":\"v\",\"query_vector\":[0.5,"
            + (i / 7f)
            + "],"
            + candidates
            + ",\"filter\":"
            + termClause(value(i, 8))
            + ",\"similarity\":0.5,\"rescore_vector\":{\"oversample\":2.0},\"_name\":"
            + quote(name(i))
            + "}";
    }

    /** Exactly {@code length} characters, and distinct for every {@code i} so that no two values share a string. */
    private static String value(int i, int length) {
        String digits = Integer.toString(i);
        return "v" + "0".repeat(length - 1 - digits.length()) + digits;
    }

    /** Distinct for every {@code i}, with characters outside Latin-1, so that a string keeps two bytes per character. */
    private static String nonAscii(int i) {
        return "検索値" + i;
    }

    /** A distinct field or entry name for every {@code i}. */
    private static String name(int i) {
        return "f" + i;
    }

    private static String quote(String s) {
        return "\"" + s + "\"";
    }

    private static String elements(int n, IntFunction<String> element) {
        StringBuilder elements = new StringBuilder();
        for (int i = 0; i < n; i++) {
            if (i > 0) {
                elements.append(',');
            }
            elements.append(element.apply(i));
        }
        return elements.toString();
    }
}
