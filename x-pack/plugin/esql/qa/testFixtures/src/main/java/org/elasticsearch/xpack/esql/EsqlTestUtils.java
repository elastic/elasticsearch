/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.apache.http.HttpEntity;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.sandbox.document.HalfFloatPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.RemoteException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.compute.data.AggregateMetricDoubleBlockBuilder;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geo.ShapeTestUtils;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.util.DateUtils;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.RLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.WildcardLike;
import org.elasticsearch.xpack.esql.expression.predicate.Range;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.inference.InferenceResolution;
import org.elasticsearch.xpack.esql.inference.InferenceRunner;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.parser.QueryParam;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.plugin.TransportActionServices;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.stats.SearchStats;
import org.elasticsearch.xpack.esql.telemetry.Metrics;
import org.elasticsearch.xpack.versionfield.Version;
import org.junit.Assert;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.time.Period;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.jar.JarInputStream;
import java.util.zip.ZipEntry;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.test.ESTestCase.assertEquals;
import static org.elasticsearch.test.ESTestCase.between;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomArray;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomByte;
import static org.elasticsearch.test.ESTestCase.randomDouble;
import static org.elasticsearch.test.ESTestCase.randomFloat;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomInt;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.test.ESTestCase.randomIp;
import static org.elasticsearch.test.ESTestCase.randomLong;
import static org.elasticsearch.test.ESTestCase.randomLongBetween;
import static org.elasticsearch.test.ESTestCase.randomMillisUpToYear9999;
import static org.elasticsearch.test.ESTestCase.randomNonNegativeLong;
import static org.elasticsearch.test.ESTestCase.randomShort;
import static org.elasticsearch.test.ESTestCase.randomZone;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.elasticsearch.xpack.esql.parser.ParserUtils.ParamClassification.IDENTIFIER;
import static org.elasticsearch.xpack.esql.parser.ParserUtils.ParamClassification.PATTERN;
import static org.elasticsearch.xpack.esql.parser.ParserUtils.ParamClassification.VALUE;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public final class EsqlTestUtils {

    public static final Literal ONE = new Literal(Source.EMPTY, 1, DataType.INTEGER);
    public static final Literal TWO = new Literal(Source.EMPTY, 2, DataType.INTEGER);
    public static final Literal THREE = new Literal(Source.EMPTY, 3, DataType.INTEGER);
    public static final Literal FOUR = new Literal(Source.EMPTY, 4, DataType.INTEGER);
    public static final Literal FIVE = new Literal(Source.EMPTY, 5, DataType.INTEGER);
    public static final Literal SIX = new Literal(Source.EMPTY, 6, DataType.INTEGER);

    public static Equals equalsOf(Expression left, Expression right) {
        return new Equals(EMPTY, left, right, null);
    }

    public static LessThan lessThanOf(Expression left, Expression right) {
        return new LessThan(EMPTY, left, right, null);
    }

    public static GreaterThan greaterThanOf(Expression left, Expression right) {
        return new GreaterThan(EMPTY, left, right, ESTestCase.randomZone());
    }

    public static NotEquals notEqualsOf(Expression left, Expression right) {
        return new NotEquals(EMPTY, left, right, ESTestCase.randomZone());
    }

    public static LessThanOrEqual lessThanOrEqualOf(Expression left, Expression right) {
        return new LessThanOrEqual(EMPTY, left, right, ESTestCase.randomZone());
    }

    public static GreaterThanOrEqual greaterThanOrEqualOf(Expression left, Expression right) {
        return new GreaterThanOrEqual(EMPTY, left, right, ESTestCase.randomZone());
    }

    public static FieldAttribute getFieldAttribute() {
        return getFieldAttribute("a");
    }

    public static FieldAttribute getFieldAttribute(String name) {
        return getFieldAttribute(name, INTEGER);
    }

    public static FieldAttribute getFieldAttribute(String name, DataType dataType) {
        return new FieldAttribute(EMPTY, name, new EsField(name + "f", dataType, emptyMap(), true));
    }

    public static FieldAttribute fieldAttribute() {
        return fieldAttribute(randomAlphaOfLength(10), randomFrom(DataType.types()));
    }

    public static FieldAttribute fieldAttribute(String name, DataType type) {
        return new FieldAttribute(EMPTY, name, new EsField(name, type, emptyMap(), randomBoolean()));
    }

    public static Literal of(Object value) {
        return of(Source.EMPTY, value);
    }

    /**
     * Utility method for creating 'in-line' Literals (out of values instead of expressions).
     */
    public static Literal of(Source source, Object value) {
        if (value instanceof Literal) {
            return (Literal) value;
        }
        return new Literal(source, value, DataType.fromJava(value));
    }

    public static ReferenceAttribute referenceAttribute(String name, DataType type) {
        return new ReferenceAttribute(EMPTY, name, type);
    }

    public static Range rangeOf(Expression value, Expression lower, boolean includeLower, Expression upper, boolean includeUpper) {
        return new Range(EMPTY, value, lower, includeLower, upper, includeUpper, randomZone());
    }

    public static EsRelation relation() {
        return new EsRelation(EMPTY, new EsIndex(randomAlphaOfLength(8), emptyMap()), IndexMode.STANDARD);
    }

    /**
     * This version of SearchStats always returns true for all fields for all boolean methods.
     * For custom behaviour either use {@link TestConfigurableSearchStats} or override the specific methods.
     */
    public static class TestSearchStats implements SearchStats {

        @Override
        public boolean exists(String field) {
            return true;
        }

        @Override
        public boolean isIndexed(String field) {
            return exists(field);
        }

        @Override
        public boolean hasDocValues(String field) {
            return exists(field);
        }

        @Override
        public boolean hasExactSubfield(String field) {
            return exists(field);
        }

        @Override
        public long count() {
            return -1;
        }

        @Override
        public long count(String field) {
            return exists(field) ? -1 : 0;
        }

        @Override
        public long count(String field, BytesRef value) {
            return exists(field) ? -1 : 0;
        }

        @Override
        public byte[] min(String field, DataType dataType) {
            return null;
        }

        @Override
        public byte[] max(String field, DataType dataType) {
            return null;
        }

        @Override
        public boolean isSingleValue(String field) {
            return false;
        }

        @Override
        public boolean canUseEqualityOnSyntheticSourceDelegate(String name, String value) {
            return false;
        }
    }

    /**
     * This version of SearchStats can be preconfigured to return true/false for various combinations of the four field settings:
     * <ol>
     *     <li>exists</li>
     *     <li>isIndexed</li>
     *     <li>hasDocValues</li>
     *     <li>hasExactSubfield</li>
     * </ol>
     * The default will return true for all fields. The include/exclude methods can be used to configure the settings for specific fields.
     * If you call 'include' with no fields, it will switch to return false for all fields.
     */
    public static class TestConfigurableSearchStats extends TestSearchStats {
        public enum Config {
            EXISTS,
            INDEXED,
            DOC_VALUES,
            EXACT_SUBFIELD
        }

        private final Map<Config, Set<String>> includes = new HashMap<>();
        private final Map<Config, Set<String>> excludes = new HashMap<>();

        public TestConfigurableSearchStats include(Config key, String... fields) {
            // If this method is called with no fields, it is interpreted to mean include none, so we include a dummy field
            for (String field : fields.length == 0 ? new String[] { "-" } : fields) {
                includes.computeIfAbsent(key, k -> new HashSet<>()).add(field);
                excludes.computeIfAbsent(key, k -> new HashSet<>()).remove(field);
            }
            return this;
        }

        public TestConfigurableSearchStats exclude(Config key, String... fields) {
            for (String field : fields) {
                includes.computeIfAbsent(key, k -> new HashSet<>()).remove(field);
                excludes.computeIfAbsent(key, k -> new HashSet<>()).add(field);
            }
            return this;
        }

        private boolean isConfigationSet(Config config, String field) {
            Set<String> in = includes.getOrDefault(config, Set.of());
            Set<String> ex = excludes.getOrDefault(config, Set.of());
            return (in.isEmpty() || in.contains(field)) && ex.contains(field) == false;
        }

        @Override
        public boolean exists(String field) {
            return isConfigationSet(Config.EXISTS, field);
        }

        @Override
        public boolean isIndexed(String field) {
            return isConfigationSet(Config.INDEXED, field);
        }

        @Override
        public boolean hasDocValues(String field) {
            return isConfigationSet(Config.DOC_VALUES, field);
        }

        @Override
        public boolean hasExactSubfield(String field) {
            return isConfigationSet(Config.EXACT_SUBFIELD, field);
        }

        @Override
        public String toString() {
            return "TestConfigurableSearchStats{" + "includes=" + includes + ", excludes=" + excludes + '}';
        }
    }

    public static final TestSearchStats TEST_SEARCH_STATS = new TestSearchStats();

    private static final Map<String, Map<String, Column>> TABLES = tables();

    public static final Configuration TEST_CFG = configuration(new QueryPragmas(Settings.EMPTY));

    public static LogicalOptimizerContext unboundLogicalOptimizerContext() {
        return new LogicalOptimizerContext(EsqlTestUtils.TEST_CFG, FoldContext.small());
    }

    public static final Verifier TEST_VERIFIER = new Verifier(new Metrics(new EsqlFunctionRegistry()), new XPackLicenseState(() -> 0L));

    public static final TransportActionServices MOCK_TRANSPORT_ACTION_SERVICES = new TransportActionServices(
        mock(TransportService.class),
        mock(SearchService.class),
        null,
        mock(ClusterService.class),
        mock(IndexNameExpressionResolver.class),
        null,
        mockInferenceRunner()
    );

    @SuppressWarnings("unchecked")
    private static InferenceRunner mockInferenceRunner() {
        InferenceRunner inferenceRunner = mock(InferenceRunner.class);
        doAnswer(i -> {
            i.getArgument(1, ActionListener.class).onResponse(emptyInferenceResolution());
            return null;
        }).when(inferenceRunner).resolveInferenceIds(any(), any());

        return inferenceRunner;
    }

    private EsqlTestUtils() {}

    public static Configuration configuration(QueryPragmas pragmas, String query) {
        return new Configuration(
            DateUtils.UTC,
            Locale.US,
            null,
            null,
            pragmas,
            EsqlPlugin.QUERY_RESULT_TRUNCATION_MAX_SIZE.getDefault(Settings.EMPTY),
            EsqlPlugin.QUERY_RESULT_TRUNCATION_DEFAULT_SIZE.getDefault(Settings.EMPTY),
            query,
            false,
            TABLES,
            System.nanoTime(),
            false
        );
    }

    public static Configuration configuration(QueryPragmas pragmas) {
        return configuration(pragmas, StringUtils.EMPTY);
    }

    public static Configuration configuration(String query) {
        return configuration(new QueryPragmas(Settings.EMPTY), query);
    }

    public static Literal L(Object value) {
        return of(value);
    }

    public static LogicalPlan emptySource() {
        return new LocalRelation(Source.EMPTY, emptyList(), LocalSupplier.EMPTY);
    }

    public static LogicalPlan localSource(BlockFactory blockFactory, List<Attribute> fields, List<Object> row) {
        return new LocalRelation(Source.EMPTY, fields, LocalSupplier.of(BlockUtils.fromListRow(blockFactory, row)));
    }

    public static <T> T as(Object node, Class<T> type) {
        Assert.assertThat(node, instanceOf(type));
        return type.cast(node);
    }

    public static Limit asLimit(Object node, Integer limitLiteral) {
        return asLimit(node, limitLiteral, null);
    }

    public static Limit asLimit(Object node, Integer limitLiteral, Boolean duplicated) {
        Limit limit = as(node, Limit.class);
        if (limitLiteral != null) {
            assertEquals(as(limit.limit(), Literal.class).value(), limitLiteral);
        }
        if (duplicated != null) {
            assertEquals(limit.duplicated(), duplicated);
        }
        return limit;
    }

    public static Map<String, EsField> loadMapping(String name) {
        return LoadMapping.loadMapping(name);
    }

    public static String loadUtf8TextFile(String name) {
        try (InputStream textStream = EsqlTestUtils.class.getResourceAsStream(name)) {
            return new String(textStream.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static EnrichResolution emptyPolicyResolution() {
        return new EnrichResolution();
    }

    public static InferenceResolution emptyInferenceResolution() {
        return InferenceResolution.EMPTY;
    }

    public static SearchStats statsForExistingField(String... names) {
        return fieldMatchingExistOrMissing(true, names);
    }

    public static SearchStats statsForMissingField(String... names) {
        return fieldMatchingExistOrMissing(false, names);
    }

    private static SearchStats fieldMatchingExistOrMissing(boolean exists, String... names) {
        return new TestSearchStats() {
            private final Set<String> fields = Set.of(names);

            @Override
            public boolean exists(String field) {
                return fields.contains(field) == exists;
            }
        };
    }

    public static List<List<Object>> getValuesList(EsqlQueryResponse results) {
        return getValuesList(results.values());
    }

    public static List<List<Object>> getValuesList(Iterator<Iterator<Object>> values) {
        var valuesList = new ArrayList<List<Object>>();
        values.forEachRemaining(row -> {
            var rowValues = new ArrayList<>();
            row.forEachRemaining(rowValues::add);
            valuesList.add(rowValues);
        });
        return valuesList;
    }

    public static List<List<Object>> getValuesList(Iterable<Iterable<Object>> values) {
        var valuesList = new ArrayList<List<Object>>();
        values.iterator().forEachRemaining(row -> {
            var rowValues = new ArrayList<>();
            row.iterator().forEachRemaining(rowValues::add);
            valuesList.add(rowValues);
        });
        return valuesList;
    }

    public static List<String> withDefaultLimitWarning(List<String> warnings) {
        List<String> result = warnings == null ? new ArrayList<>() : new ArrayList<>(warnings);
        result.add("No limit defined, adding default limit of [1000]");
        return result;
    }

    /**
     * Generates a random enrich command with or without explicit parameters
     */
    public static String randomEnrichCommand(String name, Enrich.Mode mode, String matchField, List<String> enrichFields) {
        String onField = " ";
        String withFields = " ";

        List<String> before = new ArrayList<>();
        List<String> after = new ArrayList<>();

        if (randomBoolean()) {
            // => RENAME new_match_field=match_field | ENRICH name ON new_match_field | RENAME new_match_field AS match_field
            String newMatchField = "my_" + matchField;
            before.add("RENAME " + matchField + " AS " + newMatchField);
            onField = " ON " + newMatchField;
            after.add("RENAME " + newMatchField + " AS " + matchField);
        } else if (randomBoolean()) {
            onField = " ON " + matchField;
        }
        if (randomBoolean()) {
            List<String> fields = new ArrayList<>();
            for (String f : enrichFields) {
                if (randomBoolean()) {
                    fields.add(f);
                } else {
                    // ENRICH name WITH new_a=a,b|new_c=c | RENAME new_a AS a | RENAME new_c AS c
                    fields.add("new_" + f + "=" + f);
                    after.add("RENAME new_" + f + " AS " + f);
                }
            }
            withFields = " WITH " + String.join(",", fields);
        }
        String enrich = "ENRICH ";
        if (mode != Enrich.Mode.ANY || randomBoolean()) {
            enrich += " _" + mode + ":";
        }
        enrich += name;
        enrich += onField;
        enrich += withFields;
        List<String> all = new ArrayList<>(before);
        all.add(enrich);
        all.addAll(after);
        return String.join(" | ", all);
    }

    /**
     * "tables" provided in the context for the LOOKUP command. If you
     * add to this, you must also add to {@code EsqlSpecTestCase#tables};
     */
    public static Map<String, Map<String, Column>> tables() {
        BlockFactory factory = new BlockFactory(new NoopCircuitBreaker(CircuitBreaker.REQUEST), BigArrays.NON_RECYCLING_INSTANCE);
        Map<String, Map<String, Column>> tables = new TreeMap<>();
        try (
            IntBlock.Builder ints = factory.newIntBlockBuilder(10);
            LongBlock.Builder longs = factory.newLongBlockBuilder(10);
            BytesRefBlock.Builder names = factory.newBytesRefBlockBuilder(10);
        ) {
            for (int i = 0; i < 10; i++) {
                ints.appendInt(i);
                longs.appendLong(i);
                names.appendBytesRef(new BytesRef(numberName(i)));
            }

            IntBlock intsBlock = ints.build();
            LongBlock longsBlock = longs.build();
            BytesRefBlock namesBlock = names.build();
            tables.put(
                "int_number_names",
                table(
                    Map.entry("int", new Column(DataType.INTEGER, intsBlock)),
                    Map.entry("name", new Column(DataType.KEYWORD, namesBlock))
                )
            );
            tables.put(
                "long_number_names",
                table(Map.entry("long", new Column(DataType.LONG, longsBlock)), Map.entry("name", new Column(DataType.KEYWORD, namesBlock)))
            );
        }
        for (boolean hasNull : new boolean[] { true, false }) {
            try (
                DoubleBlock.Builder doubles = factory.newDoubleBlockBuilder(2);
                BytesRefBlock.Builder names = factory.newBytesRefBlockBuilder(2);
            ) {
                doubles.appendDouble(2.03);
                names.appendBytesRef(new BytesRef("two point zero three"));
                doubles.appendDouble(2.08);
                names.appendBytesRef(new BytesRef("two point zero eight"));
                if (hasNull) {
                    doubles.appendDouble(0.0);
                    names.appendNull();
                }
                tables.put(
                    "double_number_names" + (hasNull ? "_with_null" : ""),
                    table(
                        Map.entry("double", new Column(DataType.DOUBLE, doubles.build())),
                        Map.entry("name", new Column(DataType.KEYWORD, names.build()))
                    )
                );
            }
        }
        try (
            BytesRefBlock.Builder aa = factory.newBytesRefBlockBuilder(3);
            BytesRefBlock.Builder ab = factory.newBytesRefBlockBuilder(3);
            IntBlock.Builder na = factory.newIntBlockBuilder(3);
            IntBlock.Builder nb = factory.newIntBlockBuilder(3);
        ) {
            aa.appendBytesRef(new BytesRef("foo"));
            ab.appendBytesRef(new BytesRef("zoo"));
            na.appendInt(1);
            nb.appendInt(-1);

            aa.appendBytesRef(new BytesRef("bar"));
            ab.appendBytesRef(new BytesRef("zop"));
            na.appendInt(10);
            nb.appendInt(-10);

            aa.appendBytesRef(new BytesRef("baz"));
            ab.appendBytesRef(new BytesRef("zoi"));
            na.appendInt(100);
            nb.appendInt(-100);

            aa.appendBytesRef(new BytesRef("foo"));
            ab.appendBytesRef(new BytesRef("foo"));
            na.appendInt(2);
            nb.appendInt(-2);

            tables.put(
                "big",
                table(
                    Map.entry("aa", new Column(DataType.KEYWORD, aa.build())),
                    Map.entry("ab", new Column(DataType.KEYWORD, ab.build())),
                    Map.entry("na", new Column(DataType.INTEGER, na.build())),
                    Map.entry("nb", new Column(DataType.INTEGER, nb.build()))
                )
            );
        }

        return unmodifiableMap(tables);
    }

    /**
     * Builds a table from the provided parameters. This isn't just a call to
     * {@link Map#of} because we want to maintain sort order of the columns
     */
    @SafeVarargs
    public static <T> Map<String, T> table(Map.Entry<String, T>... kv) {
        Map<String, T> table = new LinkedHashMap<>();
        for (Map.Entry<String, T> stringTEntry : kv) {
            table.put(stringTEntry.getKey(), stringTEntry.getValue());
        }
        return table;
    }

    public static String numberName(int i) {
        return switch (i) {
            case 0 -> "zero";
            case 1 -> "one";
            case 2 -> "two";
            case 3 -> "three";
            case 4 -> "four";
            case 5 -> "five";
            case 6 -> "six";
            case 7 -> "seven";
            case 8 -> "eight";
            case 9 -> "nine";
            default -> throw new IllegalArgumentException();
        };
    }

    @SuppressForbidden(reason = "need to open stream")
    public static InputStream inputStream(URL resource) throws IOException {
        URLConnection con = resource.openConnection();
        // do not to cache files (to avoid keeping file handles around)
        con.setUseCaches(false);
        return con.getInputStream();
    }

    public static BufferedReader reader(URL resource) throws IOException {
        return new BufferedReader(new InputStreamReader(inputStream(resource), StandardCharsets.UTF_8));
    }

    /**
     * Returns the classpath resources matching a simple pattern ("*.csv").
     * It supports folders separated by "/" (e.g. "/some/folder/*.txt").
     *
     * Currently able to resolve resources inside the classpath either from:
     * folders in the file-system (typically IDEs) or
     * inside jars (gradle).
     */
    @SuppressForbidden(reason = "classpath discovery")
    public static List<URL> classpathResources(String pattern) throws IOException {
        while (pattern.startsWith("/")) {
            pattern = pattern.substring(1);
        }

        Tuple<String, String> split = pathAndName(pattern);

        // the root folder searched inside the classpath - default is the root classpath
        // default file match
        final String root = split.v1();
        final String filePattern = split.v2();

        String[] resources = System.getProperty("java.class.path").split(System.getProperty("path.separator"));

        List<URL> matches = new ArrayList<>();

        for (String resource : resources) {
            Path path = PathUtils.get(resource);

            // check whether we're dealing with a jar
            // Java 7 java.nio.fileFileSystem can be used on top of ZIPs/JARs but consumes more memory
            // hence the use of the JAR API
            if (path.toString().endsWith(".jar")) {
                try (JarInputStream jar = jarInputStream(path.toUri().toURL())) {
                    ZipEntry entry = null;
                    while ((entry = jar.getNextEntry()) != null) {
                        String name = entry.getName();
                        Tuple<String, String> entrySplit = pathAndName(name);
                        if (root.equals(entrySplit.v1()) && Regex.simpleMatch(filePattern, entrySplit.v2())) {
                            matches.add(new URL("jar:" + path.toUri() + "!/" + name));
                        }
                    }
                }
            }
            // normal file access
            else if (Files.isDirectory(path)) {
                Files.walkFileTree(path, EnumSet.allOf(FileVisitOption.class), 1, new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        // remove the path folder from the URL
                        String name = Strings.replace(file.toUri().toString(), path.toUri().toString(), StringUtils.EMPTY);
                        Tuple<String, String> entrySplit = pathAndName(name);
                        if (root.equals(entrySplit.v1()) && Regex.simpleMatch(filePattern, entrySplit.v2())) {
                            matches.add(file.toUri().toURL());
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });
            }
        }
        return matches;
    }

    @SuppressForbidden(reason = "need to open jar")
    public static JarInputStream jarInputStream(URL resource) throws IOException {
        return new JarInputStream(inputStream(resource));
    }

    public static Tuple<String, String> pathAndName(String string) {
        String folder = StringUtils.EMPTY;
        String file = string;
        int lastIndexOf = string.lastIndexOf('/');
        if (lastIndexOf > 0) {
            folder = string.substring(0, lastIndexOf - 1);
            if (lastIndexOf + 1 < string.length()) {
                file = string.substring(lastIndexOf + 1);
            }
        }
        return new Tuple<>(folder, file);
    }

    /**
     * Generate a random value of the appropriate type to fit into blocks of {@code e}.
     */
    public static Literal randomLiteral(DataType type) {
        return new Literal(Source.EMPTY, switch (type) {
            case BOOLEAN -> randomBoolean();
            case BYTE -> randomByte();
            case SHORT -> randomShort();
            case INTEGER, COUNTER_INTEGER -> randomInt();
            case LONG, COUNTER_LONG -> randomLong();
            case UNSIGNED_LONG -> randomNonNegativeLong();
            case DATE_PERIOD -> Period.of(randomIntBetween(-1000, 1000), randomIntBetween(-13, 13), randomIntBetween(-32, 32));
            case DATETIME -> randomMillisUpToYear9999();
            case DATE_NANOS -> randomLongBetween(0, Long.MAX_VALUE);
            case DOUBLE, SCALED_FLOAT, COUNTER_DOUBLE -> randomDouble();
            case FLOAT -> randomFloat();
            case HALF_FLOAT -> HalfFloatPoint.sortableShortToHalfFloat(HalfFloatPoint.halfFloatToSortableShort(randomFloat()));
            case KEYWORD -> new BytesRef(randomAlphaOfLength(5));
            case IP -> new BytesRef(InetAddressPoint.encode(randomIp(randomBoolean())));
            case TIME_DURATION -> Duration.ofMillis(randomLongBetween(-604800000L, 604800000L)); // plus/minus 7 days
            case TEXT -> new BytesRef(randomAlphaOfLength(50));
            case VERSION -> randomVersion().toBytesRef();
            case GEO_POINT -> GEO.asWkb(GeometryTestUtils.randomPoint());
            case CARTESIAN_POINT -> CARTESIAN.asWkb(ShapeTestUtils.randomPoint());
            case GEO_SHAPE -> GEO.asWkb(GeometryTestUtils.randomGeometry(randomBoolean()));
            case CARTESIAN_SHAPE -> CARTESIAN.asWkb(ShapeTestUtils.randomGeometry(randomBoolean()));
            case AGGREGATE_METRIC_DOUBLE -> new AggregateMetricDoubleBlockBuilder.AggregateMetricDoubleLiteral(
                randomDouble(),
                randomDouble(),
                randomDouble(),
                randomInt()
            );
            case NULL -> null;
            case SOURCE -> {
                try {
                    yield BytesReference.bytes(
                        JsonXContent.contentBuilder().startObject().field(randomAlphaOfLength(3), randomAlphaOfLength(10)).endObject()
                    ).toBytesRef();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            case DENSE_VECTOR -> Arrays.asList(randomArray(10, 10, i -> new Float[10], ESTestCase::randomFloat));
            case UNSUPPORTED, OBJECT, DOC_DATA_TYPE, TSID_DATA_TYPE, PARTIAL_AGG -> throw new IllegalArgumentException(
                "can't make random values for [" + type.typeName() + "]"
            );
        }, type);
    }

    static Version randomVersion() {
        // TODO degenerate versions and stuff
        return switch (between(0, 2)) {
            case 0 -> new Version(Integer.toString(between(0, 100)));
            case 1 -> new Version(between(0, 100) + "." + between(0, 100));
            case 2 -> new Version(between(0, 100) + "." + between(0, 100) + "." + between(0, 100));
            default -> throw new IllegalArgumentException();
        };
    }

    public static WildcardLike wildcardLike(Expression left, String exp) {
        return new WildcardLike(EMPTY, left, new WildcardPattern(exp));
    }

    public static RLike rlike(Expression left, String exp) {
        return new RLike(EMPTY, left, new RLikePattern(exp));
    }

    public static QueryParam paramAsConstant(String name, Object value) {
        return new QueryParam(name, value, DataType.fromJava(value), VALUE);
    }

    public static QueryParam paramAsIdentifier(String name, Object value) {
        return new QueryParam(name, value, NULL, IDENTIFIER);
    }

    public static QueryParam paramAsPattern(String name, Object value) {
        return new QueryParam(name, value, NULL, PATTERN);
    }

    /**
     * Asserts that:
     * 1. Cancellation exceptions are ignored when more relevant exceptions exist.
     * 2. Transport exceptions are unwrapped, and the actual causes are reported to users.
     */
    public static void assertEsqlFailure(Exception e) {
        assertNotNull(e);
        var cancellationFailure = ExceptionsHelper.unwrapCausesAndSuppressed(e, t -> t instanceof TaskCancelledException).orElse(null);
        assertNull("cancellation exceptions must be ignored", cancellationFailure);
        ExceptionsHelper.unwrapCausesAndSuppressed(e, t -> t instanceof RemoteTransportException)
            .ifPresent(transportFailure -> assertNull("remote transport exception must be unwrapped", transportFailure.getCause()));
    }

    public static Map<String, Object> entityToMap(HttpEntity entity, XContentType expectedContentType) throws IOException {
        try (InputStream content = entity.getContent()) {
            XContentType xContentType = XContentType.fromMediaType(entity.getContentType().getValue());
            assertEquals(expectedContentType, xContentType);
            return XContentHelper.convertToMap(xContentType.xContent(), content, false /* ordered */);
        }
    }

    /**
     * Errors from remotes are wrapped in RemoteException while the ones from the local cluster
     * aren't. This utility method is useful for unwrapping in such cases.
     * @param e Exception to unwrap.
     * @return Cause of RemoteException, else the error itself.
     */
    public static Exception unwrapIfWrappedInRemoteException(Exception e) {
        if (e instanceof RemoteException rce) {
            return (Exception) rce.getCause();
        } else {
            return e;
        }
    }
}
