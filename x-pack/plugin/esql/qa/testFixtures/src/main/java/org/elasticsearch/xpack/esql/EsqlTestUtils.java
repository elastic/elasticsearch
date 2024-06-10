/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DateUtils;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.TypesTests;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;
import org.elasticsearch.xpack.esql.stats.Metrics;
import org.elasticsearch.xpack.esql.stats.SearchStats;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeRegistry;
import org.junit.Assert;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.xpack.esql.core.TestUtils.of;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertTrue;

public final class EsqlTestUtils {
    public static class TestSearchStats extends SearchStats {
        public TestSearchStats() {
            super(emptyList());
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
        public boolean exists(String field) {
            return true;
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
        public boolean isIndexed(String field) {
            return exists(field);
        }
    }

    public static final TestSearchStats TEST_SEARCH_STATS = new TestSearchStats();

    private static final Map<String, Map<String, Column>> TABLES = tables();

    public static final EsqlConfiguration TEST_CFG = configuration(new QueryPragmas(Settings.EMPTY));

    public static final Verifier TEST_VERIFIER = new Verifier(new Metrics());

    private EsqlTestUtils() {}

    public static EsqlConfiguration configuration(QueryPragmas pragmas, String query) {
        return new EsqlConfiguration(
            DateUtils.UTC,
            Locale.US,
            null,
            null,
            pragmas,
            EsqlPlugin.QUERY_RESULT_TRUNCATION_MAX_SIZE.getDefault(Settings.EMPTY),
            EsqlPlugin.QUERY_RESULT_TRUNCATION_DEFAULT_SIZE.getDefault(Settings.EMPTY),
            query,
            false,
            TABLES
        );
    }

    public static EsqlConfiguration configuration(QueryPragmas pragmas) {
        return configuration(pragmas, StringUtils.EMPTY);
    }

    public static EsqlConfiguration configuration(String query) {
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

    public static Map<String, EsField> loadMapping(String name) {
        return TypesTests.loadMapping(EsqlDataTypeRegistry.INSTANCE, name, true);
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

    public static void assertWarnings(List<String> warnings, List<String> allowedWarnings, List<Pattern> allowedWarningsRegex) {
        if (allowedWarningsRegex.isEmpty()) {
            assertMap(warnings.stream().sorted().toList(), matchesList(allowedWarnings.stream().sorted().toList()));
        } else {
            for (String warning : warnings) {
                assertTrue("Unexpected warning: " + warning, allowedWarningsRegex.stream().anyMatch(x -> x.matcher(warning).matches()));
            }
        }
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
}
