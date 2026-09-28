/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase.SuiteScopeTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.dsltranslate.QueryDslTranslator;
import org.elasticsearch.xpack.unsignedlong.UnsignedLongMapperPlugin;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Every construct the translator attempts, run against a mapped index and a dataset holding the same rows: per-field
 * constructs on every column in every bool context, the rest — {@code match_all}, {@code match_none}, the
 * {@code multi_match} forms and an unsupported clause beside a {@code term} — once per context. {@code text} and {@code version}
 * are not in the fixture; {@code RequestFilterGoldenTests} pins those.
 *
 * <p>Two properties on every filter: the dataset returns everything the index returns, and where the whole filter
 * translates, exactly that. Every column is sparse, so each shape meets rows that lack the field — where a translation
 * can agree under {@code AND} and disagree under {@code must_not}.
 */
@SuiteScopeTestCase
public class ExternalDatasetRequestFilterSweepIT extends AbstractExternalDataSourceIT {

    private static final int ROWS = 60;
    private static final String INDEX = "sweep_idx";
    private static final String DATASET = "sweep_ds";
    private static final Instant BASE = Instant.parse("2020-01-01T12:34:56Z");

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class, UnsignedLongMapperPlugin.class);
    }

    /** A sparse column: which rows carry it, their values, and a value and bounds that select part of the data. */
    private record Column(
        String name,
        DataType type,
        IntPredicate present,
        IntFunction<Object> value,
        Object sample,
        Object lower,
        Object upper
    ) {}

    /** ES|QL calls it {@code datetime}; an index mapping and a dataset declaration call it {@code date}. */
    private static String mappingType(DataType type) {
        return type == DataType.DATETIME ? "date" : type.typeName();
    }

    private static String iso(Instant instant) {
        return DateTimeFormatter.ISO_INSTANT.format(instant);
    }

    private static final List<Column> COLUMNS = List.of(
        new Column("k", DataType.KEYWORD, i -> i % 7 != 0, i -> "k" + (i % 5), "k1", "k1", "k3"),
        new Column("n", DataType.INTEGER, i -> i % 6 != 0, i -> (i * 7) % 50, 14, 5, 30),
        new Column("l", DataType.LONG, i -> i % 5 != 0, i -> i * 1000L - 20000L, 3000L, -5000L, 20000L),
        new Column("d", DataType.DOUBLE, i -> i % 4 != 0, i -> (i % 9) * 1.5 - 3.0, 1.5, -1.5, 3.0),
        new Column("b", DataType.BOOLEAN, i -> i % 3 != 0, i -> i % 2 == 0, true, false, true),
        new Column(
            "t",
            DataType.DATETIME,
            i -> i % 8 != 0,
            i -> iso(BASE.plus(Duration.ofDays(i))),
            "2020-01-11",
            "2020-01-05",
            "2020-01-20"
        ),
        new Column(
            "tn",
            DataType.DATE_NANOS,
            i -> i % 9 != 0,
            i -> iso(BASE.plus(Duration.ofHours(i))),
            "2020-01-02",
            "2020-01-01",
            "2020-01-02"
        ),
        new Column("u", DataType.UNSIGNED_LONG, i -> i % 5 != 1, i -> (long) i * 10, 100L, 50L, 300L),
        new Column("a", DataType.IP, i -> i % 4 != 1, i -> "10.0.0." + (i % 20), "10.0.0.6", "10.0.0.2", "10.0.0.9")
    );

    /** A field neither source has, swept beside the real columns. */
    private static final Column MISSING = new Column("nope", DataType.KEYWORD, i -> false, i -> null, "x", "a", "z");

    private static List<Column> sweptColumns() {
        List<Column> columns = new ArrayList<>(COLUMNS);
        columns.add(MISSING);
        return columns;
    }

    @Override
    protected void setupSuiteScopeCluster() throws Exception {
        List<String> mapping = new ArrayList<>(List.of("id", "type=integer"));
        StringBuilder csv = new StringBuilder("id:integer");
        LinkedHashMap<String, DatasetFieldMapping> declared = new LinkedHashMap<>();
        declared.put("id", new DatasetFieldMapping("integer", null));
        for (Column column : COLUMNS) {
            mapping.add(column.name());
            mapping.add("type=" + mappingType(column.type()));
            csv.append(',').append(column.name()).append(':').append(mappingType(column.type()));
            declared.put(column.name(), new DatasetFieldMapping(mappingType(column.type()), null));
        }
        csv.append('\n');
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(INDEX)
                .setSettings(Settings.builder().put("index.number_of_shards", 1))
                .setMapping(mapping.toArray(String[]::new))
        );
        for (int i = 0; i < ROWS; i++) {
            Map<String, Object> source = new HashMap<>();
            source.put("id", i);
            csv.append(i);
            for (Column column : COLUMNS) {
                csv.append(',');
                if (column.present().test(i)) {
                    Object value = column.value().apply(i);
                    source.put(column.name(), value);
                    csv.append(value);
                }
            }
            csv.append('\n');
            client().prepareIndex(INDEX).setSource(source).get();
        }
        client().admin().indices().prepareRefresh(INDEX).get();

        Path csvFile = createTempDir().resolve("sweep.csv");
        Files.writeString(csvFile, csv.toString(), StandardCharsets.UTF_8);
        // A blank cell is null, so every sparse column has genuinely missing values on the dataset too.
        registerStrictDataset(DATASET, StoragePath.fileUri(csvFile), declared, Map.of("format", "csv", "null_value", ""));
    }

    // ---- positive control ----

    /** Every column is present on some rows and absent on others, on both sources, or the sweep observes nothing. */
    public void testEveryColumnIsSparseOnBothSources() {
        for (Column column : COLUMNS) {
            List<Object> expected = IntStream.range(0, ROWS).filter(column.present()).<Object>mapToObj(i -> i).toList();
            assertTrue(column.name() + " must be present on some rows but not all", expected.size() > 0 && expected.size() < ROWS);
            assertEquals(column.name() + " on the index", expected, ids(INDEX, QueryBuilders.existsQuery(column.name())));
            assertEquals(column.name() + " on the dataset", expected, ids(DATASET, QueryBuilders.existsQuery(column.name())));
        }
    }

    // ---- the sweep: every construct, on every column, in every bool context ----

    private record Named<T>(String name, T value) {}

    private static final List<Named<UnaryOperator<QueryBuilder>>> CONTEXTS = List.of(
        new Named<>("bare", x -> x),
        new Named<>("must", x -> QueryBuilders.boolQuery().must(x)),
        new Named<>("filter", x -> QueryBuilders.boolQuery().filter(x)),
        new Named<>("must_not", x -> QueryBuilders.boolQuery().mustNot(x)),
        new Named<>("required should", x -> QueryBuilders.boolQuery().should(x)),
        new Named<>("non-required should", x -> QueryBuilders.boolQuery().must(QueryBuilders.existsQuery("id")).should(x)),
        new Named<>(
            "nested must_not",
            x -> QueryBuilders.boolQuery().mustNot(QueryBuilders.boolQuery().must(x).must(QueryBuilders.existsQuery("id")))
        )
    );

    public void testTermShapes() {
        sweep(
            List.of(
                new Named<>("term", c -> QueryBuilders.termQuery(c.name(), c.sample())),
                new Named<>("case-insensitive term", c -> QueryBuilders.termQuery(c.name(), c.sample()).caseInsensitive(true)),
                new Named<>("terms", c -> QueryBuilders.termsQuery(c.name(), c.sample(), c.lower()))
            )
        );
    }

    public void testMatchShapes() {
        sweep(
            List.of(
                new Named<>("match", c -> QueryBuilders.matchQuery(c.name(), c.sample())),
                new Named<>("lenient match", c -> QueryBuilders.matchQuery(c.name(), c.sample()).lenient(true)),
                new Named<>("match_phrase", c -> QueryBuilders.matchPhraseQuery(c.name(), c.sample()))
            )
        );
    }

    public void testExistsShape() {
        sweep(List.of(new Named<>("exists", c -> QueryBuilders.existsQuery(c.name()))));
    }

    public void testRangeShapes() {
        sweep(
            List.of(
                new Named<>("range with a lower bound", c -> QueryBuilders.rangeQuery(c.name()).gte(c.lower())),
                new Named<>("range with an upper bound", c -> QueryBuilders.rangeQuery(c.name()).lte(c.upper())),
                new Named<>("range with both bounds", c -> QueryBuilders.rangeQuery(c.name()).gte(c.lower()).lte(c.upper())),
                new Named<>("exclusive range", c -> QueryBuilders.rangeQuery(c.name()).gt(c.lower()).lt(c.upper())),
                new Named<>("range with no bounds", c -> QueryBuilders.rangeQuery(c.name()))
            )
        );
    }

    /** Constructs that are not per-column, in every bool context. */
    public void testColumnIndependentShapes() {
        List<Named<QueryBuilder>> shapes = List.of(
            new Named<>("match_all", QueryBuilders.matchAllQuery()),
            new Named<>("match_none", new MatchNoneQueryBuilder()),
            new Named<>("multi_match over two fields", QueryBuilders.multiMatchQuery("k1", "k", "n").lenient(true)),
            new Named<>("multi_match over a pattern", QueryBuilders.multiMatchQuery("k1", "k*")),
            new Named<>("fieldless multi_match", QueryBuilders.multiMatchQuery("k1")),
            new Named<>("phrase multi_match", QueryBuilders.multiMatchQuery("k1", "k").type(MultiMatchQueryBuilder.Type.PHRASE)),
            new Named<>(
                "unsupported beside a term",
                QueryBuilders.boolQuery().must(QueryBuilders.termQuery("k", "k1")).must(QueryBuilders.wildcardQuery("k", "k*"))
            )
        );
        List<String> failures = new ArrayList<>();
        for (Named<QueryBuilder> shape : shapes) {
            for (Named<UnaryOperator<QueryBuilder>> context : CONTEXTS) {
                check(shape.name() + " in " + context.name(), context.value().apply(shape.value()), failures);
            }
        }
        assertNoFailures(failures);
    }

    // ---- combinations ----

    /**
     * Random bool trees up to three levels deep, checked the same way. Leaves draw from the per-column constructs
     * except the exclusive range, plus {@code match_all}, {@code match_none} and an untranslatable {@code wildcard}.
     */
    public void testRandomFilters() {
        List<String> failures = new ArrayList<>();
        // Near the low end normally, near the high end on a nightly run. AbstractQueryTestCase draws 20 per test.

        int iterations = scaledRandomIntBetween(150, 400);
        int translatedInFull = 0;
        int droppedSomething = 0;
        int discriminating = 0;
        for (int i = 0; i < iterations; i++) {
            QueryBuilder filter = randomFilter(3);
            if (fullyTranslates(filter)) {
                translatedInFull++;
            } else {
                droppedSomething++;
            }
            if (check("random filter " + i, filter, failures)) {
                discriminating++;
            }
        }
        assertNoFailures(failures);
        // A draw that is all-translatable, all-dropped, or all-or-nothing on rows exercises only half the oracle.
        String mix = iterations
            + " filters: "
            + translatedInFull
            + " translated in full, "
            + droppedSomething
            + " with a dropped clause, "
            + discriminating
            + " selecting part of the data";
        int floor = iterations / 10;
        assertThat("too few filters translated in full — " + mix, translatedInFull, greaterThanOrEqualTo(floor));
        assertThat("too few filters had a clause dropped — " + mix, droppedSomething, greaterThanOrEqualTo(floor));
        assertThat("too few filters selected part of the data — " + mix, discriminating, greaterThanOrEqualTo(floor));
    }

    private QueryBuilder randomFilter(int depth) {
        if (depth == 0 || randomIntBetween(0, 2) == 0) {
            return randomLeaf();
        }
        BoolQueryBuilder bool = QueryBuilders.boolQuery();
        int arms = 0;
        for (int i = randomIntBetween(0, 2); i > 0; i--, arms++) {
            bool.must(randomFilter(depth - 1));
        }
        for (int i = randomIntBetween(0, 1); i > 0; i--, arms++) {
            bool.filter(randomFilter(depth - 1));
        }
        for (int i = randomIntBetween(0, 2); i > 0; i--, arms++) {
            bool.mustNot(randomFilter(depth - 1));
        }
        for (int i = randomIntBetween(0, 2); i > 0; i--, arms++) {
            bool.should(randomFilter(depth - 1));
        }
        if (arms == 0) {
            bool.must(randomLeaf());
        }
        if (bool.should().isEmpty() == false && randomIntBetween(0, 3) == 0) {
            bool.minimumShouldMatch(randomFrom("0", "1"));
        }
        return bool;
    }

    private QueryBuilder randomLeaf() {
        Column c = randomFrom(sweptColumns());
        return switch (randomIntBetween(0, 10)) {
            case 0 -> QueryBuilders.termQuery(c.name(), c.sample());
            case 1 -> QueryBuilders.termsQuery(c.name(), c.sample(), c.lower());
            case 2 -> QueryBuilders.matchQuery(c.name(), c.sample()).lenient(randomBoolean());
            case 3 -> QueryBuilders.matchPhraseQuery(c.name(), c.sample());
            case 4 -> QueryBuilders.existsQuery(c.name());
            case 5 -> QueryBuilders.rangeQuery(c.name()).gte(c.lower()).includeLower(randomBoolean());
            case 6 -> QueryBuilders.rangeQuery(c.name()).lte(c.upper()).includeUpper(randomBoolean());
            case 7 -> QueryBuilders.rangeQuery(c.name()).gte(c.lower()).lte(c.upper());
            case 8 -> QueryBuilders.rangeQuery(c.name());
            case 9 -> QueryBuilders.termQuery(c.name(), c.sample()).caseInsensitive(true);
            case 10 -> randomFrom(QueryBuilders.matchAllQuery(), new MatchNoneQueryBuilder(), QueryBuilders.wildcardQuery(c.name(), "k*"));
            default -> throw new AssertionError("unreachable: randomIntBetween(0, 10) returned out of range");
        };
    }

    // ---- the oracle ----

    private void sweep(List<Named<Function<Column, QueryBuilder>>> shapes) {
        List<String> failures = new ArrayList<>();
        for (Named<Function<Column, QueryBuilder>> shape : shapes) {
            int discriminating = 0;
            for (Column column : sweptColumns()) {
                for (Named<UnaryOperator<QueryBuilder>> context : CONTEXTS) {
                    String cell = shape.name() + " on " + column.name() + " (" + column.type().typeName() + ") in " + context.name();
                    if (check(cell, context.value().apply(shape.value().apply(column)), failures)) {
                        discriminating++;
                    }
                }
            }
            if (discriminating == 0) {
                failures.add(shape.name() + " never selected part of the data in any cell, so nothing in its sweep was observed");
            }
        }
        assertNoFailures(failures);
    }

    /**
     * Checks one filter and records any failure. Returns whether the index selected part of the data, which is what
     * makes a cell able to show a difference at all.
     */
    private boolean check(String cell, QueryBuilder filter, List<String> failures) {
        String described = cell + ": " + Strings.toString(filter);
        List<Object> onIndex;
        try {
            onIndex = ids(INDEX, filter);
        } catch (Exception indexRejects) {
            // The index refuses some shapes outright (a case-insensitive term on a number). The dataset must still answer.
            try {
                ids(DATASET, filter);
            } catch (Exception e) {
                failures.add(described + " — the index rejected it and the dataset failed the query too: " + e);
            }
            return false;
        }
        List<Object> onDataset;
        try {
            onDataset = ids(DATASET, filter);
        } catch (Exception e) {
            failures.add(described + " — the dataset failed the query: " + e);
            return false;
        }
        if (onDataset.containsAll(onIndex) == false) {
            failures.add(described + " — the dataset returned fewer rows than the index. index " + onIndex + ", dataset " + onDataset);
        } else if (fullyTranslates(filter) && onDataset.equals(onIndex) == false) {
            failures.add(described + " — translated in full but disagrees with the index. index " + onIndex + ", dataset " + onDataset);
        }
        return onIndex.isEmpty() == false && onIndex.size() < ROWS;
    }

    private static void assertNoFailures(List<String> failures) {
        if (failures.isEmpty() == false) {
            fail(failures.size() + " filter(s) diverged:\n" + String.join("\n", failures));
        }
    }

    /**
     * Whether the translator expresses the whole filter. Rebuilt here rather than read off the query; the production
     * signal is the drop warning, and {@code ExternalDatasetRequestFilterConformanceIT} pins this answer against it.
     */
    private static boolean fullyTranslates(QueryBuilder filter) {
        Map<String, DataType> types = new HashMap<>();
        types.put("id", DataType.INTEGER);
        for (Column column : COLUMNS) {
            types.put(column.name(), column.type());
        }
        Function<String, Expression> binder = name -> {
            DataType type = types.get(name);
            return type == null ? Literal.NULL : new ReferenceAttribute(Source.EMPTY, name, type);
        };
        return new QueryDslTranslator(binder, types.keySet(), TEST_CFG, TransportVersion.current()).translate(filter)
            .unsupported()
            .isEmpty();
    }

    private List<Object> ids(String source, QueryBuilder filter) {
        EsqlQueryRequest request = syncEsqlQueryRequest("FROM " + source + " | KEEP id | SORT id").filter(filter);
        try (EsqlQueryResponse response = run(request)) {
            return getValuesList(response).stream().map(row -> row.get(0)).toList();
        }
    }
}
