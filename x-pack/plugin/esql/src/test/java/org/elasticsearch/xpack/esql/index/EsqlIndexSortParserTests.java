/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.index;

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.index.IndexSortConfig.SortEntry;
import org.elasticsearch.index.IndexSortParser;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.FieldExpression;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.junit.BeforeClass;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Parses {@code index.sort} expressions as an ES|QL {@code SORT}, and resolves their computed keys against a mapping.
 */
public class EsqlIndexSortParserTests extends ESTestCase {

    private static final EsqlIndexSortParser PARSER = new EsqlIndexSortParser();
    private static final IndexSettings INDEX_SETTINGS = IndexSettingsModule.newIndexSettings("test", Settings.EMPTY);
    private static final Map<String, MappedFieldType> FIELDS = Map.of(
        "@timestamp",
        new DateFieldMapper.DateFieldType("@timestamp"),
        "count",
        new NumberFieldMapper.NumberFieldType("count", NumberFieldMapper.NumberType.LONG),
        "ratio",
        new NumberFieldMapper.NumberFieldType("ratio", NumberFieldMapper.NumberType.FLOAT),
        "host.name",
        new KeywordFieldMapper.KeywordFieldType("host.name")
    );

    @BeforeClass
    public static void initParser() {
        IndexSortParser.init(PARSER);
    }

    public void testFieldKeys() {
        List<SortEntry> entries = PARSER.parse("agent.name,  mv_min(@timestamp) desc nulls last, MV_MAX(a) ASC, b DESC");
        assertField(entries.get(0), "agent.name", SortOrder.ASC, MultiValueMode.MIN, "_last");
        assertField(entries.get(1), "@timestamp", SortOrder.DESC, MultiValueMode.MIN, "_last");
        assertField(entries.get(2), "a", SortOrder.ASC, MultiValueMode.MAX, "_last");
        assertField(entries.get(3), "b", SortOrder.DESC, MultiValueMode.MAX, "_first");
    }

    /**
     * Like in ES|QL, a descending key without {@code NULLS} puts missing values first, unlike {@code index.sort.missing}, which
     * defaults to {@code _last}.
     */
    public void testNullsDefaultToTheOrder() {
        List<SortEntry> entries = PARSER.parse("a, b ASC, c DESC, d DESC NULLS LAST, e ASC NULLS FIRST");
        assertThat(entries.stream().map(SortEntry::missing).toList(), equalTo(List.of("_last", "_last", "_first", "_last", "_first")));
    }

    public void testWhitespaceAndCase() {
        List<SortEntry> entries = PARSER.parse("a\tDESC,\n b  NULLS   FIRST ,  Mv_Max( c )   aSc ");
        assertField(entries.get(0), "a", SortOrder.DESC, MultiValueMode.MAX, "_first");
        assertField(entries.get(1), "b", SortOrder.ASC, MultiValueMode.MIN, "_first");
        assertField(entries.get(2), "c", SortOrder.ASC, MultiValueMode.MAX, "_last");
    }

    public void testQuotedFields() {
        List<SortEntry> entries = PARSER.parse("`my field` DESC, MV_MAX(`a,b``c)`), `pod-name`, `null`, `MV_MAX(a)`, MV_MAX(a)");
        assertField(entries.get(0), "my field", SortOrder.DESC, MultiValueMode.MAX, "_first");
        assertField(entries.get(1), "a,b`c)", SortOrder.ASC, MultiValueMode.MAX, "_last");
        assertField(entries.get(2), "pod-name", SortOrder.ASC, MultiValueMode.MIN, "_last");
        assertField(entries.get(3), "null", SortOrder.ASC, MultiValueMode.MIN, "_last");
        // A quoted field named like a function call isn't that call, so it isn't a duplicate either
        assertField(entries.get(4), "MV_MAX(a)", SortOrder.ASC, MultiValueMode.MIN, "_last");
        assertField(entries.get(5), "a", SortOrder.ASC, MultiValueMode.MAX, "_last");
    }

    public void testComputedKeys() {
        List<SortEntry> entries = PARSER.parse(
            "DATE_TRUNC(1 hour, @timestamp) DESC NULLS LAST, host.name, CONCAT(a, \"x,) DESC\", `b)`) == \"c\", count + 1"
        );
        assertComputedKey(entries.get(0), "DATE_TRUNC(1 hour, @timestamp)", SortOrder.DESC, "_last", Set.of("@timestamp"));
        assertField(entries.get(1), "host.name", SortOrder.ASC, MultiValueMode.MIN, "_last");
        assertComputedKey(entries.get(2), "CONCAT(a, \"x,) DESC\", `b)`) == \"c\"", SortOrder.ASC, "_last", Set.of("a", "b)"));
        assertComputedKey(entries.get(3), "count + 1", SortOrder.ASC, "_last", Set.of("count"));
        for (String key : List.of("-a", "NOT flag", "MV_MAX(a) + 1", "MV_MAX(a, b)", "MV_COUNT(a)")) {
            assertThat(key, PARSER.parse(key).getFirst().computedKey(), equalTo(key));
        }
        // ES|QL reads a field in parentheses as the field
        assertField(PARSER.parse("(a)").getFirst(), "a", SortOrder.ASC, MultiValueMode.MIN, "_last");
    }

    public void testInvalidExpressions() {
        assertParseFails("field1,", "setting [index.sort] can't be parsed");
        assertParseFails("field1, , field2", "setting [index.sort] can't be parsed");
        assertParseFails("asc", "setting [index.sort] can't be parsed");
        assertParseFails("field1 ASC DESC", "setting [index.sort] can't be parsed");
        assertParseFails("field1 NULLS SOMETIMES", "setting [index.sort] can't be parsed");
        assertParseFails("field1 SIDEWAYS", "setting [index.sort] can't be parsed");
        assertParseFails("foo.desc", "setting [index.sort] can't be parsed");
        assertParseFails("DATE_TRUNC(1 hour, @timestamp", "setting [index.sort] can't be parsed");
        assertParseFails("`field1", "setting [index.sort] can't be parsed");
        assertParseFails("a | LIMIT 1", "setting [index.sort] is not a sort in [a | LIMIT 1]");
        assertParseFails("field1, field1 DESC", "setting [index.sort] contains sort key [field1] more than once");
        assertParseFails("MV_MAX(a), mv_max(a) DESC", "setting [index.sort] contains sort key [mv_max(a)] more than once");
        assertParseFails("F(a), F(a) DESC", "uses unknown function [F]");
        assertParseFails("ABS(a), ABS(a) DESC", "setting [index.sort] contains sort key [ABS(a)] more than once");
    }

    public void testComputedKeysAreChecked() {
        assertParseFails(
            "NOW()",
            "index sort key [NOW()] uses [NOW()], which isn't guaranteed to compute the same values on every node and version"
        );
        assertParseFails(
            "DATE_DIFF(\"hour\", @timestamp, NOW())",
            "uses [NOW()], which isn't guaranteed to compute the same values on every node and version"
        );
        // Floating point math, Unicode data and time zone rules may change from one JVM to another
        assertParseFails("TO_DATETIME(host.name)", "uses [TO_DATETIME(host.name)], which isn't guaranteed");
        assertParseFails("TO_DATE_NANOS(host.name)", "uses [TO_DATE_NANOS(host.name)], which isn't guaranteed");
        assertParseFails("TO_LONG(host.name)", "uses [TO_LONG(host.name)], which isn't guaranteed");
        assertParseFails("SIN(ratio)", "uses [SIN(ratio)], which isn't guaranteed to compute the same values on every node and version");
        assertParseFails("LENGTH(TO_LOWER(host.name))", "uses [TO_LOWER(host.name)], which isn't guaranteed");
        assertParseFails("MAX(count)", "uses [MAX(count)], which can't be computed for each document");
        assertParseFails("MATCH(host.name, \"a\")", "which can't be computed for each document");
        assertParseFails("NOPE(count)", "uses unknown function [NOPE]");
        assertParseFails("1 + 2", "must read at least one field");
    }

    public void testIndexSortConfig() {
        Settings settings = Settings.builder().put("index.sort", "DATE_TRUNC(1 hour, @timestamp) DESC, host.name, MV_MAX(count)").build();
        assertThat(IndexSortConfig.sortFields(settings), equalTo(List.of("host.name", "count")));
        IndexSortConfig config = IndexSettingsModule.newIndexSettings("test", settings).getIndexSortConfig();
        assertThat(config.computedKeyFields(), equalTo(Set.of("@timestamp")));

        Sort sort = buildIndexSort(config);
        assertThat(sort.getSort()[0], instanceOf(EsqlSortField.class));
        assertThat(sort.getSort()[0].getField(), equalTo("DATE_TRUNC(1 hour, @timestamp)"));
        assertTrue(sort.getSort()[0].getReverse());
        assertThat(sort.getSort()[1].getField(), equalTo("host.name"));
        assertThat(sort.getSort()[2].getField(), equalTo("count"));
    }

    /** Slice-enabled indices sort on the slice key first, before the keys of the expression. */
    public void testSliceKeyComesFirst() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Settings settings = Settings.builder()
            .put(IndexSettings.SLICE_ENABLED.getKey(), true)
            .put("index.sort", "DATE_TRUNC(1 hour, @timestamp) DESC, count")
            .build();
        Sort sort = buildIndexSort(IndexSettingsModule.newIndexSettings("test", settings).getIndexSortConfig());
        assertThat(sort.getSort().length, equalTo(3));
        assertThat(sort.getSort()[0].getField(), equalTo(SliceIndexing.SLICE_KEY_FIELD_NAME));
        assertThat(sort.getSort()[1], instanceOf(EsqlSortField.class));
        assertThat(sort.getSort()[1].getField(), equalTo("DATE_TRUNC(1 hour, @timestamp)"));
        assertTrue(sort.getSort()[1].getReverse());
        assertThat(sort.getSort()[2].getField(), equalTo("count"));
        assertFalse(sort.getSort()[2].getReverse());
    }

    public void testSliceEnabledRejectsReservedFields() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        String field = randomFrom(
            RoutingFieldMapper.NAME,
            SliceIndexing.FIELD_NAME,
            SliceIndexing.SLICE_KEY_FIELD_NAME,
            SliceIndexing.SLICE_HASH_FIELD_NAME
        );
        Settings settings = Settings.builder()
            .put(IndexSettings.SLICE_ENABLED.getKey(), true)
            .put("index.sort", "DATE_TRUNC(1 hour, @timestamp), " + field + " DESC")
            .build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> IndexSettingsModule.newIndexSettings("test", settings)
        );
        assertThat(
            e.getMessage(),
            containsString("setting [index.sort] must not contain [_routing], [_slice], [_slice_key] or [_slice_hash]")
        );
    }

    public void testSameLuceneSortAsTheSortSettings() {
        Sort fromExpression = buildIndexSort(
            IndexSettingsModule.newIndexSettings("test", Settings.builder().put("index.sort", "count DESC, MV_MAX(@timestamp)").build())
                .getIndexSortConfig()
        );
        Settings sortSettings = Settings.builder()
            .putList("index.sort.field", "count", "@timestamp")
            .putList("index.sort.order", "desc", "asc")
            .putList("index.sort.mode", "max", "max")
            .putList("index.sort.missing", "_first", "_last")
            .build();
        assertThat(
            fromExpression,
            equalTo(buildIndexSort(IndexSettingsModule.newIndexSettings("test", sortSettings).getIndexSortConfig()))
        );
    }

    public void testComputedKeysNeedIndexVersion() {
        Settings settings = Settings.builder()
            .put("index.sort", "DATE_TRUNC(1 hour, @timestamp) DESC")
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersionUtils.getPreviousVersion(IndexVersions.INDEX_SORT_COMPUTED_KEYS))
            .build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> IndexSettingsModule.newIndexSettings("test", settings).getIndexSortConfig()
        );
        assertThat(e.getMessage(), containsString("can't have computed keys on an index created before all nodes support them"));
    }

    public void testResolve() {
        boolean reverse = randomBoolean();
        boolean nullsFirst = randomBoolean();
        EsqlSortField sortField = (EsqlSortField) resolve("DATE_TRUNC(1 hour, @timestamp)", reverse, nullsFirst, FIELDS);
        assertThat(sortField.key().type(), equalTo(DataType.DATETIME));
        assertThat(sortField.getReverse(), equalTo(reverse));
        assertThat(sortField.nullsFirst(), equalTo(nullsFirst));
        assertThat(sortField.getMissingValue(), equalTo(nullsFirst == reverse ? Long.MAX_VALUE : Long.MIN_VALUE));
    }

    public void testKeysAreAnalyzedLikeEsql() {
        // The analyzer casts the string to a date
        assertThat(type("@timestamp >= \"2026-01-01T00:00:00Z\""), equalTo(DataType.BOOLEAN));
        // and a field to the type of another argument, which is checked like the functions of the expression
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> FieldExpression.compile(expression("COALESCE(ul, l) IS NULL"), Map.of("ul", DataType.UNSIGNED_LONG, "l", DataType.LONG))
        );
        assertThat(e.getMessage(), containsString("uses [l], which isn't guaranteed"));
        // Parentheses aren't part of the key
        SortEntry entry = PARSER.parse("(count + 1) DESC").getFirst();
        assertComputedKey(entry, "count + 1", SortOrder.DESC, "_first", Set.of("count"));
        assertThat(type(entry.computedKey()), equalTo(DataType.LONG));
    }

    public void testComputedKeyIsNotTheFieldOfTheSameName() {
        IndexSortConfig config = IndexSettingsModule.newIndexSettings("test", Settings.builder().put("index.sort", "ABS(count)").build())
            .getIndexSortConfig();
        assertFalse(config.hasSortOnField("ABS(count)"));
        assertFalse(config.hasPrimarySortOnField("ABS(count)"));
        assertFalse(config.hasSortOnField("count"));
    }

    public void testKeyTypes() {
        assertThat(type("count * 2"), equalTo(DataType.LONG));
        assertThat(type("LENGTH(host.name)"), equalTo(DataType.INTEGER));
        assertThat(type("ratio * 2"), equalTo(DataType.DOUBLE));
        assertThat(type("count > 10"), equalTo(DataType.BOOLEAN));
        assertThat(type("COALESCE(count, count + LENGTH(host.name))"), equalTo(DataType.LONG));
    }

    public void testRejectsFieldsItCannotRead() {
        assertResolveFails("COALESCE(host.name, \"a\")", FIELDS, "returns type [keyword], which isn't supported yet");
        assertResolveFails("DATE_TRUNC(1 hour, missing)", FIELDS, "references unknown field [missing]");
        assertResolveFails("DATE_TRUNC(1 hour, count)", FIELDS, "second argument of [DATE_TRUNC(1 hour, count)]");
        MappedFieldType noDocValues = new DateFieldMapper.DateFieldType(
            "@timestamp",
            IndexType.points(true, false),
            false,
            DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER,
            DateFieldMapper.Resolution.MILLISECONDS,
            null,
            null,
            Map.of()
        );
        assertResolveFails("DATE_TRUNC(1 hour, @timestamp)", Map.of("@timestamp", noDocValues), "needs doc values on field [@timestamp]");
        Map<String, MappedFieldType> location = Map.of("location", new GeoPointFieldMapper.GeoPointFieldType("location"));
        assertResolveFails("location IS NULL", location, "references field [location] of unsupported type [geo_point]");
    }

    private static void assertField(SortEntry entry, String field, SortOrder order, MultiValueMode mode, String missing) {
        assertThat(entry.field(), equalTo(field));
        assertNull(entry.computedKey());
        assertThat(entry.order(), equalTo(order));
        assertThat(entry.mode(), equalTo(mode));
        assertThat(entry.missing(), equalTo(missing));
    }

    private static void assertComputedKey(SortEntry entry, String key, SortOrder order, String missing, Set<String> fields) {
        assertNull(entry.field());
        assertThat(entry.computedKey(), equalTo(key));
        assertThat(entry.order(), equalTo(order));
        assertThat(entry.missing(), equalTo(missing));
        assertThat(entry.computedKeyFields(), equalTo(fields));
    }

    private static void assertParseFails(String expression, String message) {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PARSER.parse(expression));
        assertThat(e.getMessage(), containsString(message));
    }

    private static Sort buildIndexSort(IndexSortConfig config) {
        IndicesFieldDataCache cache = new IndicesFieldDataCache(INDEX_SETTINGS.getSettings(), null);
        IndexFieldDataService fieldData = new IndexFieldDataService(INDEX_SETTINGS, cache, new NoneCircuitBreakerService());
        return config.buildIndexSort(
            FIELDS::get,
            (ft, lookup) -> fieldData.getForField(
                ft,
                new FieldDataContext("test", INDEX_SETTINGS, lookup, Set::of, () -> false, MappedFieldType.FielddataOperation.SEARCH)
            )
        );
    }

    private static Expression expression(String key) {
        return ((Eval) FieldExpression.parser().parseQuery("ROW x = 1 | EVAL k = " + key)).fields().getFirst().child();
    }

    private static DataType type(String key) {
        return ((EsqlSortField) resolve(key, false, false, FIELDS)).key().type();
    }

    private static SortField resolve(String key, boolean reverse, boolean nullsFirst, Map<String, MappedFieldType> fields) {
        return PARSER.resolve(key, reverse, nullsFirst, INDEX_SETTINGS, fields::get);
    }

    private static void assertResolveFails(String key, Map<String, MappedFieldType> fields, String message) {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> resolve(key, true, true, fields));
        assertThat(e.getMessage(), containsString(message));
    }
}
