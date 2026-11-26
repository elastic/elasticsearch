/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;

public class ColumnarNullContractTests extends AbstractColumnarNullContractTestCase {

    // The synthetic _source keeps the null elements of an array: [null] is reconstructed as [null].
    private static final String NULL_ELEMENTS_KEPT = "https://github.com/elastic/elasticsearch/issues/165909";
    // The exists query matches a document whose only values are nulls.
    private static final String NULL_ONLY_MATCHES_EXISTS = "https://github.com/elastic/elasticsearch/issues/165909";
    // The count of the exists query is lower than its hits when a document holds only nulls.
    private static final String EXISTS_COUNT_DISAGREES = "https://github.com/elastic/elasticsearch/issues/165521";

    // The reconstructed _source reorders the values of a geo_point field and drops its repeated values.

    private static final Predicate<Configuration> INDEXED = Configuration::indexed;
    private static final Predicate<Configuration> NOT_INDEXED = INDEXED.negate();
    private static final Predicate<Configuration> INDEXED_WITHOUT_VECTORDB = configuration -> configuration.indexed()
        && configuration.mode() != IndexMode.VECTORDB_COLUMNAR;
    private static final Predicate<Configuration> VECTORDB_INDEXED = configuration -> configuration.indexed()
        && configuration.mode() == IndexMode.VECTORDB_COLUMNAR;

    private static final List<String> NUMBERS = List.of("long", "integer", "short", "byte", "double", "float", "half_float");
    // NOTE: a binary field is never resolved for batch indexing, so it is tested with document indexing only.
    private static final List<String> BATCHABLE_WITHOUT_NULL_ELEMENTS_BUG = List.of(
        "long",
        "integer",
        "short",
        "byte",
        "double",
        "float",
        "half_float",
        "date",
        "date_nanos",
        "boolean",
        "ip"
    );
    private static final Map<String, Object> NULL_VALUES = Map.ofEntries(
        Map.entry("keyword", "NA"),
        Map.entry("long", 7),
        Map.entry("integer", 7),
        Map.entry("short", 7),
        Map.entry("byte", 7),
        Map.entry("double", 2.5),
        Map.entry("float", 1.5),
        Map.entry("half_float", 0.5),
        Map.entry("date", "2020-01-01T00:00:00.000Z"),
        Map.entry("date_nanos", "2020-01-01T00:00:00.000Z"),
        Map.entry("boolean", true),
        Map.entry("ip", "10.0.0.1")
    );

    private static Supplier<Object> values(String type) {
        return switch (type) {
            case "keyword", "text" -> () -> randomAlphaOfLengthBetween(1, 8);
            case "long" -> () -> randomLong();
            case "integer" -> () -> randomInt();
            case "short" -> () -> randomShort();
            case "byte" -> () -> randomByte();
            case "double" -> () -> randomDouble();
            case "float" -> () -> randomFloat();
            case "half_float" -> () -> randomIntBetween(0, 1000) / 4.0f;
            case "date", "date_nanos" -> () -> DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(
                randomLongBetween(0, 4_102_444_800_000L)
            );
            case "boolean" -> () -> randomBoolean();
            case "ip" -> () -> NetworkAddress.format(randomIp(randomBoolean()));
            case "binary" -> () -> Base64.getEncoder().encodeToString(randomByteArrayOfLength(between(1, 8)));
            default -> throw new AssertionError("no values for [" + type + "]");
        };
    }

    // NOTE: the column shapes below are the ones the mappers batch map today (doSupportsColumnarParse and doMapColumnBatch). A number
    // takes a column without a null, a date and a boolean take a column of scalars, a keyword, a text and an ip without an index take
    // every shape, and a binary field takes none. A shape that is not listed may fall back to document indexing.
    private static Scenario plain(String type) {
        final Scenario scenario = scenario(type, values(type));
        return switch (type) {
            case "keyword", "text" -> scenario.batchedGroups((configuration, shape) -> true);
            case "ip" -> scenario.batchedGroups((configuration, shape) -> configuration.indexed() == false);
            case "long", "integer", "short", "byte", "double", "float", "half_float" -> scenario.batchedGroups(
                (configuration, shape) -> shape == SCALARS || shape == ARRAYS
            );
            case "date", "date_nanos" -> scenario.withCanonical(value -> Instant.parse(String.valueOf(value)).toString())
                .batchedGroups((configuration, shape) -> shape == SCALARS);
            case "boolean" -> scenario.batchedGroups((configuration, shape) -> shape == SCALARS);
            case "binary" -> scenario.withoutIndex().withParameters(Map.of("doc_values", true));
            default -> throw new AssertionError("no scenario for [" + type + "]");
        };
    }

    private static Scenario withNullValue(String type) {
        return plain(type).withNullValue(NULL_VALUES.get(type));
    }

    // NOTE: valid and malformed values alternate so that every column mixes them. Batch indexing refuses such a column and falls back to
    // document indexing, so this scenario is only tested with document indexing. A column of only valid numbers would execute as a batch,
    // and which one a test reaches must not depend on the random values.
    private static Scenario longIgnoreMalformed() {
        final int[] calls = new int[1];
        return plain("long").withParameters(Map.of("ignore_malformed", true))
            .withValues(() -> calls[0]++ % 2 == 0 ? randomLong() : "malformed-" + randomAlphaOfLength(3))
            .batchedGroups((configuration, shape) -> false);
    }

    private static Scenario keywordIgnoreAbove() {
        return plain("keyword").withParameters(Map.of("ignore_above", 5)).withValues(() -> randomAlphaOfLengthBetween(1, 10));
    }

    private MapperService vectorMapperService(IndexMode mode) throws IOException {
        return createMapperService(
            IndexVersion.current(),
            Settings.builder().put(IndexSettings.MODE.getKey(), mode.getName()).build(),
            mapping(b -> {
                b.startObject("@timestamp").field("type", "date").endObject();
                b.startObject("dv").field("type", "dense_vector").field("dims", 3).endObject();
                b.startObject("sv").field("type", "sparse_vector").endObject();
            })
        );
    }

    private void assertRejected(MapperService mapperService, String message, CheckedConsumer<XContentBuilder, IOException> document) {
        final Exception e = expectThrows(Exception.class, () -> mapperService.documentMapper().parse(source(b -> {
            b.field("@timestamp", "2026-01-01T00:00:30.000Z");
            document.accept(b);
        })));
        assertThat(ExceptionsHelper.stackTrace(e), containsString(message));
    }

    public void testDenseVectorRejectsANullCoordinateAndAnEmptyArray() throws IOException {
        for (final IndexMode mode : List.of(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR, IndexMode.VECTORDB_COLUMNAR)) {
            final MapperService mapperService = vectorMapperService(mode);
            final String nullCoordinate = "expecting token of type [VALUE_NUMBER] but found [VALUE_NULL]";
            assertRejected(mapperService, nullCoordinate, b -> b.startArray("dv").nullValue().endArray());
            assertRejected(mapperService, nullCoordinate, b -> b.startArray("dv").value(1.0).nullValue().value(3.0).endArray());
            assertRejected(mapperService, "has a different number of dimensions [0]", b -> b.startArray("dv").endArray());
        }
    }

    public void testSparseVectorRejectsANull() throws IOException {
        for (final IndexMode mode : List.of(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR, IndexMode.VECTORDB_COLUMNAR)) {
            final MapperService mapperService = vectorMapperService(mode);
            final String message = "[sparse_vector] fields must be json objects, expected a START_OBJECT but got: VALUE_NULL";
            assertRejected(mapperService, message, b -> b.nullField("sv"));
            assertRejected(mapperService, message, b -> b.startArray("sv").nullValue().endArray());
            assertRejected(
                mapperService,
                message,
                b -> b.startArray("sv").startObject().field("a", 1.0).endObject().nullValue().endArray()
            );
        }
    }

    private static String canonicalGeoPoint(Object value) {
        final double lat;
        final double lon;
        if (value instanceof Map<?, ?> point) {
            lat = ((Number) point.get("lat")).doubleValue();
            lon = ((Number) point.get("lon")).doubleValue();
        } else {
            final String[] coordinates = String.valueOf(value).split(",");
            lat = Double.parseDouble(coordinates[0]);
            lon = Double.parseDouble(coordinates[1]);
        }
        return String.format(Locale.ROOT, "%.4f,%.4f", lat, lon);
    }

    // NOTE: the coordinates are halves, which a geo_point encodes and decodes exactly. A point given as a string, and a column of arrays of
    // points, are not batch mapped.
    private static Scenario geoPoint() {
        return scenario("geo_point", () -> Map.of("lat", randomIntBetween(-160, 160) / 2.0, "lon", randomIntBetween(-340, 340) / 2.0))
            .withoutIndex()
            .withCanonical(ColumnarNullContractTests::canonicalGeoPoint)
            .batchedGroups((configuration, shape) -> shape == SCALARS);
    }

    public void testGeoPointExistsMatchesOnlyDocumentsWithValueDocument() throws IOException {
        assertExistsMatchesOnlyDocumentsWithValue(geoPoint(), IndexingStrategy.DOCUMENT);
    }

    public void testGeoPointExistsMatchesOnlyDocumentsWithValueBatch() throws IOException {
        assertExistsMatchesOnlyDocumentsWithValue(geoPoint(), IndexingStrategy.BATCH);
    }

    public void testGeoPointExistsCountAgreesWithHitsDocument() throws IOException {
        assertExistsCountAgreesWithHits(geoPoint(), IndexingStrategy.DOCUMENT);
    }

    public void testGeoPointExistsCountAgreesWithHitsBatch() throws IOException {
        assertExistsCountAgreesWithHits(geoPoint(), IndexingStrategy.BATCH);
    }

    public void testGeoPointSourceDropsNullElementsDocument() throws IOException {
        assertSourceDropsNullElements(geoPoint(), IndexingStrategy.DOCUMENT);
    }

    public void testGeoPointSourceDropsNullElementsBatch() throws IOException {
        assertSourceDropsNullElements(geoPoint(), IndexingStrategy.BATCH);
    }

    public void testGeoPointNullValueReplacesNullDocument() throws IOException {
        final Scenario scenario = geoPoint().withNullValue("10.5,20.5");
        assertExistsMatchesOnlyDocumentsWithValue(scenario, IndexingStrategy.DOCUMENT);
        assertSourceDropsNullElements(scenario, IndexingStrategy.DOCUMENT);
    }

    private void assertNullValueSubstitution(IndexingStrategy strategy) throws IOException {
        for (final String type : NULL_VALUES.keySet()) {
            if (strategy == IndexingStrategy.BATCH && type.equals("binary")) {
                continue;
            }
            final Scenario scenario = withNullValue(type);
            assertExistsMatchesOnlyDocumentsWithValue(scenario, strategy);
            assertSourceDropsNullElements(scenario, strategy);
            assertSourceKeepsTheOrderOfTheRemainingValues(scenario, strategy);
        }
    }

    public void testExistsMatchesOnlyDocumentsWithValueDocument() throws IOException {
        for (final String type : List.of(
            "long",
            "integer",
            "short",
            "byte",
            "double",
            "float",
            "half_float",
            "date",
            "date_nanos",
            "boolean",
            "ip",
            "binary"
        )) {
            assertExistsMatchesOnlyDocumentsWithValue(plain(type), IndexingStrategy.DOCUMENT);
        }
    }

    public void testExistsMatchesOnlyDocumentsWithValueBatch() throws IOException {
        for (final String type : BATCHABLE_WITHOUT_NULL_ELEMENTS_BUG) {
            assertExistsMatchesOnlyDocumentsWithValue(plain(type), IndexingStrategy.BATCH);
        }
    }

    public void testExistsCountAgreesWithHitsDocument() throws IOException {
        for (final String type : List.of(
            "long",
            "integer",
            "short",
            "byte",
            "double",
            "float",
            "half_float",
            "date",
            "date_nanos",
            "boolean",
            "ip",
            "binary"
        )) {
            assertExistsCountAgreesWithHits(plain(type), IndexingStrategy.DOCUMENT);
        }
    }

    public void testExistsCountAgreesWithHitsBatch() throws IOException {
        for (final String type : BATCHABLE_WITHOUT_NULL_ELEMENTS_BUG) {
            assertExistsCountAgreesWithHits(plain(type), IndexingStrategy.BATCH);
        }
    }

    public void testNullValueReplacesNullDocument() throws IOException {
        assertNullValueSubstitution(IndexingStrategy.DOCUMENT);
    }

    public void testNullValueReplacesNullBatch() throws IOException {
        assertNullValueSubstitution(IndexingStrategy.BATCH);
    }

    public void testBinarySourceDropsNullElementsDocument() throws IOException {
        assertSourceDropsNullElements(plain("binary"), IndexingStrategy.DOCUMENT);
    }

    public void testRemainingValuesKeepTheirOrderDocument() throws IOException {
        for (final String type : List.of(
            "keyword",
            "text",
            "long",
            "integer",
            "short",
            "byte",
            "double",
            "float",
            "half_float",
            "date",
            "date_nanos",
            "boolean",
            "ip",
            "binary"
        )) {
            assertSourceKeepsTheOrderOfTheRemainingValues(plain(type), IndexingStrategy.DOCUMENT);
        }
    }

    public void testRemainingValuesKeepTheirOrderBatch() throws IOException {
        for (final String type : List.of(
            "keyword",
            "text",
            "long",
            "integer",
            "short",
            "byte",
            "double",
            "float",
            "half_float",
            "date",
            "date_nanos",
            "boolean",
            "ip"
        )) {
            assertSourceKeepsTheOrderOfTheRemainingValues(plain(type), IndexingStrategy.BATCH);
        }
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ELEMENTS_KEPT)
    public void testKeywordIgnoreAboveSourceDocument() throws IOException {
        assertSourceDropsNullElements(keywordIgnoreAbove(), IndexingStrategy.DOCUMENT);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ELEMENTS_KEPT)
    public void testKeywordIgnoreAboveSourceBatch() throws IOException {
        assertSourceDropsNullElements(keywordIgnoreAbove(), IndexingStrategy.BATCH);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ELEMENTS_KEPT)
    public void testLongIgnoreMalformedSourceDocument() throws IOException {
        assertSourceDropsNullElements(longIgnoreMalformed(), IndexingStrategy.DOCUMENT);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ONLY_MATCHES_EXISTS)
    public void testKeywordExistsDocument() throws IOException {
        assertExistsMatchesOnlyDocumentsWithValue(plain("keyword"), IndexingStrategy.DOCUMENT);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ONLY_MATCHES_EXISTS)
    public void testKeywordExistsBatch() throws IOException {
        assertExistsMatchesOnlyDocumentsWithValue(plain("keyword"), IndexingStrategy.BATCH);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ONLY_MATCHES_EXISTS)
    public void testTextExistsDocument() throws IOException {
        assertExistsMatchesOnlyDocumentsWithValue(plain("text"), IndexingStrategy.DOCUMENT);
    }

    public void testTextExistsBatchVectordbIndexed() throws IOException {
        assertExistsMatchesOnlyDocumentsWithValue(plain("text"), IndexingStrategy.BATCH, VECTORDB_INDEXED);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ONLY_MATCHES_EXISTS)
    public void testTextExistsBatchOtherConfigurations() throws IOException {
        assertExistsMatchesOnlyDocumentsWithValue(plain("text"), IndexingStrategy.BATCH, VECTORDB_INDEXED.negate());
    }

    @LuceneTestCase.AwaitsFix(bugUrl = EXISTS_COUNT_DISAGREES)
    public void testKeywordExistsCountDocumentIndexed() throws IOException {
        assertExistsCountAgreesWithHits(plain("keyword"), IndexingStrategy.DOCUMENT, INDEXED);
    }

    public void testKeywordExistsCountDocumentNotIndexed() throws IOException {
        assertExistsCountAgreesWithHits(plain("keyword"), IndexingStrategy.DOCUMENT, NOT_INDEXED);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = EXISTS_COUNT_DISAGREES)
    public void testKeywordExistsCountBatchIndexed() throws IOException {
        assertExistsCountAgreesWithHits(plain("keyword"), IndexingStrategy.BATCH, INDEXED);
    }

    public void testKeywordExistsCountBatchNotIndexed() throws IOException {
        assertExistsCountAgreesWithHits(plain("keyword"), IndexingStrategy.BATCH, NOT_INDEXED);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = EXISTS_COUNT_DISAGREES)
    public void testTextExistsCountDocumentIndexedWithoutVectordb() throws IOException {
        assertExistsCountAgreesWithHits(plain("text"), IndexingStrategy.DOCUMENT, INDEXED_WITHOUT_VECTORDB);
    }

    public void testTextExistsCountDocumentOtherConfigurations() throws IOException {
        assertExistsCountAgreesWithHits(plain("text"), IndexingStrategy.DOCUMENT, INDEXED_WITHOUT_VECTORDB.negate());
    }

    @LuceneTestCase.AwaitsFix(bugUrl = EXISTS_COUNT_DISAGREES)
    public void testTextExistsCountBatchIndexedWithoutVectordb() throws IOException {
        assertExistsCountAgreesWithHits(plain("text"), IndexingStrategy.BATCH, INDEXED_WITHOUT_VECTORDB);
    }

    public void testTextExistsCountBatchOtherConfigurations() throws IOException {
        assertExistsCountAgreesWithHits(plain("text"), IndexingStrategy.BATCH, INDEXED_WITHOUT_VECTORDB.negate());
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ELEMENTS_KEPT)
    public void testKeywordSourceDocument() throws IOException {
        assertSourceDropsNullElements(plain("keyword"), IndexingStrategy.DOCUMENT);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ELEMENTS_KEPT)
    public void testKeywordSourceBatch() throws IOException {
        assertSourceDropsNullElements(plain("keyword"), IndexingStrategy.BATCH);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ELEMENTS_KEPT)
    public void testTextSourceDocument() throws IOException {
        assertSourceDropsNullElements(plain("text"), IndexingStrategy.DOCUMENT);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ELEMENTS_KEPT)
    public void testTextSourceBatch() throws IOException {
        assertSourceDropsNullElements(plain("text"), IndexingStrategy.BATCH);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ELEMENTS_KEPT)
    public void testLongSourceDocument() throws IOException {
        assertSourceDropsNullElements(plain("long"), IndexingStrategy.DOCUMENT);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ELEMENTS_KEPT)
    public void testLongSourceBatch() throws IOException {
        assertSourceDropsNullElements(plain("long"), IndexingStrategy.BATCH);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ELEMENTS_KEPT)
    public void testIntegerSourceDocument() throws IOException {
        assertSourceDropsNullElements(plain("integer"), IndexingStrategy.DOCUMENT);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ELEMENTS_KEPT)
    public void testIntegerSourceBatch() throws IOException {
        assertSourceDropsNullElements(plain("integer"), IndexingStrategy.BATCH);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ELEMENTS_KEPT)
    public void testShortSourceDocument() throws IOException {
        assertSourceDropsNullElements(plain("short"), IndexingStrategy.DOCUMENT);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ELEMENTS_KEPT)
    public void testShortSourceBatch() throws IOException {
        assertSourceDropsNullElements(plain("short"), IndexingStrategy.BATCH);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ELEMENTS_KEPT)
    public void testByteSourceDocument() throws IOException {
        assertSourceDropsNullElements(plain("byte"), IndexingStrategy.DOCUMENT);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ELEMENTS_KEPT)
    public void testByteSourceBatch() throws IOException {
        assertSourceDropsNullElements(plain("byte"), IndexingStrategy.BATCH);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ELEMENTS_KEPT)
    public void testDoubleSourceDocument() throws IOException {
        assertSourceDropsNullElements(plain("double"), IndexingStrategy.DOCUMENT);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ELEMENTS_KEPT)
    public void testDoubleSourceBatch() throws IOException {
        assertSourceDropsNullElements(plain("double"), IndexingStrategy.BATCH);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ELEMENTS_KEPT)
    public void testFloatSourceDocument() throws IOException {
        assertSourceDropsNullElements(plain("float"), IndexingStrategy.DOCUMENT);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ELEMENTS_KEPT)
    public void testFloatSourceBatch() throws IOException {
        assertSourceDropsNullElements(plain("float"), IndexingStrategy.BATCH);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ELEMENTS_KEPT)
    public void testHalfFloatSourceDocument() throws IOException {
        assertSourceDropsNullElements(plain("half_float"), IndexingStrategy.DOCUMENT);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ELEMENTS_KEPT)
    public void testHalfFloatSourceBatch() throws IOException {
        assertSourceDropsNullElements(plain("half_float"), IndexingStrategy.BATCH);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ELEMENTS_KEPT)
    public void testDateSourceDocument() throws IOException {
        assertSourceDropsNullElements(plain("date"), IndexingStrategy.DOCUMENT);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ELEMENTS_KEPT)
    public void testDateSourceBatch() throws IOException {
        assertSourceDropsNullElements(plain("date"), IndexingStrategy.BATCH);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ELEMENTS_KEPT)
    public void testDateNanosSourceDocument() throws IOException {
        assertSourceDropsNullElements(plain("date_nanos"), IndexingStrategy.DOCUMENT);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ELEMENTS_KEPT)
    public void testDateNanosSourceBatch() throws IOException {
        assertSourceDropsNullElements(plain("date_nanos"), IndexingStrategy.BATCH);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ELEMENTS_KEPT)
    public void testBooleanSourceDocument() throws IOException {
        assertSourceDropsNullElements(plain("boolean"), IndexingStrategy.DOCUMENT);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ELEMENTS_KEPT)
    public void testBooleanSourceBatch() throws IOException {
        assertSourceDropsNullElements(plain("boolean"), IndexingStrategy.BATCH);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ELEMENTS_KEPT)
    public void testIpSourceDocument() throws IOException {
        assertSourceDropsNullElements(plain("ip"), IndexingStrategy.DOCUMENT);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = NULL_ELEMENTS_KEPT)
    public void testIpSourceBatch() throws IOException {
        assertSourceDropsNullElements(plain("ip"), IndexingStrategy.BATCH);
    }
}
