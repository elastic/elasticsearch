/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.stats;

import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.junit.After;
import org.junit.Before;

import java.io.Closeable;
import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.index.mapper.DateFieldMapper.DateFieldType;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateNanosToLong;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToLong;

public class SearchContextStatsTests extends MapperServiceTestCase {
    private final Directory directory = newDirectory();
    private SearchStats searchStats;
    private List<MapperService> mapperServices;
    private List<IndexReader> readers;
    private long minMillis, maxMillis, minNanos, maxNanos;

    @Before
    public void setup() throws IOException {
        int indexCount = randomIntBetween(1, 5);
        List<SearchExecutionContext> contexts = new ArrayList<>(indexCount);
        mapperServices = new ArrayList<>(indexCount);
        readers = new ArrayList<>(indexCount);
        maxMillis = minMillis = dateTimeToLong("2025-01-01T00:00:01");
        maxNanos = minNanos = dateNanosToLong("2025-01-01T00:00:01");

        MapperServiceTestCase mapperHelper = new MapperServiceTestCase() {};
        // create one or more index, so that there is one or more SearchExecutionContext in SearchStats
        for (int i = 0; i < indexCount; i++) {
            // Start with millis/nanos, numeric and keyword types in the index mapping, more data types can be covered later if needed.
            // SearchContextStats returns min/max for millis and nanos only currently, null is returned for the other types min and max.
            MapperService mapperService;
            if (i == 0) {
                mapperService = mapperHelper.createMapperService("""
                    {
                        "doc": { "properties": {
                            "byteField": { "type": "byte" },
                            "shortField": { "type": "short" },
                            "intField": { "type": "integer" },
                            "longField": { "type": "long" },
                            "floatField": { "type": "float" },
                            "doubleField": { "type": "double" },
                            "dateField": { "type": "date" },
                            "dateNanosField": { "type": "date_nanos" },
                            "keywordField": { "type": "keyword" },
                            "maybeMixedField": { "type": "long" }
                        }}
                    }""");
            } else {
                mapperService = mapperHelper.createMapperService("""
                    {
                        "doc": { "properties": {
                            "byteField": { "type": "byte" },
                            "shortField": { "type": "short" },
                            "intField": { "type": "integer" },
                            "longField": { "type": "long" },
                            "floatField": { "type": "float" },
                            "doubleField": { "type": "double" },
                            "dateField": { "type": "date" },
                            "dateNanosField": { "type": "date_nanos" },
                            "maybeMixedField": { "type": "date" }
                        }}
                    }""");
            }
            mapperServices.add(mapperService);

            int perIndexDocumentCount = randomIntBetween(1, 5);
            IndexReader reader;
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
                List<Byte> byteValues = randomList(perIndexDocumentCount, perIndexDocumentCount, ESTestCase::randomByte);
                List<Short> shortValues = randomList(perIndexDocumentCount, perIndexDocumentCount, ESTestCase::randomShort);
                List<Integer> intValues = randomList(perIndexDocumentCount, perIndexDocumentCount, ESTestCase::randomInt);
                List<Long> longValues = randomList(perIndexDocumentCount, perIndexDocumentCount, ESTestCase::randomLong);
                List<Float> floatValues = randomList(perIndexDocumentCount, perIndexDocumentCount, ESTestCase::randomFloat);
                List<Double> doubleValues = randomList(perIndexDocumentCount, perIndexDocumentCount, ESTestCase::randomDouble);
                List<String> keywordValues = randomList(perIndexDocumentCount, perIndexDocumentCount, () -> randomAlphaOfLength(5));

                for (int j = 0; j < perIndexDocumentCount; j++) {
                    long millis = minMillis + (j == 0 ? 0 : randomInt(1000));
                    long nanos = minNanos + (j == 0 ? 0 : randomInt(1000));
                    maxMillis = Math.max(millis, maxMillis);
                    maxNanos = Math.max(nanos, maxNanos);
                    minMillis = Math.min(millis, minMillis);
                    minNanos = Math.min(nanos, minNanos);
                    writer.addDocument(
                        List.of(
                            new IntField("byteField", byteValues.get(j), Field.Store.NO),
                            new IntField("shortField", shortValues.get(j), Field.Store.NO),
                            new IntField("intField", intValues.get(j), Field.Store.NO),
                            new LongField("longField", longValues.get(j), Field.Store.NO),
                            new FloatField("floatField", floatValues.get(j), Field.Store.NO),
                            new DoubleField("doubleField", doubleValues.get(j), Field.Store.NO),
                            new LongField("dateField", millis, Field.Store.NO),
                            new LongField("dateNanosField", nanos, Field.Store.NO),
                            new StringField("keywordField", keywordValues.get(j), Field.Store.NO),
                            new LongField("maybeMixedField", millis, Field.Store.NO)
                        )
                    );
                }
                reader = writer.getReader();
                readers.add(reader);
            }
            // create SearchExecutionContext for each index
            SearchExecutionContext context = mapperHelper.createSearchExecutionContext(mapperService, newSearcher(reader));
            contexts.add(context);
        }
        // create SearchContextStats
        searchStats = SearchContextStats.from(contexts);
    }

    public void testMinMax() {
        List<String> fields = List.of(
            "byteField",
            "shortField",
            "intField",
            "longField",
            "floatField",
            "doubleField",
            "dateField",
            "dateNanosField",
            "keywordField"
        );
        for (String field : fields) {
            Object min = searchStats.min(new FieldAttribute.FieldName(field));
            Object max = searchStats.max(new FieldAttribute.FieldName(field));
            if (field.startsWith("date") == false) {
                assertNull(min);
                assertNull(max);
            } else if (field.equals("dateField")) {
                assertEquals(minMillis, min);
                assertEquals(maxMillis, max);
            } else if (field.equals("dateNanosField")) {
                assertEquals(minNanos, min);
                assertEquals(maxNanos, max);
            }
        }
    }

    public void testPointValuesMinMaxDoesNotReturnSentinelValues() throws IOException {
        final MapperServiceTestCase mapperHelper = new MapperServiceTestCase() {};
        final List<SearchExecutionContext> contexts = new ArrayList<>();
        final List<Closeable> toClose = new ArrayList<>();

        try {
            for (int i = 0; i < randomIntBetween(5, 10); i++) {
                final MapperService mapperService = mapperHelper.createMapperService("""
                    { "doc": { "properties": { "date": { "type": "date" }, "keyword": { "type": "keyword" }}}}""");
                assertFalse(((DateFieldType) mapperService.fieldType("date")).hasDocValuesSkipper());
                final Directory dir = newDirectory();
                final IndexReader reader;
                try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                    writer.addDocument(List.of(new StringField("keyword", "value" + i, Field.Store.NO)));
                    reader = writer.getReader();
                }
                toClose.add(reader);
                toClose.add(mapperService);
                toClose.add(dir);
                contexts.add(mapperHelper.createSearchExecutionContext(mapperService, newSearcher(reader)));
            }

            final SearchStats stats = SearchContextStats.from(contexts);
            final FieldAttribute.FieldName dateFieldName = new FieldAttribute.FieldName("date");
            assertNull(stats.min(dateFieldName));
            assertNull(stats.max(dateFieldName));
            final Rounding.Prepared prepared = new Rounding.Builder(TimeValue.timeValueMinutes(30)).timeZone(ZoneId.of("Europe/Rome"))
                .build()
                .prepare(0L, 0L);
            assertNotNull(prepared);
        } finally {
            IOUtils.close(toClose);
        }
    }

    public void testDocValuesSkipperMinMaxDoesNotReturnSentinelValues() throws IOException {
        final List<SearchExecutionContext> contexts = new ArrayList<>();
        final List<Closeable> toClose = new ArrayList<>();

        try {
            for (int i = 0; i < randomIntBetween(5, 10); i++) {
                final MapperService mapperService = createMapperService(
                    Settings.builder().put("index.mode", "time_series").put("index.routing_path", "uid").build(),
                    """
                        { "doc": { "properties": {
                            "@timestamp": { "type": "date" },
                            "uid": { "type": "keyword", "time_series_dimension": true }
                        }}}"""
                );
                assertTrue(((DateFieldType) mapperService.fieldType("@timestamp")).hasDocValuesSkipper());
                final Directory dir = newDirectory();
                final IndexReader reader;
                try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                    writer.addDocument(List.of(new StringField("uid", "id" + i, Field.Store.NO)));
                    reader = writer.getReader();
                }
                toClose.add(reader);
                toClose.add(mapperService);
                toClose.add(dir);
                contexts.add(createSearchExecutionContext(mapperService, newSearcher(reader)));
            }

            final SearchStats stats = SearchContextStats.from(contexts);
            final FieldAttribute.FieldName timestampFieldName = new FieldAttribute.FieldName("@timestamp");
            assertNull(stats.min(timestampFieldName));
            assertNull(stats.max(timestampFieldName));
            final Rounding.Prepared prepared = new Rounding.Builder(TimeValue.timeValueMinutes(30)).timeZone(ZoneId.of("Europe/Rome"))
                .build()
                .prepare(0L, 0L);
            assertNotNull(prepared);
        } finally {
            IOUtils.close(toClose);
        }
    }

    /**
     * Verifies that {@code isSingleValue} correctly detects multi-valued keyword fields even when
     * the number of unique terms equals the number of documents — a case where the old heuristic
     * ({@code terms.size() == terms.getDocCount()}) produced a false positive.
     * <p>
     * Given doc1=["A","B"] and doc2=["A"]:
     * <ul>
     *   <li>{@code terms.size()} = 2 (unique terms: A, B)</li>
     *   <li>{@code terms.getDocCount()} = 2 (docs with the field)</li>
     *   <li>{@code terms.getSumDocFreq()} = 3 (docFreq("A")=2 + docFreq("B")=1)</li>
     * </ul>
     * The old check ({@code size == docCount}) would return {@code true} (single-valued) — wrong.
     * The new check ({@code sumDocFreq == docCount}) returns {@code false} (multi-valued) — correct.
     * <p>
     * When this false positive is visible in a scenario, {@code PushStatsToSource} pushes {@code COUNT(keyword_field)} to Lucene
     * as a document count ({@code EsStatsQueryExec}), yielding 2 instead of 3.
     */
    public void testKeywordMultiValueDetection() throws IOException {
        final MapperServiceTestCase mapperHelper = new MapperServiceTestCase() {};
        final MapperService mapperService = mapperHelper.createMapperService("""
            { "doc": { "properties": { "kw": { "type": "keyword" } } } }""");

        final Directory dir = newDirectory();
        final IndexReader reader;
        try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
            writer.addDocument(List.of(new StringField("kw", "A", Field.Store.NO), new StringField("kw", "B", Field.Store.NO)));
            writer.addDocument(List.of(new StringField("kw", "A", Field.Store.NO)));
            writer.forceMerge(1);
            reader = writer.getReader();
        }

        try {
            LeafReader leafReader = ((DirectoryReader) reader).leaves().get(0).reader();
            Terms terms = leafReader.terms("kw");
            assertNotNull(terms);

            assertEquals(2, terms.size());
            assertEquals(2, terms.getDocCount());
            assertEquals(3, terms.getSumDocFreq());

            SearchExecutionContext ctx = mapperHelper.createSearchExecutionContext(mapperService, newSearcher(reader));
            SearchStats stats = SearchContextStats.from(List.of(ctx));
            assertFalse(
                "keyword field with MVs must not be reported as single-valued",
                stats.isSingleValue(new FieldAttribute.FieldName("kw"))
            );
        } finally {
            IOUtils.close(reader, mapperService, dir);
        }
    }

    public void testDocValuesOnlyKeywordIsNotDetectedAsSingleValued() throws IOException {
        final MapperServiceTestCase mapperHelper = new MapperServiceTestCase() {};
        final MapperService mapperService = mapperHelper.createMapperService("""
            { "doc": { "properties": { "kw": { "type": "keyword", "index": false } } } }""");

        final Directory dir = newDirectory();
        final IndexReader reader;
        try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
            writer.addDocument(
                List.of(new SortedSetDocValuesField("kw", new BytesRef("A")), new SortedSetDocValuesField("kw", new BytesRef("B")))
            );
            writer.addDocument(List.of(new SortedSetDocValuesField("kw", new BytesRef("C"))));
            writer.forceMerge(1);
            reader = writer.getReader();
        }

        try {
            LeafReader leafReader = ((DirectoryReader) reader).leaves().get(0).reader();
            assertNull(leafReader.terms("kw"));

            SearchExecutionContext ctx = mapperHelper.createSearchExecutionContext(mapperService, newSearcher(reader));
            SearchStats stats = SearchContextStats.from(List.of(ctx));
            assertFalse(
                "keyword field without terms must not be reported as single-valued",
                stats.isSingleValue(new FieldAttribute.FieldName("kw"))
            );
        } finally {
            IOUtils.close(reader, mapperService, dir);
        }
    }

    /**
     * Verifies that a multi-valued numeric field without a points index or a doc-values skipper
     * ({@code index: false} in standard mode without {@code use_doc_values_skipper}) is never
     * reported as single-valued. Without the fix, {@code getPointValues()} returning {@code null}
     * was misread as "field absent → single-valued", causing {@code PushStatsToSource} to rewrite
     * {@code COUNT(n)} to a doc-count exists query — returning 2 instead of 3.
     */
    public void testDocValuesOnlyNumericIsNotDetectedAsSingleValued() throws IOException {
        final MapperServiceTestCase mapperHelper = new MapperServiceTestCase() {};
        // index:false in standard mode without USE_DOC_VALUES_SKIPPER → IndexType.points(false,true)
        // → neither hasPoints() nor hasDocValuesSkipper() → tester stays null → return false
        final MapperService mapperService = mapperHelper.createMapperService("""
            { "doc": { "properties": { "n": { "type": "long", "index": false } } } }""");

        final Directory dir = newDirectory();
        final DirectoryReader reader;
        try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
            writer.addDocument(List.of(new SortedNumericDocValuesField("n", 1L), new SortedNumericDocValuesField("n", 2L)));
            writer.addDocument(List.of(new SortedNumericDocValuesField("n", 3L)));
            writer.forceMerge(1);
            reader = writer.getReader();
        }

        try {
            LeafReader leafReader = reader.leaves().get(0).reader();
            assertNull("index:false long must have no points index", leafReader.getPointValues("n"));

            SearchExecutionContext ctx = mapperHelper.createSearchExecutionContext(mapperService, newSearcher(reader));
            SearchStats stats = SearchContextStats.from(List.of(ctx));
            assertFalse(
                "numeric field without points or skipper index must not be reported as single-valued",
                stats.isSingleValue(new FieldAttribute.FieldName("n"))
            );
        } finally {
            IOUtils.close(reader, mapperService, dir);
        }
    }

    /**
     * Same as {@link #testDocValuesOnlyNumericIsNotDetectedAsSingleValued()} but for a date field,
     * covering the {@code DateFieldType} half of the branch.
     */
    public void testDocValuesOnlyDateIsNotDetectedAsSingleValued() throws IOException {
        final MapperServiceTestCase mapperHelper = new MapperServiceTestCase() {};
        final MapperService mapperService = mapperHelper.createMapperService("""
            { "doc": { "properties": { "d": { "type": "date", "index": false } } } }""");

        final Directory dir = newDirectory();
        final DirectoryReader reader;
        try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
            writer.addDocument(List.of(new SortedNumericDocValuesField("d", 1000L), new SortedNumericDocValuesField("d", 2000L)));
            writer.addDocument(List.of(new SortedNumericDocValuesField("d", 3000L)));
            writer.forceMerge(1);
            reader = writer.getReader();
        }

        try {
            LeafReader leafReader = reader.leaves().get(0).reader();
            assertNull("index:false date must have no points index", leafReader.getPointValues("d"));

            SearchExecutionContext ctx = mapperHelper.createSearchExecutionContext(mapperService, newSearcher(reader));
            SearchStats stats = SearchContextStats.from(List.of(ctx));
            assertFalse(
                "date field without points or skipper index must not be reported as single-valued",
                stats.isSingleValue(new FieldAttribute.FieldName("d"))
            );
        } finally {
            IOUtils.close(reader, mapperService, dir);
        }
    }

    /**
     * Verifies that a truly single-valued numeric field backed by a doc-values skipper (as in
     * columnar or {@code use_doc_values_skipper} mode) is correctly detected as single-valued.
     * <p>
     * The codec records {@code globalMaxValueCount = 1} at flush time; {@code maxValueCount()}
     * returning {@code 1} lets {@code detectSingleValue} return {@code true}, enabling
     * {@code PushStatsToSource} to push {@code COUNT(n)} down to an exists-doc-count query for an
     * exact result.
     */
    public void testSkipperNumericSingleValuedIsDetectedAsSingleValued() throws IOException {
        final Settings settings = Settings.builder().put(IndexSettings.USE_DOC_VALUES_SKIPPER.getKey(), true).build();
        final MapperService mapperService = createMapperService(settings, """
            { "doc": { "properties": { "n": { "type": "long", "index": false } } } }""");

        final Directory dir = newDirectory();
        final DirectoryReader reader;
        try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
            // indexedField() sets DocValuesSkipIndexType.RANGE, causing the codec to write a real
            // DocValuesSkipper that persists globalMaxValueCount per segment.
            writer.addDocument(List.of(SortedNumericDocValuesField.indexedField("n", 1L)));
            writer.addDocument(List.of(SortedNumericDocValuesField.indexedField("n", 3L)));
            writer.forceMerge(1);
            reader = writer.getReader();
        }

        try {
            LeafReader leafReader = reader.leaves().get(0).reader();
            DocValuesSkipper skipper = leafReader.getDocValuesSkipper("n");
            assertNotNull("indexedField() must produce a DocValuesSkipper", skipper);
            assertEquals("every doc has exactly one value, so maxValueCount must be 1", 1, skipper.maxValueCount());

            SearchStats stats = SearchContextStats.from(List.of(createSearchExecutionContext(mapperService, newSearcher(reader))));
            assertTrue(
                "single-valued skipper numeric field must be reported as single-valued",
                stats.isSingleValue(new FieldAttribute.FieldName("n"))
            );
        } finally {
            IOUtils.close(reader, mapperService, dir);
        }
    }

    /**
     * Verifies that a multi-valued numeric field backed by a doc-values skipper is correctly
     * rejected: {@code maxValueCount() > 1} means at least one document has multiple values, so
     * {@code COUNT(n)} must not be pushed down to a doc-count exists query.
     */
    public void testSkipperNumericMultiValuedIsNotDetectedAsSingleValued() throws IOException {
        final Settings settings = Settings.builder().put(IndexSettings.USE_DOC_VALUES_SKIPPER.getKey(), true).build();
        final MapperService mapperService = createMapperService(settings, """
            { "doc": { "properties": { "n": { "type": "long", "index": false } } } }""");

        final Directory dir = newDirectory();
        final DirectoryReader reader;
        try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
            writer.addDocument(
                List.of(SortedNumericDocValuesField.indexedField("n", 1L), SortedNumericDocValuesField.indexedField("n", 2L))
            );
            writer.addDocument(List.of(SortedNumericDocValuesField.indexedField("n", 3L)));
            writer.forceMerge(1);
            reader = writer.getReader();
        }

        try {
            LeafReader leafReader = reader.leaves().get(0).reader();
            DocValuesSkipper skipper = leafReader.getDocValuesSkipper("n");
            assertNotNull("indexedField() must produce a DocValuesSkipper", skipper);
            assertTrue("one doc has 2 values, so maxValueCount must be > 1", skipper.maxValueCount() > 1);

            SearchStats stats = SearchContextStats.from(List.of(createSearchExecutionContext(mapperService, newSearcher(reader))));
            assertFalse(
                "multi-valued skipper numeric field must not be reported as single-valued",
                stats.isSingleValue(new FieldAttribute.FieldName("n"))
            );
        } finally {
            IOUtils.close(reader, mapperService, dir);
        }
    }

    /**
     * Date-field counterpart of {@link #testSkipperNumericSingleValuedIsDetectedAsSingleValued}:
     * a truly single-valued date field backed by a doc-values skipper must be reported as
     * single-valued so that {@code COUNT(d)} can be pushed down to an exists-doc-count query.
     */
    public void testSkipperDateSingleValuedIsDetectedAsSingleValued() throws IOException {
        final Settings settings = Settings.builder().put(IndexSettings.USE_DOC_VALUES_SKIPPER.getKey(), true).build();
        final MapperService mapperService = createMapperService(settings, """
            { "doc": { "properties": { "d": { "type": "date", "index": false } } } }""");

        final Directory dir = newDirectory();
        final DirectoryReader reader;
        try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
            writer.addDocument(List.of(SortedNumericDocValuesField.indexedField("d", 1000L)));
            writer.addDocument(List.of(SortedNumericDocValuesField.indexedField("d", 2000L)));
            writer.forceMerge(1);
            reader = writer.getReader();
        }

        try {
            LeafReader leafReader = reader.leaves().get(0).reader();
            DocValuesSkipper skipper = leafReader.getDocValuesSkipper("d");
            assertNotNull("indexedField() must produce a DocValuesSkipper", skipper);
            assertEquals("every doc has exactly one value, so maxValueCount must be 1", 1, skipper.maxValueCount());

            SearchStats stats = SearchContextStats.from(List.of(createSearchExecutionContext(mapperService, newSearcher(reader))));
            assertTrue(
                "single-valued skipper date field must be reported as single-valued",
                stats.isSingleValue(new FieldAttribute.FieldName("d"))
            );
        } finally {
            IOUtils.close(reader, mapperService, dir);
        }
    }

    /**
     * Date-field counterpart of {@link #testSkipperNumericMultiValuedIsNotDetectedAsSingleValued}:
     * a multi-valued date field backed by a doc-values skipper must not be reported as single-valued,
     * so {@code COUNT(d)} is not incorrectly pushed down to a doc-count exists query.
     */
    public void testSkipperDateMultiValuedIsNotDetectedAsSingleValued() throws IOException {
        final Settings settings = Settings.builder().put(IndexSettings.USE_DOC_VALUES_SKIPPER.getKey(), true).build();
        final MapperService mapperService = createMapperService(settings, """
            { "doc": { "properties": { "d": { "type": "date", "index": false } } } }""");

        final Directory dir = newDirectory();
        final DirectoryReader reader;
        try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
            writer.addDocument(
                List.of(SortedNumericDocValuesField.indexedField("d", 1000L), SortedNumericDocValuesField.indexedField("d", 2000L))
            );
            writer.addDocument(List.of(SortedNumericDocValuesField.indexedField("d", 3000L)));
            writer.forceMerge(1);
            reader = writer.getReader();
        }

        try {
            LeafReader leafReader = reader.leaves().get(0).reader();
            DocValuesSkipper skipper = leafReader.getDocValuesSkipper("d");
            assertNotNull("indexedField() must produce a DocValuesSkipper", skipper);
            assertTrue("one doc has 2 values, so maxValueCount must be > 1", skipper.maxValueCount() > 1);

            SearchStats stats = SearchContextStats.from(List.of(createSearchExecutionContext(mapperService, newSearcher(reader))));
            assertFalse(
                "multi-valued skipper date field must not be reported as single-valued",
                stats.isSingleValue(new FieldAttribute.FieldName("d"))
            );
        } finally {
            IOUtils.close(reader, mapperService, dir);
        }
    }

    /**
     * Reproduces the mixed-index-mapping bug reported in review: when a query spans two indices
     * where the same field has different storage characteristics — points in one, doc-values skipper
     * in the other — {@code isSingleValue} picks the {@code MappedFieldType} from the first mapped
     * context (points) and applies its tester to <em>all</em> leaf readers via {@code doWithContexts}.
     * For the skipper-backed leaves, {@code getPointValues(name)} returns {@code null}, which the
     * points tester misreads as "field absent → single-valued". The result is a false positive even
     * though the skipper-backed index contains multi-valued documents.
     */
    public void testMixedIndexMappingPointsVsSkipperIsNotFalselyDetectedAsSingleValued() throws IOException {
        final MapperServiceTestCase mapperHelper = new MapperServiceTestCase() {};
        final List<SearchExecutionContext> contexts = new ArrayList<>();
        final List<Closeable> toClose = new ArrayList<>();

        try {
            // Index A: standard long with a points index (hasPoints=true, hasDocValuesSkipper=false).
            // Placed first so its MappedFieldType is picked by the contexts loop.
            final MapperService mapperServiceA = mapperHelper.createMapperService("""
                { "doc": { "properties": { "n": { "type": "long" } } } }""");
            final Directory dirA = newDirectory();
            final IndexReader readerA;
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dirA)) {
                writer.addDocument(List.of(new LongField("n", 1L, Field.Store.NO)));
                writer.forceMerge(1);
                readerA = writer.getReader();
            }
            toClose.add(readerA);
            toClose.add(mapperServiceA);
            toClose.add(dirA);
            contexts.add(mapperHelper.createSearchExecutionContext(mapperServiceA, newSearcher(readerA)));

            // Index B: long field backed by a doc-values skipper (index:false + USE_DOC_VALUES_SKIPPER),
            // with at least one multi-valued document.
            final Settings settings = Settings.builder().put(IndexSettings.USE_DOC_VALUES_SKIPPER.getKey(), true).build();
            final MapperService mapperServiceB = createMapperService(settings, """
                { "doc": { "properties": { "n": { "type": "long", "index": false } } } }""");
            final Directory dirB = newDirectory();
            final IndexReader readerB;
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dirB)) {
                writer.addDocument(
                    List.of(SortedNumericDocValuesField.indexedField("n", 1L), SortedNumericDocValuesField.indexedField("n", 2L))
                );
                writer.addDocument(List.of(SortedNumericDocValuesField.indexedField("n", 3L)));
                writer.forceMerge(1);
                readerB = writer.getReader();
            }
            toClose.add(readerB);
            toClose.add(mapperServiceB);
            toClose.add(dirB);
            contexts.add(createSearchExecutionContext(mapperServiceB, newSearcher(readerB)));

            // Confirm the structural premise: index B has a skipper but no points for "n".
            LeafReader leafB = ((DirectoryReader) readerB).leaves().get(0).reader();
            assertNotNull("index B must have a DocValuesSkipper for 'n'", leafB.getDocValuesSkipper("n"));
            assertNull("index B must have no PointValues for 'n'", leafB.getPointValues("n"));

            final SearchStats stats = SearchContextStats.from(contexts);
            assertFalse(
                "'n' is multi-valued in index B — must not be reported as single-valued across mixed-mapping indices",
                stats.isSingleValue(new FieldAttribute.FieldName("n"))
            );
        } finally {
            IOUtils.close(toClose);
        }
    }

    /**
     * A single-valued numeric field with a point index must be reported as single-valued via the
     * {@code PointValues.size() == PointValues.getDocCount()} check. Covers the points branch of
     * {@code detectSingleValue}, as opposed to the doc-values-skipper branch.
     */
    public void testPointIndexedSingleValuedNumericIsDetectedAsSingleValued() throws IOException {
        final MapperServiceTestCase mapperHelper = new MapperServiceTestCase() {};
        final MapperService mapperService = mapperHelper.createMapperService("""
            { "doc": { "properties": { "lng": { "type": "long" } } } }""");

        final Directory dir = newDirectory();
        final DirectoryReader reader;
        try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
            writer.addDocument(List.of(new LongField("lng", 1, Field.Store.NO)));
            writer.addDocument(List.of(new LongField("lng", 2, Field.Store.NO)));
            writer.forceMerge(1);
            reader = writer.getReader();
        }

        try {
            LeafReader leafReader = reader.leaves().get(0).reader();
            PointValues points = leafReader.getPointValues("lng");
            assertNotNull(points);
            assertEquals(2, points.size());
            assertEquals(2, points.getDocCount());

            SearchExecutionContext ctx = mapperHelper.createSearchExecutionContext(mapperService, newSearcher(reader));
            SearchStats stats = SearchContextStats.from(List.of(ctx));
            assertTrue(
                "single-valued numeric field with a point index must be reported as single-valued",
                stats.isSingleValue(new FieldAttribute.FieldName("lng"))
            );
        } finally {
            IOUtils.close(reader, mapperService, dir);
        }
    }

    /**
     * A multi-valued numeric field with a point index must be reported as multi-valued via the
     * {@code PointValues.size() == PointValues.getDocCount()} check.
     */
    public void testPointIndexedMultiValuedNumericIsDetectedAsMultiValued() throws IOException {
        final MapperServiceTestCase mapperHelper = new MapperServiceTestCase() {};
        final MapperService mapperService = mapperHelper.createMapperService("""
            { "doc": { "properties": { "lng": { "type": "long" } } } }""");

        final Directory dir = newDirectory();
        final DirectoryReader reader;
        try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
            writer.addDocument(List.of(new LongField("lng", 1, Field.Store.NO), new LongField("lng", 2, Field.Store.NO)));
            writer.addDocument(List.of(new LongField("lng", 3, Field.Store.NO)));
            writer.forceMerge(1);
            reader = writer.getReader();
        }

        try {
            LeafReader leafReader = reader.leaves().get(0).reader();
            PointValues points = leafReader.getPointValues("lng");
            assertNotNull(points);
            assertEquals(3, points.size());
            assertEquals(2, points.getDocCount());

            SearchExecutionContext ctx = mapperHelper.createSearchExecutionContext(mapperService, newSearcher(reader));
            SearchStats stats = SearchContextStats.from(List.of(ctx));
            assertFalse(
                "multi-valued numeric field must not be reported as single-valued",
                stats.isSingleValue(new FieldAttribute.FieldName("lng"))
            );
        } finally {
            IOUtils.close(reader, mapperService, dir);
        }
    }

    @After
    public void cleanup() throws IOException {
        IOUtils.close(readers);
        IOUtils.close(mapperServices);
        IOUtils.close(directory);
    }
}
