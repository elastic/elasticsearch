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
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
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
            contexts.add(mapperHelper.createSearchExecutionContext(mapperService, newSearcher(reader)));
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

    public void testPointValuesMinMaxRounding() throws IOException {
        final MapperServiceTestCase mapperHelper = new MapperServiceTestCase() {
        };
        final List<SearchExecutionContext> contexts = new ArrayList<>();
        final List<Closeable> toClose = new ArrayList<>();

        try {
            for (int i = 0; i < randomIntBetween(5, 10); i++) {
                final MapperService mapperService = mapperHelper.createMapperService("""
                    { "doc": { "properties": { "date": { "type": "date" }, "keyword": { "type": "keyword" }}}}""");
                final Directory directory = newDirectory();
                final IndexReader reader;
                try (RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
                    writer.addDocument(List.of(new StringField("keyword", "value" + i, Field.Store.NO)));
                    reader = writer.getReader();
                }
                toClose.add(reader);
                toClose.add(mapperService);
                toClose.add(directory);
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
            assertEquals(0L, prepared.round(0L));
        } finally {
            IOUtils.close(toClose);
        }
    }

    public void testDocValuesSkipperMinMaxRounding() throws IOException {
        final MapperServiceTestCase tsdbHelper = new MapperServiceTestCase() {
            @Override
            protected Settings getIndexSettings() {
                return Settings.builder()
                    .put(super.getIndexSettings())
                    .put(IndexSettings.MODE.getKey(), "time_series")
                    .put("index.routing_path", "dim")
                    .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2025-01-01T00:00:00Z")
                    .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2025-02-01T00:00:00Z")
                    .build();
            }
        };
        final MapperServiceTestCase standardHelper = new MapperServiceTestCase() {
        };
        final List<SearchExecutionContext> contexts = new ArrayList<>();
        final List<Closeable> toClose = new ArrayList<>();

        try {
            final MapperService tsdbMapper = tsdbHelper.createMapperService("""
                { "_doc": { "properties": {
                    "@timestamp": { "type": "date" },
                    "dim": { "type": "keyword", "time_series_dimension": true }
                }}}""");
            final Directory tsdbDir = newDirectory();
            final IndexReader tsdbReader;
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), tsdbDir)) {
                writer.addDocument(
                    List.of(
                        new LongField("@timestamp", dateTimeToLong("2025-01-15T00:00:00"), Field.Store.NO),
                        new StringField("dim", "a", Field.Store.NO)
                    )
                );
                tsdbReader = writer.getReader();
            }
            toClose.add(tsdbReader);
            toClose.add(tsdbMapper);
            toClose.add(tsdbDir);
            contexts.add(tsdbHelper.createSearchExecutionContext(tsdbMapper, newSearcher(tsdbReader)));

            for (int i = 0; i < randomIntBetween(1, 5); i++) {
                final MapperService stdMapper = standardHelper.createMapperService("""
                    { "doc": { "properties": { "@timestamp": { "type": "date" }, "keyword": { "type": "keyword" }}}}""");
                final Directory stdDir = newDirectory();
                final IndexReader stdReader;
                try (RandomIndexWriter writer = new RandomIndexWriter(random(), stdDir)) {
                    writer.addDocument(
                        List.of(
                            new LongField("@timestamp", dateTimeToLong("2025-01-10T00:00:00"), Field.Store.NO),
                            new StringField("keyword", "value" + i, Field.Store.NO)
                        )
                    );
                    stdReader = writer.getReader();
                }
                toClose.add(stdReader);
                toClose.add(stdMapper);
                toClose.add(stdDir);
                contexts.add(standardHelper.createSearchExecutionContext(stdMapper, newSearcher(stdReader)));
            }

            final SearchStats stats = SearchContextStats.from(contexts);
            final FieldAttribute.FieldName timestampField = new FieldAttribute.FieldName("@timestamp");
            assertNull(stats.min(timestampField));
            assertNull(stats.max(timestampField));
            final Rounding.Prepared prepared = new Rounding.Builder(TimeValue.timeValueMinutes(30)).timeZone(ZoneId.of("Europe/Rome"))
                .build()
                .prepare(0L, 0L);
            assertNotNull(prepared);
            assertEquals(0L, prepared.round(0L));
        } finally {
            IOUtils.close(toClose);
        }
    }

    @After
    public void cleanup() throws IOException {
        IOUtils.close(readers);
        IOUtils.close(mapperServices);
        IOUtils.close(directory);
    }
}
