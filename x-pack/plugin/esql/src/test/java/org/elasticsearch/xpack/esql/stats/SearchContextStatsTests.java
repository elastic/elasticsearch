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
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
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
        minMillis = dateTimeToLong("2025-01-01T00:00:01");
        minNanos = dateNanosToLong("2025-01-01T00:00:01");
        maxMillis = minMillis;
        maxNanos = minNanos;
        MapperServiceTestCase mapperHelper = new MapperServiceTestCase() {
        };
        for (int i = 0; i < indexCount; i++) {
            // Start with numeric, millis/nanos, keyword types in the index mapping, more data types can be covered later if necessary.
            // SearchContextStats returns min/max for millis and nanos only currently, null is returned for the other types min and max.
            MapperService mapperService = mapperHelper.createMapperService("""
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
                        "keywordField": { "type": "keyword" }
                    }}
                }""");
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
                            new StringField("keywordField", keywordValues.get(j), Field.Store.NO)
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
            Object min = searchStats.min(field);
            Object max = searchStats.max(field);
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

    @After
    public void cleanup() throws IOException {
        IOUtils.close(readers);
        IOUtils.close(mapperServices);
        IOUtils.close(directory);
    }
}
