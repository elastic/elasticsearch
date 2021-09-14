/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.support;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;

import java.util.List;

// TODO: This whole set of tests needs to be rethought.
public class ValuesSourceConfigTests extends MapperServiceTestCase {
    public void testKeyword() throws Exception {
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "keyword")));
        withAggregationContext(mapperService, List.of(source(b -> b.field("field", "abc"))), context -> {
            ValuesSourceConfig config;
            config = ValuesSourceConfig.resolve(context, null, "field", null, null, null, null, CoreValuesSourceType.KEYWORD);
            ValuesSource.Bytes valuesSource = (ValuesSource.Bytes) config.getValuesSource();
            LeafReaderContext ctx = context.searcher().getIndexReader().leaves().get(0);
            SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            assertEquals(new BytesRef("abc"), values.nextValue());
        });
    }

    public void testEmptyKeyword() throws Exception {
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "keyword")));
        withAggregationContext(mapperService, List.of(source(b -> {})), context -> {
            ValuesSourceConfig config;
            config = ValuesSourceConfig.resolve(context, null, "field", null, null, null, null, CoreValuesSourceType.KEYWORD);
            ValuesSource.Bytes valuesSource = (ValuesSource.Bytes) config.getValuesSource();
            LeafReaderContext ctx = context.searcher().getIndexReader().leaves().get(0);
            SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
            assertFalse(values.advanceExact(0));
            assertTrue(config.alignesWithSearchIndex());

            config = ValuesSourceConfig.resolve(context, null, "field", null, "abc", null, null, CoreValuesSourceType.KEYWORD);
            valuesSource = (ValuesSource.Bytes) config.getValuesSource();
            values = valuesSource.bytesValues(ctx);
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            assertEquals(new BytesRef("abc"), values.nextValue());
            assertFalse(config.alignesWithSearchIndex());
        });
    }

    public void testUnmappedKeyword() throws Exception {
        MapperService mapperService = createMapperService(mapping(b -> {}));
        withAggregationContext(mapperService, List.of(source(b -> {})), context -> {
            ValuesSourceConfig config;
            config = ValuesSourceConfig.resolve(context, ValueType.STRING, "field", null, null, null, null, CoreValuesSourceType.KEYWORD);
            ValuesSource.Bytes valuesSource = (ValuesSource.Bytes) config.getValuesSource();
            assertNotNull(valuesSource);
            assertFalse(config.hasValues());
            assertFalse(config.alignesWithSearchIndex());

            config = ValuesSourceConfig.resolve(context, ValueType.STRING, "field", null, "abc", null, null, CoreValuesSourceType.KEYWORD);
            valuesSource = (ValuesSource.Bytes) config.getValuesSource();
            LeafReaderContext ctx = context.searcher().getIndexReader().leaves().get(0);
            SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            assertEquals(new BytesRef("abc"), values.nextValue());
            assertFalse(config.alignesWithSearchIndex());
        });
    }

    public void testLong() throws Exception {
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "long")));
        withAggregationContext(mapperService, List.of(source(b -> b.field("field", 42))), context -> {
            ValuesSourceConfig config;
            config = ValuesSourceConfig.resolve(context, null, "field", null, null, null, null, CoreValuesSourceType.KEYWORD);
            ValuesSource.Numeric valuesSource = (ValuesSource.Numeric) config.getValuesSource();
            LeafReaderContext ctx = context.searcher().getIndexReader().leaves().get(0);
            SortedNumericDocValues values = valuesSource.longValues(ctx);
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            assertEquals(42, values.nextValue());
            assertTrue(config.alignesWithSearchIndex());
        });
    }

    public void testEmptyLong() throws Exception {
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "long")));
        withAggregationContext(mapperService, List.of(source(b -> {})), context -> {
            ValuesSourceConfig config;
            config = ValuesSourceConfig.resolve(context, null, "field", null, null, null, null, CoreValuesSourceType.KEYWORD);
            ValuesSource.Numeric valuesSource = (ValuesSource.Numeric) config.getValuesSource();
            LeafReaderContext ctx = context.searcher().getIndexReader().leaves().get(0);
            SortedNumericDocValues values = valuesSource.longValues(ctx);
            assertFalse(values.advanceExact(0));
            assertTrue(config.alignesWithSearchIndex());

            config = ValuesSourceConfig.resolve(context, null, "field", null, 42, null, null, CoreValuesSourceType.KEYWORD);
            valuesSource = (ValuesSource.Numeric) config.getValuesSource();
            values = valuesSource.longValues(ctx);
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            assertEquals(42, values.nextValue());
            assertFalse(config.alignesWithSearchIndex());
        });
    }

    public void testUnmappedLong() throws Exception {
        MapperService mapperService = createMapperService(mapping(b -> {}));
        withAggregationContext(mapperService, List.of(source(b -> {})), context -> {
            ValuesSourceConfig config;
            config = ValuesSourceConfig.resolve(context, ValueType.NUMBER, "field", null, null, null, null, CoreValuesSourceType.KEYWORD);
            ValuesSource.Numeric valuesSource = (ValuesSource.Numeric) config.getValuesSource();
            assertNotNull(valuesSource);
            assertFalse(config.hasValues());
            assertFalse(config.alignesWithSearchIndex());

            config = ValuesSourceConfig.resolve(context, ValueType.NUMBER, "field", null, 42, null, null, CoreValuesSourceType.KEYWORD);
            valuesSource = (ValuesSource.Numeric) config.getValuesSource();
            LeafReaderContext ctx = context.searcher().getIndexReader().leaves().get(0);
            SortedNumericDocValues values = valuesSource.longValues(ctx);
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            assertEquals(42, values.nextValue());
            assertFalse(config.alignesWithSearchIndex());
        });
    }

    public void testBoolean() throws Exception {
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "boolean")));
        withAggregationContext(mapperService, List.of(source(b -> b.field("field", true))), context -> {
            ValuesSourceConfig config;
            config = ValuesSourceConfig.resolve(context, null, "field", null, null, null, null, CoreValuesSourceType.KEYWORD);
            ValuesSource.Numeric valuesSource = (ValuesSource.Numeric) config.getValuesSource();
            LeafReaderContext ctx = context.searcher().getIndexReader().leaves().get(0);
            SortedNumericDocValues values = valuesSource.longValues(ctx);
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            assertEquals(1, values.nextValue());
            assertTrue(config.alignesWithSearchIndex());
        });
    }

    public void testEmptyBoolean() throws Exception {
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "boolean")));
        withAggregationContext(mapperService, List.of(source(b -> {})), context -> {
            ValuesSourceConfig config;
            config = ValuesSourceConfig.resolve(context, null, "field", null, null, null, null, CoreValuesSourceType.KEYWORD);
            ValuesSource.Numeric valuesSource = (ValuesSource.Numeric) config.getValuesSource();
            LeafReaderContext ctx = context.searcher().getIndexReader().leaves().get(0);
            SortedNumericDocValues values = valuesSource.longValues(ctx);
            assertFalse(values.advanceExact(0));
            assertTrue(config.alignesWithSearchIndex());

            config = ValuesSourceConfig.resolve(context, null, "field", null, true, null, null, CoreValuesSourceType.KEYWORD);
            valuesSource = (ValuesSource.Numeric) config.getValuesSource();
            values = valuesSource.longValues(ctx);
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            assertEquals(1, values.nextValue());
            assertFalse(config.alignesWithSearchIndex());
        });
    }

    public void testUnmappedBoolean() throws Exception {
        MapperService mapperService = createMapperService(mapping(b -> {}));
        withAggregationContext(mapperService, List.of(source(b -> {})), context -> {
            ValuesSourceConfig config;
            config = ValuesSourceConfig.resolve(context, ValueType.BOOLEAN, "field", null, null, null, null, CoreValuesSourceType.KEYWORD);
            ValuesSource.Numeric valuesSource = (ValuesSource.Numeric) config.getValuesSource();
            assertNotNull(valuesSource);
            assertFalse(config.hasValues());
            assertFalse(config.alignesWithSearchIndex());

            config = ValuesSourceConfig.resolve(context, ValueType.BOOLEAN, "field", null, true, null, null, CoreValuesSourceType.KEYWORD);
            valuesSource = (ValuesSource.Numeric) config.getValuesSource();
            LeafReaderContext ctx = context.searcher().getIndexReader().leaves().get(0);
            SortedNumericDocValues values = valuesSource.longValues(ctx);
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            assertEquals(1, values.nextValue());
            assertFalse(config.alignesWithSearchIndex());
        });
    }

    public void testFieldAlias() throws Exception {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("field").field("type", "keyword").endObject();
            b.startObject("alias").field("type", "alias").field("path", "field").endObject();
        }));
        withAggregationContext(mapperService, List.of(source(b -> b.field("field", "value"))), context -> {
            ValuesSourceConfig config;
            config = ValuesSourceConfig.resolve(context, ValueType.STRING, "alias", null, null, null, null, CoreValuesSourceType.KEYWORD);
            ValuesSource.Bytes valuesSource = (ValuesSource.Bytes) config.getValuesSource();

            LeafReaderContext ctx = context.searcher().getIndexReader().leaves().get(0);
            SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            assertEquals(new BytesRef("value"), values.nextValue());
            assertTrue(config.alignesWithSearchIndex());
        });
    }
}
