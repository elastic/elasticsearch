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
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.mockito.Mockito;

import java.util.List;

public class ValuesSourceConfigTests extends MapperServiceTestCase {

    @Override
    @SuppressWarnings("unchecked")
    protected <T> T compileScript(Script script, ScriptContext<T> context) {
        AggregationScript.Factory mockFactory = Mockito.mock(AggregationScript.Factory.class);
        Mockito.when(mockFactory.newFactory(Mockito.any(), Mockito.any())).thenReturn(Mockito.mock(AggregationScript.LeafFactory.class));
        return (T) mockFactory;
    }

    /**
     * Attempting to resolve a config with neither a field nor a script specified throws an error
     */
    public void testNoFieldNoScript() {
        expectThrows(
            IllegalStateException.class,
            () -> ValuesSourceConfig.resolve(null, null, null, null, null, null, null, CoreValuesSourceType.KEYWORD)
        );
    }

    /**
     * When there's an unmapped field with no script, we should use the user value type hint if available, and fall back to the default
     * value source type if it's not available.
     */
    public void testUnmappedFieldNoScript() throws Exception {
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "long")));
        // No value type hint
        withAggregationContext(mapperService, List.of(source(b -> b.field("field", 42))), context -> {
            ValuesSourceConfig config;
            config = ValuesSourceConfig.resolve(context, null, "UnmappedField", null, null, null, null, CoreValuesSourceType.KEYWORD);
            assertEquals(CoreValuesSourceType.KEYWORD, config.valueSourceType());
        });

        // With value type hint
        withAggregationContext(mapperService, List.of(source(b -> b.field("field", 42))), context -> {
            ValuesSourceConfig config;
            config = ValuesSourceConfig.resolve(
                context,
                ValueType.IP,
                "UnmappedField",
                null,
                null,
                null,
                null,
                CoreValuesSourceType.KEYWORD
            );
            assertEquals(CoreValuesSourceType.IP, config.valueSourceType());
        });
    }

    /**
     * When the field is mapped and there's no script and no hint, use the field type
     */
    public void testMappedFieldNoScriptNoHint() throws Exception {
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "long")));
        withAggregationContext(mapperService, List.of(source(b -> b.field("field", 42))), context -> {
            ValuesSourceConfig config;
            config = ValuesSourceConfig.resolve(context, null, "field", null, null, null, null, CoreValuesSourceType.KEYWORD);
            assertEquals(CoreValuesSourceType.NUMERIC, config.valueSourceType());
        });
    }

    /**
     * When we have a mapped field and a hint, but no script, we should throw if the hint doesn't match the field,
     * and use the type of both if they do match.  Note that when there is a script, we just use the user value type
     * regardless of the field type.  This is to allow for scripts that adapt types, even though runtime fields are
     * a better solution for that in every way.
     */
    public void testMappedFieldNoScriptWithHint() throws Exception {
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "long")));
        // not matching case
        expectThrows(
            IllegalArgumentException.class,
            () -> withAggregationContext(
                mapperService,
                List.of(source(b -> b.field("field", 42))),
                context -> ValuesSourceConfig.resolve(context, ValueType.IP, "field", null, null, null, null, CoreValuesSourceType.KEYWORD)
            )
        );

        // matching case
        withAggregationContext(mapperService, List.of(source(b -> b.field("field", 42))), context -> {
            ValuesSourceConfig config;
            config = ValuesSourceConfig.resolve(context, ValueType.NUMBER, "field", null, null, null, null, CoreValuesSourceType.KEYWORD);
            assertEquals(CoreValuesSourceType.NUMERIC, config.valueSourceType());
        });
    }

    /**
     * When there's a script and the user tells us what type it produces, always use that type, regardless of if there's also a field
     */
    public void testScriptWithHint() throws Exception {
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "long")));
        // With field
        withAggregationContext(mapperService, List.of(source(b -> b.field("field", 42))), context -> {
            ValuesSourceConfig config;
            config = ValuesSourceConfig.resolve(
                context,
                ValueType.IP,
                "field",
                mockScript("mockscript"),
                null,
                null,
                null,
                CoreValuesSourceType.KEYWORD
            );
            assertEquals(CoreValuesSourceType.IP, config.valueSourceType());
        }, () -> null);

        // With unmapped field
        withAggregationContext(mapperService, List.of(source(b -> b.field("field", 42))), context -> {
            ValuesSourceConfig config;
            config = ValuesSourceConfig.resolve(
                context,
                ValueType.IP,
                "unmappedField",
                mockScript("mockscript"),
                null,
                null,
                null,
                CoreValuesSourceType.KEYWORD
            );
            assertEquals(CoreValuesSourceType.IP, config.valueSourceType());
        }, () -> null);

        // Without field
        withAggregationContext(mapperService, List.of(source(b -> b.field("field", 42))), context -> {
            ValuesSourceConfig config;
            config = ValuesSourceConfig.resolve(
                context,
                ValueType.IP,
                null,
                mockScript("mockscript"),
                null,
                null,
                null,
                CoreValuesSourceType.KEYWORD
            );
            assertEquals(CoreValuesSourceType.IP, config.valueSourceType());
        }, () -> null);
    }

    /**
     * If there's a script and the user didn't tell us what type it produces, use the field if possible, otherwise the default
     */
    public void testScriptNoHint() throws Exception {
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "long")));
        // With field
        withAggregationContext(mapperService, List.of(source(b -> b.field("field", 42))), context -> {
            ValuesSourceConfig config;
            config = ValuesSourceConfig.resolve(
                context,
                null,
                "field",
                mockScript("mockscript"),
                null,
                null,
                null,
                CoreValuesSourceType.KEYWORD
            );
            assertEquals(CoreValuesSourceType.NUMERIC, config.valueSourceType());
        }, () -> null);

        // With unmapped field
        withAggregationContext(mapperService, List.of(source(b -> b.field("field", 42))), context -> {
            ValuesSourceConfig config;
            config = ValuesSourceConfig.resolve(
                context,
                null,
                "unmappedField",
                mockScript("mockscript"),
                null,
                null,
                null,
                CoreValuesSourceType.KEYWORD
            );
            assertEquals(CoreValuesSourceType.KEYWORD, config.valueSourceType());
        }, () -> null);

        // Without field
        withAggregationContext(mapperService, List.of(source(b -> b.field("field", 42))), context -> {
            ValuesSourceConfig config;
            config = ValuesSourceConfig.resolve(
                context,
                null,
                null,
                mockScript("mockscript"),
                null,
                null,
                null,
                CoreValuesSourceType.KEYWORD
            );
            assertEquals(CoreValuesSourceType.KEYWORD, config.valueSourceType());
        }, () -> null);
    }

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
