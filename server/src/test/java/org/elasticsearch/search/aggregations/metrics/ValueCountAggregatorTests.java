/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.mapper.BooleanFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.IpFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.RangeFieldMapper;
import org.elasticsearch.index.mapper.RangeType;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Collections.singleton;

public class ValueCountAggregatorTests extends AggregatorTestCase {

    private static final String FIELD_NAME = "field";

    private static final String STRING_VALUE_SCRIPT = "string_value";
    private static final String NUMBER_VALUE_SCRIPT = "number_value";
    private static final String SINGLE_SCRIPT = "single";

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new ValueCountAggregationBuilder("foo").field(fieldName);
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return Arrays.asList(
            CoreValuesSourceType.NUMERIC,
            CoreValuesSourceType.DATE,
            CoreValuesSourceType.BOOLEAN,
            CoreValuesSourceType.KEYWORD,
            CoreValuesSourceType.IP,
            CoreValuesSourceType.GEOPOINT,
            CoreValuesSourceType.RANGE,
            CoreValuesSourceType.BOOLEAN,
            CoreValuesSourceType.DATE,
            CoreValuesSourceType.IP
        );
    }

    @Override
    protected ScriptService getMockScriptService() {
        Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

        scripts.put(STRING_VALUE_SCRIPT, vars -> (Double.valueOf((String) vars.get("_value")) + 1));
        scripts.put(NUMBER_VALUE_SCRIPT, vars -> (((Number) vars.get("_value")).doubleValue() + 1));
        scripts.put(SINGLE_SCRIPT, vars -> 1);

        MockScriptEngine scriptEngine = new MockScriptEngine(MockScriptEngine.NAME, scripts, Collections.emptyMap());
        Map<String, ScriptEngine> engines = Collections.singletonMap(scriptEngine.getType(), scriptEngine);

        return new ScriptService(Settings.EMPTY, engines, ScriptModule.CORE_CONTEXTS);
    }

    public void testGeoField() throws IOException {
        testAggregation(new MatchAllDocsQuery(), ValueType.GEOPOINT, iw -> {
            for (int i = 0; i < 10; i++) {
                Document document = new Document();
                document.add(new LatLonDocValuesField("field", 10, 10));
                iw.addDocument(document);
            }
        }, count -> assertEquals(10L, count.getValue()));
    }

    public void testDoubleField() throws IOException {
        testAggregation(new MatchAllDocsQuery(), ValueType.DOUBLE, iw -> {
            for (int i = 0; i < 15; i++) {
                Document document = new Document();
                document.add(new DoubleDocValuesField(FIELD_NAME, 23D));
                iw.addDocument(document);
            }
        }, count -> assertEquals(15L, count.getValue()));
    }

    public void testKeyWordField() throws IOException {
        testAggregation(new MatchAllDocsQuery(), ValueType.STRING, iw -> {
            for (int i = 0; i < 20; i++) {
                Document document = new Document();
                document.add(new SortedSetDocValuesField(FIELD_NAME, new BytesRef("stringValue")));
                document.add(new SortedSetDocValuesField(FIELD_NAME, new BytesRef("string11Value")));
                iw.addDocument(document);
            }
        }, count -> assertEquals(40L, count.getValue()));
    }

    public void testNoDocs() throws IOException {
        for (ValueType valueType : ValueType.values()) {
            testAggregation(new MatchAllDocsQuery(), valueType, iw -> {
                // Intentionally not writing any docs
            }, count -> {
                assertEquals(0L, count.getValue());
                assertFalse(AggregationInspectionHelper.hasValue(count));
            });
        }
    }

    public void testNoMatchingField() throws IOException {
        testAggregation(new MatchAllDocsQuery(), ValueType.LONG, iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 7)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 1)));
        }, count -> {
            assertEquals(0L, count.getValue());
            assertFalse(AggregationInspectionHelper.hasValue(count));
        });
    }

    public void testSomeMatchesSortedNumericDocValues() throws IOException {
        testAggregation(new DocValuesFieldExistsQuery(FIELD_NAME), ValueType.NUMERIC, iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 7)));
            iw.addDocument(singleton(new SortedNumericDocValuesField(FIELD_NAME, 7)));
            iw.addDocument(singleton(new SortedNumericDocValuesField(FIELD_NAME, 1)));
        }, count -> {
            assertEquals(2L, count.getValue());
            assertTrue(AggregationInspectionHelper.hasValue(count));
        });
    }

    public void testSomeMatchesNumericDocValues() throws IOException {
        testAggregation(new DocValuesFieldExistsQuery(FIELD_NAME), ValueType.NUMBER, iw -> {
            iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 7)));
            iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 1)));
        }, count -> {
            assertEquals(2L, count.getValue());
            assertTrue(AggregationInspectionHelper.hasValue(count));
        });
    }

    public void testQueryFiltering() throws IOException {
        testAggregation(IntPoint.newRangeQuery("level", 0, 5), ValueType.STRING, iw -> {
            iw.addDocument(Arrays.asList(new IntPoint("level", 0), new SortedDocValuesField(FIELD_NAME, new BytesRef("foo"))));
            iw.addDocument(Arrays.asList(new IntPoint("level", 1), new SortedDocValuesField(FIELD_NAME, new BytesRef("bar"))));
            iw.addDocument(Arrays.asList(new IntPoint("level", 3), new SortedDocValuesField(FIELD_NAME, new BytesRef("foo"))));
            iw.addDocument(Arrays.asList(new IntPoint("level", 5), new SortedDocValuesField(FIELD_NAME, new BytesRef("baz"))));
            iw.addDocument(Arrays.asList(new IntPoint("level", 7), new SortedDocValuesField(FIELD_NAME, new BytesRef("baz"))));
        }, count -> {
            assertEquals(4L, count.getValue());
            assertTrue(AggregationInspectionHelper.hasValue(count));
        });
    }

    public void testQueryFiltersAll() throws IOException {
        testAggregation(IntPoint.newRangeQuery("level", -1, 0), ValueType.STRING, iw -> {
            iw.addDocument(Arrays.asList(new IntPoint("level", 3), new SortedDocValuesField(FIELD_NAME, new BytesRef("foo"))));
            iw.addDocument(Arrays.asList(new IntPoint("level", 5), new SortedDocValuesField(FIELD_NAME, new BytesRef("baz"))));
        }, count -> {
            assertEquals(0L, count.getValue());
            assertFalse(AggregationInspectionHelper.hasValue(count));
        });
    }

    public void testUnmappedMissingString() throws IOException {
        ValueCountAggregationBuilder aggregationBuilder = new ValueCountAggregationBuilder("name").field("number").missing("🍌🍌🍌");

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("unrelatedField", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("unrelatedField", 8)));
            iw.addDocument(singleton(new NumericDocValuesField("unrelatedField", 9)));
        }, valueCount -> {
            assertEquals(3, valueCount.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(valueCount));
        });
    }

    public void testUnmappedMissingNumber() throws IOException {
        ValueCountAggregationBuilder aggregationBuilder = new ValueCountAggregationBuilder("name").field("number").missing(1234);

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("unrelatedField", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("unrelatedField", 8)));
            iw.addDocument(singleton(new NumericDocValuesField("unrelatedField", 9)));
        }, valueCount -> {
            assertEquals(3, valueCount.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(valueCount));
        });
    }

    public void testUnmappedMissingGeoPoint() throws IOException {
        ValueCountAggregationBuilder aggregationBuilder = new ValueCountAggregationBuilder("name").field("number")
            .missing(new GeoPoint(42.39561, -71.13051));

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("unrelatedField", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("unrelatedField", 8)));
            iw.addDocument(singleton(new NumericDocValuesField("unrelatedField", 9)));
        }, valueCount -> {
            assertEquals(3, valueCount.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(valueCount));
        });
    }

    public void testRangeFieldValues() throws IOException {
        RangeType rangeType = RangeType.DOUBLE;
        final RangeFieldMapper.Range range1 = new RangeFieldMapper.Range(rangeType, 1.0D, 5.0D, true, true);
        final RangeFieldMapper.Range range2 = new RangeFieldMapper.Range(rangeType, 6.0D, 10.0D, true, true);
        final String fieldName = "rangeField";
        MappedFieldType fieldType = new RangeFieldMapper.RangeFieldType(fieldName, rangeType);
        final ValueCountAggregationBuilder aggregationBuilder = new ValueCountAggregationBuilder("_name").field(fieldName);
        Set<RangeFieldMapper.Range> multiRecord = new HashSet<>(2);
        multiRecord.add(range1);
        multiRecord.add(range2);
        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new BinaryDocValuesField(fieldName, rangeType.encodeRanges(singleton(range1)))));
            iw.addDocument(singleton(new BinaryDocValuesField(fieldName, rangeType.encodeRanges(singleton(range1)))));
            iw.addDocument(singleton(new BinaryDocValuesField(fieldName, rangeType.encodeRanges(singleton(range2)))));
            iw.addDocument(singleton(new BinaryDocValuesField(fieldName, rangeType.encodeRanges(multiRecord))));
        }, count -> {
            assertEquals(4.0, count.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(count));
        }, fieldType);
    }

    public void testValueScriptNumber() throws IOException {
        ValueCountAggregationBuilder aggregationBuilder = new ValueCountAggregationBuilder("name").field(FIELD_NAME)
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, NUMBER_VALUE_SCRIPT, Collections.emptyMap()));

        MappedFieldType fieldType = createMappedFieldType(FIELD_NAME, ValueType.NUMERIC);

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 7)));
            iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 8)));
            iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 9)));
        }, valueCount -> {
            assertEquals(3, valueCount.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(valueCount));
        }, fieldType);
    }

    public void testSingleScriptNumber() throws IOException {
        ValueCountAggregationBuilder aggregationBuilder = new ValueCountAggregationBuilder("name").script(
            new Script(ScriptType.INLINE, MockScriptEngine.NAME, SINGLE_SCRIPT, Collections.emptyMap())
        );

        MappedFieldType fieldType = createMappedFieldType(FIELD_NAME, ValueType.NUMERIC);

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            Document doc = new Document();
            doc.add(new SortedNumericDocValuesField(FIELD_NAME, 7));
            doc.add(new SortedNumericDocValuesField(FIELD_NAME, 7));
            iw.addDocument(doc);

            doc = new Document();
            doc.add(new SortedNumericDocValuesField(FIELD_NAME, 8));
            doc.add(new SortedNumericDocValuesField(FIELD_NAME, 8));
            iw.addDocument(doc);

            doc = new Document();
            doc.add(new SortedNumericDocValuesField(FIELD_NAME, 1));
            doc.add(new SortedNumericDocValuesField(FIELD_NAME, 1));
            iw.addDocument(doc);
        }, valueCount -> {
            // Note: The field values won't be taken into account. The script will only be called
            // once per document, and only expect a count of 3
            assertEquals(3, valueCount.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(valueCount));
        }, fieldType);
    }

    public void testValueScriptString() throws IOException {
        ValueCountAggregationBuilder aggregationBuilder = new ValueCountAggregationBuilder("name").field(FIELD_NAME)
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, STRING_VALUE_SCRIPT, Collections.emptyMap()));

        MappedFieldType fieldType = createMappedFieldType(FIELD_NAME, ValueType.STRING);

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedDocValuesField(FIELD_NAME, new BytesRef("1"))));
            iw.addDocument(singleton(new SortedDocValuesField(FIELD_NAME, new BytesRef("2"))));
            iw.addDocument(singleton(new SortedDocValuesField(FIELD_NAME, new BytesRef("3"))));
        }, valueCount -> {
            assertEquals(3, valueCount.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(valueCount));
        }, fieldType);
    }

    public void testSingleScriptString() throws IOException {
        ValueCountAggregationBuilder aggregationBuilder = new ValueCountAggregationBuilder("name").script(
            new Script(ScriptType.INLINE, MockScriptEngine.NAME, SINGLE_SCRIPT, Collections.emptyMap())
        );

        MappedFieldType fieldType = createMappedFieldType(FIELD_NAME, ValueType.STRING);

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            Document doc = new Document();
            // Note: unlike numerics, lucene de-dupes strings so we increment here
            doc.add(new SortedSetDocValuesField(FIELD_NAME, new BytesRef("1")));
            doc.add(new SortedSetDocValuesField(FIELD_NAME, new BytesRef("2")));
            iw.addDocument(doc);

            doc = new Document();
            doc.add(new SortedSetDocValuesField(FIELD_NAME, new BytesRef("3")));
            doc.add(new SortedSetDocValuesField(FIELD_NAME, new BytesRef("4")));
            iw.addDocument(doc);

            doc = new Document();
            doc.add(new SortedSetDocValuesField(FIELD_NAME, new BytesRef("5")));
            doc.add(new SortedSetDocValuesField(FIELD_NAME, new BytesRef("6")));
            iw.addDocument(doc);
        }, valueCount -> {
            // Note: The field values won't be taken into account. The script will only be called
            // once per document, and only expect a count of 3
            assertEquals(3, valueCount.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(valueCount));
        }, fieldType);
    }

    private void testAggregation(
        Query query,
        ValueType valueType,
        CheckedConsumer<RandomIndexWriter, IOException> indexer,
        Consumer<InternalValueCount> verify
    ) throws IOException {
        // Test both with and without the userValueTypeHint
        testAggregation(query, valueType, indexer, verify, true);
        testAggregation(query, valueType, indexer, verify, false);
    }

    private void testAggregation(
        Query query,
        ValueType valueType,
        CheckedConsumer<RandomIndexWriter, IOException> indexer,
        Consumer<InternalValueCount> verify,
        boolean testWithHint
    ) throws IOException {
        MappedFieldType fieldType = createMappedFieldType(FIELD_NAME, valueType);

        ValueCountAggregationBuilder aggregationBuilder = new ValueCountAggregationBuilder("_name");
        if (valueType != null && testWithHint) {
            aggregationBuilder.userValueTypeHint(valueType);
        }
        aggregationBuilder.field(FIELD_NAME);

        testAggregation(aggregationBuilder, query, indexer, verify, fieldType);
    }

    private void testAggregation(
        AggregationBuilder aggregationBuilder,
        Query query,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<InternalValueCount> verify,
        MappedFieldType... fieldTypes
    ) throws IOException {
        testCase(aggregationBuilder, query, buildIndex, verify, fieldTypes);
    }

    private static MappedFieldType createMappedFieldType(String name, ValueType valueType) {
        switch (valueType) {
            case BOOLEAN:
                return new BooleanFieldMapper.BooleanFieldType(name);
            case STRING:
                return new KeywordFieldMapper.KeywordFieldType(name);
            case DOUBLE:
                return new NumberFieldMapper.NumberFieldType(name, NumberFieldMapper.NumberType.DOUBLE);
            case NUMBER:
            case NUMERIC:
            case LONG:
                return new NumberFieldMapper.NumberFieldType(name, NumberFieldMapper.NumberType.LONG);
            case DATE:
                return new DateFieldMapper.DateFieldType(name);
            case IP:
                return new IpFieldMapper.IpFieldType(name);
            case GEOPOINT:
                return new GeoPointFieldMapper.GeoPointFieldType(name);
            case RANGE:
                return new RangeFieldMapper.RangeFieldType(name, RangeType.DOUBLE);
            default:
                throw new IllegalArgumentException("Test does not support value type [" + valueType + "]");
        }
    }
}
