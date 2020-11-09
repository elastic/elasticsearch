/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket.missing;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
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
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.lookup.LeafDocLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.common.lucene.search.Queries.newMatchAllQuery;

public class MissingAggregatorTests extends AggregatorTestCase {

    private static final String VALUE_SCRIPT_PARAMS = "value_script_params";
    private static final String VALUE_SCRIPT = "value_script";
    private static final String FIELD_SCRIPT_PARAMS = "field_script_params";
    private static final String FIELD_SCRIPT = "field_script";
    private static final long DEFAULT_INC_PARAM = 1;
    private static final long DEFAULT_THRESHOLD_PARAM = 50;

    public void testMatchNoDocs() throws IOException {
        final int numDocs = randomIntBetween(10, 200);

        final MappedFieldType fieldType
            = new NumberFieldMapper.NumberFieldType("field", NumberType.LONG);

        final MissingAggregationBuilder builder = new MissingAggregationBuilder("_name")
            .field(fieldType.name());

        testCase(
            newMatchAllQuery(),
            builder,
            writer -> {
                for (int i = 0; i < numDocs; i++) {
                    writer.addDocument(singleton(new SortedNumericDocValuesField(fieldType.name(), randomLong())));
                }
            },
            internalMissing -> {
                assertEquals(0, internalMissing.getDocCount());
                assertFalse(AggregationInspectionHelper.hasValue(internalMissing));
            },
            singleton(fieldType)
        );
    }

    public void testMatchAllDocs() throws IOException {
        int numDocs = randomIntBetween(10, 200);

        final MappedFieldType aggFieldType = new NumberFieldMapper.NumberFieldType("agg_field", NumberType.LONG);
        final MappedFieldType anotherFieldType = new NumberFieldMapper.NumberFieldType("another_field", NumberType.LONG);

        final MissingAggregationBuilder builder = new MissingAggregationBuilder("_name")
            .field(aggFieldType.name());

        testCase(
            newMatchAllQuery(),
            builder,
            writer -> {
                for (int i = 0; i < numDocs; i++) {
                    writer.addDocument(singleton(new SortedNumericDocValuesField(anotherFieldType.name(), randomLong())));
                }
            },
            internalMissing -> {
                assertEquals(numDocs, internalMissing.getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(internalMissing));
            },
            List.of(aggFieldType, anotherFieldType)
        );
    }

    public void testMatchSparse() throws IOException {
        final MappedFieldType aggFieldType = new NumberFieldMapper.NumberFieldType("agg_field", NumberType.LONG);
        final MappedFieldType anotherFieldType = new NumberFieldMapper.NumberFieldType("another_field", NumberType.LONG);

        final MissingAggregationBuilder builder = new MissingAggregationBuilder("_name")
            .field(aggFieldType.name());

        final int numDocs = randomIntBetween(100, 200);
        int docsMissingAggField = 0;
        final List<Set<IndexableField>> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            if (randomBoolean()) {
                docs.add(singleton(new SortedNumericDocValuesField(aggFieldType.name(), randomLong())));
            } else {
                docs.add(singleton(new SortedNumericDocValuesField(anotherFieldType.name(), randomLong())));
                docsMissingAggField++;
            }
        }
        final int finalDocsMissingAggField = docsMissingAggField;

        testCase(
            newMatchAllQuery(),
            builder,
            writer -> writer.addDocuments(docs),
            internalMissing -> {
                assertEquals(finalDocsMissingAggField, internalMissing.getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(internalMissing));
            },
            List.of(aggFieldType, anotherFieldType)
        );
    }

    public void testMatchSparseRangeField() throws IOException {
        final RangeType rangeType = RangeType.DOUBLE;
        final MappedFieldType aggFieldType = new RangeFieldMapper.RangeFieldType("agg_field", rangeType);
        final MappedFieldType anotherFieldType = new RangeFieldMapper.RangeFieldType("another_field", rangeType);

        final RangeFieldMapper.Range range = new RangeFieldMapper.Range(rangeType, 1.0D, 5.0D, true, true);
        final BytesRef encodedRange = rangeType.encodeRanges(singleton(range));
        final BinaryDocValuesField encodedRangeField = new BinaryDocValuesField(aggFieldType.name(), encodedRange);

        final MissingAggregationBuilder builder = new MissingAggregationBuilder("_name")
            .field(aggFieldType.name());

        final int numDocs = randomIntBetween(100, 200);
        int docsMissingAggField = 0;
        final List<Set<IndexableField>> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            if (randomBoolean()) {
                docs.add(singleton(encodedRangeField));
            } else {
                docs.add(singleton(new SortedNumericDocValuesField(anotherFieldType.name(), randomLong())));
                docsMissingAggField++;
            }
        }
        final int finalDocsMissingAggField = docsMissingAggField;

        testCase(
            newMatchAllQuery(),
            builder,
            writer -> writer.addDocuments(docs),
            internalMissing -> {
                assertEquals(finalDocsMissingAggField, internalMissing.getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(internalMissing));
            },
            List.of(aggFieldType, anotherFieldType)
        );
    }

    public void testUnmappedWithoutMissingParam() throws IOException {
        final int numDocs = randomIntBetween(10, 20);
        final MappedFieldType aggFieldType = new NumberFieldMapper.NumberFieldType("agg_field", NumberType.LONG);

        final MissingAggregationBuilder builder = new MissingAggregationBuilder("_name")
            .field("unknown_field");

        testCase(
            newMatchAllQuery(),
            builder,
            writer -> {
                for (int i = 0; i < numDocs; i++) {
                    writer.addDocument(singleton(new SortedNumericDocValuesField(aggFieldType.name(), randomLong())));
                }
            },
            internalMissing -> {
                assertEquals(numDocs, internalMissing.getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(internalMissing));
            },
            singleton(aggFieldType)
        );
    }

    public void testUnmappedWithMissingParam() throws IOException {
        final int numDocs = randomIntBetween(10, 20);
        final MappedFieldType aggFieldType = new NumberFieldMapper.NumberFieldType("agg_field", NumberType.LONG);

        final MissingAggregationBuilder builder = new MissingAggregationBuilder("_name")
            .field("unknown_field")
            .missing(randomLong());

        testCase(
            newMatchAllQuery(),
            builder,
            writer -> {
                for (int i = 0; i < numDocs; i++) {
                    writer.addDocument(singleton(new SortedNumericDocValuesField(aggFieldType.name(), randomLong())));
                }
            },
            internalMissing -> {
                assertEquals(0, internalMissing.getDocCount());
                assertFalse(AggregationInspectionHelper.hasValue(internalMissing));
            },
            singleton(aggFieldType)
        );
    }

    public void testMissingParam() throws IOException {
        final int numDocs = randomIntBetween(10, 20);

        final MappedFieldType aggFieldType = new NumberFieldMapper.NumberFieldType("agg_field", NumberType.LONG);
        final MappedFieldType anotherFieldType = new NumberFieldMapper.NumberFieldType("another_field", NumberType.LONG);

        final MissingAggregationBuilder builder = new MissingAggregationBuilder("_name")
            .field(aggFieldType.name())
            .missing(randomLong());

        testCase(
            newMatchAllQuery(),
            builder,
            writer -> {
                for (int i = 0; i < numDocs; i++) {
                    writer.addDocument(singleton(new SortedNumericDocValuesField(anotherFieldType.name(), randomLong())));
                }
            },
            internalMissing -> {
                assertEquals(0, internalMissing.getDocCount());
                assertFalse(AggregationInspectionHelper.hasValue(internalMissing));
            },
            List.of(aggFieldType, anotherFieldType)
        );
    }

    public void testMultiValuedField() throws IOException {
        final MappedFieldType aggFieldType = new NumberFieldMapper.NumberFieldType("agg_field", NumberType.LONG);
        final MappedFieldType anotherFieldType = new NumberFieldMapper.NumberFieldType("another_field", NumberType.LONG);

        final MissingAggregationBuilder builder = new MissingAggregationBuilder("_name")
            .field(aggFieldType.name());

        final int numDocs = randomIntBetween(100, 200);
        int docsMissingAggField = 0;
        final List<Set<IndexableField>> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            if (randomBoolean()) {
                final long randomLong = randomLong();
                docs.add(Set.of(
                    new SortedNumericDocValuesField(aggFieldType.name(), randomLong),
                    new SortedNumericDocValuesField(aggFieldType.name(), randomLong + 1)
                ));
            } else {
                docs.add(singleton(new SortedNumericDocValuesField(anotherFieldType.name(), randomLong())));
                docsMissingAggField++;
            }
        }
        final int finalDocsMissingAggField = docsMissingAggField;

        testCase(
            newMatchAllQuery(),
            builder,
            writer -> writer.addDocuments(docs),
            internalMissing -> {
                assertEquals(finalDocsMissingAggField, internalMissing.getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(internalMissing));
            },
            List.of(aggFieldType, anotherFieldType)
        );
    }

    public void testSingleValuedFieldWithValueScript() throws IOException {
        valueScriptTestCase(new Script(ScriptType.INLINE, MockScriptEngine.NAME, VALUE_SCRIPT, emptyMap()));
    }

    public void testSingleValuedFieldWithValueScriptWithParams() throws IOException {
        valueScriptTestCase(new Script(ScriptType.INLINE, MockScriptEngine.NAME, VALUE_SCRIPT_PARAMS, singletonMap("inc", 10)));
    }

    private void valueScriptTestCase(Script script) throws IOException {
        final MappedFieldType aggFieldType = new NumberFieldMapper.NumberFieldType("agg_field", NumberType.LONG);
        final MappedFieldType anotherFieldType = new NumberFieldMapper.NumberFieldType("another_field", NumberType.LONG);

        final MissingAggregationBuilder builder = new MissingAggregationBuilder("_name")
            .field(aggFieldType.name())
            .script(script);

        final int numDocs = randomIntBetween(100, 200);
        int docsMissingAggField = 0;
        final List<Set<IndexableField>> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            if (randomBoolean()) {
                docs.add(singleton(new SortedNumericDocValuesField(aggFieldType.name(), randomLong())));
            } else {
                docs.add(singleton(new SortedNumericDocValuesField(anotherFieldType.name(), randomLong())));
                docsMissingAggField++;
            }
        }
        final int finalDocsMissingField = docsMissingAggField;

        testCase(
            newMatchAllQuery(),
            builder,
            writer -> writer.addDocuments(docs),
            internalMissing -> {
                assertEquals(finalDocsMissingField, internalMissing.getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(internalMissing));
            },
            List.of(aggFieldType, anotherFieldType)
        );
    }

    public void testMultiValuedFieldWithFieldScriptWithParams() throws IOException {
        final long threshold = 10;
        final Map<String, Object> params = Map.of("field", "agg_field", "threshold", threshold);
        fieldScriptTestCase(new Script(ScriptType.INLINE, MockScriptEngine.NAME, FIELD_SCRIPT_PARAMS, params), threshold);
    }

    public void testMultiValuedFieldWithFieldScript() throws IOException {
        fieldScriptTestCase(new Script(ScriptType.INLINE, MockScriptEngine.NAME, FIELD_SCRIPT, singletonMap("field", "agg_field")),
            DEFAULT_THRESHOLD_PARAM);
    }

    private void fieldScriptTestCase(Script script, long threshold) throws IOException {
        final MappedFieldType aggFieldType = new NumberFieldMapper.NumberFieldType("agg_field", NumberType.LONG);

        final MissingAggregationBuilder builder = new MissingAggregationBuilder("_name")
            .script(script);

        final int numDocs = randomIntBetween(100, 200);
        int docsBelowThreshold = 0;
        final List<Set<IndexableField>> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            final long firstValue = randomLongBetween(0, 100);
            final long secondValue = firstValue + 1;
            if (firstValue < threshold && secondValue < threshold) {
                docsBelowThreshold++;
            }
            docs.add(Set.of(
                new SortedNumericDocValuesField(aggFieldType.name(), firstValue),
                new SortedNumericDocValuesField(aggFieldType.name(), secondValue)
            ));
        }
        final int finalDocsBelowThreshold = docsBelowThreshold;

        testCase(
            newMatchAllQuery(),
            builder,
            writer -> writer.addDocuments(docs),
            internalMissing -> {
                assertEquals(finalDocsBelowThreshold, internalMissing.getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(internalMissing));
            },
            singleton(aggFieldType)
        );
    }

    private void testCase(Query query,
                          MissingAggregationBuilder builder,
                          CheckedConsumer<RandomIndexWriter, IOException> writeIndex,
                          Consumer<InternalMissing> verify,
                          Collection<MappedFieldType> fieldTypes) throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                writeIndex.accept(indexWriter);
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                final IndexSearcher indexSearcher = newSearcher(indexReader, true, true);
                final MappedFieldType[] fieldTypesArray = fieldTypes.toArray(new MappedFieldType[0]);
                final InternalMissing missing = searchAndReduce(indexSearcher, query, builder, fieldTypesArray);
                verify.accept(missing);
            }
        }
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new MissingAggregationBuilder("_name")
            .field(fieldName);
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return List.of(
            CoreValuesSourceType.NUMERIC,
            CoreValuesSourceType.BYTES,
            CoreValuesSourceType.GEOPOINT,
            CoreValuesSourceType.RANGE,
            CoreValuesSourceType.IP,
            CoreValuesSourceType.BOOLEAN,
            CoreValuesSourceType.DATE
        );
    }

    @Override
    protected ScriptService getMockScriptService() {
        final Map<String, Function<Map<String, Object>, Object>> deterministicScripts = new HashMap<>();
        deterministicScripts.put(VALUE_SCRIPT_PARAMS, vars -> {
            final double value = ((Number) vars.get("_value")).doubleValue();
            final long inc = ((Number) vars.get("inc")).longValue();
            return value + inc;
        });
        deterministicScripts.put(VALUE_SCRIPT, vars -> {
            final double value = ((Number) vars.get("_value")).doubleValue();
            return value + DEFAULT_INC_PARAM;
        });
        deterministicScripts.put(FIELD_SCRIPT_PARAMS, vars -> {
            final String fieldName = (String) vars.get("field");
            final long threshold = ((Number) vars.get("threshold")).longValue();
            return threshold(fieldName, threshold, vars);
        });
        deterministicScripts.put(FIELD_SCRIPT, vars -> {
            final String fieldName = (String) vars.get("field");
            return threshold(fieldName, DEFAULT_THRESHOLD_PARAM, vars);
        });
        final MockScriptEngine scriptEngine = new MockScriptEngine(MockScriptEngine.NAME, deterministicScripts, emptyMap(), emptyMap());
        final Map<String, ScriptEngine> engines = singletonMap(scriptEngine.getType(), scriptEngine);
        return new ScriptService(Settings.EMPTY, engines, ScriptModule.CORE_CONTEXTS);
    }

    private static List<Long> threshold(String fieldName, long threshold, Map<String, Object> vars) {
        final LeafDocLookup lookup = (LeafDocLookup) vars.get("doc");
        return lookup.get(fieldName).stream()
            .map(value -> ((Number) value).longValue())
            .filter(value -> value >= threshold)
            .collect(toList());
    }
}
