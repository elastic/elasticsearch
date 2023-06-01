/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.fielddata.ScriptDocValues;
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
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class CardinalityAggregatorTests extends AggregatorTestCase {

    /** Script to extract the value from any field **/
    public static final String VALUE_SCRIPT = "_value";

    /** Script to extract the single string value of the 'str_value' field **/
    public static final String STRING_VALUE_SCRIPT = "doc['str_value'].value";

    /** Script to extract a collection of string values from the 'str_values' field **/
    public static final String STRING_VALUES_SCRIPT = "doc['str_values']";

    /** Script to extract a single numeric value from the 'number' field **/
    public static final String NUMERIC_VALUE_SCRIPT = "doc['number'].value";

    /** Script to extract a collection of numeric values from the 'numbers' field **/
    public static final String NUMERIC_VALUES_SCRIPT = "doc['numbers']";

    public static final int HASHER_DEFAULT_SEED = 17;

    @Override
    protected ScriptService getMockScriptService() {
        final Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

        scripts.put(VALUE_SCRIPT, vars -> vars.get("_value"));

        scripts.put(STRING_VALUE_SCRIPT, vars -> {
            final Map<?, ?> doc = (Map<?, ?>) vars.get("doc");
            return doc.get("str_value");
        });

        scripts.put(STRING_VALUES_SCRIPT, vars -> {
            final Map<?, ?> doc = (Map<?, ?>) vars.get("doc");
            final ScriptDocValues.Strings strValues = (ScriptDocValues.Strings) doc.get("str_values");
            return strValues;
        });

        scripts.put(NUMERIC_VALUE_SCRIPT, vars -> {
            final Map<?, ?> doc = (Map<?, ?>) vars.get("doc");
            return doc.get("number");
        });

        scripts.put(NUMERIC_VALUES_SCRIPT, vars -> {
            final Map<?, ?> doc = (Map<?, ?>) vars.get("doc");
            return (ScriptDocValues<?>) doc.get("numbers");
        });

        MockScriptEngine scriptEngine = new MockScriptEngine(
            MockScriptEngine.NAME,
            scripts,
            Collections.emptyMap(),
            Collections.emptyMap()
        );
        Map<String, ScriptEngine> engines = Collections.singletonMap(scriptEngine.getType(), scriptEngine);

        return new ScriptService(Settings.EMPTY, engines, ScriptModule.CORE_CONTEXTS, () -> 1L);
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return CoreValuesSourceType.ALL_CORE;
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new CardinalityAggregationBuilder("cardinality").field(fieldName);
    }

    public void testNoDocs() throws IOException {
        testAggregation(new MatchAllDocsQuery(), iw -> {
            // Intentionally not writing any docs
        }, card -> {
            assertEquals(0.0, card.getValue(), 0);
            assertFalse(AggregationInspectionHelper.hasValue(card));
        });
    }

    public void testRangeFieldValues() throws IOException {
        RangeType rangeType = RangeType.DOUBLE;
        final RangeFieldMapper.Range range1 = new RangeFieldMapper.Range(rangeType, 1.0D, 5.0D, true, true);
        final RangeFieldMapper.Range range2 = new RangeFieldMapper.Range(rangeType, 6.0D, 10.0D, true, true);
        final String fieldName = "rangeField";
        MappedFieldType fieldType = new RangeFieldMapper.RangeFieldType(fieldName, rangeType);
        final CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("_name").field(fieldName);
        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new BinaryDocValuesField(fieldName, rangeType.encodeRanges(singleton(range1)))));
            iw.addDocument(singleton(new BinaryDocValuesField(fieldName, rangeType.encodeRanges(singleton(range1)))));
            iw.addDocument(singleton(new BinaryDocValuesField(fieldName, rangeType.encodeRanges(singleton(range2)))));
            iw.addDocument(singleton(new BinaryDocValuesField(fieldName, rangeType.encodeRanges(Set.of(range1, range2)))));
        }, card -> {
            assertEquals(3.0, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        }, fieldType);
    }

    public void testNoMatchingField() throws IOException {
        testAggregation(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 7)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 1)));
        }, card -> {
            assertEquals(0.0, card.getValue(), 0);
            assertFalse(AggregationInspectionHelper.hasValue(card));
        });
    }

    public void testSomeMatchesSortedNumericDocValues() throws IOException {
        testAggregation(new FieldExistsQuery("number"), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 1)));
        }, card -> {
            assertEquals(2, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        });
    }

    public void testSomeMatchesNumericDocValues() throws IOException {
        testAggregation(new FieldExistsQuery("number"), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 1)));
        }, card -> {
            assertEquals(2, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        });
    }

    public void testQueryFiltering() throws IOException {
        testAggregation(IntPoint.newRangeQuery("number", 0, 5), iw -> {
            iw.addDocument(List.of(new IntPoint("number", 7), new SortedNumericDocValuesField("number", 7)));
            iw.addDocument(List.of(new IntPoint("number", 1), new SortedNumericDocValuesField("number", 1)));
        }, card -> {
            assertEquals(1, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        });
    }

    public void testQueryFiltersAll() throws IOException {
        testAggregation(IntPoint.newRangeQuery("number", -1, 0), iw -> {
            iw.addDocument(List.of(new IntPoint("number", 7), new SortedNumericDocValuesField("number", 7)));
            iw.addDocument(List.of(new IntPoint("number", 1), new SortedNumericDocValuesField("number", 1)));
        }, card -> {
            assertEquals(0.0, card.getValue(), 0);
            assertFalse(AggregationInspectionHelper.hasValue(card));
        });
    }

    public void testSingleValuedString() throws IOException {
        final CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("name").field("str_value");
        final MappedFieldType mappedFieldTypes = new KeywordFieldMapper.KeywordFieldType("str_value");

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedDocValuesField("str_value", new BytesRef("one"))));
            iw.addDocument(singleton(new SortedDocValuesField("unrelatedField", new BytesRef("two"))));
            iw.addDocument(singleton(new SortedDocValuesField("str_value", new BytesRef("three"))));
            iw.addDocument(singleton(new SortedDocValuesField("str_value", new BytesRef("one"))));
        }, card -> {
            assertEquals(2, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        }, mappedFieldTypes);
    }

    public void testIndexedSingleValuedString() throws IOException {
        // Indexing enables dynamic pruning optimizations
        final CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("name").field("str_value");
        final MappedFieldType mappedFieldTypes = new KeywordFieldMapper.KeywordFieldType("str_value");

        CheckedConsumer<RandomIndexWriter, IOException> buildIndex = iw -> {
            iw.addDocument(
                Arrays.asList(
                    new StringField("str_value", "one", Field.Store.NO),
                    new SortedDocValuesField("str_value", new BytesRef("one"))
                )
            );
            iw.addDocument(
                Arrays.asList(
                    new StringField("unrelatedField", "two", Field.Store.NO),
                    new SortedDocValuesField("unrelatedField", new BytesRef("two"))
                )
            );
            iw.addDocument(
                Arrays.asList(
                    new StringField("str_value", "three", Field.Store.NO),
                    new SortedDocValuesField("str_value", new BytesRef("three"))
                )
            );
            iw.addDocument(
                Arrays.asList(
                    new StringField("str_value", "one", Field.Store.NO),
                    new SortedDocValuesField("str_value", new BytesRef("one"))
                )
            );
        };

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), buildIndex, card -> {
            assertEquals(2, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        }, mappedFieldTypes);

        // Enforce auto-detection of the execution mode
        aggregationBuilder.executionHint(null);
        debugTestCase(
            aggregationBuilder,
            new MatchAllDocsQuery(),
            buildIndex,
            (InternalCardinality card, Class<? extends Aggregator> impl, Map<String, Map<String, Object>> debug) -> {
                assertEquals(2, card.getValue(), 0);
                assertEquals(GlobalOrdCardinalityAggregator.class, impl);
                assertMap(
                    debug,
                    matchesMap().entry(
                        "name",
                        matchesMap().entry("dynamic_pruning_used", greaterThanOrEqualTo(1))
                            .entry("dynamic_pruning_attempted", greaterThanOrEqualTo(1))
                            .entry("skipped_due_to_no_data", 0)
                            .entry("brute_force_used", 0)
                    )
                );
            },
            mappedFieldTypes
        );
    }

    public void testIndexedSingleValuedIP() throws IOException {
        // IP addresses are interesting to test because they use sorted doc values like keywords, but index data using points rather than an
        // inverted index, so this triggers a different code path to disable dynamic pruning
        final CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("name").field("ip_value");
        final MappedFieldType mappedFieldTypes = new IpFieldMapper.IpFieldType("ip_value");

        CheckedConsumer<RandomIndexWriter, IOException> buildIndex = iw -> {
            InetAddress value = InetAddresses.forString("::1");
            byte[] encodedValue = InetAddressPoint.encode(value);
            iw.addDocument(
                Arrays.asList(new InetAddressPoint("ip_value", value), new SortedDocValuesField("ip_value", new BytesRef(encodedValue)))
            );
            value = InetAddresses.forString("192.168.0.1");
            encodedValue = InetAddressPoint.encode(value);
            iw.addDocument(
                Arrays.asList(new InetAddressPoint("ip_value", value), new SortedDocValuesField("ip_value", new BytesRef(encodedValue)))
            );
            value = InetAddresses.forString("::1");
            encodedValue = InetAddressPoint.encode(value);
            iw.addDocument(
                Arrays.asList(new InetAddressPoint("ip_value", value), new SortedDocValuesField("ip_value", new BytesRef(encodedValue)))
            );
        };

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), buildIndex, card -> {
            assertEquals(2, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        }, mappedFieldTypes);

        // Enforce auto-detection of the execution mode
        aggregationBuilder.executionHint(null);
        debugTestCase(
            aggregationBuilder,
            new MatchAllDocsQuery(),
            buildIndex,
            (InternalCardinality card, Class<? extends Aggregator> impl, Map<String, Map<String, Object>> debug) -> {
                assertEquals(2, card.getValue(), 0);
                assertEquals(GlobalOrdCardinalityAggregator.class, impl);
                assertMap(
                    debug,
                    matchesMap().entry(
                        "name",
                        matchesMap().entry("dynamic_pruning_used", 0)
                            .entry("dynamic_pruning_attempted", 0)
                            .entry("skipped_due_to_no_data", 0)
                            .entry("brute_force_used", greaterThanOrEqualTo(1))
                    )
                );
            },
            mappedFieldTypes
        );
    }

    public void testSingleValuedStringValueScript() throws IOException {
        final CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("name").field("str_value")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, "_value", emptyMap()));
        final MappedFieldType mappedFieldTypes = new KeywordFieldMapper.KeywordFieldType("str_value");

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedDocValuesField("str_value", new BytesRef("one"))));
            iw.addDocument(singleton(new SortedDocValuesField("unrelatedField", new BytesRef("two"))));
            iw.addDocument(singleton(new SortedDocValuesField("str_value", new BytesRef("three"))));
            iw.addDocument(singleton(new SortedDocValuesField("str_value", new BytesRef("one"))));
        }, card -> {
            assertEquals(2, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        }, mappedFieldTypes);
    }

    public void testSingleValuedStringScript() throws IOException {
        final CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("name").script(
            new Script(ScriptType.INLINE, MockScriptEngine.NAME, "doc['str_value'].value", emptyMap())
        );
        final MappedFieldType mappedFieldTypes = new KeywordFieldMapper.KeywordFieldType("str_value");

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedDocValuesField("str_value", new BytesRef("one"))));
            iw.addDocument(singleton(new SortedDocValuesField("unrelatedField", new BytesRef("two"))));
            iw.addDocument(singleton(new SortedDocValuesField("str_value", new BytesRef("three"))));
            iw.addDocument(singleton(new SortedDocValuesField("str_value", new BytesRef("one"))));
        }, card -> {
            assertEquals(2, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        }, mappedFieldTypes);
    }

    public void testMultiValuedStringScript() throws IOException {
        final CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("name").script(
            new Script(ScriptType.INLINE, MockScriptEngine.NAME, "doc['str_values']", emptyMap())
        );
        final MappedFieldType mappedFieldTypes = new KeywordFieldMapper.KeywordFieldType("str_values");

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField("str_values", new BytesRef("one")),
                    new SortedSetDocValuesField("str_values", new BytesRef("two"))
                )
            );
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField("unrelatedField", new BytesRef("two")),
                    new SortedSetDocValuesField("unrelatedField", new BytesRef("three"))
                )
            );
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField("str_values", new BytesRef("two")),
                    new SortedSetDocValuesField("str_values", new BytesRef("three"))
                )
            );
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField("str_values", new BytesRef("one")),
                    new SortedSetDocValuesField("str_values", new BytesRef("three"))
                )
            );
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField("str_values", new BytesRef("one")),
                    new SortedSetDocValuesField("str_values", new BytesRef("three"))
                )
            );
        }, card -> {
            assertEquals(3, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        }, mappedFieldTypes);
    }

    public void testMultiValuedStringValueScript() throws IOException {
        final CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("name").field("str_values")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, "_value", emptyMap()));
        final MappedFieldType mappedFieldTypes = new KeywordFieldMapper.KeywordFieldType("str_values");

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField("str_values", new BytesRef("one")),
                    new SortedSetDocValuesField("str_values", new BytesRef("two"))
                )
            );
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField("unrelatedField", new BytesRef("two")),
                    new SortedSetDocValuesField("unrelatedField", new BytesRef("three"))
                )
            );
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField("str_values", new BytesRef("two")),
                    new SortedSetDocValuesField("str_values", new BytesRef("three"))
                )
            );
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField("str_values", new BytesRef("one")),
                    new SortedSetDocValuesField("str_values", new BytesRef("three"))
                )
            );
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField("str_values", new BytesRef("one")),
                    new SortedSetDocValuesField("str_values", new BytesRef("three"))
                )
            );
        }, card -> {
            assertEquals(3, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        }, mappedFieldTypes);
    }

    public void testMultiValuedString() throws IOException {
        final CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("name").field("str_values");
        final MappedFieldType mappedFieldTypes = new KeywordFieldMapper.KeywordFieldType("str_values");

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField("str_values", new BytesRef("one")),
                    new SortedSetDocValuesField("str_values", new BytesRef("two"))
                )
            );
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField("str_values", new BytesRef("one")),
                    new SortedSetDocValuesField("str_values", new BytesRef("three"))
                )
            );
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField("str_values", new BytesRef("three")),
                    new SortedSetDocValuesField("str_values", new BytesRef("two"))
                )
            );
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField("str_values", new BytesRef("three")),
                    new SortedSetDocValuesField("str_values", new BytesRef("two"))
                )
            );
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField("str_values", new BytesRef("two")),
                    new SortedSetDocValuesField("str_values", new BytesRef("three"))
                )
            );
        }, card -> {
            assertEquals(3, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        }, mappedFieldTypes);
    }

    public void testIndexedMultiValuedString() throws IOException {
        // Indexing enables dynamic pruning optimizations
        final CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("name").field("str_values");
        final MappedFieldType mappedFieldTypes = new KeywordFieldMapper.KeywordFieldType("str_values");

        CheckedConsumer<RandomIndexWriter, IOException> buildIndex = iw -> {
            iw.addDocument(
                List.of(
                    new StringField("str_values", "one", Field.Store.NO),
                    new SortedSetDocValuesField("str_values", new BytesRef("one")),
                    new StringField("str_values", "two", Field.Store.NO),
                    new SortedSetDocValuesField("str_values", new BytesRef("two"))
                )
            );
            iw.addDocument(
                List.of(
                    new StringField("str_values", "one", Field.Store.NO),
                    new SortedSetDocValuesField("str_values", new BytesRef("one")),
                    new StringField("str_values", "three", Field.Store.NO),
                    new SortedSetDocValuesField("str_values", new BytesRef("three"))
                )
            );
            iw.addDocument(
                List.of(
                    new StringField("str_values", "three", Field.Store.NO),
                    new SortedSetDocValuesField("str_values", new BytesRef("three")),
                    new StringField("str_values", "two", Field.Store.NO),
                    new SortedSetDocValuesField("str_values", new BytesRef("two"))
                )
            );
            iw.addDocument(
                List.of(
                    new StringField("str_values", "three", Field.Store.NO),
                    new SortedSetDocValuesField("str_values", new BytesRef("three")),
                    new StringField("str_values", "two", Field.Store.NO),
                    new SortedSetDocValuesField("str_values", new BytesRef("two"))
                )
            );
            iw.addDocument(
                List.of(
                    new StringField("str_values", "two", Field.Store.NO),
                    new SortedSetDocValuesField("str_values", new BytesRef("two")),
                    new StringField("str_values", "three", Field.Store.NO),
                    new SortedSetDocValuesField("str_values", new BytesRef("three"))
                )
            );
        };

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), buildIndex, card -> {
            assertEquals(3, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        }, mappedFieldTypes);

        // Enforce auto-detection of the execution mode
        aggregationBuilder.executionHint(null);
        debugTestCase(
            aggregationBuilder,
            new MatchAllDocsQuery(),
            buildIndex,
            (InternalCardinality card, Class<? extends Aggregator> impl, Map<String, Map<String, Object>> debug) -> {
                assertEquals(3, card.getValue(), 0);
                assertEquals(GlobalOrdCardinalityAggregator.class, impl);
                assertMap(
                    debug,
                    matchesMap().entry(
                        "name",
                        matchesMap().entry("dynamic_pruning_used", greaterThanOrEqualTo(1))
                            .entry("dynamic_pruning_attempted", greaterThanOrEqualTo(1))
                            .entry("skipped_due_to_no_data", 0)
                            .entry("brute_force_used", 0)
                    )
                );
            },
            mappedFieldTypes
        );
    }

    public void testIndexedAllDifferentValues() throws IOException {
        // Indexing enables testing of ordinal values
        final CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("name").field("str_values");
        final MappedFieldType mappedFieldTypes = new KeywordFieldMapper.KeywordFieldType("str_values");
        int docs = randomIntBetween(50, 100);
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex = iw -> {

            for (int i = 0; i < docs; i++) {
                iw.addDocument(
                    List.of(
                        new StringField("str_values", "" + i, Field.Store.NO),
                        new SortedSetDocValuesField("str_values", new BytesRef("" + i))
                    )
                );
                if (rarely()) {
                    iw.commit();
                }
            }
        };

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), buildIndex, card -> {
            assertEquals(docs, card.getValue());
            assertTrue(AggregationInspectionHelper.hasValue(card));
        }, mappedFieldTypes);
    }

    public void testUnmappedMissingString() throws IOException {
        CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("name").field("number").missing("🍌🍌🍌");

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("unrelatedField", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("unrelatedField", 8)));
            iw.addDocument(singleton(new NumericDocValuesField("unrelatedField", 9)));
        }, card -> {
            assertEquals(1, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        });
    }

    public void testUnmappedMissingNumber() throws IOException {
        CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("name").field("number").missing(1234);

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("unrelatedField", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("unrelatedField", 8)));
            iw.addDocument(singleton(new NumericDocValuesField("unrelatedField", 9)));
        }, card -> {
            assertEquals(1, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        });
    }

    public void testSingleValuedNumericValueScript() throws IOException {
        final CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("name").field("number")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, "_value", emptyMap()));
        final MappedFieldType mappedFieldTypes = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 10)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("unrelatedField", 11)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 12)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 12)));
        }, card -> {
            assertEquals(2, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        }, mappedFieldTypes);
    }

    public void testSingleValuedNumericScript() throws IOException {
        final CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("name").script(
            new Script(ScriptType.INLINE, MockScriptEngine.NAME, "doc['number'].value", emptyMap())
        );
        final MappedFieldType mappedFieldTypes = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 10)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("unrelatedField", 11)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 12)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 12)));
        }, card -> {
            assertEquals(2, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        }, mappedFieldTypes);
    }

    public void testMultiValuedNumericValueScript() throws IOException {
        final CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("name").field("numbers")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, "_value", emptyMap()));
        final MappedFieldType mappedFieldTypes = new NumberFieldMapper.NumberFieldType("numbers", NumberFieldMapper.NumberType.INTEGER);

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(List.of(new SortedNumericDocValuesField("numbers", 10), new SortedNumericDocValuesField("numbers", 12)));
            iw.addDocument(
                List.of(new SortedNumericDocValuesField("unrelatedField", 11), new SortedNumericDocValuesField("unrelatedField", 12))
            );
            iw.addDocument(List.of(new SortedNumericDocValuesField("numbers", 11), new SortedNumericDocValuesField("numbers", 12)));
            iw.addDocument(List.of(new SortedNumericDocValuesField("numbers", 12), new SortedNumericDocValuesField("numbers", 13)));
            iw.addDocument(List.of(new SortedNumericDocValuesField("numbers", 12), new SortedNumericDocValuesField("numbers", 13)));
        }, card -> {
            assertEquals(4, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        }, mappedFieldTypes);
    }

    public void testMultiValuedNumericScript() throws IOException {
        final CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("name").script(
            new Script(ScriptType.INLINE, MockScriptEngine.NAME, "doc['numbers']", emptyMap())
        );
        final MappedFieldType mappedFieldTypes = new NumberFieldMapper.NumberFieldType("numbers", NumberFieldMapper.NumberType.INTEGER);

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(List.of(new SortedNumericDocValuesField("numbers", 10), new SortedNumericDocValuesField("numbers", 12)));
            iw.addDocument(
                List.of(new SortedNumericDocValuesField("unrelatedField", 11), new SortedNumericDocValuesField("unrelatedField", 12))
            );
            iw.addDocument(List.of(new SortedNumericDocValuesField("numbers", 11), new SortedNumericDocValuesField("numbers", 12)));
            iw.addDocument(List.of(new SortedNumericDocValuesField("numbers", 12), new SortedNumericDocValuesField("numbers", 13)));
            iw.addDocument(List.of(new SortedNumericDocValuesField("numbers", 12), new SortedNumericDocValuesField("numbers", 13)));
        }, card -> {
            assertEquals(4, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        }, mappedFieldTypes);
    }

    public void testMultiValuedNumeric() throws IOException {
        final CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("name").field("number");
        final MappedFieldType mappedFieldTypes = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(List.of(new SortedNumericDocValuesField("number", 7), new SortedNumericDocValuesField("number", 8)));
            iw.addDocument(List.of(new SortedNumericDocValuesField("number", 7), new SortedNumericDocValuesField("number", 9)));
            iw.addDocument(List.of(new SortedNumericDocValuesField("number", 9), new SortedNumericDocValuesField("number", 8)));
            iw.addDocument(List.of(new SortedNumericDocValuesField("number", 8), new SortedNumericDocValuesField("number", 7)));
        }, card -> {
            assertEquals(3, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        }, mappedFieldTypes);
    }

    public void testSingleValuedFieldGlobalAggregation() throws IOException {
        final MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.LONG);

        final AggregationBuilder aggregationBuilder = AggregationBuilders.global("global")
            .subAggregation(AggregationBuilders.cardinality("cardinality").field("number"));

        final int numDocs = 10;
        testCase(iw -> {
            for (int i = 0; i < numDocs; i++) {
                iw.addDocument(singleton(new NumericDocValuesField("number", (i + 1))));
                iw.addDocument(singleton(new NumericDocValuesField("number", (i + 1))));
            }
        }, topLevelAgg -> {
            final Global global = (Global) topLevelAgg;
            assertNotNull(global);
            assertEquals("global", global.getName());
            assertEquals(numDocs * 2, global.getDocCount());
            assertNotNull(global.getAggregations());
            assertEquals(1, global.getAggregations().asMap().size());

            final Cardinality cardinality = global.getAggregations().get("cardinality");
            assertNotNull(cardinality);
            assertEquals("cardinality", cardinality.getName());
            assertEquals(numDocs, cardinality.getValue(), 0);
            assertEquals(cardinality, ((InternalAggregation) global).getProperty("cardinality"));
            assertEquals(numDocs, (double) ((InternalAggregation) global).getProperty("cardinality.value"), 0);
            assertEquals(numDocs, (double) ((InternalAggregation) cardinality).getProperty("value"), 0);
        }, new AggTestConfig(aggregationBuilder, fieldType));
    }

    public void testUnmappedMissingGeoPoint() throws IOException {
        CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("name").field("number")
            .missing(new GeoPoint(42.39561, -71.13051));

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("unrelatedField", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("unrelatedField", 8)));
            iw.addDocument(singleton(new NumericDocValuesField("unrelatedField", 9)));
        }, card -> {
            assertEquals(1, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        });
    }

    public void testAsSubAggregation() throws IOException {
        final MappedFieldType mappedFieldTypes[] = {
            new KeywordFieldMapper.KeywordFieldType("str_value"),
            new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.LONG) };

        final AggregationBuilder aggregationBuilder = new TermsAggregationBuilder("terms").field("str_value")
            .missing("unknown")
            .subAggregation(AggregationBuilders.cardinality("cardinality").field("number"));

        // ("even", "odd")
        testCase(iw -> {
            final int numDocs = 10;
            for (int i = 0; i < numDocs; i++) {
                iw.addDocument(
                    List.of(
                        new SortedDocValuesField("str_value", new BytesRef((((i + 1) % 2 == 0) ? "even" : "odd"))),
                        new NumericDocValuesField("number", i + 1)
                    )
                );
            }
        }, topLevelAgg -> {
            int expectedTermBucketsCount = 2; // ("even", "odd")
            final Terms terms = (StringTerms) topLevelAgg;
            assertNotNull(terms);
            List<? extends Terms.Bucket> buckets = terms.getBuckets();
            assertNotNull(buckets);
            assertEquals(expectedTermBucketsCount, buckets.size());

            for (int i1 = 0; i1 < expectedTermBucketsCount; i1++) {
                final Terms.Bucket bucket = buckets.get(i1);
                assertNotNull(bucket);
                assertEquals(((i1 + 1) % 2 == 0) ? "odd" : "even", bucket.getKey());
                assertEquals(5L, bucket.getDocCount());

                final InternalCardinality cardinality = bucket.getAggregations().get("cardinality");
                assertNotNull(cardinality);
                assertEquals("cardinality", cardinality.getName());
                assertEquals(5, cardinality.getValue());
            }
        }, new AggTestConfig(aggregationBuilder, mappedFieldTypes));
    }

    public void testIndexedWithMissingValues() throws IOException {
        // Indexing enables dynamic pruning optimizations
        final CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("name").field("str_value");
        final MappedFieldType mappedFieldTypes = new KeywordFieldMapper.KeywordFieldType("str_value");

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(Collections.emptySet());
            iw.addDocument(Collections.emptySet());
            iw.addDocument(
                Arrays.asList(
                    new StringField("str_value", "one", Field.Store.NO),
                    new SortedDocValuesField("str_value", new BytesRef("one"))
                )
            );
            iw.addDocument(Collections.emptySet());
            iw.addDocument(
                Arrays.asList(
                    new StringField("unrelatedField", "two", Field.Store.NO),
                    new SortedDocValuesField("unrelatedField", new BytesRef("two"))
                )
            );
            iw.addDocument(Collections.emptySet());
            iw.addDocument(Collections.emptySet());
            iw.addDocument(
                Arrays.asList(
                    new StringField("str_value", "three", Field.Store.NO),
                    new SortedDocValuesField("str_value", new BytesRef("three"))
                )
            );
            iw.addDocument(Collections.emptySet());
            iw.addDocument(Collections.emptySet());
            iw.addDocument(Collections.emptySet());
            iw.addDocument(
                Arrays.asList(
                    new StringField("str_value", "one", Field.Store.NO),
                    new SortedDocValuesField("str_value", new BytesRef("one"))
                )
            );
        }, card -> {
            assertEquals(2, card.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(card));
        }, mappedFieldTypes);
    }

    public void testMoreThan128UniqueStringValues() throws IOException {
        // Indexing enables dynamic pruning optimizations
        // Fields with more than 128 unique values exercise slightly different code paths
        final CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("name").field("str_value");
        final MappedFieldType mappedFieldTypes = new KeywordFieldMapper.KeywordFieldType("str_value");

        CheckedConsumer<RandomIndexWriter, IOException> buildIndex = iw -> {
            for (int i = 0; i < 200; ++i) {
                final String value = Integer.toString(i);
                iw.addDocument(
                    Arrays.asList(
                        new StringField("keyword1", Integer.toString(i % 2), Field.Store.NO),
                        new StringField("keyword2", Integer.toString(i % 5), Field.Store.NO),
                        new StringField("str_value", value, Field.Store.NO),
                        new SortedDocValuesField("str_value", new BytesRef(value))
                    )
                );
            }
        };

        debugTestCase(
            aggregationBuilder,
            new TermQuery(new Term("keyword1", "0")),
            buildIndex,
            (InternalCardinality card, Class<? extends Aggregator> impl, Map<String, Map<String, Object>> debug) -> {
                assertEquals(100, card.getValue(), 0);
                assertEquals(GlobalOrdCardinalityAggregator.class, impl);
                assertMap(
                    debug,
                    matchesMap().entry(
                        "name",
                        matchesMap().entry("dynamic_pruning_used", greaterThanOrEqualTo(1))
                            .entry("dynamic_pruning_attempted", greaterThanOrEqualTo(1))
                            .entry("skipped_due_to_no_data", 0)
                            .entry("brute_force_used", 0)
                    )
                );
            },
            mappedFieldTypes
        );

        debugTestCase(
            aggregationBuilder,
            new TermQuery(new Term("keyword2", "0")),
            buildIndex,
            (InternalCardinality card, Class<? extends Aggregator> impl, Map<String, Map<String, Object>> debug) -> {
                assertEquals(40, card.getValue(), 0);
                assertEquals(GlobalOrdCardinalityAggregator.class, impl);
                assertMap(
                    debug,
                    matchesMap().entry(
                        "name",
                        matchesMap().entry("dynamic_pruning_used", 0)
                            .entry("dynamic_pruning_attempted", greaterThanOrEqualTo(1))
                            .entry("skipped_due_to_no_data", 0)
                            .entry("brute_force_used", 0)
                    )
                );
            },
            mappedFieldTypes
        );
    }

    private void testAggregation(
        Query query,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<InternalCardinality> verify
    ) throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.LONG);
        final CardinalityAggregationBuilder aggregationBuilder = new CardinalityAggregationBuilder("_name").field("number");
        testAggregation(aggregationBuilder, query, buildIndex, verify, fieldType);
    }

    private void testAggregation(
        CardinalityAggregationBuilder aggregationBuilder,
        Query query,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<InternalCardinality> verify,
        MappedFieldType... fieldTypes
    ) throws IOException {
        testCase(buildIndex, verify, new AggTestConfig(aggregationBuilder, fieldTypes).withQuery(query));
        for (CardinalityAggregatorFactory.ExecutionMode mode : CardinalityAggregatorFactory.ExecutionMode.values()) {
            aggregationBuilder.executionHint(mode.toString().toLowerCase(Locale.ROOT));
            testCase(buildIndex, verify, new AggTestConfig(aggregationBuilder, fieldTypes).withQuery(query));
        }
    }
}
