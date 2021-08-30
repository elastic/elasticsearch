/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.multiterms;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.metrics.InternalMax;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.lookup.LeafDocLookup;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.elasticsearch.xpack.analytics.multiterms.MultiTermsAggregationBuilderTests.randomTermConfig;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class MultiTermsAggregatorTests extends AggregatorTestCase {

    /**
     * Script to return the {@code _value} provided by aggs framework.
     */
    public static final String ADD_ONE_SCRIPT = "add_one";

    public static final String DATE_FIELD = "tVal";
    public static final String INT_FIELD = "iVal";
    public static final String FLOAT_FIELD = "fVal";
    public static final String KEYWORD_FIELD = "kVal";

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        logger.info(fieldType);
        return new MultiTermsAggregationBuilder("my_terms").terms(
            org.elasticsearch.core.List.of(
                new MultiValuesSourceFieldConfig.Builder().setFieldName(fieldName).build(),
                new MultiValuesSourceFieldConfig.Builder().setFieldName(fieldName).build()
            )
        );
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return org.elasticsearch.core.List.of(
            CoreValuesSourceType.NUMERIC,
            CoreValuesSourceType.DATE,
            CoreValuesSourceType.BOOLEAN,
            CoreValuesSourceType.KEYWORD,
            CoreValuesSourceType.IP
        );
    }

    @Override
    protected ScriptService getMockScriptService() {
        Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

        scripts.put(ADD_ONE_SCRIPT, vars -> {
            LeafDocLookup leafDocLookup = (LeafDocLookup) vars.get("doc");
            String fieldname = (String) vars.get("fieldname");
            ScriptDocValues<?> scriptDocValues = leafDocLookup.get(fieldname);
            return ((Number) scriptDocValues.get(0)).doubleValue() + 1.0;
        });

        MockScriptEngine scriptEngine = new MockScriptEngine(MockScriptEngine.NAME, scripts, Collections.emptyMap());
        Map<String, ScriptEngine> engines = Collections.singletonMap(scriptEngine.getType(), scriptEngine);

        return new ScriptService(Settings.EMPTY, engines, ScriptModule.CORE_CONTEXTS);
    }

    public void testIntegersFloatsAndStrings() throws IOException {
        testCase(new MatchAllDocsQuery(), new String[] { KEYWORD_FIELD, INT_FIELD, FLOAT_FIELD }, null, iw -> {
            iw.addDocument(
                org.elasticsearch.core.List.of(
                    new NumericDocValuesField(INT_FIELD, 3),
                    new FloatDocValuesField(FLOAT_FIELD, 1.0f),
                    new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("a"))
                )
            );
            iw.addDocument(
                org.elasticsearch.core.List.of(
                    new NumericDocValuesField(INT_FIELD, 3),
                    new FloatDocValuesField(FLOAT_FIELD, 1.0f),
                    new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("b"))
                )
            );
            iw.addDocument(
                org.elasticsearch.core.List.of(
                    new NumericDocValuesField(INT_FIELD, 1),
                    new FloatDocValuesField(FLOAT_FIELD, 1.0f),
                    new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("a"))
                )
            );
            iw.addDocument(
                org.elasticsearch.core.List.of(
                    new NumericDocValuesField(INT_FIELD, 2),
                    new FloatDocValuesField(FLOAT_FIELD, 2.0f),
                    new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("a"))
                )
            );
            iw.addDocument(
                org.elasticsearch.core.List.of(
                    new NumericDocValuesField(INT_FIELD, 3),
                    new FloatDocValuesField(FLOAT_FIELD, 1.0f),
                    new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("a"))
                )
            );
        }, h -> {
            assertThat(h.getBuckets(), hasSize(4));
            assertThat(h.getBuckets().get(0).getKey(), contains(equalTo("a"), equalTo(3L), equalTo(1.0)));
            assertThat(h.getBuckets().get(0).getDocCount(), equalTo(2L));
            assertThat(h.getBuckets().get(1).getKey(), contains(equalTo("a"), equalTo(1L), equalTo(1.0)));
            assertThat(h.getBuckets().get(1).getDocCount(), equalTo(1L));
            assertThat(h.getBuckets().get(2).getKey(), contains(equalTo("a"), equalTo(2L), equalTo(2.0)));
            assertThat(h.getBuckets().get(2).getDocCount(), equalTo(1L));
            assertThat(h.getBuckets().get(3).getKey(), contains(equalTo("b"), equalTo(3L), equalTo(1.0)));
            assertThat(h.getBuckets().get(3).getDocCount(), equalTo(1L));
        });
    }

    public void testNullFields() throws IOException {
        testCase(new MatchAllDocsQuery(), new String[] { KEYWORD_FIELD, INT_FIELD, FLOAT_FIELD }, null, iw -> {
            iw.addDocument(
                org.elasticsearch.core.List.of(
                    new FloatDocValuesField(FLOAT_FIELD, 1.0f),
                    new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("a"))
                )
            );
            iw.addDocument(
                org.elasticsearch.core.List.of(
                    new NumericDocValuesField(INT_FIELD, 3),
                    new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("b"))
                )
            );
            iw.addDocument(
                org.elasticsearch.core.List.of(new NumericDocValuesField(INT_FIELD, 1), new FloatDocValuesField(FLOAT_FIELD, 1.0f))
            );
            iw.addDocument(
                org.elasticsearch.core.List.of(
                    new NumericDocValuesField(INT_FIELD, 2),
                    new FloatDocValuesField(FLOAT_FIELD, 2.0f),
                    new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("a"))
                )
            );
            iw.addDocument(
                org.elasticsearch.core.List.of(
                    new NumericDocValuesField(INT_FIELD, 3),
                    new FloatDocValuesField(FLOAT_FIELD, 1.0f),
                    new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("a"))
                )
            );
            iw.addDocument(
                org.elasticsearch.core.List.of(
                    new NumericDocValuesField(INT_FIELD, 3),
                    new FloatDocValuesField(FLOAT_FIELD, 1.0f),
                    new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("a"))
                )
            );
            iw.addDocument(org.elasticsearch.core.List.of(new NumericDocValuesField("wrong_val", 3)));
        }, h -> {
            assertThat(h.getBuckets(), hasSize(2));
            assertThat(h.getBuckets().get(0).getKey(), contains(equalTo("a"), equalTo(3L), equalTo(1.0)));
            assertThat(h.getBuckets().get(0).getDocCount(), equalTo(2L));
            assertThat(h.getBuckets().get(1).getKey(), contains(equalTo("a"), equalTo(2L), equalTo(2.0)));
            assertThat(h.getBuckets().get(1).getDocCount(), equalTo(1L));
        });
    }

    public void testMissingFields() throws IOException {
        testCase(
            new MatchAllDocsQuery(),
            org.elasticsearch.core.List.of(
                new MultiValuesSourceFieldConfig.Builder().setFieldName(KEYWORD_FIELD).setMissing("z").build(),
                new MultiValuesSourceFieldConfig.Builder().setFieldName(INT_FIELD).setMissing(0).build(),
                new MultiValuesSourceFieldConfig.Builder().setFieldName(FLOAT_FIELD).setMissing(-1.0f).build()
            ),
            null,
            iw -> {
                iw.addDocument(
                    org.elasticsearch.core.List.of(
                        new FloatDocValuesField(FLOAT_FIELD, 1.0f),
                        new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("a"))
                    )
                );
                iw.addDocument(
                    org.elasticsearch.core.List.of(
                        new NumericDocValuesField(INT_FIELD, 3),
                        new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("b"))
                    )
                );
                iw.addDocument(
                    org.elasticsearch.core.List.of(new NumericDocValuesField(INT_FIELD, 1), new FloatDocValuesField(FLOAT_FIELD, 1.0f))
                );
                iw.addDocument(
                    org.elasticsearch.core.List.of(
                        new NumericDocValuesField(INT_FIELD, 2),
                        new FloatDocValuesField(FLOAT_FIELD, 2.0f),
                        new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("a"))
                    )
                );
                iw.addDocument(
                    org.elasticsearch.core.List.of(
                        new NumericDocValuesField(INT_FIELD, 3),
                        new FloatDocValuesField(FLOAT_FIELD, 1.0f),
                        new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("a"))
                    )
                );
                iw.addDocument(
                    org.elasticsearch.core.List.of(
                        new NumericDocValuesField(INT_FIELD, 3),
                        new FloatDocValuesField(FLOAT_FIELD, 1.0f),
                        new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("a"))
                    )
                );
                iw.addDocument(org.elasticsearch.core.List.of(new NumericDocValuesField("wrong_val", 3)));
            },
            h -> {
                assertThat(h.getBuckets(), hasSize(6));
                assertThat(h.getBuckets().get(0).getKey(), contains(equalTo("a"), equalTo(3L), equalTo(1.0)));
                assertThat(h.getBuckets().get(0).getDocCount(), equalTo(2L));
                assertThat(h.getBuckets().get(1).getKey(), contains(equalTo("a"), equalTo(0L), equalTo(1.0)));
                assertThat(h.getBuckets().get(1).getDocCount(), equalTo(1L));
                assertThat(h.getBuckets().get(2).getKey(), contains(equalTo("a"), equalTo(2L), equalTo(2.0)));
                assertThat(h.getBuckets().get(2).getDocCount(), equalTo(1L));
                assertThat(h.getBuckets().get(3).getKey(), contains(equalTo("b"), equalTo(3L), equalTo(-1.0)));
                assertThat(h.getBuckets().get(3).getDocCount(), equalTo(1L));
                assertThat(h.getBuckets().get(4).getKey(), contains(equalTo("z"), equalTo(0L), equalTo(-1.0)));
                assertThat(h.getBuckets().get(4).getDocCount(), equalTo(1L));
                assertThat(h.getBuckets().get(5).getKey(), contains(equalTo("z"), equalTo(1L), equalTo(1.0)));
                assertThat(h.getBuckets().get(5).getDocCount(), equalTo(1L));
            }
        );
    }

    public void testSortedNumericDocValues() throws IOException {
        testCase(new MatchAllDocsQuery(), new String[] { KEYWORD_FIELD, INT_FIELD }, null, iw -> {
            iw.addDocument(
                org.elasticsearch.core.List.of(
                    new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("a")),
                    new SortedNumericDocValuesField(INT_FIELD, 1)
                )
            );
            iw.addDocument(
                org.elasticsearch.core.List.of(
                    new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("a")),
                    new SortedNumericDocValuesField(INT_FIELD, 2)
                )
            );
            iw.addDocument(
                org.elasticsearch.core.List.of(
                    new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("b")),
                    new SortedNumericDocValuesField(INT_FIELD, 3)
                )
            );
            iw.addDocument(
                org.elasticsearch.core.List.of(
                    new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("b")),
                    new SortedNumericDocValuesField(INT_FIELD, 3)
                )
            );
        }, h -> {
            assertThat(h.getBuckets(), hasSize(3));
            assertThat(h.getBuckets().get(0).getKey(), contains(equalTo("b"), equalTo(3L)));
            assertThat(h.getBuckets().get(0).getDocCount(), equalTo(2L));
            assertThat(h.getBuckets().get(1).getKey(), contains(equalTo("a"), equalTo(1L)));
            assertThat(h.getBuckets().get(1).getDocCount(), equalTo(1L));
            assertThat(h.getBuckets().get(2).getKey(), contains(equalTo("a"), equalTo(2L)));
            assertThat(h.getBuckets().get(2).getDocCount(), equalTo(1L));
        });
    }

    public void testMultiValues() throws IOException {
        testCase(new MatchAllDocsQuery(), new String[] { KEYWORD_FIELD, INT_FIELD }, null, iw -> {
            iw.addDocument(
                org.elasticsearch.core.List.of(
                    new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("a")),
                    new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("b")),
                    new SortedNumericDocValuesField(INT_FIELD, 1),
                    new SortedNumericDocValuesField(INT_FIELD, 2)
                )
            );
            iw.addDocument(
                org.elasticsearch.core.List.of(
                    new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("b")),
                    new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("c")),
                    new SortedNumericDocValuesField(INT_FIELD, 2),
                    new SortedNumericDocValuesField(INT_FIELD, 3)
                )
            );
        }, h -> {
            assertThat(h.getBuckets(), hasSize(7));
            assertThat(h.getBuckets().get(0).getKey(), contains(equalTo("b"), equalTo(2L)));
            assertThat(h.getBuckets().get(0).getDocCount(), equalTo(2L));
            assertThat(h.getBuckets().get(1).getKey(), contains(equalTo("a"), equalTo(1L)));
            assertThat(h.getBuckets().get(1).getDocCount(), equalTo(1L));
            assertThat(h.getBuckets().get(2).getKey(), contains(equalTo("a"), equalTo(2L)));
            assertThat(h.getBuckets().get(2).getDocCount(), equalTo(1L));
            assertThat(h.getBuckets().get(3).getKey(), contains(equalTo("b"), equalTo(1L)));
            assertThat(h.getBuckets().get(3).getDocCount(), equalTo(1L));
            assertThat(h.getBuckets().get(4).getKey(), contains(equalTo("b"), equalTo(3L)));
            assertThat(h.getBuckets().get(4).getDocCount(), equalTo(1L));
            assertThat(h.getBuckets().get(5).getKey(), contains(equalTo("c"), equalTo(2L)));
            assertThat(h.getBuckets().get(5).getDocCount(), equalTo(1L));
            assertThat(h.getBuckets().get(6).getKey(), contains(equalTo("c"), equalTo(3L)));
            assertThat(h.getBuckets().get(6).getDocCount(), equalTo(1L));
        });
    }

    public void testScripts() throws IOException {
        testCase(
            new MatchAllDocsQuery(),
            org.elasticsearch.core.List.of(
                new MultiValuesSourceFieldConfig.Builder().setFieldName(KEYWORD_FIELD).build(),
                new MultiValuesSourceFieldConfig.Builder().setScript(
                    new Script(ScriptType.INLINE, MockScriptEngine.NAME, ADD_ONE_SCRIPT, Collections.singletonMap("fieldname", INT_FIELD))
                ).setUserValueTypeHint(ValueType.LONG).build(),
                new MultiValuesSourceFieldConfig.Builder().setFieldName(INT_FIELD).build()
            ),
            null,
            iw -> {
                iw.addDocument(
                    org.elasticsearch.core.List.of(
                        new NumericDocValuesField(INT_FIELD, 3),
                        new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("a"))
                    )
                );
                iw.addDocument(
                    org.elasticsearch.core.List.of(
                        new NumericDocValuesField(INT_FIELD, 3),
                        new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("b"))
                    )
                );
                iw.addDocument(
                    org.elasticsearch.core.List.of(
                        new NumericDocValuesField(INT_FIELD, 1),
                        new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("a"))
                    )
                );
                iw.addDocument(
                    org.elasticsearch.core.List.of(
                        new NumericDocValuesField(INT_FIELD, 2),
                        new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("a"))
                    )
                );
                iw.addDocument(
                    org.elasticsearch.core.List.of(
                        new NumericDocValuesField(INT_FIELD, 3),
                        new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("a"))
                    )
                );
            },
            h -> {
                assertThat(h.getBuckets(), hasSize(4));
                assertThat(h.getBuckets().get(0).getKey(), contains(equalTo("a"), equalTo(4L), equalTo(3L)));
                assertThat(h.getBuckets().get(0).getDocCount(), equalTo(2L));
                assertThat(h.getBuckets().get(1).getKey(), contains(equalTo("a"), equalTo(2L), equalTo(1L)));
                assertThat(h.getBuckets().get(1).getDocCount(), equalTo(1L));
                assertThat(h.getBuckets().get(2).getKey(), contains(equalTo("a"), equalTo(3L), equalTo(2L)));
                assertThat(h.getBuckets().get(2).getDocCount(), equalTo(1L));
                assertThat(h.getBuckets().get(3).getKey(), contains(equalTo("b"), equalTo(4L), equalTo(3L)));
                assertThat(h.getBuckets().get(3).getDocCount(), equalTo(1L));
            }
        );
    }

    public void testFilter() throws IOException {
        testCase(new TermQuery(new Term(KEYWORD_FIELD, "a")), new String[] { KEYWORD_FIELD, INT_FIELD }, null, iw -> {
            iw.addDocument(
                org.elasticsearch.core.List.of(
                    new NumericDocValuesField(INT_FIELD, 3),
                    new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("a")),
                    new StringField(KEYWORD_FIELD, "a", Field.Store.NO)
                )
            );
            iw.addDocument(
                org.elasticsearch.core.List.of(
                    new NumericDocValuesField(INT_FIELD, 3),
                    new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("a")),
                    new StringField(KEYWORD_FIELD, "b", Field.Store.NO)
                )
            );
            iw.addDocument(
                org.elasticsearch.core.List.of(
                    new NumericDocValuesField(INT_FIELD, 1),
                    new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("a")),
                    new StringField(KEYWORD_FIELD, "a", Field.Store.NO)
                )
            );
            iw.addDocument(
                org.elasticsearch.core.List.of(
                    new NumericDocValuesField(INT_FIELD, 2),
                    new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("a")),
                    new StringField(KEYWORD_FIELD, "a", Field.Store.NO)
                )
            );
            iw.addDocument(
                org.elasticsearch.core.List.of(
                    new NumericDocValuesField(INT_FIELD, 3),
                    new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("a")),
                    new StringField(KEYWORD_FIELD, "a", Field.Store.NO)
                )
            );
        }, h -> {
            assertThat(h.getBuckets(), hasSize(3));
            assertThat(h.getBuckets().get(0).getKey(), contains(equalTo("a"), equalTo(3L)));
            assertThat(h.getBuckets().get(0).getDocCount(), equalTo(2L));
            assertThat(h.getBuckets().get(1).getKey(), contains(equalTo("a"), equalTo(1L)));
            assertThat(h.getBuckets().get(1).getDocCount(), equalTo(1L));
            assertThat(h.getBuckets().get(2).getKey(), contains(equalTo("a"), equalTo(2L)));
            assertThat(h.getBuckets().get(2).getDocCount(), equalTo(1L));
        });
    }

    public void testSort() throws IOException {
        testCase(new MatchAllDocsQuery(), new String[] { INT_FIELD, KEYWORD_FIELD }, b -> {
            b.order(BucketOrder.aggregation("max_float", true));
            b.subAggregation(new MaxAggregationBuilder("max_float").field(FLOAT_FIELD));
        }, iw -> {
            iw.addDocument(
                org.elasticsearch.core.List.of(
                    new NumericDocValuesField(INT_FIELD, 3),
                    new FloatDocValuesField(FLOAT_FIELD, 1.0f),
                    new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("a"))
                )
            );
            iw.addDocument(
                org.elasticsearch.core.List.of(
                    new NumericDocValuesField(INT_FIELD, 3),
                    new FloatDocValuesField(FLOAT_FIELD, 2.0f),
                    new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("b"))
                )
            );
            iw.addDocument(
                org.elasticsearch.core.List.of(
                    new NumericDocValuesField(INT_FIELD, 1),
                    new FloatDocValuesField(FLOAT_FIELD, 3.0f),
                    new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("a"))
                )
            );
            iw.addDocument(
                org.elasticsearch.core.List.of(
                    new NumericDocValuesField(INT_FIELD, 2),
                    new FloatDocValuesField(FLOAT_FIELD, 4.0f),
                    new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("a"))
                )
            );
            iw.addDocument(
                org.elasticsearch.core.List.of(
                    new NumericDocValuesField(INT_FIELD, 3),
                    new FloatDocValuesField(FLOAT_FIELD, 5.0f),
                    new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("a"))
                )
            );
        }, h -> {
            assertThat(h.getBuckets(), hasSize(4));

            assertThat(h.getBuckets().get(0).getKey(), contains(equalTo(3L), equalTo("b")));
            assertThat(h.getBuckets().get(0).getDocCount(), equalTo(1L));
            assertThat(((InternalMax) (h.getBuckets().get(0).getAggregations().get("max_float"))).value(), closeTo(2.0, 0.00001));

            assertThat(h.getBuckets().get(1).getKey(), contains(equalTo(1L), equalTo("a")));
            assertThat(h.getBuckets().get(1).getDocCount(), equalTo(1L));
            assertThat(((InternalMax) (h.getBuckets().get(1).getAggregations().get("max_float"))).value(), closeTo(3.0, 0.00001));

            assertThat(h.getBuckets().get(2).getKey(), contains(equalTo(2L), equalTo("a")));
            assertThat(h.getBuckets().get(2).getDocCount(), equalTo(1L));
            assertThat(((InternalMax) (h.getBuckets().get(2).getAggregations().get("max_float"))).value(), closeTo(4.0, 0.00001));

            assertThat(h.getBuckets().get(3).getKey(), contains(equalTo(3L), equalTo("a")));
            assertThat(h.getBuckets().get(3).getDocCount(), equalTo(2L));
            assertThat(((InternalMax) (h.getBuckets().get(3).getAggregations().get("max_float"))).value(), closeTo(5.0, 0.00001));
        });
    }

    public void testFormatter() throws IOException {
        testCase(
            new MatchAllDocsQuery(),
            org.elasticsearch.core.List.of(
                new MultiValuesSourceFieldConfig.Builder().setFieldName(KEYWORD_FIELD).build(),
                new MultiValuesSourceFieldConfig.Builder().setFieldName(INT_FIELD).setFormat("0000").build()
            ),
            null,
            iw -> {
                iw.addDocument(
                    org.elasticsearch.core.List.of(
                        new NumericDocValuesField(INT_FIELD, 3),
                        new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("a"))
                    )
                );
                iw.addDocument(
                    org.elasticsearch.core.List.of(
                        new NumericDocValuesField(INT_FIELD, 3),
                        new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("b"))
                    )
                );
                iw.addDocument(
                    org.elasticsearch.core.List.of(
                        new NumericDocValuesField(INT_FIELD, 1),
                        new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("a"))
                    )
                );
                iw.addDocument(
                    org.elasticsearch.core.List.of(
                        new NumericDocValuesField(INT_FIELD, 2),
                        new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("a"))
                    )
                );
                iw.addDocument(
                    org.elasticsearch.core.List.of(
                        new NumericDocValuesField(INT_FIELD, 3),
                        new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef("a"))
                    )
                );
            },
            h -> {
                assertThat(h.getBuckets(), hasSize(4));
                assertThat(h.getBuckets().get(0).getKeyAsString(), equalTo("a|0003"));
                assertThat(h.getBuckets().get(0).getDocCount(), equalTo(2L));
                assertThat(h.getBuckets().get(1).getKeyAsString(), equalTo("a|0001"));
                assertThat(h.getBuckets().get(1).getDocCount(), equalTo(1L));
                assertThat(h.getBuckets().get(2).getKeyAsString(), equalTo("a|0002"));
                assertThat(h.getBuckets().get(2).getDocCount(), equalTo(1L));
                assertThat(h.getBuckets().get(3).getKeyAsString(), equalTo("b|0003"));
                assertThat(h.getBuckets().get(3).getDocCount(), equalTo(1L));
            }
        );
    }

    public void testDates() throws IOException {
        testCase(
            new MatchAllDocsQuery(),
            org.elasticsearch.core.List.of(
                new MultiValuesSourceFieldConfig.Builder().setFieldName(DATE_FIELD).build(),
                new MultiValuesSourceFieldConfig.Builder().setFieldName(INT_FIELD).setFormat("0000").build()
            ),
            null,
            iw -> {
                iw.addDocument(docWithDate("2020-01-01", new NumericDocValuesField(INT_FIELD, 3)));
                iw.addDocument(docWithDate("2020-01-01", new NumericDocValuesField(INT_FIELD, 4)));
                iw.addDocument(docWithDate("2020-01-01", new NumericDocValuesField(INT_FIELD, 3)));
                iw.addDocument(docWithDate("2020-01-02", new NumericDocValuesField(INT_FIELD, 5)));

            },
            h -> {
                assertThat(h.getBuckets(), hasSize(3));
                assertThat(h.getBuckets().get(0).getKeyAsString(), equalTo("2020-01-01|0003"));
                assertThat(h.getBuckets().get(0).getDocCount(), equalTo(2L));
                assertThat(h.getBuckets().get(1).getKeyAsString(), equalTo("2020-01-01|0004"));
                assertThat(h.getBuckets().get(1).getDocCount(), equalTo(1L));
                assertThat(h.getBuckets().get(2).getKeyAsString(), equalTo("2020-01-02|0005"));
                assertThat(h.getBuckets().get(2).getDocCount(), equalTo(1L));
            }
        );
    }

    public void testMinDocCount() throws IOException {
        testCase(
            new MatchAllDocsQuery(),
            org.elasticsearch.core.List.of(
                new MultiValuesSourceFieldConfig.Builder().setFieldName(DATE_FIELD).build(),
                new MultiValuesSourceFieldConfig.Builder().setFieldName(INT_FIELD).setFormat("0000").build()
            ),
            ab -> ab.minDocCount(2),
            iw -> {
                iw.addDocument(docWithDate("2020-01-01", new NumericDocValuesField(INT_FIELD, 3)));
                iw.addDocument(docWithDate("2020-01-01", new NumericDocValuesField(INT_FIELD, 4)));
                iw.addDocument(docWithDate("2020-01-01", new NumericDocValuesField(INT_FIELD, 3)));
                iw.addDocument(docWithDate("2020-01-02", new NumericDocValuesField(INT_FIELD, 5)));

            },
            h -> {
                assertThat(h.getBuckets(), hasSize(1));
                assertThat(h.getBuckets().get(0).getKeyAsString(), equalTo("2020-01-01|0003"));
                assertThat(h.getBuckets().get(0).getDocCount(), equalTo(2L));
            }
        );
    }

    public void testNoTerms() {
        for (List<MultiValuesSourceFieldConfig> terms : Arrays.<List<MultiValuesSourceFieldConfig>>asList(
            Collections.singletonList(randomTermConfig()),
            Collections.emptyList(),
            null
        )) {

            IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> testCase(
                    new MatchAllDocsQuery(),
                    terms,
                    null,
                    iw -> { iw.addDocument(docWithDate("2020-01-01", new NumericDocValuesField(INT_FIELD, 3))); },
                    h -> fail("Should have thrown exception")
                )
            );
            if (terms == null) {
                assertEquals("[terms] must not be null: [my_terms]", ex.getMessage());
            } else {
                assertEquals(
                    "The [terms] parameter in the aggregation [my_terms] must be present and have at least 2 fields or scripts."
                        + (terms.isEmpty() == false ? " For a single field user terms aggregation." : ""),
                    ex.getMessage()
                );
            }
        }

    }

    private void testCase(
        Query query,
        String[] terms,
        Consumer<MultiTermsAggregationBuilder> builderSetup,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<InternalMultiTerms> verify
    ) throws IOException {
        List<MultiValuesSourceFieldConfig> termConfigs = new ArrayList<>();
        for (String term : terms) {
            termConfigs.add(new MultiValuesSourceFieldConfig.Builder().setFieldName(term).build());
        }
        testCase(query, termConfigs, builderSetup, buildIndex, verify);
    }

    private void testCase(
        Query query,
        List<MultiValuesSourceFieldConfig> terms,
        Consumer<MultiTermsAggregationBuilder> builderSetup,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<InternalMultiTerms> verify
    ) throws IOException {
        MappedFieldType dateType = dateFieldType(DATE_FIELD);
        MappedFieldType intType = new NumberFieldMapper.NumberFieldType(INT_FIELD, NumberFieldMapper.NumberType.INTEGER);
        MappedFieldType floatType = new NumberFieldMapper.NumberFieldType(FLOAT_FIELD, NumberFieldMapper.NumberType.FLOAT);
        MappedFieldType keywordType = new KeywordFieldMapper.KeywordFieldType(KEYWORD_FIELD);
        MultiTermsAggregationBuilder builder = new MultiTermsAggregationBuilder("my_terms");
        builder.terms(terms);
        if (builderSetup != null) {
            builderSetup.accept(builder);
        } else {
            // Set some random settings that shouldn't affect most of tests
            if (randomBoolean()) {
                builder.showTermDocCountError(randomBoolean());
            }
            if (randomBoolean()) {
                builder.shardSize(randomIntBetween(10, 200));
            }
            if (randomBoolean()) {
                builder.size(randomIntBetween(10, 200));
            }
        }
        testCase(builder, query, buildIndex, verify, dateType, intType, floatType, keywordType);
    }

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return Collections.singletonList(new AnalyticsPlugin(Settings.EMPTY));
    }

    private DateFieldMapper.DateFieldType dateFieldType(String name) {
        return new DateFieldMapper.DateFieldType(
            name,
            true,
            false,
            true,
            DateFormatter.forPattern("strict_date"),
            DateFieldMapper.Resolution.MILLISECONDS,
            null,
            null,
            Collections.emptyMap()
        );
    }

    private Iterable<IndexableField> docWithDate(String date, IndexableField... fields) {
        List<IndexableField> indexableFields = new ArrayList<>();
        long instant = dateFieldType(DATE_FIELD).parse(date);
        indexableFields.add(new SortedNumericDocValuesField(DATE_FIELD, instant));
        indexableFields.add(new LongPoint(DATE_FIELD, instant));
        indexableFields.addAll(Arrays.asList(fields));
        return indexableFields;
    }

}
