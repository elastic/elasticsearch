/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.aggregatemetric.mapper;

import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.lucene.search.function.ScriptScoreQuery;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.DocReader;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.runtime.DoubleScriptFieldRangeQuery;
import org.elasticsearch.search.runtime.DoubleScriptFieldTermQuery;
import org.elasticsearch.search.runtime.DoubleScriptFieldTermsQuery;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateMetricDoubleFieldMapper.AggregateMetricDoubleFieldType;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateMetricDoubleFieldMapper.Metric;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.aggregatemetric.mapper.AggregateMetricDoubleFieldMapper.subfieldName;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AggregateMetricDoubleFieldTypeTests extends FieldTypeTestCase {

    protected AggregateMetricDoubleFieldType createDefaultFieldType(String name, Map<String, String> meta) {
        return createFieldType(name, meta, Metric.values());
    }

    protected AggregateMetricDoubleFieldType createFieldType(String name, Map<String, String> meta, Metric... metrics) {
        EnumMap<Metric, NumberFieldMapper.NumberFieldType> metricFields = new EnumMap<>(Metric.class);
        for (Metric m : metrics) {
            String subfieldName = subfieldName(name, m);
            NumberFieldMapper.NumberFieldType subfield = new NumberFieldMapper.NumberFieldType(
                subfieldName,
                NumberFieldMapper.NumberType.DOUBLE
            );
            metricFields.put(m, subfield);
        }
        return new AggregateMetricDoubleFieldType(name, null, metricFields, meta);
    }

    public void testTermQuery() {
        {
            final MappedFieldType fieldType = createDefaultFieldType("foo", Collections.emptyMap());
            Query query = fieldType.termQuery(55.2, MOCK_CONTEXT);
            assertThat(query, instanceOf(DoubleScriptFieldTermQuery.class));
            assertThat(query.toString(), equalTo("foo:55.2"));
        }
        {
            // Single metric
            Metric singleMetric = randomFrom(Metric.values());
            final MappedFieldType fieldType = createFieldType("foo", Collections.emptyMap(), singleMetric);
            Query query = fieldType.termQuery(55.2, MOCK_CONTEXT);
            assertThat(query, equalTo(DoubleField.newRangeQuery("foo." + singleMetric.name(), 55.2, 55.2)));
        }
        {
            // No default metric
            final MappedFieldType fieldType = createFieldType("foo", Collections.emptyMap(), Metric.min, Metric.max);
            Query query = fieldType.termQuery(55.2, MOCK_CONTEXT);
            assertThat(query, equalTo(Queries.NO_DOCS_INSTANCE));
        }
    }

    public void testTermsQuery() {
        {
            final MappedFieldType fieldType = createDefaultFieldType("foo", Collections.emptyMap());
            Query query = fieldType.termsQuery(asList(55.2, 500.3), MOCK_CONTEXT);
            assertThat(query, instanceOf(DoubleScriptFieldTermsQuery.class));
            assertThat(query.toString(), equalTo("foo:[55.2, 500.3]"));
        }
        {
            // Single metric
            Metric singleMetric = randomFrom(Metric.values());
            final MappedFieldType fieldType = createFieldType("foo", Collections.emptyMap(), singleMetric);
            Query query = fieldType.termsQuery(asList(55.2, 500.3), MOCK_CONTEXT);
            assertThat(query, equalTo(DoublePoint.newSetQuery("foo." + singleMetric.name(), 55.2, 500.3)));
        }
        {
            // No default metric
            final MappedFieldType fieldType = createFieldType("foo", Collections.emptyMap(), Metric.min, Metric.max);
            Query query = fieldType.termsQuery(asList(55.2, 500.3), MOCK_CONTEXT);
            assertThat(query, equalTo(Queries.NO_DOCS_INSTANCE));
        }
    }

    public void testRangeQuery() {
        {
            final MappedFieldType fieldType = createDefaultFieldType("foo", Collections.emptyMap());
            Query query = fieldType.rangeQuery(10.1, 100.1, true, true, null, null, null, MOCK_CONTEXT);
            assertThat(query, instanceOf(DoubleScriptFieldRangeQuery.class));
            assertThat(query.toString(), equalTo("foo:[10.1 TO 100.1]"));
        }
        {
            // Single metric
            Metric singleMetric = randomFrom(Metric.values());
            final MappedFieldType fieldType = createFieldType("foo", Collections.emptyMap(), singleMetric);
            Query query = fieldType.rangeQuery(10.1, 100.1, true, true, null, null, null, MOCK_CONTEXT);
            assertThat(query, instanceOf(IndexOrDocValuesQuery.class));
        }
        {
            // No default metric
            final MappedFieldType fieldType = createFieldType("foo", Collections.emptyMap(), Metric.min, Metric.max);
            Query query = fieldType.rangeQuery(10.1, 100.1, true, true, null, null, null, MOCK_CONTEXT);
            assertThat(query, equalTo(Queries.NO_DOCS_INSTANCE));
        }
    }

    public void testFetchSourceValueWithOneMetric() throws IOException {
        final MappedFieldType fieldType = createDefaultFieldType("field", Collections.emptyMap());
        final double min = 45.8;
        final Map<String, Object> metric = Collections.singletonMap("min", min);
        assertEquals(List.of(metric), fetchSourceValue(fieldType, metric));
    }

    public void testFetchSourceValueWithMultipleMetrics() throws IOException {
        final MappedFieldType fieldType = createDefaultFieldType("field", Collections.emptyMap());
        final double max = 45.8;
        final double min = 14.2;
        final Map<String, Object> metric = Map.of("min", min, "max", max);
        assertEquals(List.of(metric), fetchSourceValue(fieldType, metric));
    }

    /** Tests that aggregate_metric_double uses the average subfield's doc-values as values in scripts */
    public void testUsedInScript() throws IOException {
        final MappedFieldType mappedFieldType = createDefaultFieldType("field", Collections.emptyMap());
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(createDocument("field", Map.of(Metric.max, 10, Metric.min, 2, Metric.sum, 20, Metric.value_count, 2)));
            iw.addDocument(createDocument("field", Map.of(Metric.max, 4, Metric.min, 1, Metric.sum, 20, Metric.value_count, 5)));
            iw.addDocument(createDocument("field", Map.of(Metric.max, 7, Metric.min, 4, Metric.sum, 21, Metric.value_count, 3)));
            try (DirectoryReader reader = iw.getReader()) {
                SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
                when(searchExecutionContext.getFieldType(anyString())).thenReturn(mappedFieldType);
                when(searchExecutionContext.allowExpensiveQueries()).thenReturn(true);
                SearchLookup lookup = new SearchLookup(
                    searchExecutionContext::getFieldType,
                    (mft, lookupSupplier, fdo) -> mft.fielddataBuilder(
                        new FieldDataContext("test", null, lookupSupplier, searchExecutionContext::sourcePath, fdo)
                    ).build(null, null),
                    (ctx, doc) -> null
                );
                when(searchExecutionContext.lookup()).thenReturn(lookup);
                IndexSearcher searcher = newSearcher(reader);
                assertThat(
                    searcher.count(new ScriptScoreQuery(Queries.ALL_DOCS_INSTANCE, new Script("test"), new ScoreScript.LeafFactory() {
                        @Override
                        public boolean needs_score() {
                            return false;
                        }

                        @Override
                        public boolean needs_termStats() {
                            return false;
                        }

                        @Override
                        public ScoreScript newInstance(DocReader docReader) {
                            return new ScoreScript(Map.of(), searchExecutionContext.lookup(), docReader) {
                                @Override
                                public double execute(ExplanationHolder explanation) {
                                    Map<String, ScriptDocValues<?>> doc = getDoc();
                                    ScriptDocValues.Doubles doubles = (ScriptDocValues.Doubles) doc.get("field");
                                    return doubles.get(0);
                                }
                            };
                        }
                    }, searchExecutionContext.lookup(), 7f, "test", 0, IndexVersion.current())),
                    equalTo(2)
                );
            }
        }
    }

    /** Tests that aggregate_metric_double uses a single subfield's doc-values as values in scripts */
    public void testSingleFieldUsedInScript() throws IOException {
        Metric singleMetric = randomFrom(Metric.values());
        final MappedFieldType mappedFieldType = createFieldType("field", Collections.emptyMap(), singleMetric);
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(createDocument("field", Map.of(singleMetric, 10)));
            iw.addDocument(createDocument("field", Map.of(singleMetric, 4)));
            iw.addDocument(createDocument("field", Map.of(singleMetric, 7)));
            try (DirectoryReader reader = iw.getReader()) {
                SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
                when(searchExecutionContext.getFieldType(anyString())).thenReturn(mappedFieldType);
                when(searchExecutionContext.allowExpensiveQueries()).thenReturn(true);
                SearchLookup lookup = new SearchLookup(
                    searchExecutionContext::getFieldType,
                    (mft, lookupSupplier, fdo) -> mft.fielddataBuilder(
                        new FieldDataContext("test", null, lookupSupplier, searchExecutionContext::sourcePath, fdo)
                    ).build(null, null),
                    (ctx, doc) -> null
                );
                when(searchExecutionContext.lookup()).thenReturn(lookup);
                IndexSearcher searcher = newSearcher(reader);
                assertThat(
                    searcher.count(new ScriptScoreQuery(Queries.ALL_DOCS_INSTANCE, new Script("test"), new ScoreScript.LeafFactory() {
                        @Override
                        public boolean needs_score() {
                            return false;
                        }

                        @Override
                        public boolean needs_termStats() {
                            return false;
                        }

                        @Override
                        public ScoreScript newInstance(DocReader docReader) {
                            return new ScoreScript(Map.of(), searchExecutionContext.lookup(), docReader) {
                                @Override
                                public double execute(ExplanationHolder explanation) {
                                    Map<String, ScriptDocValues<?>> doc = getDoc();
                                    ScriptDocValues.Doubles doubles = (ScriptDocValues.Doubles) doc.get("field");
                                    return doubles.get(0);
                                }
                            };
                        }
                    }, searchExecutionContext.lookup(), 7f, "test", 0, IndexVersion.current())),
                    equalTo(2)
                );
            }
        }
    }

    private static List<NumericDocValuesField> createDocument(String fieldName, Map<Metric, Number> values) {
        return values.entrySet()
            .stream()
            .map(
                entry -> new NumericDocValuesField(
                    subfieldName(fieldName, entry.getKey()),
                    entry.getKey() == Metric.value_count
                        ? entry.getValue().longValue()
                        : Double.doubleToLongBits(entry.getValue().doubleValue())
                )
            )
            .toList();
    }
}
