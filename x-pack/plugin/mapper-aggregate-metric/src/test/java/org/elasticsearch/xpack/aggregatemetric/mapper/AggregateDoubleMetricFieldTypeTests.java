/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.aggregatemetric.mapper;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.elasticsearch.Version;
import org.elasticsearch.common.lucene.search.function.ScriptScoreQuery;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.DocReader;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateDoubleMetricFieldMapper.AggregateDoubleMetricFieldType;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateDoubleMetricFieldMapper.Metric;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.aggregatemetric.mapper.AggregateDoubleMetricFieldMapper.subfieldName;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AggregateDoubleMetricFieldTypeTests extends FieldTypeTestCase {

    protected AggregateDoubleMetricFieldType createDefaultFieldType(String name, Map<String, String> meta, Metric defaultMetric) {
        AggregateDoubleMetricFieldType fieldType = new AggregateDoubleMetricFieldType(name, meta, null);
        for (AggregateDoubleMetricFieldMapper.Metric m : List.of(
            AggregateDoubleMetricFieldMapper.Metric.min,
            AggregateDoubleMetricFieldMapper.Metric.max
        )) {
            String subfieldName = subfieldName(fieldType.name(), m);
            NumberFieldMapper.NumberFieldType subfield = new NumberFieldMapper.NumberFieldType(
                subfieldName,
                NumberFieldMapper.NumberType.DOUBLE
            );
            fieldType.addMetricField(m, subfield);
        }
        fieldType.setDefaultMetric(defaultMetric);
        return fieldType;
    }

    public void testTermQuery() {
        final MappedFieldType fieldType = createDefaultFieldType("foo", Collections.emptyMap(), Metric.max);
        Query query = fieldType.termQuery(55.2, null);
        assertThat(query, equalTo(DoublePoint.newRangeQuery("foo.max", 55.2, 55.2)));
    }

    public void testTermsQuery() {
        final MappedFieldType fieldType = createDefaultFieldType("foo", Collections.emptyMap(), Metric.max);
        Query query = fieldType.termsQuery(asList(55.2, 500.3), null);
        assertThat(query, equalTo(DoublePoint.newSetQuery("foo.max", 55.2, 500.3)));
    }

    public void testRangeQuery() {
        final MappedFieldType fieldType = createDefaultFieldType("foo", Collections.emptyMap(), Metric.max);
        Query query = fieldType.rangeQuery(10.1, 100.1, true, true, null, null, null, null);
        assertThat(query, instanceOf(IndexOrDocValuesQuery.class));
    }

    public void testFetchSourceValueWithOneMetric() throws IOException {
        final MappedFieldType fieldType = createDefaultFieldType("field", Collections.emptyMap(), Metric.min);
        final double defaultValue = 45.8;
        final Map<String, Object> metric = Collections.singletonMap("min", defaultValue);
        assertEquals(List.of(defaultValue), fetchSourceValue(fieldType, metric));
    }

    public void testFetchSourceValueWithMultipleMetrics() throws IOException {
        final MappedFieldType fieldType = createDefaultFieldType("field", Collections.emptyMap(), Metric.max);
        final double defaultValue = 45.8;
        final Map<String, Object> metric = Map.of("min", 14.2, "max", defaultValue);
        assertEquals(List.of(defaultValue), fetchSourceValue(fieldType, metric));
    }

    /** Tests that aggregate_metric_double uses the default_metric subfield's doc-values as values in scripts */
    public void testUsedInScript() throws IOException {
        final MappedFieldType mappedFieldType = createDefaultFieldType("field", Collections.emptyMap(), Metric.max);
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(
                List.of(
                    new NumericDocValuesField(subfieldName("field", Metric.max), Double.doubleToLongBits(10)),
                    new NumericDocValuesField(subfieldName("field", Metric.min), Double.doubleToLongBits(2))
                )
            );
            iw.addDocument(
                List.of(
                    new NumericDocValuesField(subfieldName("field", Metric.max), Double.doubleToLongBits(4)),
                    new NumericDocValuesField(subfieldName("field", Metric.min), Double.doubleToLongBits(1))
                )
            );
            iw.addDocument(
                List.of(
                    new NumericDocValuesField(subfieldName("field", Metric.max), Double.doubleToLongBits(7)),
                    new NumericDocValuesField(subfieldName("field", Metric.min), Double.doubleToLongBits(4))
                )
            );
            try (DirectoryReader reader = iw.getReader()) {
                SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
                when(searchExecutionContext.getFieldType(anyString())).thenReturn(mappedFieldType);
                when(searchExecutionContext.allowExpensiveQueries()).thenReturn(true);
                SearchLookup lookup = new SearchLookup(
                    searchExecutionContext::getFieldType,
                    (mft, lookupSupplier) -> mft.fielddataBuilder("test", lookupSupplier).build(null, null)
                );
                when(searchExecutionContext.lookup()).thenReturn(lookup);
                IndexSearcher searcher = newSearcher(reader);
                assertThat(searcher.count(new ScriptScoreQuery(new MatchAllDocsQuery(), new Script("test"), new ScoreScript.LeafFactory() {
                    @Override
                    public boolean needs_score() {
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
                }, searchExecutionContext.lookup(), 7f, "test", 0, Version.CURRENT)), equalTo(2));
            }
        }
    }

}
