/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.aggregatemetric.mapper;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateDoubleMetricFieldMapper.AggregateDoubleMetricFieldType;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateDoubleMetricFieldMapper.Metric;

import java.util.EnumMap;
import java.util.List;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class AggregateDoubleMetricFieldTypeTests extends FieldTypeTestCase {

    @Override
    protected MappedFieldType createDefaultFieldType() {
        AggregateDoubleMetricFieldType fieldType = new AggregateDoubleMetricFieldType();
        EnumMap<Metric, NumberFieldMapper.NumberFieldType> metricFields = new EnumMap<>(Metric.class);
        for (Metric m : List.of(Metric.min, Metric.max)) {
            String fieldName = "foo" + "._" + m.name();
            NumberFieldMapper.NumberFieldType subfield = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.DOUBLE);
            subfield.setName(fieldName);
            metricFields.put(m, subfield);
        }
        fieldType.setMetricFields(metricFields);
        fieldType.setDefaultMetric(Metric.max);
        return fieldType;
    }

    public void testTermQuery() {
        final MappedFieldType fieldType = createDefaultFieldType();
        Query query = fieldType.termQuery(55.2, null);
        assertThat(query, equalTo(DoublePoint.newRangeQuery("foo._max", 55.2, 55.2)));
    }

    public void testTermsQuery() {
        final MappedFieldType fieldType = createDefaultFieldType();
        Query query = fieldType.termsQuery(asList(55.2, 500.3), null);
        assertThat(query, equalTo(DoublePoint.newSetQuery("foo._max", 55.2, 500.3)));
    }

    public void testRangeQuery() throws Exception {
        final MappedFieldType fieldType = createDefaultFieldType();
        Query query = fieldType.rangeQuery(10.1, 100.1, true, true, null, null, null, null);
        assertThat(query, instanceOf(IndexOrDocValuesQuery.class));
    }
}
