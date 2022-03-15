/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.support;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;


public class AggregationContextIsMultivalueTest extends AggregatorTestCase {
    @Test
    public void testIsMultiValued() throws IOException {
        String sv_field_name = "single_value_field";
        String mv_field_name = "multi_valued_field";
        NumberFieldMapper.NumberFieldType svft = new NumberFieldMapper.NumberFieldType(
            sv_field_name,
            NumberFieldMapper.NumberType.LONG,
            true,  // isIndexed
            true,  // isStored
            true,  // hasDocValues
            false, // coerce
            0,     // nullValue
            Collections.emptyMap(), // meta
            null, // Script
            false, // isDimension
            null // metricType
        );
        NumberFieldMapper.NumberFieldType mvft = new NumberFieldMapper.NumberFieldType(
            mv_field_name,
            NumberFieldMapper.NumberType.LONG,
            true,  // isIndexed
            true,  // isStored
            true,  // hasDocValues
            false, // coerce
            0,     // nullValue
            Collections.emptyMap(), // meta
            null, // Script
            false, // isDimension
            null // metricType
        );
        try (Directory directory = newDirectory(); RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
            indexWriter.addDocument(
                List.of(
                    new SortedNumericDocValuesField(sv_field_name, 1L),
                    new LongPoint(sv_field_name, 1L),
                    new SortedNumericDocValuesField(mv_field_name, 2L),
                    new LongPoint(mv_field_name, 2L),
                    new SortedNumericDocValuesField(mv_field_name, 3L),
                    new LongPoint(mv_field_name, 3L)
                )
            );
            indexWriter.addDocument(
                List.of(
                    new SortedNumericDocValuesField(sv_field_name, 1L),
                    new LongPoint(sv_field_name, 1L),
                    new SortedNumericDocValuesField(mv_field_name, 2L),
                    new LongPoint(mv_field_name, 2L),
                    new SortedNumericDocValuesField(mv_field_name, 3L),
                    new LongPoint(mv_field_name, 3L)
                )
            );
            indexWriter.addDocument(
                List.of(
                    new SortedNumericDocValuesField(sv_field_name, 1L),
                    new LongPoint(sv_field_name, 1L),
                    new SortedNumericDocValuesField(mv_field_name, 2L),
                    new LongPoint(mv_field_name, 2L),
                    new SortedNumericDocValuesField(mv_field_name, 3L),
                    new LongPoint(mv_field_name, 3L)
                )
            );
            try (IndexReader reader = indexWriter.getReader()) {
                AggregationContext context = createAggregationContext(new IndexSearcher(reader), new MatchAllDocsQuery(), svft, mvft);
                assertTrue(context.isMultiValued(mv_field_name));
                assertFalse(context.isMultiValued(sv_field_name));
            }
        }
    }

    @Test
    public void testIsMultiValuedDocValuesOnly() throws IOException {
        String sv_field_name = "single_value_field";
        String mv_field_name = "multi_valued_field";
        NumberFieldMapper.NumberFieldType svft = new NumberFieldMapper.NumberFieldType(
            sv_field_name,
            NumberFieldMapper.NumberType.LONG,
            false,  // isIndexed
            true,  // isStored
            true,  // hasDocValues
            false, // coerce
            0,     // nullValue
            Collections.emptyMap(), // meta
            null, // Script
            false, // isDimension
            null // metricType
        );
        NumberFieldMapper.NumberFieldType mvft = new NumberFieldMapper.NumberFieldType(
            mv_field_name,
            NumberFieldMapper.NumberType.LONG,
            false,  // isIndexed
            true,  // isStored
            true,  // hasDocValues
            false, // coerce
            0,     // nullValue
            Collections.emptyMap(), // meta
            null, // Script
            false, // isDimension
            null // metricType
        );
        try (Directory directory = newDirectory(); RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
            indexWriter.addDocument(
                List.of(
                    new SortedNumericDocValuesField(sv_field_name, 1L),
                    new SortedNumericDocValuesField(mv_field_name, 2L),
                    new SortedNumericDocValuesField(mv_field_name, 3L)
                )
            );
            indexWriter.addDocument(
                List.of(
                    new SortedNumericDocValuesField(sv_field_name, 1L),
                    new SortedNumericDocValuesField(mv_field_name, 2L),
                    new SortedNumericDocValuesField(mv_field_name, 3L)
                )
            );
            indexWriter.addDocument(
                List.of(
                    new SortedNumericDocValuesField(sv_field_name, 1L),
                    new SortedNumericDocValuesField(mv_field_name, 2L),
                    new SortedNumericDocValuesField(mv_field_name, 3L)
                )
            );
            try (IndexReader reader = indexWriter.getReader()) {
                AggregationContext context = createAggregationContext(new IndexSearcher(reader), new MatchAllDocsQuery(), svft, mvft);
                assertTrue(context.isMultiValued(mv_field_name));
                assertFalse(context.isMultiValued(sv_field_name));
            }
        }
    }
    @Test
    public void testIsMultiValuedNoValues() throws IOException {
        String sv_field_name = "single_value_field";
        NumberFieldMapper.NumberFieldType svft = new NumberFieldMapper.NumberFieldType(
            sv_field_name,
            NumberFieldMapper.NumberType.LONG,
            true,  // isIndexed
            true,  // isStored
            true,  // hasDocValues
            false, // coerce
            0,     // nullValue
            Collections.emptyMap(), // meta
            null, // Script
            false, // isDimension
            null // metricType
        );
        try (Directory directory = newDirectory(); RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
            indexWriter.addDocument(
                List.of(
                    new LongPoint("unrelated_field", 100)
                )
            );
            try (IndexReader reader = indexWriter.getReader()) {
                AggregationContext context = createAggregationContext(new IndexSearcher(reader), new MatchAllDocsQuery(), svft);
                assertFalse(context.isMultiValued(sv_field_name));
            }
        }
    }
}
