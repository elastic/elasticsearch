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

package org.elasticsearch.search.aggregations.bucket.nested;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NestedPathFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalMax;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.search.aggregations.AggregationBuilders.max;
import static org.elasticsearch.search.aggregations.AggregationBuilders.nested;
import static org.elasticsearch.search.aggregations.AggregationBuilders.reverseNested;

public class ReverseNestedAggregatorTests extends AggregatorTestCase {

    private static final String VALUE_FIELD_NAME = "number";
    private static final String NESTED_OBJECT = "nested_object";
    private static final String NESTED_AGG = "nestedAgg";
    private static final String REVERSE_AGG_NAME = "reverseNestedAgg";
    private static final String MAX_AGG_NAME = "maxAgg";

    /**
     * For each provided field type, we also register an alias with name {@code <field>-alias}.
     */
    @Override
    protected Map<String, MappedFieldType> getFieldAliases(MappedFieldType... fieldTypes) {
        return Arrays.stream(fieldTypes).collect(Collectors.toMap(
            ft -> ft.name() + "-alias",
            Function.identity()));
    }

    public void testNoDocs() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                // intentionally not writing any docs
            }
            try (IndexReader indexReader = wrap(DirectoryReader.open(directory))) {
                NestedAggregationBuilder nestedBuilder = new NestedAggregationBuilder(NESTED_AGG,
                        NESTED_OBJECT);
                ReverseNestedAggregationBuilder reverseNestedBuilder
                    = new ReverseNestedAggregationBuilder(REVERSE_AGG_NAME);
                nestedBuilder.subAggregation(reverseNestedBuilder);
                MaxAggregationBuilder maxAgg = new MaxAggregationBuilder(MAX_AGG_NAME)
                        .field(VALUE_FIELD_NAME);
                reverseNestedBuilder.subAggregation(maxAgg);
                MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(
                        NumberFieldMapper.NumberType.LONG);
                fieldType.setName(VALUE_FIELD_NAME);

                Nested nested = search(newSearcher(indexReader, false, true),
                        new MatchAllDocsQuery(), nestedBuilder, fieldType);
                ReverseNested reverseNested = (ReverseNested)
                        ((InternalAggregation)nested).getProperty(REVERSE_AGG_NAME);
                assertEquals(REVERSE_AGG_NAME, reverseNested.getName());
                assertEquals(0, reverseNested.getDocCount());

                InternalMax max = (InternalMax)
                        ((InternalAggregation)reverseNested).getProperty(MAX_AGG_NAME);
                assertEquals(MAX_AGG_NAME, max.getName());
                assertEquals(Double.NEGATIVE_INFINITY, max.getValue(), Double.MIN_VALUE);
            }
        }
    }

    public void testMaxFromParentDocs() throws IOException {
        int numParentDocs = randomIntBetween(1, 20);
        int expectedParentDocs = 0;
        int expectedNestedDocs = 0;
        double expectedMaxValue = Double.NEGATIVE_INFINITY;
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                for (int i = 0; i < numParentDocs; i++) {
                    List<Document> documents = new ArrayList<>();
                    int numNestedDocs = randomIntBetween(0, 20);
                    for (int nested = 0; nested < numNestedDocs; nested++) {
                        Document document = new Document();
                        document.add(new Field(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(i)),
                                IdFieldMapper.Defaults.NESTED_FIELD_TYPE));
                        document.add(new Field(NestedPathFieldMapper.NAME, NESTED_OBJECT,
                            NestedPathFieldMapper.Defaults.FIELD_TYPE));
                        documents.add(document);
                        expectedNestedDocs++;
                    }
                    Document document = new Document();
                    document.add(new Field(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(i)),
                            IdFieldMapper.Defaults.FIELD_TYPE));
                    document.add(new Field(NestedPathFieldMapper.NAME, "test",
                        NestedPathFieldMapper.Defaults.FIELD_TYPE));
                    long value = randomNonNegativeLong() % 10000;
                    document.add(new SortedNumericDocValuesField(VALUE_FIELD_NAME, value));
                    document.add(SeqNoFieldMapper.SequenceIDFields.emptySeqID().primaryTerm);
                    if (numNestedDocs > 0) {
                        expectedMaxValue = Math.max(expectedMaxValue, value);
                        expectedParentDocs++;
                    }
                    documents.add(document);
                    iw.addDocuments(documents);
                }
                iw.commit();
            }
            try (IndexReader indexReader = wrap(DirectoryReader.open(directory))) {
                NestedAggregationBuilder nestedBuilder = new NestedAggregationBuilder(NESTED_AGG,
                        NESTED_OBJECT);
                ReverseNestedAggregationBuilder reverseNestedBuilder
                    = new ReverseNestedAggregationBuilder(REVERSE_AGG_NAME);
                nestedBuilder.subAggregation(reverseNestedBuilder);
                MaxAggregationBuilder maxAgg = new MaxAggregationBuilder(MAX_AGG_NAME)
                        .field(VALUE_FIELD_NAME);
                reverseNestedBuilder.subAggregation(maxAgg);
                MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(
                        NumberFieldMapper.NumberType.LONG);
                fieldType.setName(VALUE_FIELD_NAME);

                Nested nested = search(newSearcher(indexReader, false, true),
                        new MatchAllDocsQuery(), nestedBuilder, fieldType);
                assertEquals(expectedNestedDocs, nested.getDocCount());

                ReverseNested reverseNested = (ReverseNested)
                        ((InternalAggregation)nested).getProperty(REVERSE_AGG_NAME);
                assertEquals(REVERSE_AGG_NAME, reverseNested.getName());
                assertEquals(expectedParentDocs, reverseNested.getDocCount());

                InternalMax max = (InternalMax)
                        ((InternalAggregation)reverseNested).getProperty(MAX_AGG_NAME);
                assertEquals(MAX_AGG_NAME, max.getName());
                assertEquals(expectedMaxValue, max.getValue(), Double.MIN_VALUE);
            }
        }
    }

    public void testFieldAlias() throws IOException {
        int numParentDocs = randomIntBetween(1, 20);
        int expectedParentDocs = 0;

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(
            NumberFieldMapper.NumberType.LONG);
        fieldType.setName(VALUE_FIELD_NAME);

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                for (int i = 0; i < numParentDocs; i++) {
                    List<Document> documents = new ArrayList<>();
                    int numNestedDocs = randomIntBetween(0, 20);
                    if (numNestedDocs > 0) {
                        expectedParentDocs++;
                    }

                    for (int nested = 0; nested < numNestedDocs; nested++) {
                        Document document = new Document();
                        document.add(new Field(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(i)),
                                IdFieldMapper.Defaults.NESTED_FIELD_TYPE));
                        document.add(new Field(NestedPathFieldMapper.NAME, NESTED_OBJECT,
                            NestedPathFieldMapper.Defaults.FIELD_TYPE));
                        documents.add(document);
                    }
                    Document document = new Document();
                    document.add(new Field(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(i)),
                            IdFieldMapper.Defaults.FIELD_TYPE));
                    document.add(new Field(NestedPathFieldMapper.NAME, "test",
                        NestedPathFieldMapper.Defaults.FIELD_TYPE));

                    long value = randomNonNegativeLong() % 10000;
                    document.add(new SortedNumericDocValuesField(VALUE_FIELD_NAME, value));
                    document.add(SeqNoFieldMapper.SequenceIDFields.emptySeqID().primaryTerm);
                    documents.add(document);
                    iw.addDocuments(documents);
                }
                iw.commit();
            }

            try (IndexReader indexReader = wrap(DirectoryReader.open(directory))) {
                MaxAggregationBuilder maxAgg = max(MAX_AGG_NAME).field(VALUE_FIELD_NAME);
                MaxAggregationBuilder aliasMaxAgg = max(MAX_AGG_NAME).field(VALUE_FIELD_NAME + "-alias");

                NestedAggregationBuilder agg = nested(NESTED_AGG, NESTED_OBJECT).subAggregation(
                    reverseNested(REVERSE_AGG_NAME).subAggregation(maxAgg));
                NestedAggregationBuilder aliasAgg = nested(NESTED_AGG, NESTED_OBJECT).subAggregation(
                    reverseNested(REVERSE_AGG_NAME).subAggregation(aliasMaxAgg));

                Nested nested = search(newSearcher(indexReader, false, true),
                        new MatchAllDocsQuery(), agg, fieldType);
                Nested aliasNested = search(newSearcher(indexReader, false, true),
                    new MatchAllDocsQuery(), aliasAgg, fieldType);

                ReverseNested reverseNested = nested.getAggregations().get(REVERSE_AGG_NAME);
                ReverseNested aliasReverseNested = aliasNested.getAggregations().get(REVERSE_AGG_NAME);

                assertEquals(reverseNested, aliasReverseNested);
                assertEquals(expectedParentDocs, reverseNested.getDocCount());
            }
        }
    }

}
