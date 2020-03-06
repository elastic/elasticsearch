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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.index.mapper.RangeFieldMapper;
import org.elasticsearch.index.mapper.RangeType;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static java.util.Collections.singleton;


public class MissingAggregatorTests extends AggregatorTestCase {
    public void testMatchNoDocs() throws IOException {
        int numDocs = randomIntBetween(10, 200);
        final MappedFieldType fieldType = new NumberFieldMapper.Builder("_name", NumberType.LONG).fieldType();
        fieldType.setName("field");
        testBothCases(numDocs,
            fieldType.name(),
            Queries.newMatchAllQuery(),
            builder -> {},
            (index, doc) -> doc.add(new SortedNumericDocValuesField(fieldType.name(), randomLong())),
            internalMissing -> {
                assertEquals(internalMissing.getDocCount(), 0);
                assertFalse(AggregationInspectionHelper.hasValue(internalMissing));
            },
            singleton(fieldType));
    }

    public void testMatchAllDocs() throws IOException {
        int numDocs = randomIntBetween(10, 200);

        final MappedFieldType fieldType = new NumberFieldMapper.Builder("_name", NumberType.LONG).fieldType();
        fieldType.setName("field");
        final MappedFieldType anotherFieldType = new NumberFieldMapper.Builder("_name", NumberType.LONG).fieldType();
        anotherFieldType.setName("another_field");

        testBothCases(numDocs,
            fieldType.name(),
            Queries.newMatchAllQuery(),
            builder -> {},
            (index, doc) -> doc.add(new SortedNumericDocValuesField(anotherFieldType.name(), randomLong())),
            internalMissing -> {
                assertEquals(internalMissing.getDocCount(), numDocs);
                assertTrue(AggregationInspectionHelper.hasValue(internalMissing));
            },
            List.of(fieldType, anotherFieldType));
    }

    public void testMatchSparse() throws IOException {
        int numDocs = randomIntBetween(100, 200);
        final AtomicInteger count = new AtomicInteger();

        final MappedFieldType fieldType = new NumberFieldMapper.Builder("_name", NumberType.LONG).fieldType();
        fieldType.setName("field");
        final MappedFieldType anotherFieldType = new NumberFieldMapper.Builder("_name", NumberType.LONG).fieldType();
        anotherFieldType.setName("another_field");

        testBothCases(numDocs,
            fieldType.name(),
            Queries.newMatchAllQuery(),
            builder -> {},
            (index, doc) -> {
                if (randomBoolean()) {
                    doc.add(new SortedNumericDocValuesField(anotherFieldType.name(), randomLong()));
                    count.incrementAndGet();
                } else {
                    doc.add(new SortedNumericDocValuesField(fieldType.name(), randomLong()));
                }
            },
            internalMissing -> {
                assertEquals(internalMissing.getDocCount(), count.get());
                count.set(0);
                assertTrue(AggregationInspectionHelper.hasValue(internalMissing));
            },
            List.of(fieldType, anotherFieldType));
    }

    public void testMatchSparseRangeField() throws IOException {
        int numDocs = randomIntBetween(100, 200);
        final AtomicInteger count = new AtomicInteger();

        final String fieldName = "field";
        final RangeType rangeType = RangeType.DOUBLE;
        MappedFieldType fieldType = new RangeFieldMapper.Builder("_name", rangeType).fieldType();
        fieldType.setName(fieldName);

        final RangeFieldMapper.Range range = new RangeFieldMapper.Range(rangeType, 1.0D, 5.0D, true, true);
        final BytesRef encodedRange = rangeType.encodeRanges(singleton(range));
        final BinaryDocValuesField field = new BinaryDocValuesField(fieldName, encodedRange);

        final MappedFieldType anotherFieldType = new RangeFieldMapper.Builder("_name", rangeType).fieldType();
        anotherFieldType.setName("another_field");

        testBothCases(numDocs,
            fieldName,
            Queries.newMatchAllQuery(),
            builder -> {},
            (index, doc) -> {
                if (randomBoolean()) {
                    doc.add(new SortedNumericDocValuesField(anotherFieldType.name(), randomLong()));
                    count.incrementAndGet();
                } else {
                    doc.add(field);
                }
            },
            internalMissing -> {
                assertEquals(internalMissing.getDocCount(), count.get());
                count.set(0);
                assertTrue(AggregationInspectionHelper.hasValue(internalMissing));
            },
            List.of(fieldType, anotherFieldType));
    }


    public void testUnmappedWithoutMissingParam() throws IOException {
        int numDocs = randomIntBetween(10, 20);
        final MappedFieldType fieldType = new NumberFieldMapper.Builder("_name", NumberType.LONG).fieldType();
        fieldType.setName("field");
        testBothCases(numDocs,
            "unknown_field",
            Queries.newMatchAllQuery(),
            builder -> {},
            (index, doc) -> doc.add(new SortedNumericDocValuesField(fieldType.name(), randomLong())),
            internalMissing -> {
                assertEquals(internalMissing.getDocCount(), numDocs);
                assertTrue(AggregationInspectionHelper.hasValue(internalMissing));
            },
            singleton(fieldType));
    }

    public void testUnmappedWithMissingParam() throws IOException {
        final int numDocs = randomIntBetween(10, 20);
        final MappedFieldType fieldType = new NumberFieldMapper.Builder("_name", NumberType.LONG).fieldType();
        fieldType.setName("field");
        testBothCases(numDocs,
            "unknown_field",
            Queries.newMatchAllQuery(),
            builder -> builder.missing(randomLong()),
            (index, doc) -> doc.add(new SortedNumericDocValuesField(fieldType.name(), randomLong())),
            internalMissing -> {
                assertEquals(internalMissing.getDocCount(), 0);
                assertFalse(AggregationInspectionHelper.hasValue(internalMissing));
            },
            singleton(fieldType));
    }

    public void testMissingParam() throws IOException {
        final int numDocs = randomIntBetween(100, 200);

        final MappedFieldType fieldType = new NumberFieldMapper.Builder("_name", NumberType.LONG).fieldType();
        fieldType.setName("field");
        final MappedFieldType anotherFieldType = new NumberFieldMapper.Builder("_name", NumberType.LONG).fieldType();
        anotherFieldType.setName("another_field");

        testBothCases(numDocs,
            anotherFieldType.name(),
            Queries.newMatchAllQuery(),
            builder -> builder.missing(randomLong()),
            (index, doc) -> {
                doc.add(new SortedNumericDocValuesField(fieldType.name(), randomLong()));
                if (index % 10 == 0) {
                    doc.add(new SortedNumericDocValuesField(anotherFieldType.name(), randomLong()));
                }
            },
            internalMissing -> {
                assertEquals(internalMissing.getDocCount(), 0);
                assertFalse(AggregationInspectionHelper.hasValue(internalMissing));
            },
            List.of(fieldType, anotherFieldType));
    }

    private void testBothCases(int numDocs,
                               String fieldName,
                               Query query,
                               Consumer<MissingAggregationBuilder> builderConsumer,
                               BiConsumer<Integer, Document> documentConsumer,
                               Consumer<InternalMissing> verify,
                               Collection<MappedFieldType> fieldTypes) throws IOException {
        executeTestCase(numDocs, fieldName, query, builderConsumer, documentConsumer, verify, false, fieldTypes);
        executeTestCase(numDocs, fieldName, query, builderConsumer, documentConsumer, verify, true, fieldTypes);

    }

    private void executeTestCase(int numDocs,
                                 String fieldName,
                                 Query query,
                                 Consumer<MissingAggregationBuilder> builderConsumer,
                                 BiConsumer<Integer, Document> documentConsumer,
                                 Consumer<InternalMissing> verify,
                                 boolean reduced,
                                 Collection<MappedFieldType> fieldTypes) throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                for (int i = 0; i < numDocs; i++) {
                    if (frequently()) {
                        indexWriter.commit();
                    }
                    documentConsumer.accept(i, document);
                    indexWriter.addDocument(document);
                    document.clear();
                }
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher =
                    newSearcher(indexReader, true, true);
                MissingAggregationBuilder builder = new MissingAggregationBuilder("_name", null);
                builder.field(fieldName);
                builderConsumer.accept(builder);

                final MappedFieldType[] fieldTypesArray = fieldTypes.toArray(new MappedFieldType[0]);
                InternalMissing missing;
                if (reduced) {
                    missing = searchAndReduce(indexSearcher, query, builder, fieldTypesArray);
                } else {
                    missing = search(indexSearcher, query, builder, fieldTypesArray);
                }
                verify.accept(missing);
            }
        }
    }
}
