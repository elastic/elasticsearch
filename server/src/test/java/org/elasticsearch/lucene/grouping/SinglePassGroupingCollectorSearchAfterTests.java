/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.lucene.grouping;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MockFieldMapper;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * This test is adapted from {@link SinglePassGroupingCollectorTests} with
 * modifications to test {@link FieldDoc after} parameter.
 */
public class SinglePassGroupingCollectorSearchAfterTests extends ESTestCase {
    interface CollapsingDocValuesProducer<T extends Comparable<?>> {
        T randomGroup(int maxGroup);

        void add(Document doc, T value);

        SortField sortField(boolean reversed);
    }

    private <T extends Comparable<T>> void assertSearchCollapse(CollapsingDocValuesProducer<T> dvProducers, boolean numeric)
        throws IOException {
        assertSearchCollapse(dvProducers, numeric, false);
        assertSearchCollapse(dvProducers, numeric, true);
    }

    private <T extends Comparable<T>> void assertSearchCollapse(
        CollapsingDocValuesProducer<T> dvProducers,
        boolean numeric,
        boolean reverseSort
    ) throws IOException {
        Directory dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir);

        Set<T> values = new HashSet<>();
        int totalHits = 0;
        boolean docsWithMissingField = false;

        int numDocs = randomIntBetween(1000, 2000);
        int maxGroup = randomIntBetween(2, 500);
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            if (frequently()) {
                T value = dvProducers.randomGroup(maxGroup);
                values.add(value);
                dvProducers.add(doc, value);
            } else {
                // Introduce some documents with missing sort values.
                doc.add(new SortedNumericDocValuesField("other-field", randomInt()));
                docsWithMissingField = true;
            }
            w.addDocument(doc);
            totalHits++;
        }

        IndexReader reader = w.getReader();
        IndexSearcher searcher = newSearcher(reader);

        SortField sortField = dvProducers.sortField(reverseSort);
        MappedFieldType fieldType = new MockFieldMapper.FakeFieldType(sortField.getField());
        Sort sort = new Sort(sortField);

        Comparator<T> comparator = reverseSort ? Collections.reverseOrder() : Comparator.naturalOrder();
        List<T> sortedValues = new ArrayList<>(values);
        sortedValues.sort(comparator);

        int randomIndex = randomIntBetween(0, sortedValues.size() - 1);
        int expectedNumGroups = values.size() - randomIndex - 1;
        if (docsWithMissingField) {
            expectedNumGroups++;
        }

        FieldDoc after = new FieldDoc(Integer.MAX_VALUE, 0, new Object[] { sortedValues.get(randomIndex) });
        SinglePassGroupingCollector<?> collapsingCollector = numeric
            ? SinglePassGroupingCollector.createNumeric("field", fieldType, sort, expectedNumGroups, after)
            : SinglePassGroupingCollector.createKeyword("field", fieldType, sort, expectedNumGroups, after);

        TopFieldCollector topFieldCollector = TopFieldCollector.create(sort, totalHits, after, Integer.MAX_VALUE);
        Query query = new MatchAllDocsQuery();
        searcher.search(query, collapsingCollector);
        searcher.search(query, topFieldCollector);
        TopFieldGroups collapseTopFieldDocs = collapsingCollector.getTopGroups(0);
        TopFieldDocs topDocs = topFieldCollector.topDocs();
        assertEquals(sortField.getField(), collapseTopFieldDocs.field);
        assertEquals(totalHits, collapseTopFieldDocs.totalHits.value);
        assertEquals(expectedNumGroups, collapseTopFieldDocs.scoreDocs.length);

        assertEquals(TotalHits.Relation.EQUAL_TO, collapseTopFieldDocs.totalHits.relation);
        assertEquals(totalHits, topDocs.totalHits.value);

        Object currentValue = null;
        int topDocsIndex = 0;
        for (int i = 0; i < expectedNumGroups; i++) {
            FieldDoc fieldDoc = null;
            for (; topDocsIndex < topDocs.scoreDocs.length; topDocsIndex++) {
                fieldDoc = (FieldDoc) topDocs.scoreDocs[topDocsIndex];
                if (Objects.equals(fieldDoc.fields[0], currentValue) == false) {
                    break;
                }
            }
            FieldDoc collapseFieldDoc = (FieldDoc) collapseTopFieldDocs.scoreDocs[i];
            assertNotNull(fieldDoc);
            assertEquals(collapseFieldDoc.fields[0], fieldDoc.fields[0]);
            currentValue = fieldDoc.fields[0];
        }

        for (; topDocsIndex < topDocs.scoreDocs.length; topDocsIndex++) {
            FieldDoc fieldDoc = (FieldDoc) topDocs.scoreDocs[topDocsIndex];
            assertEquals(fieldDoc.fields[0], currentValue);
        }

        w.close();
        reader.close();
        dir.close();
    }

    public void testCollapseLong() throws Exception {
        CollapsingDocValuesProducer<Long> producer = new CollapsingDocValuesProducer<>() {
            @Override
            public Long randomGroup(int maxGroup) {
                return randomNonNegativeLong() % maxGroup;
            }

            @Override
            public void add(Document doc, Long value) {
                doc.add(new NumericDocValuesField("field", value));
            }

            @Override
            public SortField sortField(boolean reversed) {
                SortField sortField = new SortField("field", SortField.Type.LONG, reversed);
                sortField.setMissingValue(reversed ? Long.MIN_VALUE : Long.MAX_VALUE);
                return sortField;
            }
        };
        assertSearchCollapse(producer, true);
    }

    public void testCollapseInt() throws Exception {
        CollapsingDocValuesProducer<Integer> producer = new CollapsingDocValuesProducer<>() {
            @Override
            public Integer randomGroup(int maxGroup) {
                return randomIntBetween(0, maxGroup - 1);
            }

            @Override
            public void add(Document doc, Integer value) {
                doc.add(new NumericDocValuesField("field", value));
            }

            @Override
            public SortField sortField(boolean reversed) {
                SortField sortField = new SortField("field", SortField.Type.INT, reversed);
                sortField.setMissingValue(reversed ? Integer.MIN_VALUE : Integer.MAX_VALUE);
                return sortField;
            }
        };
        assertSearchCollapse(producer, true);
    }

    public void testCollapseFloat() throws Exception {
        CollapsingDocValuesProducer<Float> producer = new CollapsingDocValuesProducer<>() {
            @Override
            public Float randomGroup(int maxGroup) {
                return Float.valueOf(randomIntBetween(0, maxGroup - 1));
            }

            @Override
            public void add(Document doc, Float value) {
                doc.add(new NumericDocValuesField("field", Float.floatToIntBits(value)));
            }

            @Override
            public SortField sortField(boolean reversed) {
                SortField sortField = new SortField("field", SortField.Type.FLOAT, reversed);
                sortField.setMissingValue(reversed ? Float.NEGATIVE_INFINITY : Float.POSITIVE_INFINITY);
                return sortField;
            }
        };
        assertSearchCollapse(producer, true);
    }

    public void testCollapseDouble() throws Exception {
        CollapsingDocValuesProducer<Double> producer = new CollapsingDocValuesProducer<>() {
            @Override
            public Double randomGroup(int maxGroup) {
                return Double.valueOf(randomIntBetween(0, maxGroup - 1));
            }

            @Override
            public void add(Document doc, Double value) {
                doc.add(new NumericDocValuesField("field", Double.doubleToLongBits(value)));
            }

            @Override
            public SortField sortField(boolean reversed) {
                SortField sortField = new SortField("field", SortField.Type.DOUBLE, reversed);
                sortField.setMissingValue(reversed ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY);
                return sortField;
            }
        };
        assertSearchCollapse(producer, true);
    }

    public void testCollapseString() throws Exception {
        CollapsingDocValuesProducer<BytesRef> producer = new CollapsingDocValuesProducer<>() {
            @Override
            public BytesRef randomGroup(int maxGroup) {
                return new BytesRef(Integer.toString(randomIntBetween(0, maxGroup - 1)));
            }

            @Override
            public void add(Document doc, BytesRef value) {
                doc.add(new SortedDocValuesField("field", value));
            }

            @Override
            public SortField sortField(boolean reversed) {
                SortField sortField = new SortField("field", SortField.Type.STRING, reversed);
                sortField.setMissingValue(reversed ? SortField.STRING_FIRST : SortField.STRING_LAST);
                return sortField;
            }
        };
        assertSearchCollapse(producer, false);
    }
}
