/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.lucene.grouping;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.CompositeReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.search.CheckHits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.columnar.string.StringBinaryPayload;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fielddata.SortableBinaryDocValues;
import org.elasticsearch.index.mapper.BinaryDocValuesFormat;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MockFieldMapper;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

public class SinglePassGroupingCollectorTests extends ESTestCase {
    private static class SegmentSearcher extends IndexSearcher {
        private final LeafReaderContextPartition[] ctx;

        SegmentSearcher(LeafReaderContext ctx, IndexReaderContext parent) {
            super(parent);
            this.ctx = new LeafReaderContextPartition[] { IndexSearcher.LeafReaderContextPartition.createForEntireSegment(ctx) };
        }

        public void search(Weight weight, Collector collector) throws IOException {
            search(ctx, weight, collector);
        }

        @Override
        public String toString() {
            return "ShardSearcher(" + ctx[0] + ")";
        }
    }

    interface CollapsingDocValuesProducer<T extends Comparable<?>> {
        T randomGroup(int maxGroup);

        void add(Document doc, T value, boolean multivalued);

        SortField sortField(boolean multivalued);
    }

    <T extends Comparable<T>> void assertSearchCollapse(CollapsingDocValuesProducer<T> dvProducers, boolean numeric) throws IOException {
        assertSearchCollapse(dvProducers, numeric, true);
        assertSearchCollapse(dvProducers, numeric, false);
    }

    private <T extends Comparable<T>> void assertSearchCollapse(
        CollapsingDocValuesProducer<T> dvProducers,
        boolean numeric,
        boolean multivalued
    ) throws IOException {
        final int numDocs = randomIntBetween(1000, 2000);
        int maxGroup = randomIntBetween(2, 500);
        final Directory dir = newDirectory();
        final RandomIndexWriter w = new RandomIndexWriter(random(), dir);
        Set<T> values = new HashSet<>();
        int totalHits = 0;
        for (int i = 0; i < numDocs; i++) {
            final T value = dvProducers.randomGroup(maxGroup);
            values.add(value);
            Document doc = new Document();
            dvProducers.add(doc, value, multivalued);
            doc.add(new NumericDocValuesField("sort1", randomIntBetween(0, 10)));
            doc.add(new NumericDocValuesField("sort2", randomLong()));
            w.addDocument(doc);
            totalHits++;
        }

        List<T> valueList = new ArrayList<>(values);
        Collections.sort(valueList);
        final IndexReader reader = w.getReader();
        final IndexSearcher searcher = newSearcher(reader);
        final SortField collapseField = dvProducers.sortField(multivalued);
        final SortField sort1 = new SortField("sort1", SortField.Type.INT);
        final SortField sort2 = new SortField("sort2", SortField.Type.LONG);
        Sort sort = new Sort(sort1, sort2, collapseField);

        MappedFieldType fieldType = new MockFieldMapper.FakeFieldType(collapseField.getField());

        int expectedNumGroups = values.size();

        final SinglePassGroupingCollector<?> collapsingCollector;
        if (numeric) {
            collapsingCollector = SinglePassGroupingCollector.createNumeric(
                collapseField.getField(),
                fieldType,
                sort,
                expectedNumGroups,
                null
            );
        } else {
            collapsingCollector = SinglePassGroupingCollector.createKeyword(
                collapseField.getField(),
                fieldType,
                null,
                sort,
                expectedNumGroups,
                null
            );
        }

        TopFieldCollectorManager topFieldCollectorManager = new TopFieldCollectorManager(sort, totalHits, Integer.MAX_VALUE);
        Query query = Queries.ALL_DOCS_INSTANCE;
        searcher.search(query, collapsingCollector);
        TopFieldDocs topDocs = searcher.search(query, topFieldCollectorManager);
        TopFieldGroups collapseTopFieldDocs = collapsingCollector.getTopGroups(0);
        assertEquals(collapseField.getField(), collapseTopFieldDocs.field);
        assertEquals(expectedNumGroups, collapseTopFieldDocs.scoreDocs.length);
        assertEquals(totalHits, collapseTopFieldDocs.totalHits.value());
        assertEquals(TotalHits.Relation.EQUAL_TO, collapseTopFieldDocs.totalHits.relation());
        assertEquals(totalHits, topDocs.scoreDocs.length);
        assertEquals(totalHits, topDocs.totalHits.value());

        Set<Object> seen = new HashSet<>();
        // collapse field is the last sort
        int collapseIndex = sort.getSort().length - 1;
        int topDocsIndex = 0;
        for (int i = 0; i < expectedNumGroups; i++) {
            FieldDoc fieldDoc = null;
            for (; topDocsIndex < totalHits; topDocsIndex++) {
                fieldDoc = (FieldDoc) topDocs.scoreDocs[topDocsIndex];
                if (seen.contains(fieldDoc.fields[collapseIndex]) == false) {
                    break;
                }
            }
            FieldDoc collapseFieldDoc = (FieldDoc) collapseTopFieldDocs.scoreDocs[i];
            assertNotNull(fieldDoc);
            assertEquals(collapseFieldDoc.doc, fieldDoc.doc);
            assertArrayEquals(collapseFieldDoc.fields, fieldDoc.fields);
            seen.add(fieldDoc.fields[fieldDoc.fields.length - 1]);
        }
        for (; topDocsIndex < totalHits; topDocsIndex++) {
            FieldDoc fieldDoc = (FieldDoc) topDocs.scoreDocs[topDocsIndex];
            assertTrue(seen.contains(fieldDoc.fields[collapseIndex]));
        }

        // check merge
        final IndexReaderContext ctx = searcher.getTopReaderContext();
        final SegmentSearcher[] subSearchers;
        final int[] docStarts;

        if (ctx instanceof LeafReaderContext) {
            subSearchers = new SegmentSearcher[1];
            docStarts = new int[1];
            subSearchers[0] = new SegmentSearcher((LeafReaderContext) ctx, ctx);
            docStarts[0] = 0;
        } else {
            final CompositeReaderContext compCTX = (CompositeReaderContext) ctx;
            final int size = compCTX.leaves().size();
            subSearchers = new SegmentSearcher[size];
            docStarts = new int[size];
            int docBase = 0;
            for (int searcherIDX = 0; searcherIDX < subSearchers.length; searcherIDX++) {
                final LeafReaderContext leave = compCTX.leaves().get(searcherIDX);
                subSearchers[searcherIDX] = new SegmentSearcher(leave, compCTX);
                docStarts[searcherIDX] = docBase;
                docBase += leave.reader().maxDoc();
            }
        }

        final TopFieldGroups[] shardHits = new TopFieldGroups[subSearchers.length];
        final Weight weight = searcher.createWeight(searcher.rewrite(Queries.ALL_DOCS_INSTANCE), ScoreMode.COMPLETE, 1f);
        for (int shardIDX = 0; shardIDX < subSearchers.length; shardIDX++) {
            final SegmentSearcher subSearcher = subSearchers[shardIDX];
            final SinglePassGroupingCollector<?> c;
            if (numeric) {
                c = SinglePassGroupingCollector.createNumeric(collapseField.getField(), fieldType, sort, expectedNumGroups, null);
            } else {
                c = SinglePassGroupingCollector.createKeyword(collapseField.getField(), fieldType, null, sort, expectedNumGroups, null);
            }
            subSearcher.search(weight, c);
            shardHits[shardIDX] = c.getTopGroups(0);
        }
        TopFieldGroups mergedFieldDocs = TopFieldGroups.merge(sort, 0, expectedNumGroups, shardHits, true);
        assertTopDocsEquals(query, mergedFieldDocs, collapseTopFieldDocs);
        w.close();
        reader.close();
        dir.close();
    }

    private static void assertTopDocsEquals(Query query, TopFieldGroups topDocs1, TopFieldGroups topDocs2) {
        CheckHits.checkEqual(query, topDocs1.scoreDocs, topDocs2.scoreDocs);
        assertArrayEquals(topDocs1.groupValues, topDocs2.groupValues);
    }

    public void testCollapseLong() throws Exception {
        CollapsingDocValuesProducer<Long> producer = new CollapsingDocValuesProducer<Long>() {
            @Override
            public Long randomGroup(int maxGroup) {
                return randomNonNegativeLong() % maxGroup;
            }

            @Override
            public void add(Document doc, Long value, boolean multivalued) {
                if (multivalued) {
                    doc.add(new SortedNumericDocValuesField("field", value));
                } else {
                    doc.add(new NumericDocValuesField("field", value));
                }
            }

            @Override
            public SortField sortField(boolean multivalued) {
                if (multivalued) {
                    return new SortedNumericSortField("field", SortField.Type.LONG);
                } else {
                    return new SortField("field", SortField.Type.LONG);
                }
            }
        };
        assertSearchCollapse(producer, true);
    }

    public void testCollapseInt() throws Exception {
        CollapsingDocValuesProducer<Integer> producer = new CollapsingDocValuesProducer<Integer>() {
            @Override
            public Integer randomGroup(int maxGroup) {
                return randomIntBetween(0, maxGroup - 1);
            }

            @Override
            public void add(Document doc, Integer value, boolean multivalued) {
                if (multivalued) {
                    doc.add(new SortedNumericDocValuesField("field", value));
                } else {
                    doc.add(new NumericDocValuesField("field", value));
                }
            }

            @Override
            public SortField sortField(boolean multivalued) {
                if (multivalued) {
                    return new SortedNumericSortField("field", SortField.Type.INT);
                } else {
                    return new SortField("field", SortField.Type.INT);
                }
            }
        };
        assertSearchCollapse(producer, true);
    }

    public void testCollapseFloat() throws Exception {
        CollapsingDocValuesProducer<Float> producer = new CollapsingDocValuesProducer<Float>() {
            @Override
            public Float randomGroup(int maxGroup) {
                return Float.valueOf(randomIntBetween(0, maxGroup - 1));
            }

            @Override
            public void add(Document doc, Float value, boolean multivalued) {
                if (multivalued) {
                    doc.add(new SortedNumericDocValuesField("field", NumericUtils.floatToSortableInt(value)));
                } else {
                    doc.add(new NumericDocValuesField("field", Float.floatToIntBits(value)));
                }
            }

            @Override
            public SortField sortField(boolean multivalued) {
                if (multivalued) {
                    return new SortedNumericSortField("field", SortField.Type.FLOAT);
                } else {
                    return new SortField("field", SortField.Type.FLOAT);
                }
            }
        };
        assertSearchCollapse(producer, true);
    }

    public void testCollapseDouble() throws Exception {
        CollapsingDocValuesProducer<Double> producer = new CollapsingDocValuesProducer<Double>() {
            @Override
            public Double randomGroup(int maxGroup) {
                return Double.valueOf(randomIntBetween(0, maxGroup - 1));
            }

            @Override
            public void add(Document doc, Double value, boolean multivalued) {
                if (multivalued) {
                    doc.add(new SortedNumericDocValuesField("field", NumericUtils.doubleToSortableLong(value)));
                } else {
                    doc.add(new NumericDocValuesField("field", Double.doubleToLongBits(value)));
                }
            }

            @Override
            public SortField sortField(boolean multivalued) {
                if (multivalued) {
                    return new SortedNumericSortField("field", SortField.Type.DOUBLE);
                } else {
                    return new SortField("field", SortField.Type.DOUBLE);
                }
            }
        };
        assertSearchCollapse(producer, true);
    }

    public void testCollapseString() throws Exception {
        CollapsingDocValuesProducer<BytesRef> producer = new CollapsingDocValuesProducer<BytesRef>() {
            @Override
            public BytesRef randomGroup(int maxGroup) {
                return new BytesRef(Integer.toString(randomIntBetween(0, maxGroup - 1)));
            }

            @Override
            public void add(Document doc, BytesRef value, boolean multivalued) {
                if (multivalued) {
                    doc.add(new SortedSetDocValuesField("field", value));
                } else {
                    doc.add(new SortedDocValuesField("field", value));
                }
            }

            @Override
            public SortField sortField(boolean multivalued) {
                if (multivalued) {
                    return new SortedSetSortField("field", false);
                } else {
                    return new SortField("field", SortField.Type.STRING);
                }
            }
        };
        assertSearchCollapse(producer, false);
    }

    public void testEmptyNumericSegment() throws Exception {
        final Directory dir = newDirectory();
        final RandomIndexWriter w = new RandomIndexWriter(random(), dir);
        Document doc = new Document();
        doc.add(new NumericDocValuesField("group", 0));
        w.addDocument(doc);
        doc.clear();
        doc.add(new NumericDocValuesField("group", 1));
        w.addDocument(doc);
        w.commit();
        doc.clear();
        doc.add(new NumericDocValuesField("group", 10));
        w.addDocument(doc);
        w.commit();
        doc.clear();
        doc.add(new NumericDocValuesField("category", 0));
        w.addDocument(doc);
        w.commit();
        final IndexReader reader = w.getReader();
        final IndexSearcher searcher = newSearcher(reader);

        MappedFieldType fieldType = new MockFieldMapper.FakeFieldType("group");

        SortField sortField = new SortField("group", SortField.Type.LONG, false, Long.MAX_VALUE);
        Sort sort = new Sort(sortField);

        final SinglePassGroupingCollector<?> collapsingCollector = SinglePassGroupingCollector.createNumeric(
            "group",
            fieldType,
            sort,
            10,
            null
        );
        searcher.search(Queries.ALL_DOCS_INSTANCE, collapsingCollector);
        TopFieldGroups collapseTopFieldDocs = collapsingCollector.getTopGroups(0);
        assertEquals(4, collapseTopFieldDocs.scoreDocs.length);
        assertEquals(4, collapseTopFieldDocs.groupValues.length);
        assertEquals(0L, collapseTopFieldDocs.groupValues[0]);
        assertEquals(1L, collapseTopFieldDocs.groupValues[1]);
        assertEquals(10L, collapseTopFieldDocs.groupValues[2]);
        assertNull(collapseTopFieldDocs.groupValues[3]);
        w.close();
        reader.close();
        dir.close();
    }

    public void testEmptySortedSegment() throws Exception {
        final Directory dir = newDirectory();
        final RandomIndexWriter w = new RandomIndexWriter(random(), dir);
        Document doc = new Document();
        doc.add(new SortedDocValuesField("group", new BytesRef("0")));
        w.addDocument(doc);
        doc.clear();
        doc.add(new SortedDocValuesField("group", new BytesRef("1")));
        w.addDocument(doc);
        w.commit();
        doc.clear();
        doc.add(new SortedDocValuesField("group", new BytesRef("10")));
        w.addDocument(doc);
        w.commit();
        doc.clear();
        doc.add(new NumericDocValuesField("category", 0));
        w.addDocument(doc);
        w.commit();
        final IndexReader reader = w.getReader();
        final IndexSearcher searcher = newSearcher(reader);

        MappedFieldType fieldType = new MockFieldMapper.FakeFieldType("group");

        Sort sort = new Sort(new SortField("group", SortField.Type.STRING));

        final SinglePassGroupingCollector<?> collapsingCollector = SinglePassGroupingCollector.createKeyword(
            "group",
            fieldType,
            null,
            sort,
            10,
            null
        );
        searcher.search(Queries.ALL_DOCS_INSTANCE, collapsingCollector);
        TopFieldGroups collapseTopFieldDocs = collapsingCollector.getTopGroups(0);
        assertEquals(4, collapseTopFieldDocs.scoreDocs.length);
        assertEquals(4, collapseTopFieldDocs.groupValues.length);
        assertNull(collapseTopFieldDocs.groupValues[0]);
        assertEquals(new BytesRef("0"), collapseTopFieldDocs.groupValues[1]);
        assertEquals(new BytesRef("1"), collapseTopFieldDocs.groupValues[2]);
        assertEquals(new BytesRef("10"), collapseTopFieldDocs.groupValues[3]);
        w.close();
        reader.close();
        dir.close();
    }

    public void testCollapseColumnarPayloadKeyword() throws Exception {
        final Directory dir = newDirectory();
        final RandomIndexWriter w = new RandomIndexWriter(random(), dir);
        final String[] groups = { "a", "b", "a", "c" };
        for (int i = 0; i < groups.length; i++) {
            final Document doc = new Document();
            doc.add(new Field("group", columnarPayload(groups[i]), binaryDocValuesType()));
            doc.add(new SortedNumericDocValuesField("sort", i));
            w.addDocument(doc);
        }
        final IndexReader reader = w.getReader();
        final IndexSearcher searcher = newSearcher(reader);

        final MappedFieldType fieldType = new MockFieldMapper.FakeFieldType("group");
        final Sort sort = new Sort(new SortedNumericSortField("sort", SortField.Type.LONG));

        final SinglePassGroupingCollector<?> collapsingCollector = SinglePassGroupingCollector.createKeyword(
            "group",
            fieldType,
            leaf -> SortableBinaryDocValues.forFormat(leaf, "group", IndexVersion.current(), BinaryDocValuesFormat.COLUMNAR_PAYLOAD),
            sort,
            10,
            null
        );
        searcher.search(Queries.ALL_DOCS_INSTANCE, collapsingCollector);
        final TopFieldGroups collapseTopFieldDocs = collapsingCollector.getTopGroups(0);
        assertEquals(3, collapseTopFieldDocs.groupValues.length);
        assertEquals(new BytesRef("a"), collapseTopFieldDocs.groupValues[0]);
        assertEquals(new BytesRef("b"), collapseTopFieldDocs.groupValues[1]);
        assertEquals(new BytesRef("c"), collapseTopFieldDocs.groupValues[2]);
        w.close();
        reader.close();
        dir.close();
    }

    public void testCollapseColumnarPayloadKeywordRejectsMultipleValues() throws Exception {
        final Directory dir = newDirectory();
        final RandomIndexWriter w = new RandomIndexWriter(random(), dir);
        final Document doc = new Document();
        doc.add(new Field("group", columnarPayload("a", "b"), binaryDocValuesType()));
        doc.add(new SortedNumericDocValuesField("sort", 0));
        w.addDocument(doc);
        final IndexReader reader = w.getReader();
        final IndexSearcher searcher = newSearcher(reader);

        final MappedFieldType fieldType = new MockFieldMapper.FakeFieldType("group");
        final Sort sort = new Sort(new SortedNumericSortField("sort", SortField.Type.LONG));

        final SinglePassGroupingCollector<?> collapsingCollector = SinglePassGroupingCollector.createKeyword(
            "group",
            fieldType,
            leaf -> SortableBinaryDocValues.forFormat(leaf, "group", IndexVersion.current(), BinaryDocValuesFormat.COLUMNAR_PAYLOAD),
            sort,
            10,
            null
        );
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> searcher.search(Queries.ALL_DOCS_INSTANCE, collapsingCollector)
        );
        assertEquals("failed to extract doc:0, the grouping field must be single valued", e.getMessage());
        w.close();
        reader.close();
        dir.close();
    }

    public void testCollapsePlainBinaryKeyword() throws Exception {
        final Directory dir = newDirectory();
        final RandomIndexWriter w = new RandomIndexWriter(random(), dir);
        final String[] groups = { "a", "b", "a", "c" };
        for (int i = 0; i < groups.length; i++) {
            final Document doc = new Document();
            doc.add(new Field("group", new BytesRef(groups[i]), binaryDocValuesType()));
            doc.add(new SortedNumericDocValuesField("sort", i));
            w.addDocument(doc);
        }
        final IndexReader reader = w.getReader();
        final IndexSearcher searcher = newSearcher(reader);

        final MappedFieldType fieldType = new MockFieldMapper.FakeFieldType("group");
        final Sort sort = new Sort(new SortedNumericSortField("sort", SortField.Type.LONG));

        final SinglePassGroupingCollector<?> collapsingCollector = SinglePassGroupingCollector.createKeyword(
            "group",
            fieldType,
            leaf -> SortableBinaryDocValues.forFormat(leaf, "group", IndexVersion.current(), BinaryDocValuesFormat.PLAIN),
            sort,
            10,
            null
        );
        searcher.search(Queries.ALL_DOCS_INSTANCE, collapsingCollector);
        final TopFieldGroups collapseTopFieldDocs = collapsingCollector.getTopGroups(0);
        assertEquals(3, collapseTopFieldDocs.groupValues.length);
        assertEquals(new BytesRef("a"), collapseTopFieldDocs.groupValues[0]);
        assertEquals(new BytesRef("b"), collapseTopFieldDocs.groupValues[1]);
        assertEquals(new BytesRef("c"), collapseTopFieldDocs.groupValues[2]);
        w.close();
        reader.close();
        dir.close();
    }

    public void testCollapseColumnarPayloadKeywordWithLeafMissingTheField() throws Exception {
        final Directory dir = newDirectory();
        final IndexWriter w = new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE));

        Document doc = new Document();
        doc.add(new Field("group", columnarPayload("host-a"), binaryDocValuesType()));
        doc.add(new SortedNumericDocValuesField("sort", 0));
        w.addDocument(doc);
        w.commit();

        doc = new Document();
        doc.add(new SortedNumericDocValuesField("sort", 1));
        w.addDocument(doc);
        w.commit();

        doc = new Document();
        doc.add(new Field("group", columnarPayload("host-b"), binaryDocValuesType()));
        doc.add(new SortedNumericDocValuesField("sort", 2));
        w.addDocument(doc);
        w.commit();

        final IndexReader reader = DirectoryReader.open(w);
        assertEquals(3, reader.leaves().size());
        assertNull(reader.leaves().get(1).reader().getFieldInfos().fieldInfo("group"));

        final IndexSearcher searcher = newSearcher(reader);
        final MappedFieldType fieldType = new MockFieldMapper.FakeFieldType("group");
        final Sort sort = new Sort(new SortedNumericSortField("sort", SortField.Type.LONG));

        final SinglePassGroupingCollector<?> collapsingCollector = SinglePassGroupingCollector.createKeyword(
            "group",
            fieldType,
            leaf -> SortableBinaryDocValues.forFormat(leaf, "group", IndexVersion.current(), BinaryDocValuesFormat.COLUMNAR_PAYLOAD),
            sort,
            10,
            null
        );
        searcher.search(Queries.ALL_DOCS_INSTANCE, collapsingCollector);
        final TopFieldGroups collapseTopFieldDocs = collapsingCollector.getTopGroups(0);
        assertEquals(3, collapseTopFieldDocs.groupValues.length);
        assertEquals(new BytesRef("host-a"), collapseTopFieldDocs.groupValues[0]);
        assertNull(collapseTopFieldDocs.groupValues[1]);
        assertEquals(new BytesRef("host-b"), collapseTopFieldDocs.groupValues[2]);
        w.close();
        reader.close();
        dir.close();
    }

    public void testCollapseBinaryKeywordWithoutADecoderThrows() throws Exception {
        final Directory dir = newDirectory();
        final RandomIndexWriter w = new RandomIndexWriter(random(), dir);
        final Document doc = new Document();
        doc.add(new Field("group", columnarPayload("host-a"), binaryDocValuesType()));
        doc.add(new SortedNumericDocValuesField("sort", 0));
        w.addDocument(doc);
        final IndexReader reader = w.getReader();
        final IndexSearcher searcher = newSearcher(reader);

        final MappedFieldType fieldType = new MockFieldMapper.FakeFieldType("group");
        final Sort sort = new Sort(new SortedNumericSortField("sort", SortField.Type.LONG));

        final SinglePassGroupingCollector<?> collapsingCollector = SinglePassGroupingCollector.createKeyword(
            "group",
            fieldType,
            null,
            sort,
            10,
            null
        );
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> searcher.search(Queries.ALL_DOCS_INSTANCE, collapsingCollector)
        );
        assertEquals("field `group` has binary doc values but its mapping does not name their format", e.getMessage());
        w.close();
        reader.close();
        dir.close();
    }

    public void testCollapseSortedKeywordNeverAsksForABinaryDecoder() throws Exception {
        assertNoBinaryDecoderRequested(value -> new SortedDocValuesField("group", value));
    }

    public void testCollapseSortedSetKeywordNeverAsksForABinaryDecoder() throws Exception {
        assertNoBinaryDecoderRequested(value -> new SortedSetDocValuesField("group", value));
    }

    public void testCollapseAbsentFieldNeverAsksForABinaryDecoder() throws Exception {
        assertNoBinaryDecoderRequested(null);
    }

    private void assertNoBinaryDecoderRequested(Function<BytesRef, Field> groupField) throws Exception {
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
                for (int i = 0; i < 2; i++) {
                    Document doc = new Document();
                    if (groupField != null) {
                        doc.add(groupField.apply(new BytesRef("host-" + i)));
                    }
                    doc.add(new SortedNumericDocValuesField("sort", i));
                    w.addDocument(doc);
                }
            }
            try (IndexReader reader = DirectoryReader.open(dir)) {
                final IndexSearcher searcher = newSearcher(reader);
                final SinglePassGroupingCollector<?> collapsingCollector = SinglePassGroupingCollector.createKeyword(
                    "group",
                    new MockFieldMapper.FakeFieldType("group"),
                    leaf -> {
                        throw new AssertionError("a binary decoder was requested for an ordinal backed field");
                    },
                    new Sort(new SortedNumericSortField("sort", SortField.Type.LONG)),
                    10,
                    null
                );
                searcher.search(Queries.ALL_DOCS_INSTANCE, collapsingCollector);
                assertEquals(groupField == null ? 1 : 2, collapsingCollector.getTopGroups(0).groupValues.length);
            }
        }
    }

    private static BytesRef columnarPayload(String... slots) {
        final List<BytesRef> refs = new ArrayList<>(slots.length);
        for (String slot : slots) {
            refs.add(slot == null ? null : new BytesRef(slot));
        }
        return BytesRef.deepCopyOf(new StringBinaryPayload.Builder().encode(refs));
    }

    private static FieldType binaryDocValuesType() {
        final FieldType type = new FieldType();
        type.setDocValuesType(DocValuesType.BINARY);
        type.freeze();
        return type;
    }
}
