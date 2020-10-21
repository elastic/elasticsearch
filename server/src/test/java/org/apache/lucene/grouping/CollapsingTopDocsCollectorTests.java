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
package org.apache.lucene.grouping;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.CompositeReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.CheckHits;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.grouping.CollapseTopFieldDocs;
import org.apache.lucene.search.grouping.CollapsingTopDocsCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MockFieldMapper;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class CollapsingTopDocsCollectorTests extends ESTestCase {
    private static class SegmentSearcher extends IndexSearcher {
        private final List<LeafReaderContext> ctx;

        SegmentSearcher(LeafReaderContext ctx, IndexReaderContext parent) {
            super(parent);
            this.ctx = Collections.singletonList(ctx);
        }

        public void search(Weight weight, Collector collector) throws IOException {
            search(ctx, weight, collector);
        }

        @Override
        public String toString() {
            return "ShardSearcher(" + ctx.get(0) + ")";
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

    private <T extends Comparable<T>> void assertSearchCollapse(CollapsingDocValuesProducer<T> dvProducers,
                                                                boolean numeric, boolean multivalued) throws IOException {
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

        final CollapsingTopDocsCollector<?> collapsingCollector;
        if (numeric) {
            collapsingCollector =
                CollapsingTopDocsCollector.createNumeric(collapseField.getField(), fieldType, sort, expectedNumGroups);
        } else {
            collapsingCollector =
                CollapsingTopDocsCollector.createKeyword(collapseField.getField(), fieldType, sort, expectedNumGroups);
        }

        TopFieldCollector topFieldCollector =
            TopFieldCollector.create(sort, totalHits, Integer.MAX_VALUE);
        Query query = new MatchAllDocsQuery();
        searcher.search(query, collapsingCollector);
        searcher.search(query, topFieldCollector);
        CollapseTopFieldDocs collapseTopFieldDocs = collapsingCollector.getTopDocs();
        TopFieldDocs topDocs = topFieldCollector.topDocs();
        assertEquals(collapseField.getField(), collapseTopFieldDocs.field);
        assertEquals(expectedNumGroups, collapseTopFieldDocs.scoreDocs.length);
        assertEquals(totalHits, collapseTopFieldDocs.totalHits.value);
        assertEquals(TotalHits.Relation.EQUAL_TO, collapseTopFieldDocs.totalHits.relation);
        assertEquals(totalHits, topDocs.scoreDocs.length);
        assertEquals(totalHits, topDocs.totalHits.value);

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

        final CollapseTopFieldDocs[] shardHits = new CollapseTopFieldDocs[subSearchers.length];
        final Weight weight = searcher.createWeight(searcher.rewrite(new MatchAllDocsQuery()), ScoreMode.COMPLETE, 1f);
        for (int shardIDX = 0; shardIDX < subSearchers.length; shardIDX++) {
            final SegmentSearcher subSearcher = subSearchers[shardIDX];
            final CollapsingTopDocsCollector<?> c;
            if (numeric) {
                c = CollapsingTopDocsCollector.createNumeric(collapseField.getField(), fieldType, sort, expectedNumGroups);
            } else {
                c = CollapsingTopDocsCollector.createKeyword(collapseField.getField(), fieldType, sort, expectedNumGroups);
            }
            subSearcher.search(weight, c);
            shardHits[shardIDX] = c.getTopDocs();
        }
        CollapseTopFieldDocs mergedFieldDocs = CollapseTopFieldDocs.merge(sort, 0, expectedNumGroups, shardHits, true);
        assertTopDocsEquals(query, mergedFieldDocs, collapseTopFieldDocs);
        w.close();
        reader.close();
        dir.close();
    }

    private static void assertTopDocsEquals(Query query, CollapseTopFieldDocs topDocs1, CollapseTopFieldDocs topDocs2) {
        CheckHits.checkEqual(query, topDocs1.scoreDocs, topDocs2.scoreDocs);
        assertArrayEquals(topDocs1.collapseValues, topDocs2.collapseValues);
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
                    return new SortField("field", SortField.Type.STRING_VAL);
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

        SortField sortField = new SortField("group", SortField.Type.LONG);
        sortField.setMissingValue(Long.MAX_VALUE);
        Sort sort = new Sort(sortField);

        final CollapsingTopDocsCollector<?> collapsingCollector =
                CollapsingTopDocsCollector.createNumeric("group", fieldType, sort, 10);
        searcher.search(new MatchAllDocsQuery(), collapsingCollector);
        CollapseTopFieldDocs collapseTopFieldDocs = collapsingCollector.getTopDocs();
        assertEquals(4, collapseTopFieldDocs.scoreDocs.length);
        assertEquals(4, collapseTopFieldDocs.collapseValues.length);
        assertEquals(0L, collapseTopFieldDocs.collapseValues[0]);
        assertEquals(1L, collapseTopFieldDocs.collapseValues[1]);
        assertEquals(10L, collapseTopFieldDocs.collapseValues[2]);
        assertNull(collapseTopFieldDocs.collapseValues[3]);
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

        Sort sort = new Sort(new SortField("group", SortField.Type.STRING_VAL));

        final CollapsingTopDocsCollector<?> collapsingCollector =
            CollapsingTopDocsCollector.createKeyword("group", fieldType, sort, 10);
        searcher.search(new MatchAllDocsQuery(), collapsingCollector);
        CollapseTopFieldDocs collapseTopFieldDocs = collapsingCollector.getTopDocs();
        assertEquals(4, collapseTopFieldDocs.scoreDocs.length);
        assertEquals(4, collapseTopFieldDocs.collapseValues.length);
        assertNull(collapseTopFieldDocs.collapseValues[0]);
        assertEquals(new BytesRef("0"), collapseTopFieldDocs.collapseValues[1]);
        assertEquals(new BytesRef("1"), collapseTopFieldDocs.collapseValues[2]);
        assertEquals(new BytesRef("10"), collapseTopFieldDocs.collapseValues[3]);
        w.close();
        reader.close();
        dir.close();
    }
}
