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
package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.LeafBucketCollector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class CompositeValuesCollectorQueueTests extends AggregatorTestCase {
    static class ClassAndName {
        final String name;
        final Class<? extends Comparable<?>> clazz;

        ClassAndName(String name, Class<? extends Comparable<?>> clazz) {
            this.name = name;
            this.clazz = clazz;
        }
    }

    public void testRandomLong() throws IOException {
        testRandomCase(new ClassAndName("long", Long.class));
    }

    public void testRandomDouble() throws IOException {
        testRandomCase(new ClassAndName("double", Double.class));
    }

    public void testRandomDoubleAndLong() throws IOException {
        testRandomCase(new ClassAndName("double", Double.class), new ClassAndName("long", Long.class));
    }

    public void testRandomDoubleAndKeyword() throws IOException {
        testRandomCase(new ClassAndName("double", Double.class), new ClassAndName("keyword", BytesRef.class));
    }

    public void testRandomKeyword() throws IOException {
        testRandomCase(new ClassAndName("keyword", BytesRef.class));
    }

    public void testRandomLongAndKeyword() throws IOException {
        testRandomCase(new ClassAndName("long", Long.class), new ClassAndName("keyword", BytesRef.class));
    }

    public void testRandomLongAndDouble() throws IOException {
        testRandomCase(new ClassAndName("long", Long.class), new ClassAndName("double", Double.class));
    }

    public void testRandomKeywordAndLong() throws IOException {
        testRandomCase(new ClassAndName("keyword", BytesRef.class), new ClassAndName("long", Long.class));
    }

    public void testRandomKeywordAndDouble() throws IOException {
        testRandomCase(new ClassAndName("keyword", BytesRef.class), new ClassAndName("double", Double.class));
    }

    public void testRandom() throws IOException {
        int numTypes = randomIntBetween(3, 8);
        ClassAndName[] types = new ClassAndName[numTypes];
        for (int i = 0; i < numTypes; i++) {
            int rand = randomIntBetween(0, 2);
            switch (rand) {
                case 0:
                    types[i] = new ClassAndName(Integer.toString(i), Long.class);
                    break;
                case 1:
                    types[i] = new ClassAndName(Integer.toString(i), Double.class);
                    break;
                case 2:
                    types[i] = new ClassAndName(Integer.toString(i), BytesRef.class);
                    break;
                default:
                    assert(false);
            }
        }
        testRandomCase(true, types);
    }

    private void testRandomCase(ClassAndName... types) throws IOException {
        testRandomCase(true, types);
        testRandomCase(false, types);
    }

    private void testRandomCase(boolean forceMerge, ClassAndName... types) throws IOException {
        int numDocs = randomIntBetween(50, 100);
        List<Comparable<?>[]> possibleValues = new ArrayList<>();
        for (ClassAndName type : types) {
            int numValues = randomIntBetween(1, numDocs*2);
            Comparable<?>[] values = new Comparable[numValues];
            if (type.clazz == Long.class) {
                for (int i = 0; i < numValues; i++) {
                    values[i] = randomLong();
                }
            } else if (type.clazz == Double.class) {
                for (int i = 0; i < numValues; i++) {
                    values[i] = randomDouble();
                }
            } else if (type.clazz == BytesRef.class) {
                for (int i = 0; i < numValues; i++) {
                    values[i] = new BytesRef(randomAlphaOfLengthBetween(5, 50));
                }
            } else {
                assert(false);
            }
            possibleValues.add(values);
        }

        Set<CompositeKey> keys = new HashSet<>();
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory, new KeywordAnalyzer())) {
                for (int i = 0; i < numDocs; i++) {
                    Document document = new Document();
                    List<List<Comparable<?>>> docValues = new ArrayList<>();
                    boolean hasAllField = true;
                    for (int j = 0; j < types.length; j++) {
                        int numValues = randomIntBetween(0, 5);
                        if (numValues == 0) {
                            hasAllField = false;
                        }
                        List<Comparable<?>> values = new ArrayList<>();
                        for (int k = 0; k < numValues; k++) {
                            values.add(possibleValues.get(j)[randomIntBetween(0, possibleValues.get(j).length-1)]);
                            if (types[j].clazz == Long.class) {
                                long value = (Long) values.get(k);
                                document.add(new SortedNumericDocValuesField(types[j].name, value));
                                document.add(new LongPoint(types[j].name, value));
                            } else if (types[j].clazz == Double.class) {
                                document.add(new SortedNumericDocValuesField(types[j].name,
                                    NumericUtils.doubleToSortableLong((Double) values.get(k))));
                            } else if (types[j].clazz == BytesRef.class) {
                                BytesRef value = (BytesRef) values.get(k);
                                document.add(new SortedSetDocValuesField(types[j].name, (BytesRef) values.get(k)));
                                document.add(new TextField(types[j].name, value.utf8ToString(), Field.Store.NO));
                            } else {
                                assert(false);
                            }
                        }
                        docValues.add(values);
                    }
                    if (hasAllField) {
                        List<CompositeKey> comb = createListCombinations(docValues);
                        keys.addAll(comb);
                    }
                    indexWriter.addDocument(document);
                }
                if (forceMerge) {
                    indexWriter.forceMerge(1);
                }
            }
            IndexReader reader = DirectoryReader.open(directory);
            int size = randomIntBetween(1, keys.size());
            CompositeValuesSource<?>[] sources = new CompositeValuesSource[types.length];
            for (int i = 0; i < types.length; i++) {
                final String name = types[i].name;
                if (types[i].clazz == Long.class) {
                    sources[i] = CompositeValuesSource.createLong(
                        context -> context.reader().getSortedNumericDocValues(name), DocValueFormat.RAW, size, 1
                    );
                } else if (types[i].clazz == Double.class) {
                    sources[i] = CompositeValuesSource.createDouble(
                        context -> FieldData.sortableLongBitsToDoubles(context.reader().getSortedNumericDocValues(name)), size, 1
                    );
                } else if (types[i].clazz == BytesRef.class) {
                    if (forceMerge) {
                        // we don't create global ordinals but we test this mode when the reader has a single segment
                        // since ordinals are global in this case.
                        sources[i] = CompositeValuesSource.createGlobalOrdinals(
                            context -> context.reader().getSortedSetDocValues(name), size, 1
                        );
                    } else {
                        sources[i] = CompositeValuesSource.createBinary(
                            context -> FieldData.toString(context.reader().getSortedSetDocValues(name)), size, 1
                        );
                    }
                } else {
                    assert(false);
                }
            }
            final SortedDocsProducer producer;
            if (types[0].clazz == BytesRef.class) {
                producer = SortedDocsProducer.createTerms(types[0].name);
            } else if (types[0].clazz == Long.class) {
                producer = SortedDocsProducer.createLong(types[0].name);
            } else {
                producer = null;
            }

            CompositeKey[] expected = keys.toArray(new CompositeKey[0]);
            Arrays.sort(expected, (a, b) -> compareKey(a, b));
            for (boolean withProducer : new boolean[] {true, false}) {
                if (withProducer && producer == null) {
                    continue;
                }
                int pos = 0;
                CompositeKey last = null;
                while (pos < size) {
                    CompositeValuesCollectorQueue queue =
                        new CompositeValuesCollectorQueue(sources, producer, size);
                    if (last != null) {
                        queue.setAfter(last.values());
                    }

                    for (LeafReaderContext leafReaderContext : reader.leaves()) {
                        final LeafBucketCollector leafCollector = new LeafBucketCollector() {
                            @Override
                            public void collect(int doc, long bucket) throws IOException {
                                queue.addIfCompetitive();
                            }
                        };
                        if (withProducer) {
                            final LeafBucketCollector empty = new LeafBucketCollector() {
                                @Override
                                public void collect(int doc, long bucket) throws IOException {
                                }
                            };
                            queue.getDocsProducer().processLeaf(new MatchAllDocsQuery(), queue, leafReaderContext, empty);
                        } else {
                            final LeafBucketCollector queueCollector = queue.getLeafCollector(leafReaderContext, leafCollector);
                            final Bits liveDocs = leafReaderContext.reader().getLiveDocs();
                            for (int i = 0; i < leafReaderContext.reader().maxDoc(); i++) {
                                if (liveDocs == null || liveDocs.get(i)) {
                                    queueCollector.collect(i);
                                }
                            }
                        }
                    }
                    assertEquals(size, Math.min(queue.size(), expected.length - pos));
                    int ptr = 0;
                    for (int slot : queue.getSortedSlot()) {
                        CompositeKey key = queue.toCompositeKey(slot);
                        assertThat(key, equalTo(expected[ptr++]));
                        last = key;
                    }
                    pos += queue.size();
                }
            }
            reader.close();
        }
    }

    private int compareKey(CompositeKey key1, CompositeKey key2) {
        assert key1.size() == key2.size();
        for (int i = 0; i < key1.size(); i++) {
            Comparable<Object> cmp1 = (Comparable<Object>) key1.get(i);
            int cmp = cmp1.compareTo(key2.get(i));
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    private static List<CompositeKey> createListCombinations(List<List<Comparable<?>>> values) {
        List<CompositeKey> keys = new ArrayList<>();
        createListCombinations(new Comparable[values.size()], values, 0, values.size(), keys);
        return keys;
    }

    private static void createListCombinations(Comparable<?>[] key, List<List<Comparable<?>>> values,
                                               int pos, int maxPos, List<CompositeKey> keys) {
        if (pos == maxPos) {
            keys.add(new CompositeKey(key.clone()));
        } else {
            for (Comparable<?> val : values.get(pos)) {
                key[pos] = val;
                createListCombinations(key, values, pos + 1, maxPos, keys);
            }
        }
    }
}
