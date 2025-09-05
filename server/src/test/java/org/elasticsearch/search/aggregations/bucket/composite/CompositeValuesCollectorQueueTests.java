/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafMetaData;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.TermVectors;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.index.mapper.NumberFieldMapper.NumberType.DOUBLE;
import static org.elasticsearch.index.mapper.NumberFieldMapper.NumberType.LONG;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class CompositeValuesCollectorQueueTests extends AggregatorTestCase {
    static class ClassAndName {
        final MappedFieldType fieldType;
        final Class<? extends Comparable<?>> clazz;

        ClassAndName(MappedFieldType fieldType, Class<? extends Comparable<?>> clazz) {
            this.fieldType = fieldType;
            this.clazz = clazz;
        }
    }

    private IndexReader indexReader;

    @Before
    public void set() {
        indexReader = new DummyReader();
    }

    public void testRandomLong() throws IOException {
        testRandomCase(new ClassAndName(createNumber("long", LONG), Long.class));
    }

    public void testRandomDouble() throws IOException {
        testRandomCase(new ClassAndName(createNumber("double", DOUBLE), Double.class));
    }

    public void testRandomDoubleAndLong() throws IOException {
        testRandomCase(
            new ClassAndName(createNumber("double", DOUBLE), Double.class),
            new ClassAndName(createNumber("long", LONG), Long.class)
        );
    }

    public void testRandomDoubleAndKeyword() throws IOException {
        testRandomCase(
            new ClassAndName(createNumber("double", DOUBLE), Double.class),
            new ClassAndName(createKeyword("keyword"), BytesRef.class)
        );
    }

    public void testRandomKeyword() throws IOException {
        testRandomCase(new ClassAndName(createKeyword("keyword"), BytesRef.class));
    }

    public void testRandomLongAndKeyword() throws IOException {
        testRandomCase(
            new ClassAndName(createNumber("long", LONG), Long.class),
            new ClassAndName(createKeyword("keyword"), BytesRef.class)
        );
    }

    public void testRandomLongAndDouble() throws IOException {
        testRandomCase(
            new ClassAndName(createNumber("long", LONG), Long.class),
            new ClassAndName(createNumber("double", DOUBLE), Double.class)
        );
    }

    public void testRandomKeywordAndLong() throws IOException {
        testRandomCase(
            new ClassAndName(createKeyword("keyword"), BytesRef.class),
            new ClassAndName(createNumber("long", LONG), Long.class)
        );
    }

    public void testRandomKeywordAndDouble() throws IOException {
        testRandomCase(
            new ClassAndName(createKeyword("keyword"), BytesRef.class),
            new ClassAndName(createNumber("double", DOUBLE), Double.class)
        );
    }

    public void testRandom() throws IOException {
        int numTypes = randomIntBetween(3, 8);
        ClassAndName[] types = new ClassAndName[numTypes];
        for (int i = 0; i < numTypes; i++) {
            int rand = randomIntBetween(0, 2);
            switch (rand) {
                case 0:
                    types[i] = new ClassAndName(createNumber(Integer.toString(i), LONG), Long.class);
                    break;
                case 1:
                    types[i] = new ClassAndName(createNumber(Integer.toString(i), DOUBLE), Double.class);
                    break;
                case 2:
                    types[i] = new ClassAndName(createKeyword(Integer.toString(i)), BytesRef.class);
                    break;
                default:
                    assert (false);
            }
        }
        testRandomCase(types);
    }

    private void testRandomCase(ClassAndName... types) throws IOException {
        for (int i = 0; i < types.length; i++) {
            testRandomCase(true, true, i, types);
            testRandomCase(true, false, i, types);
            testRandomCase(false, true, i, types);
            testRandomCase(false, false, i, types);
        }
    }

    private void testRandomCase(boolean forceMerge, boolean missingBucket, int indexSortSourcePrefix, ClassAndName... types)
        throws IOException {
        final BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
        int numDocs = randomIntBetween(50, 100);
        List<Comparable<?>[]> possibleValues = new ArrayList<>();
        SortField[] indexSortFields = indexSortSourcePrefix == 0 ? null : new SortField[indexSortSourcePrefix];
        for (int i = 0; i < types.length; i++) {
            ClassAndName type = types[i];
            final Comparable<?>[] values;
            int numValues = randomIntBetween(1, numDocs * 2);
            values = new Comparable<?>[numValues];
            if (type.clazz == Long.class) {
                if (i < indexSortSourcePrefix) {
                    indexSortFields[i] = new SortedNumericSortField(type.fieldType.name(), SortField.Type.LONG);
                }
                for (int j = 0; j < numValues; j++) {
                    values[j] = randomLong();
                }
            } else if (type.clazz == Double.class) {
                if (i < indexSortSourcePrefix) {
                    indexSortFields[i] = new SortedNumericSortField(type.fieldType.name(), SortField.Type.DOUBLE);
                }
                for (int j = 0; j < numValues; j++) {
                    values[j] = randomDouble();
                }
            } else if (type.clazz == BytesRef.class) {
                if (i < indexSortSourcePrefix) {
                    indexSortFields[i] = new SortedSetSortField(type.fieldType.name(), false);
                }
                for (int j = 0; j < numValues; j++) {
                    values[j] = new BytesRef(randomAlphaOfLengthBetween(5, 50));
                }
            } else {
                assert (false);
            }
            possibleValues.add(values);
        }

        Set<CompositeKey> keys = new HashSet<>();
        try (Directory directory = newDirectory()) {
            final IndexWriterConfig writerConfig = newIndexWriterConfig();
            if (indexSortFields != null) {
                writerConfig.setIndexSort(new Sort(indexSortFields));
            }
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory, new KeywordAnalyzer())) {
                for (int i = 0; i < numDocs; i++) {
                    Document document = new Document();
                    List<List<Comparable<?>>> docValues = new ArrayList<>();
                    boolean hasAllField = true;
                    for (int j = 0; j < types.length; j++) {
                        int numValues = indexSortSourcePrefix - 1 >= j ? 1 : randomIntBetween(0, 5);
                        List<Comparable<?>> values = new ArrayList<>();
                        if (numValues == 0) {
                            hasAllField = false;
                            if (missingBucket) {
                                values.add(null);
                            }
                        } else {
                            for (int k = 0; k < numValues; k++) {
                                values.add(possibleValues.get(j)[randomIntBetween(0, possibleValues.get(j).length - 1)]);
                                if (types[j].clazz == Long.class) {
                                    long value = (Long) values.get(k);
                                    document.add(new SortedNumericDocValuesField(types[j].fieldType.name(), value));
                                    document.add(new LongPoint(types[j].fieldType.name(), value));
                                } else if (types[j].clazz == Double.class) {
                                    document.add(
                                        new SortedNumericDocValuesField(
                                            types[j].fieldType.name(),
                                            NumericUtils.doubleToSortableLong((Double) values.get(k))
                                        )
                                    );
                                } else if (types[j].clazz == BytesRef.class) {
                                    BytesRef value = (BytesRef) values.get(k);
                                    document.add(new SortedSetDocValuesField(types[j].fieldType.name(), (BytesRef) values.get(k)));
                                    document.add(new TextField(types[j].fieldType.name(), value.utf8ToString(), Field.Store.NO));
                                } else {
                                    assert (false);
                                }
                            }
                        }
                        docValues.add(values);
                    }
                    if (hasAllField || missingBucket) {
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
            int size = keys.size() > 1 ? randomIntBetween(1, keys.size()) : 1;
            SingleDimensionValuesSource<?>[] sources = new SingleDimensionValuesSource<?>[types.length];
            for (int i = 0; i < types.length; i++) {
                final MappedFieldType fieldType = types[i].fieldType;
                if (types[i].clazz == Long.class) {
                    sources[i] = new LongValuesSource(
                        bigArrays,
                        fieldType,
                        context -> DocValues.getSortedNumeric(context.reader(), fieldType.name()),
                        value -> value,
                        DocValueFormat.RAW,
                        missingBucket,
                        MissingOrder.DEFAULT,
                        size,
                        1
                    );
                } else if (types[i].clazz == Double.class) {
                    sources[i] = new DoubleValuesSource(
                        bigArrays,
                        fieldType,
                        context -> FieldData.sortableLongBitsToDoubles(DocValues.getSortedNumeric(context.reader(), fieldType.name())),
                        DocValueFormat.RAW,
                        missingBucket,
                        MissingOrder.DEFAULT,
                        size,
                        1
                    );
                } else if (types[i].clazz == BytesRef.class) {
                    if (forceMerge) {
                        // we don't create global ordinals but we test this mode when the reader has a single segment
                        // since ordinals are global in this case.
                        sources[i] = new GlobalOrdinalValuesSource(
                            bigArrays,
                            fieldType,
                            DocValues.getSortedSet(reader.leaves().get(0).reader(), fieldType.name()).getValueCount(),
                            context -> DocValues.getSortedSet(context.reader(), fieldType.name()),
                            DocValueFormat.RAW,
                            missingBucket,
                            MissingOrder.DEFAULT,
                            size,
                            1
                        );
                    } else {
                        sources[i] = new BinaryValuesSource(
                            bigArrays,
                            (b) -> {},
                            fieldType,
                            context -> FieldData.toString(DocValues.getSortedSet(context.reader(), fieldType.name())),
                            DocValueFormat.RAW,
                            missingBucket,
                            MissingOrder.DEFAULT,
                            size,
                            1
                        );
                    }
                } else {
                    assert (false);
                }
            }
            CompositeKey[] expected = keys.toArray(new CompositeKey[0]);
            Arrays.sort(expected, (a, b) -> compareKey(a, b));
            for (boolean withProducer : new boolean[] { true, false }) {
                int pos = 0;
                CompositeKey last = null;
                while (pos < size) {
                    final CompositeValuesCollectorQueue queue = new CompositeValuesCollectorQueue(
                        BigArrays.NON_RECYCLING_INSTANCE,
                        sources,
                        size,
                        indexReader
                    );
                    if (last != null) {
                        queue.setAfterKey(last);
                    }
                    final SortedDocsProducer docsProducer = sources[0].createSortedDocsProducerOrNull(reader, new MatchAllDocsQuery());
                    for (LeafReaderContext leafReaderContext : reader.leaves()) {
                        if (docsProducer != null && withProducer) {
                            assertEquals(DocIdSet.EMPTY, docsProducer.processLeaf(queue, leafReaderContext, false));
                        } else {
                            final LeafBucketCollector leafCollector = new LeafBucketCollector() {
                                @Override
                                public void collect(int doc, long bucket) throws IOException {
                                    queue.addIfCompetitive(indexSortSourcePrefix, 1);
                                }
                            };
                            final LeafBucketCollector queueCollector = queue.getLeafCollector(leafReaderContext, leafCollector);
                            final Bits liveDocs = leafReaderContext.reader().getLiveDocs();
                            for (int i = 0; i < leafReaderContext.reader().maxDoc(); i++) {
                                if (liveDocs == null || liveDocs.get(i)) {
                                    try {
                                        queueCollector.collect(i);
                                    } catch (CollectionTerminatedException exc) {
                                        assertThat(indexSortSourcePrefix, greaterThan(0));
                                    }
                                }
                            }
                        }
                    }
                    assertEquals(size, Math.min(queue.size(), expected.length - pos));
                    int ptr = pos + ((int) queue.size() - 1);
                    pos += Math.toIntExact(queue.size());
                    last = null;
                    while (queue.size() > pos) {
                        CompositeKey key = queue.toCompositeKey(queue.pop());
                        if (last == null) {
                            last = key;
                        }
                        assertThat(key, equalTo(expected[ptr--]));
                    }
                }
            }
            reader.close();
        }
    }

    private static MappedFieldType createNumber(String name, NumberFieldMapper.NumberType type) {
        return new NumberFieldMapper.NumberFieldType(name, type);
    }

    private static MappedFieldType createKeyword(String name) {
        return new KeywordFieldMapper.KeywordFieldType(name);
    }

    private static int compareKey(CompositeKey key1, CompositeKey key2) {
        assert key1.size() == key2.size();
        for (int i = 0; i < key1.size(); i++) {
            if (key1.get(i) == null) {
                if (key2.get(i) == null) {
                    continue;
                }
                return -1;
            } else if (key2.get(i) == null) {
                return 1;
            }
            @SuppressWarnings("unchecked")
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
        createListCombinations(new Comparable<?>[values.size()], values, 0, values.size(), keys);
        return keys;
    }

    private static void createListCombinations(
        Comparable<?>[] key,
        List<List<Comparable<?>>> values,
        int pos,
        int maxPos,
        List<CompositeKey> keys
    ) {
        if (pos == maxPos) {
            keys.add(new CompositeKey(key.clone()));
        } else {
            for (Comparable<?> val : values.get(pos)) {
                key[pos] = val;
                createListCombinations(key, values, pos + 1, maxPos, keys);
            }
        }
    }

    static class DummyReader extends LeafReader {
        @Override
        public CacheHelper getCoreCacheHelper() {
            return null;
        }

        @Override
        public Terms terms(String field) throws IOException {
            return null;
        }

        @Override
        public NumericDocValues getNumericDocValues(String field) throws IOException {
            return null;
        }

        @Override
        public BinaryDocValues getBinaryDocValues(String field) throws IOException {
            return null;
        }

        @Override
        public SortedDocValues getSortedDocValues(String field) throws IOException {
            return null;
        }

        @Override
        public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
            return null;
        }

        @Override
        public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
            return null;
        }

        @Override
        public NumericDocValues getNormValues(String field) throws IOException {
            return null;
        }

        @Override
        public DocValuesSkipper getDocValuesSkipper(String field) throws IOException {
            return null;
        }

        @Override
        public FloatVectorValues getFloatVectorValues(String field) throws IOException {
            return null;
        }

        @Override
        public ByteVectorValues getByteVectorValues(String field) throws IOException {
            return null;
        }

        @Override
        public void searchNearestVectors(String field, float[] target, KnnCollector knnCollector, Bits acceptDocs) throws IOException {

        }

        @Override
        public void searchNearestVectors(String field, byte[] target, KnnCollector knnCollector, Bits acceptDocs) throws IOException {

        }

        @Override
        public FieldInfos getFieldInfos() {
            return null;
        }

        @Override
        public Bits getLiveDocs() {
            return null;
        }

        @Override
        public PointValues getPointValues(String field) throws IOException {
            return null;
        }

        @Override
        public void checkIntegrity() throws IOException {

        }

        @Override
        public LeafMetaData getMetaData() {
            return null;
        }

        @Override
        public TermVectors termVectors() throws IOException {
            return null;
        }

        @Override
        public int numDocs() {
            return 0;
        }

        @Override
        public int maxDoc() {
            return 0;
        }

        @Override
        public StoredFields storedFields() throws IOException {
            return null;
        }

        @Override
        protected void doClose() throws IOException {

        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return null;
        }
    }
}
