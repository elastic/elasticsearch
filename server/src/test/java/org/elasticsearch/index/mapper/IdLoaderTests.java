/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class IdLoaderTests extends ESTestCase {

    private final int routingHash = randomInt();

    public void testSynthesizeIdSimple() throws Exception {
        final boolean useSyntheticIds = randomBoolean();
        var idLoader = IdLoader.createTsIdLoader(null, null, useSyntheticIds);

        long startTime = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2023-01-01T00:00:00Z");
        List<Doc> docs = List.of(
            new Doc(startTime, List.of(new Dimension("dim1", "aaa"), new Dimension("dim2", "xxx"))),
            new Doc(startTime + 1, List.of(new Dimension("dim1", "aaa"), new Dimension("dim2", "yyy"))),
            new Doc(startTime + 2, List.of(new Dimension("dim1", "bbb"), new Dimension("dim2", "xxx")))
        );
        CheckedConsumer<IndexReader, IOException> verify = indexReader -> {
            assertThat(indexReader.leaves(), hasSize(1));
            LeafReader leafReader = indexReader.leaves().get(0).reader();
            assertThat(leafReader.numDocs(), equalTo(3));
            var leaf = idLoader.leaf(null, leafReader, new int[] { 0, 1, 2 });
            // NOTE: time series data is ordered by (tsid, timestamp)
            assertThat(leaf.getId(0), equalTo(expectedId(docs.get(2), routingHash, useSyntheticIds)));
            assertThat(leaf.getId(1), equalTo(expectedId(docs.get(0), routingHash, useSyntheticIds)));
            assertThat(leaf.getId(2), equalTo(expectedId(docs.get(1), routingHash, useSyntheticIds)));
        };
        prepareIndexReader(indexAndForceMerge(docs, routingHash), verify, false);
    }

    public void testSynthesizeIdLazy() throws Exception {
        final boolean useSyntheticIds = randomBoolean();
        var idLoader = IdLoader.createTsIdLoader(null, null, useSyntheticIds);

        long startTime = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2023-01-01T00:00:00Z");
        List<Doc> docs = List.of(
            new Doc(startTime, List.of(new Dimension("dim1", "aaa"), new Dimension("dim2", "xxx"))),
            new Doc(startTime + 1, List.of(new Dimension("dim1", "aaa"), new Dimension("dim2", "yyy"))),
            new Doc(startTime + 2, List.of(new Dimension("dim1", "bbb"), new Dimension("dim2", "xxx")))
        );
        CheckedConsumer<IndexReader, IOException> verify = indexReader -> {
            assertThat(indexReader.leaves(), hasSize(1));
            LeafReader leafReader = indexReader.leaves().get(0).reader();
            assertThat(leafReader.numDocs(), equalTo(3));
            var leaf = idLoader.leaf(null, leafReader, null);
            assertThat(leaf, instanceOf(IdLoader.LazyTsIdLeaf.class));
            // NOTE: time series data is ordered by (tsid, timestamp)
            assertThat(leaf.getId(0), equalTo(expectedId(docs.get(2), routingHash, useSyntheticIds)));
            assertThat(leaf.getId(1), equalTo(expectedId(docs.get(0), routingHash, useSyntheticIds)));
            assertThat(leaf.getId(2), equalTo(expectedId(docs.get(1), routingHash, useSyntheticIds)));
        };
        prepareIndexReader(indexAndForceMerge(docs, routingHash), verify, false);
    }

    public void testSynthesizeIdMultipleSegments() throws Exception {
        final boolean useSyntheticIds = randomBoolean();
        var idLoader = IdLoader.createTsIdLoader(null, null, useSyntheticIds);

        long startTime = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2023-01-01T00:00:00Z");
        List<Doc> docs1 = List.of(
            new Doc(startTime, List.of(new Dimension("dim1", "aaa"), new Dimension("dim2", "xxx"))),
            new Doc(startTime - 1, List.of(new Dimension("dim1", "aaa"), new Dimension("dim2", "xxx"))),
            new Doc(startTime - 2, List.of(new Dimension("dim1", "aaa"), new Dimension("dim2", "xxx"))),
            new Doc(startTime - 3, List.of(new Dimension("dim1", "aaa"), new Dimension("dim2", "xxx")))
        );
        List<Doc> docs2 = List.of(
            new Doc(startTime, List.of(new Dimension("dim1", "aaa"), new Dimension("dim2", "yyy"))),
            new Doc(startTime - 1, List.of(new Dimension("dim1", "aaa"), new Dimension("dim2", "yyy"))),
            new Doc(startTime - 2, List.of(new Dimension("dim1", "aaa"), new Dimension("dim2", "yyy"))),
            new Doc(startTime - 3, List.of(new Dimension("dim1", "aaa"), new Dimension("dim2", "yyy")))
        );
        List<Doc> docs3 = List.of(
            new Doc(startTime, List.of(new Dimension("dim1", "bbb"), new Dimension("dim2", "yyy"))),
            new Doc(startTime - 1, List.of(new Dimension("dim1", "bbb"), new Dimension("dim2", "yyy"))),
            new Doc(startTime - 2, List.of(new Dimension("dim1", "bbb"), new Dimension("dim2", "yyy"))),
            new Doc(startTime - 3, List.of(new Dimension("dim1", "bbb"), new Dimension("dim2", "yyy")))
        );
        CheckedConsumer<IndexWriter, IOException> buildIndex = writer -> {
            for (Doc doc : docs1) {
                indexDoc(writer, doc, routingHash);
            }
            writer.flush();
            for (Doc doc : docs2) {
                indexDoc(writer, doc, routingHash);
            }
            writer.flush();
            for (Doc doc : docs3) {
                indexDoc(writer, doc, routingHash);
            }
            writer.flush();
        };
        CheckedConsumer<IndexReader, IOException> verify = indexReader -> {
            assertThat(indexReader.leaves(), hasSize(3));
            {
                LeafReader leafReader = indexReader.leaves().get(0).reader();
                assertThat(leafReader.numDocs(), equalTo(docs1.size()));
                var leaf = idLoader.leaf(null, leafReader, IntStream.range(0, docs1.size()).toArray());
                for (int i = 0; i < docs1.size(); i++) {
                    assertThat(leaf.getId(i), equalTo(expectedId(docs1.get(i), routingHash, useSyntheticIds)));
                }
            }
            {
                LeafReader leafReader = indexReader.leaves().get(1).reader();
                assertThat(leafReader.numDocs(), equalTo(docs2.size()));
                var leaf = idLoader.leaf(null, leafReader, new int[] { 0, 3 });
                assertThat(leaf.getId(0), equalTo(expectedId(docs2.get(0), routingHash, useSyntheticIds)));
                assertThat(leaf.getId(3), equalTo(expectedId(docs2.get(3), routingHash, useSyntheticIds)));
            }
            {
                LeafReader leafReader = indexReader.leaves().get(2).reader();
                assertThat(leafReader.numDocs(), equalTo(docs3.size()));
                var leaf = idLoader.leaf(null, leafReader, new int[] { 1, 2 });
                assertThat(leaf.getId(1), equalTo(expectedId(docs3.get(1), routingHash, useSyntheticIds)));
                assertThat(leaf.getId(2), equalTo(expectedId(docs3.get(2), routingHash, useSyntheticIds)));
            }
            {
                LeafReader leafReader = indexReader.leaves().get(2).reader();
                assertThat(leafReader.numDocs(), equalTo(docs3.size()));
                var leaf = idLoader.leaf(null, leafReader, new int[] { 3 });
                expectThrows(IllegalArgumentException.class, () -> leaf.getId(0));
            }
        };
        prepareIndexReader(buildIndex, verify, true);
    }

    public void testSynthesizeIdRandom() throws Exception {
        final boolean useSyntheticIds = randomBoolean();
        var idLoader = IdLoader.createTsIdLoader(null, null, useSyntheticIds);

        long startTime = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2023-01-01T00:00:00Z");
        Set<String> expectedIDs = new HashSet<>();
        List<Doc> randomDocs = new ArrayList<>();
        int numberOfTimeSeries = randomIntBetween(8, 64);
        for (int i = 0; i < numberOfTimeSeries; i++) {
            int numberOfDimensions = randomIntBetween(1, 6);
            List<Dimension> dimensions = new ArrayList<>(numberOfDimensions);
            for (int j = 1; j <= numberOfDimensions; j++) {
                String fieldName = "dim" + j;
                Object value;
                if (j == 5) {
                    value = randomLongBetween(1, 20);
                } else {
                    value = randomAlphaOfLength(4);
                }
                dimensions.add(new Dimension(fieldName, value));
            }
            int numberOfSamples = randomIntBetween(1, 16);
            for (int j = 0; j < numberOfSamples; j++) {
                Doc doc = new Doc(startTime++, dimensions);
                randomDocs.add(doc);
                expectedIDs.add(expectedId(doc, routingHash, useSyntheticIds));
            }
        }
        CheckedConsumer<IndexReader, IOException> verify = indexReader -> {
            assertThat(indexReader.leaves(), hasSize(1));
            LeafReader leafReader = indexReader.leaves().get(0).reader();
            assertThat(leafReader.numDocs(), equalTo(randomDocs.size()));
            var leaf = idLoader.leaf(null, leafReader, IntStream.range(0, randomDocs.size()).toArray());
            for (int i = 0; i < randomDocs.size(); i++) {
                String actualId = leaf.getId(i);
                assertTrue("docId=" + i + " id=" + actualId, expectedIDs.remove(actualId));
            }
        };
        prepareIndexReader(indexAndForceMerge(randomDocs, routingHash), verify, false);
        assertThat(expectedIDs, empty());
    }

    /**
     * In a slice-enabled index the {@code _id} is stored as a composite {@code (slice, id)} term. The {@link IdLoader.SliceIdLoader}
     * recovers the plain, user-visible id from the stored value as it is iterated, mirroring the fetch path which feeds it a
     * {@link LeafStoredFieldLoader} positioned on each doc.
     */
    public void testSliceIdLoaderRecoversPlainId() throws Exception {
        var idLoader = new IdLoader.SliceIdLoader();
        // Note: the same plain id "1" appears under two different slices, which is exactly what slice-scoped uniqueness allows.
        List<SliceDoc> docs = List.of(new SliceDoc("slice-a", "1"), new SliceDoc("slice-a", "2"), new SliceDoc("slice-b", "1"));
        CheckedConsumer<IndexReader, IOException> verify = indexReader -> {
            assertThat(indexReader.leaves(), hasSize(1));
            LeafReaderContext leafContext = indexReader.leaves().get(0);
            assertThat(leafContext.reader().numDocs(), equalTo(docs.size()));
            LeafStoredFieldLoader storedFieldLoader = StoredFieldLoader.create(false, Set.of(IdFieldMapper.NAME))
                .getLoader(leafContext, null);
            var leaf = idLoader.leaf(storedFieldLoader, leafContext.reader(), new int[] { 0, 1, 2 });
            assertThat(leaf, instanceOf(IdLoader.SliceIdLeaf.class));
            for (int i = 0; i < docs.size(); i++) {
                storedFieldLoader.advanceTo(i);
                assertThat(leaf.getId(i), equalTo(docs.get(i).id()));
            }
        };
        prepareSliceIndexReader(docs, verify);
    }

    public void testSliceIdLoaderRandom() throws Exception {
        var idLoader = new IdLoader.SliceIdLoader();
        int numDocs = randomIntBetween(1, 64);
        List<SliceDoc> docs = new ArrayList<>(numDocs);
        for (int i = 0; i < numDocs; i++) {
            docs.add(new SliceDoc(randomAlphaOfLengthBetween(1, 12), randomAlphaOfLengthBetween(1, 16)));
        }
        CheckedConsumer<IndexReader, IOException> verify = indexReader -> {
            assertThat(indexReader.leaves(), hasSize(1));
            LeafReaderContext leafContext = indexReader.leaves().get(0);
            assertThat(leafContext.reader().numDocs(), equalTo(docs.size()));
            LeafStoredFieldLoader storedFieldLoader = StoredFieldLoader.create(false, Set.of(IdFieldMapper.NAME))
                .getLoader(leafContext, null);
            var leaf = idLoader.leaf(storedFieldLoader, leafContext.reader(), IntStream.range(0, docs.size()).toArray());
            for (int i = 0; i < docs.size(); i++) {
                storedFieldLoader.advanceTo(i);
                assertThat(leaf.getId(i), equalTo(docs.get(i).id()));
            }
        };
        prepareSliceIndexReader(docs, verify);
    }

    private void prepareSliceIndexReader(List<SliceDoc> docs, CheckedConsumer<IndexReader, IOException> verify) throws IOException {
        try (Directory directory = newDirectory()) {
            IndexWriterConfig config = LuceneTestCase.newIndexWriterConfig(random(), new MockAnalyzer(random()));
            try (IndexWriter indexWriter = new IndexWriter(directory, config)) {
                for (SliceDoc doc : docs) {
                    List<IndexableField> fields = new ArrayList<>();
                    // Mirror the indexing path: the composite "slice#id" (standard-encoded) is what gets stored in the _id field.
                    fields.add(IdFieldMapper.standardIdField(Uid.encodeId(Uid.compositeId(doc.slice(), doc.id())), Field.Store.YES));
                    indexWriter.addDocument(fields);
                }
                // Force a single segment so the doc ids are stable and in insertion order.
                indexWriter.forceMerge(1);
            }
            try (DirectoryReader indexReader = DirectoryReader.open(directory)) {
                verify.accept(indexReader);
            }
        }
    }

    record SliceDoc(String slice, String id) {}

    private static CheckedConsumer<IndexWriter, IOException> indexAndForceMerge(List<Doc> docs, int routingHash) {
        return writer -> {
            for (Doc doc : docs) {
                indexDoc(writer, doc, routingHash);
            }
            writer.forceMerge(1);
        };
    }

    private void prepareIndexReader(
        CheckedConsumer<IndexWriter, IOException> buildIndex,
        CheckedConsumer<IndexReader, IOException> verify,
        boolean noMergePolicy
    ) throws IOException {
        try (Directory directory = newDirectory()) {
            IndexWriterConfig config = LuceneTestCase.newIndexWriterConfig(random(), new MockAnalyzer(random()));
            if (noMergePolicy) {
                config.setMergePolicy(NoMergePolicy.INSTANCE);
                config.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
            }
            Sort sort = new Sort(
                new SortField(TimeSeriesIdFieldMapper.NAME, SortField.Type.STRING, false),
                new SortField(TimeSeriesRoutingHashFieldMapper.NAME, SortField.Type.STRING, false),
                new SortedNumericSortField(DataStreamTimestampFieldMapper.DEFAULT_PATH, SortField.Type.LONG, true)
            );
            config.setIndexSort(sort);
            IndexWriter indexWriter = new IndexWriter(directory, config);
            buildIndex.accept(indexWriter);
            indexWriter.close();

            try (DirectoryReader indexReader = DirectoryReader.open(directory);) {
                verify.accept(indexReader);
            }
        }
    }

    private static void indexDoc(IndexWriter iw, Doc doc, int routingHash) throws IOException {
        var routingFields = new RoutingPathFields(null);

        final List<IndexableField> fields = new ArrayList<>();
        fields.add(new SortedNumericDocValuesField(DataStreamTimestampFieldMapper.DEFAULT_PATH, doc.timestamp));
        fields.add(new LongPoint(DataStreamTimestampFieldMapper.DEFAULT_PATH, doc.timestamp));
        for (Dimension dimension : doc.dimensions) {
            if (dimension.value instanceof Number n) {
                routingFields.addLong(dimension.field, n.longValue());
                fields.add(new SortedNumericDocValuesField(dimension.field, ((Number) dimension.value).longValue()));
            } else {
                routingFields.addString(dimension.field, dimension.value.toString());
                fields.add(new SortedSetDocValuesField(dimension.field, new BytesRef(dimension.value.toString())));
            }
        }
        BytesRef tsid = routingFields.buildHash().toBytesRef();
        fields.add(new SortedDocValuesField(TimeSeriesIdFieldMapper.NAME, tsid));
        fields.add(
            new SortedDocValuesField(
                TimeSeriesRoutingHashFieldMapper.NAME,
                Uid.encodeId(TimeSeriesRoutingHashFieldMapper.encode(routingHash))
            )
        );
        iw.addDocument(fields);
    }

    private static String expectedId(Doc doc, int routingHash, boolean useSyntheticIds) {
        var routingFields = new RoutingPathFields(null);
        for (Dimension dimension : doc.dimensions) {
            if (dimension.value instanceof Number n) {
                routingFields.addLong(dimension.field, n.longValue());
            } else {
                routingFields.addString(dimension.field, dimension.value.toString());
            }
        }
        if (useSyntheticIds) {
            return TsidExtractingIdFieldMapper.createSyntheticId(routingFields.buildHash().toBytesRef(), doc.timestamp, routingHash);
        } else {
            return TsidExtractingIdFieldMapper.createId(routingHash, routingFields.buildHash().toBytesRef(), doc.timestamp);
        }
    }

    record Doc(long timestamp, List<Dimension> dimensions) {}

    record Dimension(String field, Object value) {}

}
