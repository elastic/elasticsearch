/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.commits;

import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.test.FakeStatelessNode;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoDeletionPolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SoftDeletesDirectoryReaderWrapper;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Streams;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesRoutingHashFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.store.LuceneFilesExtensions;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.commits.BatchedCompoundCommit;
import org.elasticsearch.xpack.stateless.commits.BlobLocation;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.commits.VirtualBatchedCompoundCommit;
import org.elasticsearch.xpack.stateless.lucene.StatelessCommitRef;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static co.elastic.elasticsearch.stateless.commits.VirtualBatchedCompoundCommitTests.deserializeBatchedCompoundCommit;
import static org.hamcrest.Matchers.equalTo;

public class IndexCommitTimestampFieldRangeTests extends MapperServiceTestCase {

    public void testFieldValueRangeForTimeSeriesModeWithCFS() throws Exception {
        testFieldValueRange(true, IndexMode.TIME_SERIES);
    }

    public void testFieldValueRangeForTimeSeriesModeNoCFS() throws Exception {
        testFieldValueRange(false, IndexMode.TIME_SERIES);
    }

    public void testFieldValueRangeForLogsDBModeWithCFS() throws Exception {
        testFieldValueRange(true, IndexMode.LOGSDB);
    }

    public void testFieldValueRangeForLogsDBModeNoCFS() throws Exception {
        testFieldValueRange(false, IndexMode.LOGSDB);
    }

    public void testFieldValueRangeForStandardModeWithCFS() throws Exception {
        testFieldValueRange(true, IndexMode.STANDARD);
    }

    public void testFieldValueRangeForStandardModeNoCFS() throws Exception {
        testFieldValueRange(false, IndexMode.STANDARD);
    }

    public void testFieldValueRangeForLookupModeWithCFS() throws Exception {
        testFieldValueRange(true, IndexMode.LOOKUP);
    }

    public void testFieldValueRangeForLookupModeNoCFS() throws Exception {
        testFieldValueRange(false, IndexMode.LOOKUP);
    }

    public void testSoftDeletesAreAlmostAlwaysDisregardedForTimestampRange() throws Exception {
        IndexMode indexMode = randomFrom(IndexMode.values());
        DocumentMapper mapper = getDocumentMapper(indexMode);
        boolean useCFS = randomBoolean();
        IndexWriterConfig indexWriterConfig = getIndexWriterConfig(useCFS, randomBoolean());
        IndexCommit previousCommit = null;
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, indexWriterConfig)) {
                String docId1 = indexMode == IndexMode.TIME_SERIES ? TimeSeriesRoutingHashFieldMapper.encode(1) : "docId1";
                long timestamp1 = randomLongBetween(10_000L, 1_000_000L);
                indexWriter.addDocument(mapper.parse(source(docId1, b -> { b.field("@timestamp", timestamp1); }, null)).rootDoc());
                String docId2 = indexMode == IndexMode.TIME_SERIES ? TimeSeriesRoutingHashFieldMapper.encode(2) : "docId2";
                long timestamp2 = randomLongBetween(10_000L, 1_000_000L);
                indexWriter.addDocument(mapper.parse(source(docId2, b -> { b.field("@timestamp", timestamp2); }, null)).rootDoc());
                boolean doc1Deleted = randomBoolean();
                deleteDoc(doc1Deleted ? docId1 : docId2, indexWriter, indexMode);
                indexWriter.commit();
                try (DirectoryReader indexReader = DirectoryReader.open(indexWriter)) {
                    assertThat(totalDocCount(indexReader), equalTo(1));
                    IndexCommit currentCommit = indexReader.getIndexCommit();
                    // the deletion of docId1 or docId2 is effectively ignored
                    assertTimestampRange(
                        previousCommit,
                        currentCommit,
                        mapper.mappers().getTimestampFieldType(),
                        Math.min(timestamp1, timestamp2),
                        Math.max(timestamp1, timestamp2),
                        useCFS
                    );
                    previousCommit = currentCommit;
                }
                String docId3 = indexMode == IndexMode.TIME_SERIES ? TimeSeriesRoutingHashFieldMapper.encode(3) : "docId3";
                long timestamp3 = randomLongBetween(10_000L, 1_000_000L);
                indexWriter.addDocument(mapper.parse(source(docId3, b -> { b.field("@timestamp", timestamp3); }, null)).rootDoc());
                // create a 1-doc segment
                indexWriter.flush();
                String docId4 = indexMode == IndexMode.TIME_SERIES ? TimeSeriesRoutingHashFieldMapper.encode(4) : "docId4";
                long timestamp4 = randomLongBetween(10_000L, 1_000_000L);
                indexWriter.addDocument(mapper.parse(source(docId4, b -> { b.field("@timestamp", timestamp4); }, null)).rootDoc());
                deleteDoc(doc1Deleted ? docId2 : docId1, indexWriter, indexMode);
                // delete doc from the previous 1-doc segment
                deleteDoc(docId3, indexWriter, indexMode);
                indexWriter.commit();
                try (DirectoryReader indexReader = DirectoryReader.open(indexWriter)) {
                    assertThat(totalDocCount(indexReader), equalTo(1));
                    IndexCommit currentCommit = indexReader.getIndexCommit();
                    // the deletion of docId3 is indeed effective (not ignored) because the whole segment is deleted
                    assertTimestampRange(
                        previousCommit,
                        currentCommit,
                        mapper.mappers().getTimestampFieldType(),
                        timestamp4,
                        timestamp4,
                        useCFS
                    );
                    // both docId1 and docId2 are deleted by this point
                    assertTimestampRange(null, currentCommit, mapper.mappers().getTimestampFieldType(), timestamp4, timestamp4, useCFS);
                }
            }
        }
    }

    public void testFieldValueRangeForBatchedCompoundCommit() throws Exception {
        IndexMode indexMode = randomFrom(IndexMode.values());
        DocumentMapper mapper = getDocumentMapper(indexMode);
        boolean useCFS = randomBoolean();
        Map<String, BlobLocation> uploadedBlobLocations = new HashMap<>();

        var primaryTerm = 1;
        AtomicInteger docId = new AtomicInteger();
        try (var fakeNode = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm)) {
            var numberOfBatchedCompoundCommits = randomIntBetween(1, 5);
            for (int batchNumber = 0; batchNumber < numberOfBatchedCompoundCommits; batchNumber++) {
                var numberOfNewCommits = randomIntBetween(1, 5);
                List<Long> timestampInCommits = randomList(
                    numberOfNewCommits,
                    numberOfNewCommits,
                    () -> randomLongBetween(10_000L, 1_000_000L) // make sure timestamp doesn't look like a year (has more than 4 digits)
                );
                IndexWriterConfig indexWriterConfig = getIndexWriterConfig(useCFS, false);
                var indexCommits = fakeNode.generateIndexCommits(numberOfNewCommits, false, false, generation -> {}, (commitId) -> {
                    try {
                        return mapper.parse(
                            source(
                                indexMode == IndexMode.TIME_SERIES
                                    ? TimeSeriesRoutingHashFieldMapper.encode(docId.incrementAndGet())
                                    : String.valueOf(docId.incrementAndGet()),
                                b -> {
                                    b.field("@timestamp", timestampInCommits.get(commitId));
                                },
                                null
                            )
                        ).rootDoc();
                    } catch (IOException e) {
                        fail(e);
                        return null;
                    }
                }, indexWriterConfig);

                long firstCommitGeneration = indexCommits.get(0).getGeneration();
                var virtualBatchedCompoundCommit = new VirtualBatchedCompoundCommit(
                    fakeNode.shardId,
                    "node-id",
                    primaryTerm,
                    firstCommitGeneration,
                    uploadedBlobLocations::get,
                    ESTestCase::randomNonNegativeLong,
                    fakeNode.sharedCacheService.getRegionSize(),
                    randomIntBetween(0, fakeNode.sharedCacheService.getRegionSize())
                );
                for (StatelessCommitRef statelessCommitRef : indexCommits) {
                    assertTrue(
                        virtualBatchedCompoundCommit.appendCommit(
                            statelessCommitRef,
                            randomBoolean(),
                            readTimestampFieldValueRangeAcrossAdditionalSegments(statelessCommitRef, mapper.mappers())
                        )
                    );
                }
                virtualBatchedCompoundCommit.freeze();

                try (BytesStreamOutput output = new BytesStreamOutput()) {
                    assertTrue(virtualBatchedCompoundCommit.isFrozen());
                    try (var frozenInputStream = virtualBatchedCompoundCommit.getFrozenInputStreamForUpload()) {
                        Streams.copy(frozenInputStream, output, false);
                    }
                    var batchedCompoundCommit = virtualBatchedCompoundCommit.getFrozenBatchedCompoundCommit();
                    virtualBatchedCompoundCommit.close();
                    BatchedCompoundCommit deserializedBatchedCompoundCommit = deserializeBatchedCompoundCommit(
                        virtualBatchedCompoundCommit.getBlobName(),
                        output
                    );
                    assertEquals(batchedCompoundCommit, deserializedBatchedCompoundCommit);

                    for (int i = 0; i < deserializedBatchedCompoundCommit.compoundCommits().size(); i++) {
                        StatelessCompoundCommit compoundCommit = deserializedBatchedCompoundCommit.compoundCommits().get(i);
                        // Update uploaded blob locations that can be used in the next batched compound commits
                        uploadedBlobLocations.putAll(compoundCommit.commitFiles());

                        Map<String, BlobLocation> commitFiles = compoundCommit.commitFiles();
                        // Make sure that all internal files are on the same blob
                        Set<String> internalFiles = compoundCommit.getInternalFiles();
                        internalFiles.forEach(f -> assertEquals(virtualBatchedCompoundCommit.getBlobName(), commitFiles.get(f).blobName()));
                        // Check that internalFiles are sorted according to the file size and name
                        assertThat(
                            internalFiles.stream().sorted(Comparator.comparingLong(e -> commitFiles.get(e).offset())).toList(),
                            equalTo(
                                internalFiles.stream()
                                    .sorted(Comparator.<String>comparingLong(e -> commitFiles.get(e).fileLength()).thenComparing(it -> it))
                                    .toList()
                            )
                        );

                        // all docs have a timestamp field (and mapping correctly interprets it as such)
                        assertNotNull(compoundCommit.timestampFieldValueRange());
                        // commits only have a single doc (that's neither merged nor deleted)
                        assertThat(
                            compoundCommit.timestampFieldValueRange().minMillis(),
                            equalTo(compoundCommit.timestampFieldValueRange().maxMillis())
                        );
                        assertThat(compoundCommit.timestampFieldValueRange().minMillis(), equalTo(timestampInCommits.get(i)));
                    }
                }
            }
        }
    }

    private void testFieldValueRange(boolean useCFS, IndexMode indexMode) throws Exception {
        DocumentMapper mapper = getDocumentMapper(indexMode);
        IndexWriterConfig indexWriterConfig = getIndexWriterConfig(useCFS, randomBoolean());
        int docCount = randomIntBetween(1, 100);
        // how often (in no of indexed docs) to commit
        int commitEveryNDocCount = randomIntBetween(1, 10);
        // how often (in no of indexed docs) to flush (create another segment without creating a commit point)
        int flushEveryNDocCount = randomIntBetween(1, 10);
        long minTimestamp = Long.MAX_VALUE;
        long maxTimestamp = Long.MIN_VALUE;
        IndexCommit previousCommit = null;
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = new IndexWriter(directory, indexWriterConfig)) {
                for (int i = 0; i < docCount; i++) {
                    // make sure timestamp doesn't look like a year
                    long timestamp = randomLongBetween(10_000L, 1_000_000L);
                    String docId = indexMode == IndexMode.TIME_SERIES ? TimeSeriesRoutingHashFieldMapper.encode(i) : randomUUID();
                    indexWriter.addDocument(mapper.parse(source(docId, b -> { b.field("@timestamp", timestamp); }, null)).rootDoc());
                    flushEveryNDocCount--;
                    commitEveryNDocCount--;
                    minTimestamp = Long.min(minTimestamp, timestamp);
                    maxTimestamp = Long.max(maxTimestamp, timestamp);
                    // create new segment
                    if (flushEveryNDocCount <= 0) {
                        indexWriter.flush();
                        flushEveryNDocCount = randomIntBetween(1, 10);
                    }
                    // create new generation
                    if (commitEveryNDocCount <= 0) {
                        indexWriter.commit();
                        try (DirectoryReader indexReader = DirectoryReader.open(indexWriter)) {
                            IndexCommit currentCommit = indexReader.getIndexCommit();
                            assertTimestampRange(
                                previousCommit,
                                currentCommit,
                                mapper.mappers().getTimestampFieldType(),
                                minTimestamp,
                                maxTimestamp,
                                useCFS
                            );
                            previousCommit = currentCommit;
                        }
                        minTimestamp = Long.MAX_VALUE;
                        maxTimestamp = Long.MIN_VALUE;
                        commitEveryNDocCount = randomIntBetween(1, 10);
                    }
                }
            }
            try (
                DirectoryReader indexReader = new SoftDeletesDirectoryReaderWrapper(
                    DirectoryReader.open(directory),
                    Lucene.SOFT_DELETES_FIELD
                )
            ) {
                IndexCommit currentCommit = indexReader.getIndexCommit();
                assertTimestampRange(
                    previousCommit,
                    currentCommit,
                    mapper.mappers().getTimestampFieldType(),
                    minTimestamp,
                    maxTimestamp,
                    useCFS
                );
            }
        }
    }

    private void deleteDoc(String docIdToDelete, IndexWriter indexWriter, IndexMode indexMode) throws IOException {
        var deletedDoc = ParsedDocument.deleteTombstone(
            indexMode == IndexMode.TIME_SERIES || indexMode == IndexMode.LOGSDB
                ? SeqNoFieldMapper.SeqNoIndexOptions.DOC_VALUES_ONLY
                : SeqNoFieldMapper.SeqNoIndexOptions.POINTS_AND_DOC_VALUES,
            docIdToDelete
        ).docs().get(0);
        var softDeletesField = Lucene.newSoftDeletesField();
        deletedDoc.add(softDeletesField);
        indexWriter.softUpdateDocument(new Term(IdFieldMapper.NAME, Uid.encodeId(docIdToDelete)), deletedDoc, softDeletesField);
    }

    private int totalDocCount(IndexReader indexReader) throws IOException {
        IndexSearcher indexSearcher = newSearcher(indexReader);
        var topDocs = indexSearcher.search(Queries.ALL_DOCS_INSTANCE, Integer.MAX_VALUE);
        return (int) topDocs.totalHits.value();
    }

    private DocumentMapper getDocumentMapper(IndexMode indexMode) throws IOException {
        if (indexMode == IndexMode.STANDARD || indexMode == IndexMode.LOOKUP) {
            boolean nanosTimestampResolution = randomBoolean();
            if (nanosTimestampResolution) {
                return createDocumentMapper(mapping(b -> {
                    b.startObject("@timestamp")
                        .field("type", "date_nanos")
                        .field("format", "epoch_millis")
                        .field("doc_values", randomBoolean())
                        .field("store", randomBoolean())
                        .endObject();
                }), indexMode);
            } else {
                return createDocumentMapper(mapping(b -> {
                    b.startObject("@timestamp")
                        .field("type", "date")
                        .field("format", "epoch_millis")
                        .field("doc_values", randomBoolean())
                        .field("store", randomBoolean())
                        .endObject();
                }), indexMode);
            }
        } else {
            // TIME_SERIES and LOGSDB indexing modes use a fixed mapping for the @timestamp field
            return createDocumentMapper(mapping(b -> {}), indexMode);
        }
    }

    private IndexWriterConfig getIndexWriterConfig(boolean useCFS, boolean deletePreviousCommits) {
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig();
        indexWriterConfig.setUseCompoundFile(useCFS);
        if (deletePreviousCommits == false) {
            indexWriterConfig.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);
        }
        // this test assumes all docs are added to new segments, for which we assert the timestamp intervals
        // merging falsifies this test assumption, because it creates a new segment from previous ones
        indexWriterConfig.setMergePolicy(NoMergePolicy.INSTANCE);
        if (useCFS) {
            indexWriterConfig.getMergePolicy().setMaxCFSSegmentSizeMB(Double.POSITIVE_INFINITY);
            indexWriterConfig.getMergePolicy().setNoCFSRatio(1.0D);
        } else {
            indexWriterConfig.getMergePolicy().setMaxCFSSegmentSizeMB(0.0D);
            indexWriterConfig.getMergePolicy().setNoCFSRatio(0.0D);
        }
        indexWriterConfig.setSoftDeletesField(Lucene.SOFT_DELETES_FIELD);
        return indexWriterConfig;
    }

    private void assertTimestampRange(
        IndexCommit previousIndexCommit,
        IndexCommit currentIndexCommit,
        DateFieldMapper.DateFieldType timestampFieldType,
        long minTimestamp,
        long maxTimestamp,
        boolean useCFS
    ) throws IOException {
        var subsetSegmentNames = Lucene.additionalFileNames(previousIndexCommit, currentIndexCommit)
            .stream()
            .filter(luceneFileName -> Objects.equals(LuceneFilesExtensions.fromFile(luceneFileName), LuceneFilesExtensions.SI))
            .map(IndexFileNames::stripExtension)
            .collect(Collectors.toUnmodifiableSet());
        var timestampFieldValueRange = IndexEngine.readTimestampFieldValueRangeAcrossSegments(
            currentIndexCommit,
            subsetSegmentNames,
            timestampFieldType
        );
        if (subsetSegmentNames.isEmpty()) {
            assertNull(timestampFieldValueRange);
        } else {
            assertNotNull(timestampFieldValueRange);
            assertThat(timestampFieldValueRange.minMillis(), equalTo(minTimestamp));
            assertThat(timestampFieldValueRange.maxMillis(), equalTo(maxTimestamp));
            // this only covers test correctness
            assertCFSSegments(currentIndexCommit, useCFS);
        }
    }

    private @Nullable StatelessCompoundCommit.TimestampFieldValueRange readTimestampFieldValueRangeAcrossAdditionalSegments(
        StatelessCommitRef reference,
        MappingLookup mappingLookup
    ) throws IOException {
        assert Thread.holdsLock(this) == false : "This method does file IO so better avoid holding locks";
        var additionalSegmentNames = reference.getAdditionalFiles()
            .stream()
            .filter(luceneFileName -> Objects.equals(LuceneFilesExtensions.fromFile(luceneFileName), LuceneFilesExtensions.SI))
            .map(IndexFileNames::stripExtension)
            .collect(Collectors.toUnmodifiableSet());
        return IndexEngine.readTimestampFieldValueRangeAcrossSegments(
            reference.getIndexCommit(),
            additionalSegmentNames,
            mappingLookup.getTimestampFieldType()
        );
    }

    private void assertCFSSegments(IndexCommit indexCommit, boolean usesCFS) throws IOException {
        final var segmentInfos = SegmentInfos.readCommit(indexCommit.getDirectory(), indexCommit.getSegmentsFileName());
        for (var segmentCommitInfo : segmentInfos) {
            assertThat(segmentCommitInfo.info.getUseCompoundFile(), equalTo(usesCFS));
        }
    }
}
