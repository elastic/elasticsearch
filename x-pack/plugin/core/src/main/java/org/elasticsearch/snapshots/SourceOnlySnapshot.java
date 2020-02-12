/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.snapshots;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.blocktree.BlockTreeTermsReader;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SoftDeletesDirectoryReaderWrapper;
import org.apache.lucene.index.StandardDirectoryReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.core.internal.io.IOUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.lucene.codecs.compressing.CompressingStoredFieldsWriter.FIELDS_EXTENSION;
import static org.apache.lucene.codecs.compressing.CompressingStoredFieldsWriter.INDEX_EXTENSION_PREFIX;
import static org.apache.lucene.codecs.compressing.FieldsIndexWriter.FIELDS_INDEX_EXTENSION_SUFFIX;
import static org.apache.lucene.codecs.compressing.FieldsIndexWriter.FIELDS_META_EXTENSION_SUFFIX;

public class SourceOnlySnapshot {

    private static final String FIELDS_INDEX_EXTENSION = INDEX_EXTENSION_PREFIX + FIELDS_INDEX_EXTENSION_SUFFIX;
    private static final String FIELDS_META_EXTENSION = INDEX_EXTENSION_PREFIX + FIELDS_META_EXTENSION_SUFFIX;
    private final Directory targetDirectory;
    private final Supplier<Query> deleteByQuerySupplier;

    public SourceOnlySnapshot(Directory targetDirectory, Supplier<Query> deleteByQuerySupplier) {
        this.targetDirectory = targetDirectory;
        this.deleteByQuerySupplier = deleteByQuerySupplier;
    }

    public SourceOnlySnapshot(Directory targetDirectory) {
        this(targetDirectory, null);
    }

    public synchronized List<String> syncSnapshot(IndexCommit commit) throws IOException {
        long generation;
        Map<BytesRef, SegmentCommitInfo> existingSegments = new HashMap<>();
        if (Lucene.indexExists(targetDirectory)) {
            SegmentInfos existingsSegmentInfos = Lucene.readSegmentInfos(targetDirectory);
            for (SegmentCommitInfo info : existingsSegmentInfos) {
                existingSegments.put(new BytesRef(info.info.getId()), info);
            }
            generation = existingsSegmentInfos.getGeneration();
        } else {
            generation = 1;
        }
        List<String> createdFiles = new ArrayList<>();
        String segmentFileName;
        try (Lock writeLock = targetDirectory.obtainLock(IndexWriter.WRITE_LOCK_NAME);
             StandardDirectoryReader reader = (StandardDirectoryReader) DirectoryReader.open(commit,
                 Collections.singletonMap(BlockTreeTermsReader.FST_MODE_KEY, BlockTreeTermsReader.FSTLoadMode.OFF_HEAP.name()))) {
            SegmentInfos segmentInfos = reader.getSegmentInfos().clone();
            DirectoryReader wrappedReader = wrapReader(reader);
            List<SegmentCommitInfo> newInfos = new ArrayList<>();
            for (LeafReaderContext ctx : wrappedReader.leaves()) {
                LeafReader leafReader = ctx.reader();
                SegmentCommitInfo info = Lucene.segmentReader(leafReader).getSegmentInfo();
                LiveDocs liveDocs = getLiveDocs(leafReader);
                if (leafReader.numDocs() != 0) { // fully deleted segments don't need to be processed
                    SegmentCommitInfo newInfo = syncSegment(info, liveDocs, leafReader.getFieldInfos(), existingSegments, createdFiles);
                    newInfos.add(newInfo);
                }
            }
            segmentInfos.clear();
            segmentInfos.addAll(newInfos);
            segmentInfos.setNextWriteGeneration(Math.max(segmentInfos.getGeneration(), generation) + 1);
            String pendingSegmentFileName = IndexFileNames.fileNameFromGeneration(IndexFileNames.PENDING_SEGMENTS,
                "", segmentInfos.getGeneration());
            try (IndexOutput segnOutput = targetDirectory.createOutput(pendingSegmentFileName, IOContext.DEFAULT)) {
                segmentInfos.write(targetDirectory, segnOutput);
            }
            targetDirectory.sync(Collections.singleton(pendingSegmentFileName));
            targetDirectory.sync(createdFiles);
            segmentFileName = IndexFileNames.fileNameFromGeneration(IndexFileNames.SEGMENTS, "", segmentInfos.getGeneration());
            targetDirectory.rename(pendingSegmentFileName, segmentFileName);
        }
        Lucene.pruneUnreferencedFiles(segmentFileName, targetDirectory);
        assert assertCheckIndex();
        return Collections.unmodifiableList(createdFiles);
    }

    private LiveDocs getLiveDocs(LeafReader reader) throws IOException {
        if (deleteByQuerySupplier != null) {
            // we have this additional delete by query functionality to filter out documents before we snapshot them
            // we can't filter after the fact since we don't have an index anymore.
            Query query = deleteByQuerySupplier.get();
            IndexSearcher s = new IndexSearcher(reader);
            s.setQueryCache(null);
            Query rewrite = s.rewrite(query);
            Weight weight = s.createWeight(rewrite, ScoreMode.COMPLETE_NO_SCORES, 1.0f);
            Scorer scorer = weight.scorer(reader.getContext());
            if (scorer != null) {
                DocIdSetIterator iterator = scorer.iterator();
                if (iterator != null) {
                    Bits liveDocs = reader.getLiveDocs();
                    final FixedBitSet bits;
                    if (liveDocs != null) {
                        bits = FixedBitSet.copyOf(liveDocs);
                    } else {
                        bits = new FixedBitSet(reader.maxDoc());
                        bits.set(0, reader.maxDoc());
                    }
                    int newDeletes = apply(iterator, bits);
                    if (newDeletes != 0) {
                        int numDeletes = reader.numDeletedDocs() + newDeletes;
                        return new LiveDocs(numDeletes, bits);
                    }
                }
            }
        }
        return new LiveDocs(reader.numDeletedDocs(), reader.getLiveDocs());
    }

    private int apply(DocIdSetIterator iterator, FixedBitSet bits) throws IOException {
        int docID = -1;
        int newDeletes = 0;
        while ((docID = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            if (bits.get(docID)) {
                bits.clear(docID);
                newDeletes++;
            }
        }
        return newDeletes;
    }


    private boolean assertCheckIndex() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream(1024);
        try (CheckIndex checkIndex = new CheckIndex(targetDirectory)) {
            checkIndex.setFailFast(true);
            checkIndex.setInfoStream(new PrintStream(output, false, IOUtils.UTF_8), false);
            CheckIndex.Status status = checkIndex.checkIndex();
            if (status == null || status.clean == false) {
                throw new RuntimeException("CheckIndex failed: " + output.toString(IOUtils.UTF_8));
            }
            return true;
        }
    }

    DirectoryReader wrapReader(DirectoryReader reader) throws IOException {
        String softDeletesField = null;
        for (LeafReaderContext ctx : reader.leaves()) {
            String field = ctx.reader().getFieldInfos().getSoftDeletesField();
            if (field != null) {
                softDeletesField = field;
                break;
            }
        }
        return softDeletesField == null ? reader : new SoftDeletesDirectoryReaderWrapper(reader, softDeletesField);
    }

    private SegmentCommitInfo syncSegment(SegmentCommitInfo segmentCommitInfo, LiveDocs liveDocs, FieldInfos fieldInfos,
                                          Map<BytesRef, SegmentCommitInfo> existingSegments, List<String> createdFiles) throws IOException {
        SegmentInfo si = segmentCommitInfo.info;
        Codec codec = si.getCodec();
        final String segmentSuffix = "";
        SegmentCommitInfo newInfo;
        final TrackingDirectoryWrapper trackingDir = new TrackingDirectoryWrapper(targetDirectory);
        BytesRef segmentId = new BytesRef(si.getId());
        boolean exists = existingSegments.containsKey(segmentId);
        if (exists == false) {
            SegmentInfo newSegmentInfo = new SegmentInfo(si.dir, si.getVersion(), si.getMinVersion(), si.name, si.maxDoc(), false,
                si.getCodec(), si.getDiagnostics(), si.getId(), si.getAttributes(), null);
            // we drop the sort on purpose since the field we sorted on doesn't exist in the target index anymore.
            newInfo = new SegmentCommitInfo(newSegmentInfo, 0, 0, -1, -1, -1);
            List<FieldInfo> fieldInfoCopy = new ArrayList<>(fieldInfos.size());
            for (FieldInfo fieldInfo : fieldInfos) {
                fieldInfoCopy.add(new FieldInfo(fieldInfo.name, fieldInfo.number,
                    false, false, false, IndexOptions.NONE, DocValuesType.NONE, -1, fieldInfo.attributes(), 0, 0, 0,
                    fieldInfo.isSoftDeletesField()));
            }
            FieldInfos newFieldInfos = new FieldInfos(fieldInfoCopy.toArray(new FieldInfo[0]));
            codec.fieldInfosFormat().write(trackingDir, newSegmentInfo, segmentSuffix, newFieldInfos, IOContext.DEFAULT);
            newInfo.setFieldInfosFiles(trackingDir.getCreatedFiles());
            String idxFile = IndexFileNames.segmentFileName(newSegmentInfo.name, segmentSuffix, FIELDS_INDEX_EXTENSION);
            String dataFile = IndexFileNames.segmentFileName(newSegmentInfo.name, segmentSuffix, FIELDS_EXTENSION);
            String metaFile = IndexFileNames.segmentFileName(newSegmentInfo.name, segmentSuffix, FIELDS_META_EXTENSION);
            Directory sourceDir = newSegmentInfo.dir;
            if (si.getUseCompoundFile()) {
                sourceDir = codec.compoundFormat().getCompoundReader(sourceDir, si, IOContext.DEFAULT);
            }
            trackingDir.copyFrom(sourceDir, idxFile, idxFile, IOContext.DEFAULT);
            trackingDir.copyFrom(sourceDir, dataFile, dataFile, IOContext.DEFAULT);
             if (Arrays.asList(sourceDir.listAll()).contains(metaFile)) { // only exists for Lucene 8.5+ indices
                 trackingDir.copyFrom(sourceDir, metaFile, metaFile, IOContext.DEFAULT);
             }
            if (sourceDir != newSegmentInfo.dir) {
                sourceDir.close();
            }
        } else {
            newInfo = existingSegments.get(segmentId);
            assert newInfo.info.getUseCompoundFile() == false;
        }
        if (liveDocs.bits != null && liveDocs.numDeletes != 0 && liveDocs.numDeletes != newInfo.getDelCount()) {
            if (newInfo.getDelCount() != 0) {
                assert assertLiveDocs(liveDocs.bits, liveDocs.numDeletes);
            }
            codec.liveDocsFormat().writeLiveDocs(liveDocs.bits, trackingDir, newInfo, liveDocs.numDeletes - newInfo.getDelCount(),
                IOContext.DEFAULT);
            SegmentCommitInfo info = new SegmentCommitInfo(newInfo.info, liveDocs.numDeletes, 0, newInfo.getNextDelGen(), -1, -1);
            info.setFieldInfosFiles(newInfo.getFieldInfosFiles());
            info.info.setFiles(trackingDir.getCreatedFiles());
            newInfo = info;
        }
        if (exists == false) {
            newInfo.info.setFiles(trackingDir.getCreatedFiles());
            codec.segmentInfoFormat().write(trackingDir, newInfo.info, IOContext.DEFAULT);
        }
        createdFiles.addAll(trackingDir.getCreatedFiles());
        return newInfo;
    }

    private boolean assertLiveDocs(Bits liveDocs, int deletes) {
        int actualDeletes = 0;
        for (int i = 0; i < liveDocs.length(); i++) {
            if (liveDocs.get(i) == false) {
                actualDeletes++;
            }
        }
        assert actualDeletes == deletes : " actual: " + actualDeletes + " deletes: " + deletes;
        return true;
    }

    private static class LiveDocs {
        final int numDeletes;
        final Bits bits;

        LiveDocs(int numDeletes, Bits bits) {
            this.numDeletes = numDeletes;
            this.bits = bits;
        }
    }
}
