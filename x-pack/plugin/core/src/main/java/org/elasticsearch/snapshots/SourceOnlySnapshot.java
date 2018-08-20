/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.snapshots;

import org.apache.lucene.codecs.Codec;
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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.core.internal.io.IOUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.lucene.codecs.compressing.CompressingStoredFieldsWriter.FIELDS_EXTENSION;
import static org.apache.lucene.codecs.compressing.CompressingStoredFieldsWriter.FIELDS_INDEX_EXTENSION;

public final class SourceOnlySnapshot {
    private final Directory targetDirectory;
    private final String softDeletesField;
    private final List<String> createdFiles = new ArrayList<>();

    public SourceOnlySnapshot(Directory targetDirectory, String softDeletesField) {
        this.targetDirectory = targetDirectory;
        this.softDeletesField = softDeletesField;
    }

    public List<String> getCreatedFiles() {
        return createdFiles;
    }

    public synchronized void syncSnapshot(IndexCommit commit) throws IOException {
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
        String segmentFileName;
        try (Lock writeLock = targetDirectory.obtainLock(IndexWriter.WRITE_LOCK_NAME);
             StandardDirectoryReader reader = (StandardDirectoryReader) DirectoryReader.open(commit)) {
            SegmentInfos segmentInfos = reader.getSegmentInfos();
            DirectoryReader wrapper = wrapReader(reader);
            List<SegmentCommitInfo> newInfos = new ArrayList<>();
            for (LeafReaderContext ctx : wrapper.leaves()) {
                SegmentCommitInfo info = segmentInfos.info(ctx.ord);
                LeafReader leafReader = ctx.reader();
                Bits liveDocs = leafReader.getLiveDocs();
                SegmentCommitInfo newInfo = syncSegment(info, liveDocs, leafReader.getFieldInfos(), existingSegments);
                newInfos.add(newInfo);
            }
            segmentInfos.clear();
            segmentInfos.addAll(newInfos);
            segmentInfos.setNextWriteGeneration(Math.max(segmentInfos.getGeneration(), generation)+1);
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
    }

    private boolean assertCheckIndex() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream(1024);
        try (CheckIndex checkIndex = new CheckIndex(targetDirectory)) {
            checkIndex.setFailFast(true);
            checkIndex.setCrossCheckTermVectors(false);
            checkIndex.setInfoStream(new PrintStream(output, false, IOUtils.UTF_8), false);
            CheckIndex.Status status = checkIndex.checkIndex();
            if (status == null || status.clean == false) {
                throw new RuntimeException("CheckIndex failed: " + output.toString(IOUtils.UTF_8));
            }
            return true;
        }
    }

    DirectoryReader wrapReader(DirectoryReader reader) throws IOException {
        return softDeletesField == null ? reader : new SoftDeletesDirectoryReaderWrapper(reader, softDeletesField);
    }

    private SegmentCommitInfo syncSegment(SegmentCommitInfo segmentCommitInfo, Bits liveDocs, FieldInfos fieldInfos,
                                          Map<BytesRef, SegmentCommitInfo> existingSegments) throws IOException {
        SegmentInfo si = segmentCommitInfo.info;
        Codec codec = si.getCodec();
        final String segmentSuffix = "";
        SegmentCommitInfo newInfo;
        final TrackingDirectoryWrapper trackingDir = new TrackingDirectoryWrapper(targetDirectory);
        BytesRef segmentId = new BytesRef(si.getId());
        boolean exists = existingSegments.containsKey(segmentId);
        if (exists == false) {
            SegmentInfo newSegmentInfo = new SegmentInfo(si.dir, si.getVersion(), si.getMinVersion(), si.name, si.maxDoc(), false,
                si.getCodec(), si.getDiagnostics(), si.getId(), si.getAttributes(), si.getIndexSort()); // TODO should we drop the sort?
            newInfo = new SegmentCommitInfo(newSegmentInfo, 0, 0, -1, -1, -1);
            List<FieldInfo> fieldInfoCopy = new ArrayList<>(fieldInfos.size());
            for (FieldInfo fieldInfo : fieldInfos) {
                    fieldInfoCopy.add(new FieldInfo(fieldInfo.name, fieldInfo.number,
                        false, false, false, IndexOptions.NONE, DocValuesType.NONE, -1, fieldInfo.attributes(), 0, 0, false));
            }
            FieldInfos newFieldInfos = new FieldInfos(fieldInfoCopy.toArray(new FieldInfo[0]));
            codec.fieldInfosFormat().write(trackingDir, newSegmentInfo, segmentSuffix, newFieldInfos, IOContext.DEFAULT);
            newInfo.setFieldInfosFiles(trackingDir.getCreatedFiles());
            String idxFile = IndexFileNames.segmentFileName(newSegmentInfo.name, segmentSuffix, FIELDS_INDEX_EXTENSION);
            String dataFile = IndexFileNames.segmentFileName(newSegmentInfo.name, segmentSuffix, FIELDS_EXTENSION);
            Directory sourceDir = newSegmentInfo.dir;
            if (si.getUseCompoundFile()) {
                sourceDir = codec.compoundFormat().getCompoundReader(sourceDir, si, IOContext.DEFAULT);
            }
            trackingDir.copyFrom(sourceDir, idxFile, idxFile, IOContext.DEFAULT);
            trackingDir.copyFrom(sourceDir, dataFile, dataFile, IOContext.DEFAULT);
            if (sourceDir != newSegmentInfo.dir) {
                sourceDir.close();
            }
        } else {
            newInfo = existingSegments.get(segmentId);
            assert newInfo.info.getUseCompoundFile() == false;
        }
        int deletes = segmentCommitInfo.getDelCount() + segmentCommitInfo.getSoftDelCount();
        if (liveDocs != null && deletes != 0 && deletes != newInfo.getDelCount()) {
            if (newInfo.getDelCount() != 0) {
                assert assertLiveDocs(liveDocs, deletes);
            }
            codec.liveDocsFormat().writeLiveDocs(liveDocs, trackingDir, newInfo, deletes - newInfo.getDelCount(),
                IOContext.DEFAULT);
            SegmentCommitInfo info = new SegmentCommitInfo(newInfo.info, deletes, 0, newInfo.getNextDelGen(), -1, -1);
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
        for (int i = 0; i < liveDocs.length(); i++ ) {
            if (liveDocs.get(i) == false) {
                actualDeletes++;
            }
        }
        assert actualDeletes == deletes : " actual: " + actualDeletes + " deletes: " + deletes;
        return true;
    }
}
