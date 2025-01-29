/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.snapshots.sourceonly;

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
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.core.IOUtils;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsWriter.FIELDS_EXTENSION;
import static org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsWriter.INDEX_EXTENSION;
import static org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsWriter.META_EXTENSION;

public class SourceOnlySnapshot {

    private static final String FIELDS_INDEX_EXTENSION = INDEX_EXTENSION;
    private static final String FIELDS_META_EXTENSION = META_EXTENSION;
    private final LinkedFilesDirectory targetDirectory;
    private final Supplier<Query> deleteByQuerySupplier;

    public SourceOnlySnapshot(LinkedFilesDirectory targetDirectory, Supplier<Query> deleteByQuerySupplier) {
        this.targetDirectory = targetDirectory;
        this.deleteByQuerySupplier = deleteByQuerySupplier;
    }

    public SourceOnlySnapshot(LinkedFilesDirectory targetDirectory) {
        this(targetDirectory, null);
    }

    public synchronized List<String> syncSnapshot(IndexCommit commit) throws IOException {
        long generation;
        Map<BytesRef, SegmentCommitInfo> existingSegments = new HashMap<>();
        if (Lucene.indexExists(targetDirectory.getWrapped())) {
            SegmentInfos existingsSegmentInfos = Lucene.readSegmentInfos(targetDirectory.getWrapped());
            for (SegmentCommitInfo info : existingsSegmentInfos) {
                existingSegments.put(new BytesRef(info.info.getId()), info);
            }
            generation = existingsSegmentInfos.getGeneration();
        } else {
            generation = 1;
        }
        List<String> createdFiles = new ArrayList<>();
        String segmentFileName;
        try (
            Lock writeLock = targetDirectory.obtainLock(IndexWriter.WRITE_LOCK_NAME);
            StandardDirectoryReader reader = (StandardDirectoryReader) DirectoryReader.open(commit)
        ) {
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
            String pendingSegmentFileName = IndexFileNames.fileNameFromGeneration(
                IndexFileNames.PENDING_SEGMENTS,
                "",
                segmentInfos.getGeneration()
            );
            try (IndexOutput segnOutput = targetDirectory.createOutput(pendingSegmentFileName, IOContext.DEFAULT)) {
                segmentInfos.write(segnOutput);
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

    private static int apply(DocIdSetIterator iterator, FixedBitSet bits) throws IOException {
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

    private SegmentCommitInfo syncSegment(
        SegmentCommitInfo segmentCommitInfo,
        LiveDocs liveDocs,
        FieldInfos fieldInfos,
        Map<BytesRef, SegmentCommitInfo> existingSegments,
        List<String> createdFiles
    ) throws IOException {
        Directory toClose = null;
        try {
            SegmentInfo si = segmentCommitInfo.info;
            Codec codec = si.getCodec();
            Directory sourceDir = si.dir;
            if (si.getUseCompoundFile()) {
                sourceDir = new LinkedFilesDirectory.CloseMePleaseWrapper(
                    codec.compoundFormat().getCompoundReader(sourceDir, si, IOContext.DEFAULT)
                );
                toClose = sourceDir;
            }
            final String segmentSuffix = "";
            SegmentCommitInfo newInfo;
            final TrackingDirectoryWrapper trackingDir = new TrackingDirectoryWrapper(targetDirectory);
            BytesRef segmentId = new BytesRef(si.getId());
            boolean exists = existingSegments.containsKey(segmentId);
            if (exists == false) {
                SegmentInfo newSegmentInfo = new SegmentInfo(
                    targetDirectory,
                    si.getVersion(),
                    si.getMinVersion(),
                    si.name,
                    si.maxDoc(),
                    false,
                    si.getHasBlocks(),
                    si.getCodec(),
                    si.getDiagnostics(),
                    si.getId(),
                    si.getAttributes(),
                    null
                );
                // we drop the sort on purpose since the field we sorted on doesn't exist in the target index anymore.
                newInfo = new SegmentCommitInfo(newSegmentInfo, 0, 0, -1, -1, -1, StringHelper.randomId());
                List<FieldInfo> fieldInfoCopy = new ArrayList<>(fieldInfos.size());
                for (FieldInfo fieldInfo : fieldInfos) {
                    fieldInfoCopy.add(
                        new FieldInfo(
                            fieldInfo.name,
                            fieldInfo.number,
                            false,
                            false,
                            false,
                            IndexOptions.NONE,
                            DocValuesType.NONE,
                            fieldInfo.docValuesSkipIndexType(),
                            -1,
                            fieldInfo.attributes(),
                            0,
                            0,
                            0,
                            0,
                            VectorEncoding.FLOAT32,
                            VectorSimilarityFunction.EUCLIDEAN,
                            fieldInfo.isSoftDeletesField(),
                            fieldInfo.isParentField()
                        )
                    );
                }
                FieldInfos newFieldInfos = new FieldInfos(fieldInfoCopy.toArray(new FieldInfo[0]));
                codec.fieldInfosFormat().write(trackingDir, newSegmentInfo, segmentSuffix, newFieldInfos, IOContext.DEFAULT);
                newInfo.setFieldInfosFiles(trackingDir.getCreatedFiles());
            } else {
                newInfo = existingSegments.get(segmentId);
                assert newInfo.info.getUseCompoundFile() == false;
            }

            // link files for stored fields to target directory
            final String idxFile = IndexFileNames.segmentFileName(newInfo.info.name, segmentSuffix, FIELDS_INDEX_EXTENSION);
            final String dataFile = IndexFileNames.segmentFileName(newInfo.info.name, segmentSuffix, FIELDS_EXTENSION);
            final String metaFile = IndexFileNames.segmentFileName(newInfo.info.name, segmentSuffix, FIELDS_META_EXTENSION);
            trackingDir.copyFrom(sourceDir, idxFile, idxFile, IOContext.DEFAULT);
            assert targetDirectory.linkedFiles.containsKey(idxFile);
            assert trackingDir.getCreatedFiles().contains(idxFile);
            trackingDir.copyFrom(sourceDir, dataFile, dataFile, IOContext.DEFAULT);
            assert targetDirectory.linkedFiles.containsKey(dataFile);
            assert trackingDir.getCreatedFiles().contains(dataFile);
            if (Arrays.asList(sourceDir.listAll()).contains(metaFile)) { // only exists for Lucene 8.5+ indices
                trackingDir.copyFrom(sourceDir, metaFile, metaFile, IOContext.DEFAULT);
                assert targetDirectory.linkedFiles.containsKey(metaFile);
                assert trackingDir.getCreatedFiles().contains(metaFile);
            }

            if (liveDocs.bits != null && liveDocs.numDeletes != 0 && liveDocs.numDeletes != newInfo.getDelCount()) {
                assert newInfo.getDelCount() == 0 || assertLiveDocs(liveDocs.bits, liveDocs.numDeletes);
                codec.liveDocsFormat()
                    .writeLiveDocs(liveDocs.bits, trackingDir, newInfo, liveDocs.numDeletes - newInfo.getDelCount(), IOContext.DEFAULT);
                SegmentCommitInfo info = new SegmentCommitInfo(
                    newInfo.info,
                    liveDocs.numDeletes,
                    0,
                    newInfo.getNextDelGen(),
                    -1,
                    -1,
                    StringHelper.randomId()
                );
                info.setFieldInfosFiles(newInfo.getFieldInfosFiles());
                info.info.setFiles(trackingDir.getCreatedFiles());
                newInfo = info;
            }
            if (exists == false) {
                newInfo.info.setFiles(trackingDir.getCreatedFiles());
                codec.segmentInfoFormat().write(trackingDir, newInfo.info, IOContext.DEFAULT);
            }
            final Set<String> createdFilesForThisSegment = trackingDir.getCreatedFiles();
            createdFilesForThisSegment.remove(idxFile);
            createdFilesForThisSegment.remove(dataFile);
            createdFilesForThisSegment.remove(metaFile);
            createdFiles.addAll(createdFilesForThisSegment);
            return newInfo;
        } finally {
            IOUtils.close(toClose);
        }
    }

    private static boolean assertLiveDocs(Bits liveDocs, int deletes) {
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

    public static class LinkedFilesDirectory extends Directory {

        private final Directory wrapped;
        private final Map<String, Directory> linkedFiles = new HashMap<>();

        public LinkedFilesDirectory(Directory wrapped) {
            this.wrapped = wrapped;
        }

        public Directory getWrapped() {
            return wrapped;
        }

        @Override
        public String[] listAll() throws IOException {
            Set<String> files = new HashSet<>();
            Collections.addAll(files, wrapped.listAll());
            files.addAll(linkedFiles.keySet());
            String[] result = files.toArray(Strings.EMPTY_ARRAY);
            Arrays.sort(result);
            return result;
        }

        @Override
        public void deleteFile(String name) throws IOException {
            final Directory directory = linkedFiles.remove(name);
            if (directory == null) {
                wrapped.deleteFile(name);
            } else {
                try (directory) {
                    wrapped.deleteFile(name);
                } catch (NoSuchFileException | FileNotFoundException e) {
                    // ignore
                }
            }
        }

        @Override
        public long fileLength(String name) throws IOException {
            final Directory linkedDir = linkedFiles.get(name);
            if (linkedDir != null) {
                return linkedDir.fileLength(name);
            } else {
                return wrapped.fileLength(name);
            }
        }

        @Override
        public IndexOutput createOutput(String name, IOContext context) throws IOException {
            if (linkedFiles.containsKey(name)) {
                throw new IllegalArgumentException("file cannot be created as linked file with name " + name + " already exists");
            } else {
                return wrapped.createOutput(name, context);
            }
        }

        @Override
        public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
            return wrapped.createTempOutput(prefix, suffix, context);
        }

        @Override
        public void sync(Collection<String> names) throws IOException {
            final List<String> primaryNames = new ArrayList<>();

            for (String name : names) {
                if (linkedFiles.containsKey(name) == false) {
                    primaryNames.add(name);
                }
            }

            if (primaryNames.isEmpty() == false) {
                wrapped.sync(primaryNames);
            }
        }

        @Override
        public void syncMetaData() throws IOException {
            wrapped.syncMetaData();
        }

        @Override
        public void rename(String source, String dest) throws IOException {
            if (linkedFiles.containsKey(source) || linkedFiles.containsKey(dest)) {
                throw new IllegalArgumentException(
                    "file cannot be renamed as linked file with name " + source + " or " + dest + " already exists"
                );
            } else {
                wrapped.rename(source, dest);
            }
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            final Directory linkedDir = linkedFiles.get(name);
            if (linkedDir != null) {
                return linkedDir.openInput(name, context);
            } else {
                return wrapped.openInput(name, context);
            }
        }

        @Override
        public Lock obtainLock(String name) throws IOException {
            return wrapped.obtainLock(name);
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(() -> IOUtils.close(linkedFiles.values()), linkedFiles::clear, wrapped);
        }

        @Override
        public void copyFrom(Directory from, String src, String dest, IOContext context) throws IOException {
            if (src.equals(dest) == false) {
                throw new IllegalArgumentException();
            } else {
                final Directory previous;
                if (from instanceof CloseMePleaseWrapper) {
                    ((CloseMePleaseWrapper) from).incRef();
                    previous = linkedFiles.put(src, from);
                } else {
                    previous = linkedFiles.put(src, new FilterDirectory(from) {
                        @Override
                        public void close() {
                            // ignore
                        }
                    });
                }
                IOUtils.close(previous);
            }
        }

        static class CloseMePleaseWrapper extends FilterDirectory {

            private final AtomicInteger refCount = new AtomicInteger(1);

            CloseMePleaseWrapper(Directory in) {
                super(in);
            }

            public void incRef() {
                int ref = refCount.incrementAndGet();
                assert ref > 1;
            }

            @Override
            public void close() throws IOException {
                if (refCount.decrementAndGet() == 0) {
                    in.close();
                }
            }
        }

        @Override
        public Set<String> getPendingDeletions() throws IOException {
            return wrapped.getPendingDeletions();
        }
    }
}
