/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.oldcodecs;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompoundDirectory;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.lucene87.Lucene87Codec;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.Version;
import org.apache.lucene5_shaded.util.StringHelper;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Lucene5IndexEventListener implements IndexEventListener {

    @Override
    public void afterFilesRestoredFromRepository(IndexShard indexShard) {
        //assert Thread.currentThread().getName().contains(ThreadPool.Names.GENERIC);
        final ShardId shardId = indexShard.shardId();
        assert Lucene5Plugin.isLucene5Store(indexShard.indexSettings().getSettings()) : "Expected a Lucene5 shard " + shardId;

        indexShard.store().incRef();

        // Rewrite into new segments info format on disk
        try {
            final Directory newStyleDirectory = indexShard.store().directory();

            final org.apache.lucene5_shaded.index.SegmentInfos oldSegmentInfos;
            try {
                oldSegmentInfos = org.apache.lucene5_shaded.index.SegmentInfos.readLatestCommit(wrap(newStyleDirectory));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            final SegmentInfos segmentInfos = convert(oldSegmentInfos);
            try {
                segmentInfos.commit(newStyleDirectory);
                Lucene.pruneUnreferencedFiles(segmentInfos.getSegmentsFileName(), newStyleDirectory);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        } finally {
            indexShard.store().decRef();
        }
    }

    private SegmentInfos convert(org.apache.lucene5_shaded.index.SegmentInfos oldSegmentInfos) {
        final SegmentInfos segmentInfos = new SegmentInfos(Version.LATEST.major);
        segmentInfos.setNextWriteGeneration(oldSegmentInfos.getGeneration() + 1);
        final Map<String, String> map = new HashMap<>(oldSegmentInfos.getUserData());
        map.put(Engine.HISTORY_UUID_KEY, UUIDs.randomBase64UUID());
        map.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(SequenceNumbers.NO_OPS_PERFORMED));
        map.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(SequenceNumbers.NO_OPS_PERFORMED));
        map.put(Engine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID, "-1");
        segmentInfos.setUserData(map, true);
        for (org.apache.lucene5_shaded.index.SegmentCommitInfo infoPerCommit : oldSegmentInfos.asList()) {
            org.apache.lucene5_shaded.index.SegmentInfo info = infoPerCommit.info;
            // Same info just changing the dir:
//            SegmentInfo newInfo = new SegmentInfo(destDir, Version.LATEST, Version.LATEST, info.name, info.maxDoc(),
//                info.getUseCompoundFile(), new WrappedLucene54Codec() /*new Lucene5Codec()*/,
//                info.getDiagnostics(), info.getId(), Collections.emptyMap(), null);
//            newInfo.setFiles(info.files());
            SegmentInfo newInfo = wrap(info);

            segmentInfos.add(new SegmentCommitInfo(newInfo, infoPerCommit.getDelCount(), 0,
                infoPerCommit.getDelGen(), infoPerCommit.getFieldInfosGen(),
                infoPerCommit.getDocValuesGen(), /* use info id as fake id */ info.getId()));
        }
        return segmentInfos;
    }

    public static class Lucene5DirectoryWrapper extends org.apache.lucene5_shaded.store.Directory {

        final Directory newStyleDirectory;

        public Lucene5DirectoryWrapper(Directory newStyleDirectory) {
            this.newStyleDirectory = newStyleDirectory;
        }

        @Override
        public String[] listAll() throws IOException {
            return newStyleDirectory.listAll();
        }

        @Override
        public void deleteFile(String name) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long fileLength(String name) throws IOException {
            return newStyleDirectory.fileLength(name);
        }

        @Override
        public org.apache.lucene5_shaded.store.IndexOutput createOutput(String name, org.apache.lucene5_shaded.store.IOContext context)
            throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void sync(Collection<String> names) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void renameFile(String source, String dest) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public org.apache.lucene5_shaded.store.IndexInput openInput(String name, org.apache.lucene5_shaded.store.IOContext context)
            throws IOException {
            return new Lucene5IndexInputWrapper(name, newStyleDirectory.openInput(name, IOContext.DEFAULT));
        }

        @Override
        public org.apache.lucene5_shaded.store.Lock obtainLock(String name) throws IOException {
            return wrap(newStyleDirectory.obtainLock(name));
        }

        @Override
        public void close() throws IOException {
            newStyleDirectory.close();
        }
    }

    public static class Lucene5DirectoryReverseWrapper extends Directory {

        final org.apache.lucene5_shaded.store.Directory oldStyleDirectory;

        public Lucene5DirectoryReverseWrapper(org.apache.lucene5_shaded.store.Directory oldStyleDirectory) {
            this.oldStyleDirectory = oldStyleDirectory;
        }

        @Override
        public String[] listAll() throws IOException {
            return oldStyleDirectory.listAll();
        }

        @Override
        public void deleteFile(String name) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long fileLength(String name) throws IOException {
            return oldStyleDirectory.fileLength(name);
        }

        @Override
        public IndexOutput createOutput(String name, IOContext context)
            throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void sync(Collection<String> names) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void syncMetaData() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void rename(String source, String dest) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public IndexInput openInput(String name, IOContext context)
            throws IOException {
            return new Lucene5IndexInputReverseWrapper(name, oldStyleDirectory.openInput(name,
                org.apache.lucene5_shaded.store.IOContext.DEFAULT));
        }

        @Override
        public Lock obtainLock(String name) throws IOException {
            return wrap(oldStyleDirectory.obtainLock(name));
        }

        @Override
        public void close() throws IOException {
            oldStyleDirectory.close();
        }

        @Override
        public Set<String> getPendingDeletions() throws IOException {
            throw new UnsupportedOperationException();
        }
    }

    public static class Lucene5CompoundDirectoryReverseWrapper extends CompoundDirectory {

        private final org.apache.lucene5_shaded.store.Directory newStyleDirectory;

        public Lucene5CompoundDirectoryReverseWrapper(org.apache.lucene5_shaded.store.Directory newStyleDirectory) {
            this.newStyleDirectory = newStyleDirectory;
        }

        @Override
        public String[] listAll() throws IOException {
            return newStyleDirectory.listAll();
        }

        @Override
        public long fileLength(String name) throws IOException {
            return newStyleDirectory.fileLength(name);
        }

        @Override
        public IndexInput openInput(String name, IOContext context)
            throws IOException {
            return new Lucene5IndexInputReverseWrapper(name, newStyleDirectory.openInput(name,
                org.apache.lucene5_shaded.store.IOContext.DEFAULT));
        }

        @Override
        public void close() throws IOException {
            newStyleDirectory.close();
        }

        @Override
        public Set<String> getPendingDeletions() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkIntegrity() throws IOException {
            // nothing to do
        }
    }

    static org.apache.lucene5_shaded.store.Lock wrap(Lock lock) {
        return new org.apache.lucene5_shaded.store.Lock() {

            @Override
            public void close() throws IOException {
                lock.close();
            }

            @Override
            public void ensureValid() throws IOException {
                lock.ensureValid();
            }
        };
    }

    static Lock wrap(org.apache.lucene5_shaded.store.Lock lock) {
        return new Lock() {

            @Override
            public void close() throws IOException {
                lock.close();
            }

            @Override
            public void ensureValid() throws IOException {
                lock.ensureValid();
            }
        };
    }

    private static class Lucene5IndexInputWrapper extends org.apache.lucene5_shaded.store.IndexInput {

        private final IndexInput indexInput;

        protected Lucene5IndexInputWrapper(String resourceDescription, IndexInput indexInput) {
            super(resourceDescription);
            this.indexInput = indexInput;
        }

        @Override
        public byte readByte() throws IOException {
            return indexInput.readByte();
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            indexInput.readBytes(b, offset, len);
        }

        @Override
        public void close() throws IOException {
            indexInput.close();
        }

        @Override
        public long getFilePointer() {
            return indexInput.getFilePointer();
        }

        @Override
        public void seek(long pos) throws IOException {
            indexInput.seek(pos);
        }

        @Override
        public long length() {
            return indexInput.length();
        }

        @Override
        public org.apache.lucene5_shaded.store.IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            return new Lucene5IndexInputWrapper(sliceDescription, indexInput.slice(sliceDescription, offset, length));
        }
    }

    private static class Lucene5IndexInputReverseWrapper extends IndexInput {

        private final org.apache.lucene5_shaded.store.IndexInput indexInput;

        protected Lucene5IndexInputReverseWrapper(String resourceDescription, org.apache.lucene5_shaded.store.IndexInput indexInput) {
            super(resourceDescription);
            this.indexInput = indexInput;
        }

        @Override
        public byte readByte() throws IOException {
            return indexInput.readByte();
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            indexInput.readBytes(b, offset, len);
        }

        @Override
        public void close() throws IOException {
            indexInput.close();
        }

        @Override
        public long getFilePointer() {
            return indexInput.getFilePointer();
        }

        @Override
        public void seek(long pos) throws IOException {
            indexInput.seek(pos);
        }

        @Override
        public long length() {
            return indexInput.length();
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            return new Lucene5IndexInputReverseWrapper(sliceDescription, indexInput.slice(sliceDescription, offset, length));
        }
    }

    static class Lucene5CodecWrapper extends Codec {

        private final Codec originalCodec = new Lucene87Codec();

        final org.apache.lucene5_shaded.codecs.Codec wrappedCodec;

        Lucene5CodecWrapper(org.apache.lucene5_shaded.codecs.Codec wrappedCodec) {
            super(wrappedCodec.getName());
            this.wrappedCodec = wrappedCodec;
        }

        @Override
        public PostingsFormat postingsFormat() {
            return originalCodec.postingsFormat();
        }

        @Override
        public DocValuesFormat docValuesFormat() {
            return originalCodec.docValuesFormat();
        }

        @Override
        public StoredFieldsFormat storedFieldsFormat() {
            return new Lucene5StoredFieldsFormatWrapper(wrappedCodec.storedFieldsFormat());
        }

        @Override
        public TermVectorsFormat termVectorsFormat() {
            return originalCodec.termVectorsFormat();
        }

        @Override
        public FieldInfosFormat fieldInfosFormat() {
            return new Lucene5FieldInfosFormatWrapper(wrappedCodec.fieldInfosFormat());
        }

        @Override
        public SegmentInfoFormat segmentInfoFormat() {
            return new Lucene5SegmentInfoFormatWrapper(wrappedCodec.segmentInfoFormat());
        }

        @Override
        public NormsFormat normsFormat() {
            return originalCodec.normsFormat();
        }

        @Override
        public LiveDocsFormat liveDocsFormat() {
            return new Lucene5LiveDocsFormatWrapper(wrappedCodec.liveDocsFormat());
        }

        @Override
        public CompoundFormat compoundFormat() {
            return new Lucene5CompoundFormatWrapper(wrappedCodec.compoundFormat());
        }

        @Override
        public PointsFormat pointsFormat() {
            return originalCodec.pointsFormat();
        }
    }

    static class Lucene5CompoundFormatWrapper extends CompoundFormat {

        Lucene5CompoundFormatWrapper(org.apache.lucene5_shaded.codecs.CompoundFormat compoundFormat) {
            this.compoundFormat = compoundFormat;
        }

        private final org.apache.lucene5_shaded.codecs.CompoundFormat compoundFormat;

        @Override
        public CompoundDirectory getCompoundReader(Directory dir, SegmentInfo si, IOContext context) throws IOException {
            return new Lucene5CompoundDirectoryReverseWrapper(
                compoundFormat.getCompoundReader(wrap(dir), wrap(si), wrap(context)));
        }

        @Override
        public void write(Directory dir, SegmentInfo si, IOContext context) throws IOException {
            throw new UnsupportedOperationException();
        }
    }

    static class Lucene5StoredFieldsFormatWrapper extends StoredFieldsFormat {

        Lucene5StoredFieldsFormatWrapper(org.apache.lucene5_shaded.codecs.StoredFieldsFormat storedFieldsFormat) {
            this.storedFieldsFormat = storedFieldsFormat;
        }

        private final org.apache.lucene5_shaded.codecs.StoredFieldsFormat storedFieldsFormat;

        @Override
        public StoredFieldsReader fieldsReader(Directory directory, SegmentInfo si, FieldInfos fn, IOContext context) throws IOException {
            return wrap(storedFieldsFormat.fieldsReader(wrap(directory), wrap(si), wrap(fn), wrap(context)));
        }

        @Override
        public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si, IOContext context) throws IOException {
            return null;
        }
    }

    static class Lucene5FieldInfosFormatWrapper extends FieldInfosFormat {

        Lucene5FieldInfosFormatWrapper(org.apache.lucene5_shaded.codecs.FieldInfosFormat fieldInfosFormat) {
            this.fieldInfosFormat = fieldInfosFormat;
        }

        private final org.apache.lucene5_shaded.codecs.FieldInfosFormat fieldInfosFormat;


        @Override
        public FieldInfos read(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, IOContext iocontext) throws IOException {
            return wrap(fieldInfosFormat.read(wrap(directory), wrap(segmentInfo), segmentSuffix, wrap(iocontext)));
        }

        @Override
        public void write(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, FieldInfos infos, IOContext context)
            throws IOException {
            throw new UnsupportedOperationException();
        }
    }

    static class Lucene5SegmentInfoFormatWrapper extends SegmentInfoFormat {

        Lucene5SegmentInfoFormatWrapper(org.apache.lucene5_shaded.codecs.SegmentInfoFormat segmentInfoFormat) {
            this.segmentInfoFormat = segmentInfoFormat;
        }

        private final org.apache.lucene5_shaded.codecs.SegmentInfoFormat segmentInfoFormat;

        @Override
        public SegmentInfo read(Directory directory, String segmentName, byte[] segmentID, IOContext context) throws IOException {
            // lie about version so that it passes checks
            return wrap(segmentInfoFormat.read(wrap(directory), segmentName, segmentID, wrap(context)));
        }

        @Override
        public void write(Directory dir, SegmentInfo info, IOContext ioContext) throws IOException {
            throw new UnsupportedOperationException();
        }
    }

    static class Lucene5LiveDocsFormatWrapper extends LiveDocsFormat {

        private final org.apache.lucene5_shaded.codecs.LiveDocsFormat wrappedLiveDocsFormat;


        Lucene5LiveDocsFormatWrapper(org.apache.lucene5_shaded.codecs.LiveDocsFormat wrappedLiveDocsFormat) {
            this.wrappedLiveDocsFormat = wrappedLiveDocsFormat;
        }

        @Override
        public Bits readLiveDocs(Directory dir, SegmentCommitInfo info, IOContext context) throws IOException {
            return wrap(wrappedLiveDocsFormat.readLiveDocs(wrap(dir), wrap(info), wrap(context)));
        }

        @Override
        public void writeLiveDocs(Bits bits, Directory dir, SegmentCommitInfo info, int newDelCount, IOContext context) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void files(SegmentCommitInfo info, Collection<String> files) throws IOException {
            wrappedLiveDocsFormat.files(wrap(info), files);
        }
    }

    static org.apache.lucene5_shaded.index.SegmentCommitInfo wrap(SegmentCommitInfo segmentCommitInfo) {
        return new org.apache.lucene5_shaded.index.SegmentCommitInfo(wrap(segmentCommitInfo.info), segmentCommitInfo.getDelCount(),
            segmentCommitInfo.getDelGen(), segmentCommitInfo.getFieldInfosGen(), segmentCommitInfo.getDocValuesGen());
    }

    static org.apache.lucene5_shaded.index.SegmentInfo wrap(SegmentInfo segmentInfo) {
        org.apache.lucene5_shaded.index.SegmentInfo segmentInfo1 =
            new org.apache.lucene5_shaded.index.SegmentInfo(wrap(segmentInfo.dir), wrap(segmentInfo.getVersion()),
            segmentInfo.name, segmentInfo.maxDoc(), segmentInfo.getUseCompoundFile(), wrap(segmentInfo.getCodec()),
            segmentInfo.getDiagnostics(), segmentInfo.getId(), segmentInfo.getAttributes());
        segmentInfo1.setFiles(segmentInfo.files());
        return segmentInfo1;
    }

    static SegmentInfo wrap(org.apache.lucene5_shaded.index.SegmentInfo segmentInfo) {
        // Use Version.LATEST instead of original version, otherwise SegmentCommitInfo will bark when processing (N-1 limitation)
        // TODO: alternatively store the original version information in attributes?
        byte[] id = segmentInfo.getId();
        if (id == null) {
            id = StringHelper.randomId();
        }
        SegmentInfo segmentInfo1 = new SegmentInfo(wrap(segmentInfo.dir),
            Version.LATEST /*wrap(segmentInfo.getVersion())*/, Version.LATEST /*wrap(segmentInfo.getVersion())*/,
            segmentInfo.name, segmentInfo.maxDoc(), segmentInfo.getUseCompoundFile(), wrap(segmentInfo.getCodec()),
            segmentInfo.getDiagnostics(), id, segmentInfo.getAttributes(), null);
        segmentInfo1.setFiles(segmentInfo.files());
        return segmentInfo1;
    }

    static org.apache.lucene5_shaded.store.Directory wrap(Directory directory) {
        if (directory instanceof Lucene5DirectoryReverseWrapper) {
            return ((Lucene5DirectoryReverseWrapper) directory).oldStyleDirectory;
        }
        return new Lucene5DirectoryWrapper(directory);
    }

    static Directory wrap(org.apache.lucene5_shaded.store.Directory directory) {
        if (directory instanceof Lucene5DirectoryWrapper) {
            return ((Lucene5DirectoryWrapper) directory).newStyleDirectory;
        }
        return new Lucene5DirectoryReverseWrapper(directory);
    }

    static org.apache.lucene5_shaded.util.Version wrap(Version version) {
        return org.apache.lucene5_shaded.util.Version.fromBits(version.major, version.minor, version.bugfix);
    }

    static Version wrap(org.apache.lucene5_shaded.util.Version version) {
        return Version.fromBits(version.major, version.minor, version.bugfix);
    }

    static org.apache.lucene5_shaded.codecs.Codec wrap(Codec codec) {
        if (codec instanceof Lucene5CodecWrapper) {
            return ((Lucene5CodecWrapper) codec).wrappedCodec;
        }
        throw new UnsupportedOperationException();
    }

    static Codec wrap(org.apache.lucene5_shaded.codecs.Codec codec) {
        if (codec == null) {
            return null;
        }
        return new Lucene5CodecWrapper(codec);
    }

    static org.apache.lucene5_shaded.store.IOContext wrap(IOContext ioContext) {
        // TODO: give it a bit more effort to convert
        return org.apache.lucene5_shaded.store.IOContext.DEFAULT;
    }

    static Bits wrap(org.apache.lucene5_shaded.util.Bits bits) {
        return new Bits() {
            @Override
            public boolean get(int index) {
                return bits.get(index);
            }

            @Override
            public int length() {
                return bits.length();
            }
        };
    }

    static FieldInfos wrap(org.apache.lucene5_shaded.index.FieldInfos fieldInfos) {
        FieldInfo[] infos = new FieldInfo[fieldInfos.size()];
        for (int i = 0; i < fieldInfos.size(); i++) {
            infos[i] = wrap(fieldInfos.fieldInfo(i));
        }
        return new FieldInfos(infos);
    }

    static org.apache.lucene5_shaded.index.FieldInfos wrap(FieldInfos fieldInfos) {
        org.apache.lucene5_shaded.index.FieldInfo[] infos = new org.apache.lucene5_shaded.index.FieldInfo[fieldInfos.size()];
        for (int i = 0; i < fieldInfos.size(); i++) {
            infos[i] = wrap(fieldInfos.fieldInfo(i));
        }
        return new org.apache.lucene5_shaded.index.FieldInfos(infos);
    }

    static FieldInfo wrap(org.apache.lucene5_shaded.index.FieldInfo fieldInfo) {
        return new FieldInfo(fieldInfo.name, fieldInfo.number,
            false, false, false, IndexOptions.NONE, DocValuesType.NONE, -1,
            fieldInfo.attributes(), 0, 0, 0,
            false);
    }

    static org.apache.lucene5_shaded.index.FieldInfo wrap(FieldInfo fieldInfo) {
        return new org.apache.lucene5_shaded.index.FieldInfo(fieldInfo.name, fieldInfo.number,
            false, false, false, org.apache.lucene5_shaded.index.IndexOptions.NONE,
            org.apache.lucene5_shaded.index.DocValuesType.NONE, -1, fieldInfo.attributes());
    }

    static StoredFieldsReader wrap(org.apache.lucene5_shaded.codecs.StoredFieldsReader storedFieldsReader) {
        return new StoredFieldsReader() {
            @Override
            public void visitDocument(int docID, StoredFieldVisitor visitor) throws IOException {
                storedFieldsReader.visitDocument(docID, wrap(visitor));
            }

            @Override
            public StoredFieldsReader clone() {
                return wrap(storedFieldsReader.clone());
            }

            @Override
            public void checkIntegrity() throws IOException {
                storedFieldsReader.checkIntegrity();
            }

            @Override
            public void close() throws IOException {
                storedFieldsReader.close();
            }

            @Override
            public long ramBytesUsed() {
                return storedFieldsReader.ramBytesUsed();
            }
        };
    }

    static org.apache.lucene5_shaded.index.StoredFieldVisitor wrap(StoredFieldVisitor storedFieldVisitor) {
        return new org.apache.lucene5_shaded.index.StoredFieldVisitor() {

            @Override
            public org.apache.lucene5_shaded.index.StoredFieldVisitor.Status
                needsField(org.apache.lucene5_shaded.index.FieldInfo fieldInfo) throws IOException {
                return wrap(storedFieldVisitor.needsField(wrap(fieldInfo)));
            }

            @Override
            public void binaryField(org.apache.lucene5_shaded.index.FieldInfo fieldInfo, byte[] value) throws IOException {
                storedFieldVisitor.binaryField(wrap(fieldInfo), value);
            }

            @Override
            public void stringField(org.apache.lucene5_shaded.index.FieldInfo fieldInfo, byte[] value) throws IOException {
                storedFieldVisitor.stringField(wrap(fieldInfo), value);
            }

            @Override
            public void intField(org.apache.lucene5_shaded.index.FieldInfo fieldInfo, int value) throws IOException {
                storedFieldVisitor.intField(wrap(fieldInfo), value);
            }

            @Override
            public void longField(org.apache.lucene5_shaded.index.FieldInfo fieldInfo, long value) throws IOException {
                storedFieldVisitor.longField(wrap(fieldInfo), value);
            }

            @Override
            public void floatField(org.apache.lucene5_shaded.index.FieldInfo fieldInfo, float value) throws IOException {
                storedFieldVisitor.floatField(wrap(fieldInfo), value);
            }

            @Override
            public void doubleField(org.apache.lucene5_shaded.index.FieldInfo fieldInfo, double value) throws IOException {
                storedFieldVisitor.doubleField(wrap(fieldInfo), value);
            }
        };
    }

    static org.apache.lucene5_shaded.index.StoredFieldVisitor.Status wrap(StoredFieldVisitor.Status status) {
        switch (status) {
            case YES: return org.apache.lucene5_shaded.index.StoredFieldVisitor.Status.YES;
            case NO: return org.apache.lucene5_shaded.index.StoredFieldVisitor.Status.NO;
            case STOP: return org.apache.lucene5_shaded.index.StoredFieldVisitor.Status.STOP;
        }
        throw new UnsupportedOperationException();
    }

}
