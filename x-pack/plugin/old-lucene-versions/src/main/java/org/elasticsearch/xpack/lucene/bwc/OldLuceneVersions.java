/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.lucene.bwc;

import org.apache.lucene.backward_codecs.lucene70.Lucene70Codec;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.elasticsearch.Build;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.plugins.Plugin;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;

public class OldLuceneVersions extends Plugin implements IndexStorePlugin {

    @Override
    public void onIndexModule(IndexModule indexModule) {
        if (Build.CURRENT.isSnapshot()) {
            indexModule.addIndexEventListener(new IndexEventListener() {
                @Override
                public void afterFilesRestoredFromRepository(IndexShard indexShard) {
                    maybeConvertToNewFormat(indexShard);
                }
            });
        }
    }

    private static void maybeConvertToNewFormat(IndexShard indexShard) {
        indexShard.store().incRef();
        try {
            try {
                Version version = getLuceneVersion(indexShard.store().directory());
                // Lucene version in [7.0.0, 8.0.0)
                if (version != null
                    && version.onOrAfter(Version.fromBits(7, 0, 0))
                    && version.onOrAfter(Version.fromBits(8, 0, 0)) == false) {
                    final OldSegmentInfos oldSegmentInfos = OldSegmentInfos.readLatestCommit(indexShard.store().directory(), 7);
                    final SegmentInfos segmentInfos = convertLucene7x(oldSegmentInfos);
                    // write upgraded segments file
                    segmentInfos.commit(indexShard.store().directory());

                    // validate that what we have written can be read using standard path
                    // TODO: norelease: remove this when development completes
                    SegmentInfos segmentInfos1 = SegmentInfos.readLatestCommit(indexShard.store().directory());

                    // clean older segments file
                    Lucene.pruneUnreferencedFiles(segmentInfos1.getSegmentsFileName(), indexShard.store().directory());
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        } finally {
            indexShard.store().decRef();
        }
    }

    private static Version getLuceneVersion(Directory directory) throws IOException {
        final String segmentFileName = SegmentInfos.getLastCommitSegmentsFileName(directory);
        if (segmentFileName != null) {
            long generation = SegmentInfos.generationFromSegmentsFileName(segmentFileName);
            try (ChecksumIndexInput input = directory.openChecksumInput(segmentFileName, IOContext.READ)) {
                CodecUtil.checkHeader(input, "segments", 0, Integer.MAX_VALUE);
                byte[] id = new byte[StringHelper.ID_LENGTH];
                input.readBytes(id, 0, id.length);
                CodecUtil.checkIndexHeaderSuffix(input, Long.toString(generation, Character.MAX_RADIX));

                Version luceneVersion = Version.fromBits(input.readVInt(), input.readVInt(), input.readVInt());
                int indexCreatedVersion = input.readVInt();
                return luceneVersion;
            } catch (Exception e) {
                // ignore
            }
        }
        return null;
    }

    private static SegmentInfos convertLucene7x(OldSegmentInfos oldSegmentInfos) {
        final SegmentInfos segmentInfos = new SegmentInfos(org.apache.lucene.util.Version.LATEST.major);
        segmentInfos.setNextWriteGeneration(oldSegmentInfos.getGeneration() + 1);
        final Map<String, String> map = new HashMap<>(oldSegmentInfos.getUserData());
        map.put(Engine.HISTORY_UUID_KEY, UUIDs.randomBase64UUID());
        map.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(SequenceNumbers.NO_OPS_PERFORMED));
        map.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(SequenceNumbers.NO_OPS_PERFORMED));
        map.put(Engine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID, "-1");
        segmentInfos.setUserData(map, true);
        for (SegmentCommitInfo infoPerCommit : oldSegmentInfos.asList()) {
            SegmentInfo info = infoPerCommit.info;
            SegmentInfo newInfo = wrap(info);

            segmentInfos.add(
                new SegmentCommitInfo(
                    newInfo,
                    infoPerCommit.getDelCount(),
                    0,
                    infoPerCommit.getDelGen(),
                    infoPerCommit.getFieldInfosGen(),
                    infoPerCommit.getDocValuesGen(),
                    infoPerCommit.getId()
                )
            );
        }
        return segmentInfos;
    }

    static SegmentInfo wrap(SegmentInfo segmentInfo) {
        // Use Version.LATEST instead of original version, otherwise SegmentCommitInfo will bark when processing (N-1 limitation)
        // TODO: alternatively store the original version information in attributes?
        byte[] id = segmentInfo.getId();
        if (id == null) {
            id = StringHelper.randomId();
        }
        Codec codec = segmentInfo.getCodec() instanceof Lucene70Codec ? new BWCLucene70Codec() : segmentInfo.getCodec();
        SegmentInfo segmentInfo1 = new SegmentInfo(
            segmentInfo.dir,
            org.apache.lucene.util.Version.LATEST,
            org.apache.lucene.util.Version.LATEST,
            segmentInfo.name,
            segmentInfo.maxDoc(),
            segmentInfo.getUseCompoundFile(),
            codec,
            segmentInfo.getDiagnostics(),
            id,
            segmentInfo.getAttributes(),
            null
        );
        segmentInfo1.setFiles(segmentInfo.files());
        return segmentInfo1;
    }

    @Override
    public Map<String, DirectoryFactory> getDirectoryFactories() {
        return Map.of();
    }
}
