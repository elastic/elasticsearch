/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.lucene.bwc;

import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.elasticsearch.Version;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.lucene.bwc.codecs.BWCCodec;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;

public class OldLuceneVersions extends Plugin implements IndexStorePlugin {

    @Override
    public void onIndexModule(IndexModule indexModule) {
        if (indexModule.indexSettings().getIndexVersionCreated().before(Version.CURRENT.minimumIndexCompatibilityVersion())) {
            indexModule.addIndexEventListener(new IndexEventListener() {
                @Override
                public void afterFilesRestoredFromRepository(IndexShard indexShard) {
                    convertToNewFormat(indexShard);
                }
            });
        }
    }

    /**
     * The trick used to allow newer Lucene versions to read older Lucene indices is to convert the old directory to a directory that new
     * Lucene versions happily operate on. The way newer Lucene versions happily comply with reading older data is to put in place a
     * segments file that the newer Lucene version can open, using codecs that allow reading everything from the old files, making it
     * available under the newer interfaces. The way this works is to read in the old segments file using a special class
     * {@link OldSegmentInfos} that supports reading older Lucene {@link SegmentInfos}, and then write out an updated segments file that
     * newer Lucene versions can understand.
     */
    private static void convertToNewFormat(IndexShard indexShard) {
        indexShard.store().incRef();
        try {
            final OldSegmentInfos oldSegmentInfos = OldSegmentInfos.readLatestCommit(indexShard.store().directory(), 6);
            final SegmentInfos segmentInfos = convertToNewerLuceneVersion(oldSegmentInfos);
            // write upgraded segments file
            segmentInfos.commit(indexShard.store().directory());

            // what we have written can be read using standard path
            assert SegmentInfos.readLatestCommit(indexShard.store().directory()) != null;

            // clean older segments file
            Lucene.pruneUnreferencedFiles(segmentInfos.getSegmentsFileName(), indexShard.store().directory());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            indexShard.store().decRef();
        }
    }

    private static SegmentInfos convertToNewerLuceneVersion(OldSegmentInfos oldSegmentInfos) {
        final SegmentInfos segmentInfos = new SegmentInfos(org.apache.lucene.util.Version.LATEST.major);
        segmentInfos.version = oldSegmentInfos.version;
        segmentInfos.counter = oldSegmentInfos.counter;
        segmentInfos.setNextWriteGeneration(oldSegmentInfos.getGeneration() + 1);
        final Map<String, String> map = new HashMap<>(oldSegmentInfos.getUserData());
        if (map.containsKey(Engine.HISTORY_UUID_KEY) == false) {
            map.put(Engine.HISTORY_UUID_KEY, UUIDs.randomBase64UUID());
        }
        if (map.containsKey(SequenceNumbers.LOCAL_CHECKPOINT_KEY) == false) {
            map.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(SequenceNumbers.NO_OPS_PERFORMED));
        }
        if (map.containsKey(SequenceNumbers.MAX_SEQ_NO) == false) {
            map.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(SequenceNumbers.NO_OPS_PERFORMED));
        }
        if (map.containsKey(Engine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID) == false) {
            map.put(Engine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID, "-1");
        }
        segmentInfos.setUserData(map, false);
        for (SegmentCommitInfo infoPerCommit : oldSegmentInfos.asList()) {
            final SegmentInfo newInfo = BWCCodec.wrap(infoPerCommit.info);
            final SegmentCommitInfo commitInfo = new SegmentCommitInfo(
                newInfo,
                infoPerCommit.getDelCount(),
                infoPerCommit.getSoftDelCount(),
                infoPerCommit.getDelGen(),
                infoPerCommit.getFieldInfosGen(),
                infoPerCommit.getDocValuesGen(),
                infoPerCommit.getId()
            );
            commitInfo.setDocValuesUpdatesFiles(infoPerCommit.getDocValuesUpdatesFiles());
            commitInfo.setFieldInfosFiles(infoPerCommit.getFieldInfosFiles());
            segmentInfos.add(commitInfo);
        }
        return segmentInfos;
    }

    @Override
    public Map<String, DirectoryFactory> getDirectoryFactories() {
        return Map.of();
    }
}
