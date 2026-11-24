/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.OptionalLong;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class SizeTrackingOneMergeTests extends ESTestCase {

    public void testMergedSegmentSizeInBytes() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
                writer.addDocument(new Document());
                writer.commit();
            }
            SegmentCommitInfo segment = SegmentInfos.readLatestCommit(dir).info(0);

            // committed segment: size is read from the merge info
            SizeTrackingOneMerge committed = newMerge(segment);
            committed.mergeFinished(true, false);
            assertThat(committed.mergedSegmentSizeInBytes(), equalTo(OptionalLong.of(segment.sizeInBytes())));
            assertThat(committed.mergedSegmentSizeInBytes().getAsLong(), greaterThan(0L));

            // dropped segment: the merge produced no bytes
            SizeTrackingOneMerge dropped = newMerge(segment);
            dropped.mergeFinished(true, true);
            assertThat(dropped.mergedSegmentSizeInBytes(), equalTo(OptionalLong.of(0L)));

            // unsuccessful merge: nothing recorded
            SizeTrackingOneMerge failed = newMerge(segment);
            failed.mergeFinished(false, false);
            assertThat(failed.mergedSegmentSizeInBytes(), equalTo(OptionalLong.empty()));
        }
    }

    private static SizeTrackingOneMerge newMerge(SegmentCommitInfo segment) {
        SizeTrackingOneMerge merge = new SizeTrackingOneMerge(new MergePolicy.OneMerge(List.of(segment)));
        merge.setMergeInfo(segment);
        return merge;
    }
}
