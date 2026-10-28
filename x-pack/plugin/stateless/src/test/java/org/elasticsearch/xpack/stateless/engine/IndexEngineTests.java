/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.engine;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class IndexEngineTests extends ESTestCase {

    public void testReadNewMergedSegmentsSizes() throws IOException {
        try (Directory directory = newDirectory()) {
            // Use an explicit merge policy so that only the force-merge below produces a merge segment; the per-doc commits each create a
            // separate flush segment.
            final IndexWriterConfig config = new IndexWriterConfig().setMergePolicy(new TieredMergePolicy());
            try (IndexWriter writer = new IndexWriter(directory, config)) {
                writer.addDocument(doc("1"));
                writer.commit(); // flush segment
                writer.addDocument(doc("2"));
                writer.commit(); // flush segment
                writer.forceMerge(1);
                writer.commit(); // merge segment (replaces the two flush segments above)
                writer.addDocument(doc("3"));
                writer.commit(); // flush segment, left un-merged
            }

            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                final IndexCommit indexCommit = reader.getIndexCommit();
                final SegmentInfos segmentInfos = Lucene.readSegmentInfos(indexCommit);

                final Map<String, Long> expectedMergeSizes = new HashMap<>();
                final Set<String> mergeSegmentNames = new HashSet<>();
                final Set<String> flushSegmentNames = new HashSet<>();
                for (SegmentCommitInfo segmentCommitInfo : segmentInfos) {
                    final String source = segmentCommitInfo.info.getDiagnostics().get(IndexWriter.SOURCE);
                    if (IndexWriter.SOURCE_MERGE.equals(source)) {
                        expectedMergeSizes.put(segmentCommitInfo.info.name, segmentCommitInfo.sizeInBytes());
                        mergeSegmentNames.add(segmentCommitInfo.info.name);
                    } else {
                        flushSegmentNames.add(segmentCommitInfo.info.name);
                    }
                }
                // Sanity check that this commit contains exactly one merge segment and at least one flush segment.
                assertThat(expectedMergeSizes.keySet(), hasSize(1));
                assertThat(flushSegmentNames, not(empty()));

                final Set<String> allSegmentNames = new HashSet<>(mergeSegmentNames);
                allSegmentNames.addAll(flushSegmentNames);

                // When every segment is considered new, the merge segment is returned with its size and the flush segment is excluded.
                assertThat(
                    IndexEngine.readAdditionalSegmentsInfos(indexCommit, allSegmentNames, null, true).newMergedSegmentsSizes(),
                    equalTo(expectedMergeSizes)
                );

                // When only the flush segment is new, the merge segment is not "new" and nothing is returned.
                assertThat(
                    IndexEngine.readAdditionalSegmentsInfos(indexCommit, flushSegmentNames, null, true).newMergedSegmentsSizes(),
                    anEmptyMap()
                );

                // No new segments at all -> empty.
                assertThat(
                    IndexEngine.readAdditionalSegmentsInfos(indexCommit, Set.of(), null, true).newMergedSegmentsSizes(),
                    anEmptyMap()
                );

                // When merge-produced segments are not requested, the map is null and the timestamp range is also null (no @timestamp).
                final IndexEngine.AdditionalSegmentsInfos disabled = IndexEngine.readAdditionalSegmentsInfos(
                    indexCommit,
                    allSegmentNames,
                    null,
                    false
                );
                assertNull(disabled.newMergedSegmentsSizes());
                assertNull(disabled.timestampFieldValueRange());
            }
        }
    }

    private static Document doc(String id) {
        final Document document = new Document();
        document.add(new StringField("id", id, Field.Store.NO));
        return document;
    }
}
