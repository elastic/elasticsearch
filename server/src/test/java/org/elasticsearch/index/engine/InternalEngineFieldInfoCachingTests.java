/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.store.FieldInfoCachingDirectory;
import org.hamcrest.Matchers;

import java.util.HashMap;
import java.util.Map;

/**
 * End-to-end test that {@link FieldInfoCachingDirectory} kicks in on the {@link InternalEngine} write path and produces
 * canonical {@link FieldInfo} instances shared across segments under the feature flag.
 */
public class InternalEngineFieldInfoCachingTests extends EngineTestCase {

    public void testFieldInfoSharedAcrossSegments() throws Exception {
        assumeTrue("requires the per-Directory FieldInfo cache feature flag", FieldInfoCachingDirectory.FEATURE_FLAG.isEnabled());

        // Write three segments by indexing + flushing repeatedly.
        for (int seg = 0; seg < 3; seg++) {
            for (int d = 0; d < 3; d++) {
                String id = seg + "-" + d;
                engine.index(indexForDoc(createParsedDoc(id, null)));
            }
            engine.flush();
        }
        engine.refresh("test");

        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            DirectoryReader reader = searcher.getDirectoryReader();
            assertThat("test requires multiple segments", reader.leaves().size(), Matchers.greaterThan(1));

            // Confirm at least one leaf's SegmentInfo.dir unwraps to our FieldInfoCachingDirectory — that's how the
            // CachingFieldInfosFormat reaches the per-Directory cache.
            boolean sawCachingDir = false;
            Map<String, FieldInfo> firstSeen = new HashMap<>();
            for (LeafReaderContext leaf : reader.leaves()) {
                SegmentReader sr = Lucene.tryUnwrapSegmentReader(leaf.reader());
                assertNotNull("expected SegmentReader at leaf", sr);
                if (FieldInfoCachingDirectory.unwrap(sr.getSegmentInfo().info.dir) != null) {
                    sawCachingDir = true;
                }
                for (FieldInfo fi : sr.getFieldInfos()) {
                    if (fi.isSoftDeletesField()) {
                        // dvGen may differ between segments after updates; skip for the identity assertion.
                        continue;
                    }
                    FieldInfo prior = firstSeen.putIfAbsent(fi.getName(), fi);
                    if (prior != null) {
                        assertSame("FieldInfo for [" + fi.getName() + "] must be reference-equal across segments under caching", prior, fi);
                    }
                }
            }
            assertTrue("expected at least one segment to see FieldInfoCachingDirectory via SegmentInfo.dir", sawCachingDir);
        }
    }
}
