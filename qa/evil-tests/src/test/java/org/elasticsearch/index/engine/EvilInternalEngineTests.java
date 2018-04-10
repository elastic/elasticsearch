/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.elasticsearch.index.mapper.ParsedDocument;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;

public class EvilInternalEngineTests extends EngineTestCase {

    public void testOutOfMemoryErrorWhileMergingIsRethrownAndIsUncaught() throws IOException, InterruptedException {
        engine.close();
        final AtomicReference<Throwable> maybeFatal = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        final Thread.UncaughtExceptionHandler uncaughtExceptionHandler = Thread.getDefaultUncaughtExceptionHandler();
        try {
            /*
             * We want to test that the out of memory error thrown from the merge goes uncaught; this gives us confidence that an out of
             * memory error thrown while merging will lead to the node being torn down.
             */
            Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
                maybeFatal.set(e);
                latch.countDown();
            });
            final AtomicReference<List<SegmentCommitInfo>> segmentsReference = new AtomicReference<>();

            try (Engine e = createEngine(
                    defaultSettings,
                    store,
                    primaryTranslogDir,
                    newMergePolicy(),
                    (directory, iwc) -> new IndexWriter(directory, iwc) {
                        @Override
                        public void merge(final MergePolicy.OneMerge merge) throws IOException {
                            throw new OutOfMemoryError("640K ought to be enough for anybody");
                        }

                        @Override
                        public synchronized MergePolicy.OneMerge getNextMerge() {
                            /*
                             * This will be called when we flush when we will not be ready to return the segments. After the segments are on
                             * disk, we can only return them from here once or the merge scheduler will be stuck in a loop repeatedly
                             * peeling off the same segments to schedule for merging.
                             */
                            if (segmentsReference.get() == null) {
                                return super.getNextMerge();
                            } else {
                                final List<SegmentCommitInfo> segments = segmentsReference.getAndSet(null);
                                return new MergePolicy.OneMerge(segments);
                            }
                        }
                    },
                    null,
                    null)) {
                // force segments to exist on disk
                final ParsedDocument doc1 = testParsedDocument("1", null, testDocumentWithTextField(), B_1, null);
                e.index(indexForDoc(doc1));
                e.flush();
                final List<SegmentCommitInfo> segments =
                        StreamSupport.stream(e.getLastCommittedSegmentInfos().spliterator(), false).collect(Collectors.toList());
                segmentsReference.set(segments);
                // trigger a background merge that will be managed by the concurrent merge scheduler
                e.forceMerge(randomBoolean(), 0, false, false, false);
                /*
                 * Merging happens in the background on a merge thread, and the maybeDie handler is invoked on yet another thread; we have
                 * to wait for these events to finish.
                 */
                latch.await();
                assertNotNull(maybeFatal.get());
                assertThat(maybeFatal.get(), instanceOf(OutOfMemoryError.class));
                assertThat(maybeFatal.get(), hasToString(containsString("640K ought to be enough for anybody")));
            }
        } finally {
            Thread.setDefaultUncaughtExceptionHandler(uncaughtExceptionHandler);
        }
    }


}
