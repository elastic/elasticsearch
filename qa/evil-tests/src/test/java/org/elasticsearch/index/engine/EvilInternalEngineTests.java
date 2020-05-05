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

import org.apache.lucene.index.FilterMergePolicy;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.mapper.ParsedDocument;

import java.io.IOException;
import java.util.List;
import java.util.Map;
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

            final FilterMergePolicy mergePolicy = new FilterMergePolicy(newMergePolicy()) {
                @Override
                public MergeSpecification findForcedMerges(SegmentInfos segmentInfos, int maxSegmentCount,
                                                           Map<SegmentCommitInfo, Boolean> segmentsToMerge,
                                                           MergeContext mergeContext) throws IOException {
                    final List<SegmentCommitInfo> segments = segmentsReference.get();
                    if (segments != null) {
                        final MergeSpecification spec = new MergeSpecification();
                        spec.add(new OneMerge(segments));
                        return spec;
                    }
                    return super.findForcedMerges(segmentInfos, maxSegmentCount, segmentsToMerge, mergeContext);
                }

                @Override
                public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos segmentInfos,
                                                     MergeContext mergeContext) throws IOException {
                    final List<SegmentCommitInfo> segments = segmentsReference.get();
                    if (segments != null) {
                        final MergeSpecification spec = new MergeSpecification();
                        spec.add(new OneMerge(segments));
                        return spec;
                    }
                    return super.findMerges(mergeTrigger, segmentInfos, mergeContext);
                }
            };

            try (Engine e = createEngine(
                    defaultSettings,
                    store,
                    primaryTranslogDir,
                    mergePolicy,
                    (directory, iwc) -> {
                        final MergeScheduler mergeScheduler = iwc.getMergeScheduler();
                        assertNotNull(mergeScheduler);
                        iwc.setMergeScheduler(new FilterMergeScheduler(mergeScheduler) {
                            @Override
                            public void merge(MergeSource mergeSource, MergeTrigger trigger) throws IOException {
                                final FilterMergeSource wrappedMergeSource = new FilterMergeSource(mergeSource) {
                                    @Override
                                    public MergePolicy.OneMerge getNextMerge() {
                                        synchronized (mergeSource) {
                                            /*
                                             * This will be called when we flush when we will not be ready to return the segments.
                                             * After the segments are on disk, we can only return them from here once or the merge
                                             * scheduler will be stuck in a loop repeatedly peeling off the same segments to schedule
                                             * for merging.
                                             */
                                            if (segmentsReference.get() == null) {
                                                return super.getNextMerge();
                                            } else {
                                                final List<SegmentCommitInfo> segments = segmentsReference.getAndSet(null);
                                                return new MergePolicy.OneMerge(segments);
                                            }
                                        }
                                    }

                                    @Override
                                    public void merge(MergePolicy.OneMerge merge) {
                                        throw new OutOfMemoryError("640K ought to be enough for anybody");
                                    }
                                };
                                super.merge(wrappedMergeSource, trigger);
                            }
                        });
                        return new IndexWriter(directory, iwc);
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
                e.forceMerge(randomBoolean(), 0, false, false, false, UUIDs.randomBase64UUID());
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

    static class FilterMergeScheduler extends MergeScheduler {
        private final MergeScheduler delegate;

        FilterMergeScheduler(MergeScheduler delegate) {
            this.delegate = delegate;
        }

        @Override
        public Directory wrapForMerge(MergePolicy.OneMerge merge, Directory in) {
            return delegate.wrapForMerge(merge, in);
        }

        @Override
        public void merge(MergeSource mergeSource, MergeTrigger trigger) throws IOException {
            delegate.merge(mergeSource, trigger);
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    static class FilterMergeSource implements MergeScheduler.MergeSource {
        private final MergeScheduler.MergeSource delegate;

        FilterMergeSource(MergeScheduler.MergeSource delegate) {
            this.delegate = delegate;
        }

        @Override
        public MergePolicy.OneMerge getNextMerge() {
            return delegate.getNextMerge();
        }

        @Override
        public void onMergeFinished(MergePolicy.OneMerge merge) {
            delegate.onMergeFinished(merge);
        }

        @Override
        public boolean hasPendingMerges() {
            return delegate.hasPendingMerges();
        }

        @Override
        public void merge(MergePolicy.OneMerge merge) throws IOException {
            delegate.merge(merge);
        }
    }
}
