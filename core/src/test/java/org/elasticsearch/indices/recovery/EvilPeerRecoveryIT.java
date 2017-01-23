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

package org.elasticsearch.indices.recovery;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AnalyzerProvider;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class EvilPeerRecoveryIT extends ESIntegTestCase {

    private static AtomicReference<CountDownLatch> indexLatch = new AtomicReference<>();
    private static AtomicReference<CountDownLatch> waitForOpsToCompleteLatch = new AtomicReference<>();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(LatchAnalysisPlugin.class);
    }

    public static class LatchAnalysisPlugin extends Plugin implements AnalysisPlugin {

        @Override
        public Map<String, AnalysisModule.AnalysisProvider<AnalyzerProvider<? extends Analyzer>>> getAnalyzers() {
            return Collections.singletonMap("latch_analyzer", (a, b, c, d) -> new LatchAnalyzerProvider());
        }

    }

    static class LatchAnalyzerProvider implements AnalyzerProvider<LatchAnalyzer> {

        @Override
        public String name() {
            return "latch_analyzer";
        }

        @Override
        public AnalyzerScope scope() {
            return AnalyzerScope.INDICES;
        }

        @Override
        public LatchAnalyzer get() {
            return new LatchAnalyzer();
        }

    }

    static class LatchAnalyzer extends Analyzer {

        @Override
        protected TokenStreamComponents createComponents(final String fieldName) {
            return new TokenStreamComponents(new LatchTokenizer());
        }

    }

    static class LatchTokenizer extends Tokenizer {

        @Override
        public final boolean incrementToken() throws IOException {
            try {
                if (indexLatch.get() != null) {
                    // latch that all exected operations are in the engine
                    indexLatch.get().countDown();
                }

                if (waitForOpsToCompleteLatch.get() != null) {
                    // latch that waits for the replica to restart and allows recovery to proceed
                    waitForOpsToCompleteLatch.get().await();
                }

            } catch (final InterruptedException e) {
                throw new RuntimeException(e);
            }
            return false;
        }

    }

    @Override
    protected Settings nodeSettings(final int nodeOrdinal) {
        final Settings nodeSettings = super.nodeSettings(nodeOrdinal);
        final int processors = randomIntBetween(1, 4);
        /*
         * We have to do this to ensure that there are sufficiently many threads to accept the indexing requests, otherwise operations will
         * instead be queued and never trip the latch that all operations are inside the engine.
         */
        return Settings.builder().put(nodeSettings).put("processors", processors).put("thread_pool.bulk.size", 1 + processors).build();
    }

    /*
     * This tests that sequence-number-based recoveries wait for in-flight operations to complete. The trick here is simple. We latch some
     * in-flight operations inside the engine after sequence numbers are assigned. While these operations are latched, we restart a replica.
     * Sequence-number-based recovery on this replica has to wait until these in-flight operations complete to proceed. We verify at the end
     * of recovery that a file-based recovery was not completed, and that the expected number of operations was replayed via the translog.
     */
    public void testRecoveryWaitsForOps() throws Exception {
        final int docs = randomIntBetween(1, 64);
        final int numberOfProcessors = EsExecutors.numberOfProcessors(nodeSettings(0));
        final int latchedDocs = randomIntBetween(1, 1 + numberOfProcessors);

        try {
            internalCluster().startMasterOnlyNode();
            final String primaryNode = internalCluster().startDataOnlyNode(nodeSettings(0));

            // prepare mapping that uses our latch analyzer
            final XContentBuilder mapping = jsonBuilder();
            mapping.startObject();
            {
                mapping.startObject("type");
                {
                    mapping.startObject("properties");
                    {
                        mapping.startObject("foo");
                        {
                            mapping.field("type", "text");
                            mapping.field("analyzer", "latch_analyzer");
                            mapping.endObject();
                        }
                        mapping.endObject();
                    }
                    mapping.endObject();
                }
                mapping.endObject();
            }

            // create the index with our mapping
            client()
                .admin()
                .indices()
                .prepareCreate("index")
                .addMapping("type", mapping)
                .setSettings(Settings.builder().put("number_of_shards", 1))
                .get();

            // start the replica node; we do this after creating the index so we can control which node is holds the primary shard
            final String replicaNode = internalCluster().startDataOnlyNode(nodeSettings(1));
            ensureGreen();

            // index some documents so that the replica will attempt a sequence-number-based recovery upon restart
            for (int foo = 0; foo < docs; foo++) {
                index(randomFrom(primaryNode, replicaNode), foo);
            }

            if (randomBoolean()) {
                client().admin().indices().flush(new FlushRequest()).get();
            }

            // start some in-flight operations that will get latched in the engine
            final List<Thread> threads = new ArrayList<>();
            indexLatch.set(new CountDownLatch(latchedDocs));
            waitForOpsToCompleteLatch.set(new CountDownLatch(1));
            for (int i = docs; i < docs + latchedDocs; i++) {
                final int foo = i;
                // we have to index through the primary since we are going to restart the replica
                final Thread thread = new Thread(() -> index(primaryNode, foo));
                threads.add(thread);
                thread.start();
            }

            // latch until all operations are inside the engine
            indexLatch.get().await();

            internalCluster().restartNode(replicaNode, new InternalTestCluster.RestartCallback());

            final Index index = resolveIndex("index");

            // wait until recovery starts
            assertBusy(() -> {
                    final IndicesService primaryService = internalCluster().getInstance(IndicesService.class, primaryNode);
                    assertThat(primaryService.indexServiceSafe(index).getShard(0).recoveryStats().currentAsSource(), equalTo(1));
                    final IndicesService replicaService = internalCluster().getInstance(IndicesService.class, replicaNode);
                    assertThat(replicaService.indexServiceSafe(index).getShard(0).recoveryStats().currentAsTarget(), equalTo(1));
                }
            );

            // unlatch the operations that are latched inside the engine
            waitForOpsToCompleteLatch.get().countDown();

            for (final Thread thread : threads) {
                thread.join();
            }

            // recovery should complete successfully
            ensureGreen();

            // verify that a sequence-number-based recovery was completed
            final org.elasticsearch.action.admin.indices.recovery.RecoveryResponse response =
                client().admin().indices().prepareRecoveries("index").get();
            final List<RecoveryState> states = response.shardRecoveryStates().get("index");
            for (final RecoveryState state : states) {
                if (state.getTargetNode().getName().equals(replicaNode)) {
                    assertThat(state.getTranslog().recoveredOperations(), equalTo(latchedDocs));
                    assertThat(state.getIndex().recoveredFilesPercent(), equalTo(0f));
                }
            }
        } finally {
            internalCluster().close();
        }

    }

    private void index(final String node, final int foo) {
        client(node).prepareIndex("index", "type").setSource("{\"foo\":\"" + Integer.toString(foo) + "\"}").get();
    }

}
