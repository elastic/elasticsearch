/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.engine;

import co.elastic.elasticsearch.stateless.Stateless;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.util.SameThreadExecutorService;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.engine.ElasticsearchMergeScheduler;
import org.elasticsearch.index.engine.MergeTracking;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.merge.OnGoingMerge;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.elasticsearch.common.Strings.format;

/**
 * A merge scheduler which uses a provided thread pool to execute merges. The Lucene
 * {@link org.apache.lucene.index.ConcurrentMergeScheduler} creates a new thread for every merge. This allows numerous merges to execute
 * concurrently. This scheduler will limit the number of concurrent merges to the number of threads available on the pool.
 *
 * In contrast to regular ES this:
 *
 * Does not do any index throttling
 * Does not do IO throttling of merging (that is for search primarily so not relevant)
 * Does not prioritize small merges first (see ConcurrentMergeScheduler.MergeThread#compareTo)
 */
public class ThreadPoolMergeScheduler extends MergeScheduler implements ElasticsearchMergeScheduler {

    public static final Setting<Boolean> MERGE_THREAD_POOL_SCHEDULER = Setting.boolSetting(
        "stateless.merge.use_thread_pool_scheduler",
        false,
        Setting.Property.NodeScope
    );

    public static final Setting<Boolean> MERGE_PREWARM = Setting.boolSetting("stateless.merge.prewarm", true, Setting.Property.NodeScope);

    private final Logger logger;
    private final ShardId shardId;
    private final boolean prewarm;
    private final ThreadPool threadPool;
    private final Supplier<MergeMetrics> mergeMetrics;
    private final BiConsumer<String, MergePolicy.OneMerge> warmer;
    private final Consumer<Exception> exceptionHandler;
    private final MergeTracking mergeTracking;
    private final SameThreadExecutorService sameThreadExecutorService = new SameThreadExecutorService();

    // TODO: We could consider using a prioritized executor to compare merges. In particular, when comparing two merges between the same
    // shard perhaps we should prefer to execute a smaller merge first. This should probably be follow-up work.
    public ThreadPoolMergeScheduler(
        ShardId shardId,
        boolean prewarm,
        ThreadPool threadPool,
        Supplier<MergeMetrics> mergeMetrics,
        BiConsumer<String, MergePolicy.OneMerge> warmer,
        Consumer<Exception> exceptionHandler
    ) {
        this.logger = Loggers.getLogger(getClass(), shardId);
        this.shardId = shardId;
        this.prewarm = prewarm;
        this.threadPool = threadPool;
        this.mergeMetrics = mergeMetrics;
        this.warmer = warmer;
        this.exceptionHandler = exceptionHandler;
        this.mergeTracking = new MergeTracking(logger, () -> Double.POSITIVE_INFINITY);
    }

    @Override
    // Overridden until investigation in https://github.com/apache/lucene/pull/13475 is complete
    public Executor getIntraMergeExecutor(MergePolicy.OneMerge merge) {
        return sameThreadExecutorService;
    }

    @Override
    // Overridden until investigation in https://github.com/apache/lucene/pull/13475 is complete
    public void close() throws IOException {
        super.close();
        sameThreadExecutorService.shutdown();
    }

    @Override
    public void merge(MergeSource mergeSource, MergeTrigger trigger) throws IOException {
        MergePolicy.OneMerge merge = mergeSource.getNextMerge();
        if (merge != null) {
            AbstractRunnable command = mergeRunnable(mergeSource, merge);
            threadPool.executor(Stateless.MERGE_THREAD_POOL).execute(command);
        }
    }

    private AbstractRunnable mergeRunnable(MergeSource mergeSource, MergePolicy.OneMerge currentMerge) {
        final OnGoingMerge onGoingMerge = new OnGoingMerge(currentMerge);
        logger.trace("merge [{}] scheduling with thread pool", onGoingMerge.getId());
        return new AbstractRunnable() {

            @Override
            public void onAfter() {
                MergePolicy.OneMerge nextMerge;
                try {
                    nextMerge = mergeSource.getNextMerge();
                } catch (IllegalStateException e) {
                    logger.debug("merge poll failed, likely that index writer is failed", e);
                    return; // ignore exception, we expect the IW failure to be logged elsewhere
                }
                if (nextMerge != null) {
                    AbstractRunnable command = mergeRunnable(mergeSource, nextMerge);
                    threadPool.executor(Stateless.MERGE_THREAD_POOL).execute(command);
                }
            }

            @Override
            public void onRejection(Exception e) {
                // This would interrupt an IndexWriter if it were actually performing the merge. We just set this here because it seems
                // appropriate as we are not going to move forward with the merge.
                currentMerge.setAborted();
                logger.debug(() -> format("merge [%s] rejected by thread pool", onGoingMerge.getId()), e);
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof MergePolicy.MergeAbortedException) {
                    // TODO: How does this impact merge tracking?
                    logger.trace("merge [{}] aborted", onGoingMerge.getId());
                    // OK to ignore. This is what Lucene's ConcurrentMergeScheduler does
                } else {
                    exceptionHandler.accept(e);
                }
            }

            @Override
            protected void doRun() throws Exception {
                long timeNS = System.nanoTime();
                mergeTracking.mergeStarted(onGoingMerge);
                boolean success = false;
                try {
                    if (prewarm) {
                        warmer.accept(onGoingMerge.getId(), currentMerge);
                    }
                    mergeSource.merge(currentMerge);
                    success = true;
                } finally {
                    long tookMS = TimeValue.nsecToMSec(System.nanoTime() - timeNS);
                    if (success) {
                        mergeMetrics.get()
                            .markMergeMetrics(currentMerge, tookMS, MergeMetrics.mergeIdentifiers(shardId, onGoingMerge.getId()));
                    }
                    // TODO: Consider if we should adjust these metrics when a failure happens
                    mergeTracking.mergeFinished(currentMerge, onGoingMerge, tookMS);
                }
            }
        };
    }

    @Override
    public Set<OnGoingMerge> onGoingMerges() {
        return mergeTracking.onGoingMerges();
    }

    @Override
    public MergeStats stats() {
        return mergeTracking.stats();
    }

    @Override
    public void refreshConfig() {
        // No-op
    }

    @Override
    public MergeScheduler getMergeScheduler() {
        return this;
    }
}
