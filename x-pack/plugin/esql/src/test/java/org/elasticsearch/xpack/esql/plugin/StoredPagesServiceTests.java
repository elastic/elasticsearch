/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.plugin.TransportReadStreamPagesAction.Response;
import org.elasticsearch.xpack.esql.plugin.TransportReadStreamPagesAction.StoredPage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * The refreshes run by {@link StoredPagesService} are completed by the test, and its tasks run on a {@link DeterministicTaskQueue},
 * so the test orders every step of a wait.
 */
public class StoredPagesServiceTests extends ESTestCase {

    private static final ShardId SHARD = new ShardId("pages", "_na_", 0);
    private static final String RESULT = "result";
    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);

    private final DeterministicTaskQueue queue = new DeterministicTaskQueue();
    private final List<ShardId> refreshed = new ArrayList<>();
    private final List<ActionListener<Void>> running = new ArrayList<>();
    private final List<String> waiting = new ArrayList<>();
    private boolean executorFull;
    private final StoredPagesService service = new StoredPagesService(queue.getThreadPool(), new Executor() {
        @Override
        public void execute(Runnable command) {
            if (executorFull && (command instanceof AbstractRunnable r && r.isForceExecution()) == false) {
                throw new EsRejectedExecutionException("full", false);
            }
            queue.getThreadPool().generic().execute(command);
        }
    }, (shardId, l) -> {
        refreshed.add(shardId);
        running.add(l);
    });

    // what a read of the shard finds
    private List<StoredPage> visible = List.of();
    private boolean ended;
    private int unreleased;

    {
        service.setWaitingListener(waiting::add);
    }

    /**
     * The reads that ask while a refresh runs share the next one, which starts once it is done.
     */
    public void testReadsShareTheNextRefresh() {
        PlainActionFuture<Void> first = refresh(SHARD);
        List<PlainActionFuture<Void>> next = new ArrayList<>();
        for (int i = between(1, 10); i > 0; i--) {
            next.add(refresh(SHARD));
        }
        assertThat(refreshed.size(), equalTo(1));

        completeRefreshes();
        assertTrue(first.isDone());
        // the refresh that ran started before they asked, so it may miss what they wait for
        next.forEach(f -> assertFalse(f.isDone()));
        assertThat(refreshed.size(), equalTo(2));

        completeRefreshes();
        next.forEach(f -> assertTrue(f.isDone()));
        assertThat(refreshed.size(), equalTo(2));
    }

    public void testShardsRefreshOnTheirOwn() {
        ShardId other = new ShardId(SHARD.getIndex(), 1);
        PlainActionFuture<Void> first = refresh(SHARD);
        PlainActionFuture<Void> second = refresh(other);
        assertThat(refreshed, equalTo(List.of(SHARD, other)));
        completeRefreshes();
        assertTrue(first.isDone());
        assertTrue(second.isDone());
    }

    public void testFailedRefreshFailsTheReadsItServes() {
        PlainActionFuture<Void> first = refresh(SHARD);
        PlainActionFuture<Void> next = refresh(SHARD);
        running.removeFirst().onFailure(new IllegalStateException("shard closed"));
        expectThrows(IllegalStateException.class, first::actionGet);
        assertFalse(next.isDone());
        completeRefreshes();
        assertTrue(next.isDone());
    }

    public void testVisiblePagesAnswerAtOnce() throws Exception {
        visible = pages(1);
        Answers answers = read(SHARD, TIMEOUT);
        assertThat(answers.pages, equalTo(visible));
        assertThat(refreshed.size(), equalTo(0));
    }

    public void testEndedResultAnswersAtOnce() throws Exception {
        ended = true;
        Answers answers = read(SHARD, TIMEOUT);
        assertThat(answers.count, equalTo(1));
        assertThat(refreshed.size(), equalTo(0));
    }

    /**
     * A read that finds nothing refreshes its shard, then waits for a document of its result.
     */
    public void testWokenReadAnswersWithNewPages() throws Exception {
        Answers answers = startWaiting(SHARD);
        assertThat(service.waitingReads(), equalTo(1));

        visible = pages(1);
        service.wake(RESULT);
        completeRefreshes();
        queue.runAllRunnableTasks();
        assertThat(answers.pages, equalTo(visible));
        assertThat(service.waitingReads(), equalTo(0));

        // its timeout does nothing more
        queue.runAllTasksInTimeOrder();
        assertThat(answers.count, equalTo(1));
        assertThat(unreleased, equalTo(0));
    }

    public void testTimeoutAnswersWithWhatIsVisible() throws Exception {
        Answers answers = startWaiting(SHARD);
        queue.runAllTasksInTimeOrder();
        assertThat(answers.count, equalTo(1));
        assertThat(answers.pages, equalTo(List.of()));
        assertThat(service.waitingReads(), equalTo(0));
        assertThat(unreleased, equalTo(0));
    }

    /**
     * Nothing else ends a wait once its result gets no more documents, so its timeout runs on a full executor.
     */
    public void testTimeoutRunsOnFullExecutor() throws Exception {
        Answers answers = startWaiting(SHARD);
        executorFull = true;
        queue.runAllTasksInTimeOrder();
        assertThat(answers.count, equalTo(1));
    }

    /**
     * A read woken after it found nothing but before it set its timeout answers once, and does not say it waits.
     */
    public void testWokenBeforeTimeoutIsSet() throws Exception {
        Answers answers = read(SHARD, TIMEOUT);
        completeRefreshes();
        service.wake(RESULT);
        queue.runAllRunnableTasks();
        assertThat(waiting, equalTo(List.of()));
        assertThat(answers.count, equalTo(0));

        completeRefreshes();
        queue.runAllTasksInTimeOrder();
        assertThat(answers.count, equalTo(1));
        assertThat(service.waitingReads(), equalTo(0));
    }

    public void testShardCloseFailsItsWaits() throws Exception {
        ShardId other = new ShardId(SHARD.getIndex(), 1);
        Answers closed = startWaiting(SHARD);
        Answers open = startWaiting(other);
        service.afterIndexShardClosed(SHARD, null, Settings.EMPTY);
        assertThat(closed.failure, instanceOf(IndexShardClosedException.class));
        assertThat(open.count, equalTo(0));
        assertThat(service.waitingReads(), equalTo(1));
    }

    public void testTooManyWaitsAreRejected() throws Exception {
        for (int i = 0; i < StoredPagesService.MAX_WAITING_READS; i++) {
            read(SHARD, TIMEOUT);
        }
        Answers rejected = read(SHARD, TIMEOUT);
        assertThat(rejected.failure, instanceOf(EsRejectedExecutionException.class));
        assertThat(service.waitingReads(), equalTo(StoredPagesService.MAX_WAITING_READS));
    }

    private Answers startWaiting(ShardId shardId) throws Exception {
        Answers answers = read(shardId, TIMEOUT);
        completeRefreshes();
        queue.runAllRunnableTasks();
        assertThat(waiting.getLast(), equalTo(RESULT));
        assertThat(answers.count, equalTo(0));
        return answers;
    }

    private Answers read(ShardId shardId, TimeValue timeout) throws Exception {
        Answers answers = new Answers();
        service.read(shardId, RESULT, () -> {
            unreleased++;
            return new Response(visible, ended, () -> unreleased--);
        }, timeout, answers);
        return answers;
    }

    private PlainActionFuture<Void> refresh(ShardId shardId) {
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        service.refresh(shardId, future);
        return future;
    }

    private void completeRefreshes() {
        List<ActionListener<Void>> toComplete = new ArrayList<>(running);
        running.clear();
        toComplete.forEach(l -> l.onResponse(null));
    }

    private static List<StoredPage> pages(int count) {
        List<StoredPage> pages = new ArrayList<>();
        for (int seq = 1; seq <= count; seq++) {
            pages.add(new StoredPage(seq, (seq - 1) * 10L, 10, TransportVersion.current(), new BytesArray(new byte[10])));
        }
        return pages;
    }

    private static class Answers implements ActionListener<Response> {
        private int count;
        private List<StoredPage> pages;
        private Exception failure;

        @Override
        public void onResponse(Response response) {
            count++;
            pages = response.pages();
        }

        @Override
        public void onFailure(Exception e) {
            count++;
            failure = e;
        }
    }
}
