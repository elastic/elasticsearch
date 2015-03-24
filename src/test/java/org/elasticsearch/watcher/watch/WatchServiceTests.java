/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.watch;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.watcher.WatcherException;
import org.elasticsearch.watcher.history.HistoryService;
import org.elasticsearch.watcher.scheduler.Scheduler;
import org.elasticsearch.watcher.scheduler.schedule.Schedule;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 */
public class WatchServiceTests extends ElasticsearchTestCase {

    private Scheduler scheduler;
    private WatchStore watchStore;
    private WatchService watchService;
    private HistoryService historyService;
    private WatchLockService watchLockService;

    @Before
    public void init() throws Exception {
        scheduler = mock(Scheduler.class);
        watchStore = mock(WatchStore.class);
        historyService =  mock(HistoryService.class);
        watchLockService = mock(WatchLockService.class);
        watchService = new WatchService(ImmutableSettings.EMPTY, scheduler, watchStore, historyService, watchLockService);
        Field field = WatchService.class.getDeclaredField("state");
        field.setAccessible(true);
        AtomicReference<WatchService.State> state = (AtomicReference<WatchService.State>) field.get(watchService);
        state.set(WatchService.State.STARTED);
    }

    @Test
    public void testPutWatch() {
        IndexResponse indexResponse = mock(IndexResponse.class);
        Watch watch = mock(Watch.class);
        WatchStore.WatchPut watchPut = mock(WatchStore.WatchPut.class);
        when(watchPut.indexResponse()).thenReturn(indexResponse);
        when(watchPut.current()).thenReturn(watch);

        WatchLockService.Lock lock = mock(WatchLockService.Lock.class);
        when(watchLockService.acquire(any(String.class))).thenReturn(lock);
        when(watchStore.put(any(String.class), any(BytesReference.class))).thenReturn(watchPut);
        IndexResponse response = watchService.putWatch("_name", new BytesArray("{}"));
        assertThat(response, sameInstance(indexResponse));

        verify(scheduler, times(1)).add(any(Scheduler.Job.class));
    }

    @Test
    public void testPutWatch_NotSchedule() {
        Schedule schedule = mock(Schedule.class);

        IndexResponse indexResponse = mock(IndexResponse.class);
        Watch watch = mock(Watch.class);
        when(watch.schedule()).thenReturn(schedule);
        WatchStore.WatchPut watchPut = mock(WatchStore.WatchPut.class);
        when(watchPut.indexResponse()).thenReturn(indexResponse);
        when(watchPut.current()).thenReturn(watch);
        Watch previousWatch = mock(Watch.class);
        when(previousWatch.schedule()).thenReturn(schedule);
        when(watchPut.previous()).thenReturn(previousWatch);

        WatchLockService.Lock lock = mock(WatchLockService.Lock.class);
        when(watchLockService.acquire(any(String.class))).thenReturn(lock);
        when(watchStore.put(any(String.class), any(BytesReference.class))).thenReturn(watchPut);
        IndexResponse response = watchService.putWatch("_name", new BytesArray("{}"));
        assertThat(response, sameInstance(indexResponse));

        verifyZeroInteractions(scheduler);
    }

    @Test
    public void testDeleteWatch() throws Exception {
        WatchLockService.Lock lock = mock(WatchLockService.Lock.class);
        when(watchLockService.acquire("_name")).thenReturn(lock);

        WatchStore.WatchDelete expectedWatchDelete = mock(WatchStore.WatchDelete.class);
        DeleteResponse deleteResponse = mock(DeleteResponse.class);
        when(deleteResponse.isFound()).thenReturn(true);
        when(expectedWatchDelete.deleteResponse()).thenReturn(deleteResponse);
        when(watchStore.delete("_name")).thenReturn(expectedWatchDelete);
        WatchStore.WatchDelete watchDelete = watchService.deleteWatch("_name");

        assertThat(watchDelete, sameInstance(expectedWatchDelete));
        verify(scheduler, times(1)).remove("_name");
    }

    @Test
    public void testDeleteWatch_NotFound() throws Exception {
        WatchLockService.Lock lock = mock(WatchLockService.Lock.class);
        when(watchLockService.acquire("_name")).thenReturn(lock);

        WatchStore.WatchDelete expectedWatchDelete = mock(WatchStore.WatchDelete.class);
        DeleteResponse deleteResponse = mock(DeleteResponse.class);
        when(deleteResponse.isFound()).thenReturn(false);
        when(expectedWatchDelete.deleteResponse()).thenReturn(deleteResponse);
        when(watchStore.delete("_name")).thenReturn(expectedWatchDelete);
        WatchStore.WatchDelete watchDelete = watchService.deleteWatch("_name");

        assertThat(watchDelete, sameInstance(expectedWatchDelete));
        verifyZeroInteractions(scheduler);
    }

    @Test
    public void testAckWatch() throws Exception {
        WatchLockService.Lock lock = mock(WatchLockService.Lock.class);
        when(watchLockService.acquire("_name")).thenReturn(lock);
        Watch watch = mock(Watch.class);
        when(watch.ack()).thenReturn(true);
        Watch.Status status = new Watch.Status();
        when(watch.status()).thenReturn(status);
        when(watchStore.get("_name")).thenReturn(watch);

        Watch.Status result = watchService.ackWatch("_name");
        assertThat(result, not(sameInstance(status)));

        verify(watchStore, times(1)).updateStatus(watch);
    }

    @Test
    public void testAckWatch_NotAck() throws Exception {
        WatchLockService.Lock lock = mock(WatchLockService.Lock.class);
        when(watchLockService.acquire("_name")).thenReturn(lock);
        Watch watch = mock(Watch.class);
        when(watch.ack()).thenReturn(false);
        Watch.Status status = new Watch.Status();
        when(watch.status()).thenReturn(status);
        when(watchStore.get("_name")).thenReturn(watch);

        Watch.Status result = watchService.ackWatch("_name");
        assertThat(result, not(sameInstance(status)));

        verify(watchStore, never()).updateStatus(watch);
    }

    @Test
    public void testAckWatch_NoWatch() throws Exception {
        WatchLockService.Lock lock = mock(WatchLockService.Lock.class);
        when(watchLockService.acquire("_name")).thenReturn(lock);
        when(watchStore.get("_name")).thenReturn(null);

        try {
            watchService.ackWatch("_name");
            fail();
        } catch (WatcherException e) {
            // expected
        }

        verify(watchStore, never()).updateStatus(any(Watch.class));
    }

}
