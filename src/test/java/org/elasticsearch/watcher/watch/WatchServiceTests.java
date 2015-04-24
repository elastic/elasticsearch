/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.watch;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.watcher.WatcherException;
import org.elasticsearch.watcher.WatcherService;
import org.elasticsearch.watcher.execution.ExecutionService;
import org.elasticsearch.watcher.trigger.Trigger;
import org.elasticsearch.watcher.trigger.TriggerEngine;
import org.elasticsearch.watcher.trigger.TriggerService;
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

    private TriggerService triggerService;
    private WatchStore watchStore;
    private Watch.Parser watchParser;
    private WatcherService watcherService;
    private ExecutionService executionService;
    private WatchLockService watchLockService;

    @Before
    public void init() throws Exception {
        triggerService = mock(TriggerService.class);
        watchStore = mock(WatchStore.class);
        watchParser = mock(Watch.Parser.class);
        executionService =  mock(ExecutionService.class);
        watchLockService = mock(WatchLockService.class);
        watcherService = new WatcherService(ImmutableSettings.EMPTY, triggerService, watchStore, watchParser, executionService, watchLockService);
        Field field = WatcherService.class.getDeclaredField("state");
        field.setAccessible(true);
        AtomicReference<WatcherService.State> state = (AtomicReference<WatcherService.State>) field.get(watcherService);
        state.set(WatcherService.State.STARTED);
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
        when(watchParser.parseWithSecrets(any(String.class), eq(false), any(BytesReference.class))).thenReturn(watch);
        when(watchStore.put(watch)).thenReturn(watchPut);
        IndexResponse response = watcherService.putWatch("_name", new BytesArray("{}"));
        assertThat(response, sameInstance(indexResponse));

        verify(triggerService, times(1)).add(any(TriggerEngine.Job.class));
    }

    @Test
    public void testPutWatch_NotSchedule() {
        Trigger trigger = mock(Trigger.class);

        IndexResponse indexResponse = mock(IndexResponse.class);
        Watch watch = mock(Watch.class);
        when(watch.trigger()).thenReturn(trigger);
        WatchStore.WatchPut watchPut = mock(WatchStore.WatchPut.class);
        when(watchPut.indexResponse()).thenReturn(indexResponse);
        when(watchPut.current()).thenReturn(watch);
        Watch previousWatch = mock(Watch.class);
        when(previousWatch.trigger()).thenReturn(trigger);
        when(watchPut.previous()).thenReturn(previousWatch);

        WatchLockService.Lock lock = mock(WatchLockService.Lock.class);
        when(watchLockService.acquire(any(String.class))).thenReturn(lock);
        when(watchParser.parseWithSecrets(any(String.class), eq(false), any(BytesReference.class))).thenReturn(watch);
        when(watchStore.put(watch)).thenReturn(watchPut);
        IndexResponse response = watcherService.putWatch("_name", new BytesArray("{}"));
        assertThat(response, sameInstance(indexResponse));

        verifyZeroInteractions(triggerService);
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
        WatchStore.WatchDelete watchDelete = watcherService.deleteWatch("_name");

        assertThat(watchDelete, sameInstance(expectedWatchDelete));
        verify(triggerService, times(1)).remove("_name");
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
        WatchStore.WatchDelete watchDelete = watcherService.deleteWatch("_name");

        assertThat(watchDelete, sameInstance(expectedWatchDelete));
        verifyZeroInteractions(triggerService);
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

        Watch.Status result = watcherService.ackWatch("_name");
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

        Watch.Status result = watcherService.ackWatch("_name");
        assertThat(result, not(sameInstance(status)));

        verify(watchStore, never()).updateStatus(watch);
    }

    @Test
    public void testAckWatch_NoWatch() throws Exception {
        WatchLockService.Lock lock = mock(WatchLockService.Lock.class);
        when(watchLockService.acquire("_name")).thenReturn(lock);
        when(watchStore.get("_name")).thenReturn(null);

        try {
            watcherService.ackWatch("_name");
            fail();
        } catch (WatcherException e) {
            // expected
        }

        verify(watchStore, never()).updateStatus(any(Watch.class));
    }

}
