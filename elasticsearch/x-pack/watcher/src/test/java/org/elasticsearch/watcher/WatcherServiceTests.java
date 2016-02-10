/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.execution.ExecutionService;
import org.elasticsearch.watcher.support.WatcherIndexTemplateRegistry;
import org.elasticsearch.watcher.support.clock.ClockMock;
import org.elasticsearch.watcher.support.clock.SystemClock;
import org.elasticsearch.watcher.trigger.Trigger;
import org.elasticsearch.watcher.trigger.TriggerEngine;
import org.elasticsearch.watcher.trigger.TriggerService;
import org.elasticsearch.watcher.watch.Watch;
import org.elasticsearch.watcher.watch.WatchLockService;
import org.elasticsearch.watcher.watch.WatchStatus;
import org.elasticsearch.watcher.watch.WatchStore;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;

import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

/**
 *
 */
public class WatcherServiceTests extends ESTestCase {
    private TriggerService triggerService;
    private WatchStore watchStore;
    private Watch.Parser watchParser;
    private WatcherService watcherService;
    private WatchLockService watchLockService;
    private ClockMock clock;

    @Before
    public void init() throws Exception {
        triggerService = mock(TriggerService.class);
        watchStore = mock(WatchStore.class);
        watchParser = mock(Watch.Parser.class);
        ExecutionService executionService =  mock(ExecutionService.class);
        watchLockService = mock(WatchLockService.class);
        clock = new ClockMock();
        WatcherIndexTemplateRegistry watcherIndexTemplateRegistry = mock(WatcherIndexTemplateRegistry.class);
        watcherService = new WatcherService(Settings.EMPTY, clock, triggerService, watchStore, watchParser, executionService,
                watchLockService, watcherIndexTemplateRegistry);
        AtomicReference<WatcherState> state = watcherService.state;
        state.set(WatcherState.STARTED);
    }

    public void testPutWatch() throws Exception {
        boolean activeByDefault = randomBoolean();

        IndexResponse indexResponse = mock(IndexResponse.class);
        Watch newWatch = mock(Watch.class);
        WatchStatus status = mock(WatchStatus.class);
        when(status.state()).thenReturn(new WatchStatus.State(activeByDefault, clock.nowUTC()));
        when(newWatch.status()).thenReturn(status);

        WatchStore.WatchPut watchPut = mock(WatchStore.WatchPut.class);
        when(watchPut.indexResponse()).thenReturn(indexResponse);
        when(watchPut.current()).thenReturn(newWatch);

        TimeValue timeout = TimeValue.timeValueSeconds(5);
        WatchLockService.Lock lock = mock(WatchLockService.Lock.class);
        when(watchLockService.tryAcquire(any(String.class), eq(timeout))).thenReturn(lock);
        when(watchParser.parseWithSecrets(any(String.class), eq(false), any(BytesReference.class), any(DateTime.class)))
                .thenReturn(newWatch);
        when(watchStore.put(newWatch)).thenReturn(watchPut);
        IndexResponse response = watcherService.putWatch("_id", new BytesArray("{}"), timeout, activeByDefault);
        assertThat(response, sameInstance(indexResponse));

        verify(newWatch, times(1)).setState(activeByDefault, clock.nowUTC());
        if (activeByDefault) {
            verify(triggerService, times(1)).add(any(TriggerEngine.Job.class));
        } else {
            verifyZeroInteractions(triggerService);
        }
    }

    public void testPutWatchTimeout() throws Exception {
        TimeValue timeout = TimeValue.timeValueSeconds(5);
        when(watchLockService.tryAcquire("_id", timeout)).thenReturn(null);
        try {
            watcherService.putWatch("_id", new BytesArray("{}"), timeout, randomBoolean());
            fail("Expected ElasticsearchTimeoutException");
        } catch (ElasticsearchTimeoutException e) {
            assertThat(e.getMessage(), containsString("could not put watch"));
            assertThat(e.getMessage(), containsString("wait and try again"));
            assertThat(e.getMessage(), containsString("there is a high chance that the watch execution is stuck"));
        }
    }

    public void testPutWatchDifferentActiveStates() throws Exception {
        Trigger trigger = mock(Trigger.class);

        IndexResponse indexResponse = mock(IndexResponse.class);

        Watch watch = mock(Watch.class);
        when(watch.id()).thenReturn("_id");
        WatchStatus status = mock(WatchStatus.class);
        boolean active = randomBoolean();
        when(status.state()).thenReturn(new WatchStatus.State(active, clock.nowUTC()));
        when(watch.status()).thenReturn(status);
        when(watch.trigger()).thenReturn(trigger);
        WatchStore.WatchPut watchPut = mock(WatchStore.WatchPut.class);
        when(watchPut.indexResponse()).thenReturn(indexResponse);
        when(watchPut.current()).thenReturn(watch);

        Watch previousWatch = mock(Watch.class);
        WatchStatus previousStatus = mock(WatchStatus.class);
        boolean prevActive = randomBoolean();
        when(previousStatus.state()).thenReturn(new WatchStatus.State(prevActive, clock.nowUTC()));
        when(previousWatch.status()).thenReturn(previousStatus);
        when(previousWatch.trigger()).thenReturn(trigger);
        when(watchPut.previous()).thenReturn(previousWatch);

        TimeValue timeout = TimeValue.timeValueSeconds(5);
        WatchLockService.Lock lock = mock(WatchLockService.Lock.class);
        when(watchLockService.tryAcquire(any(String.class), eq(timeout))).thenReturn(lock);
        when(watchParser.parseWithSecrets(any(String.class), eq(false), any(BytesReference.class), eq(clock.nowUTC()))).thenReturn(watch);
        when(watchStore.put(watch)).thenReturn(watchPut);

        IndexResponse response = watcherService.putWatch("_id", new BytesArray("{}"), timeout, active);
        assertThat(response, sameInstance(indexResponse));

        if (!active) {
            // we should always remove the watch from the trigger service, just to be safe
            verify(triggerService, times(1)).remove("_id");
        } else if (prevActive) {
            // if both the new watch and the prev one are active, we should do nothing
            verifyZeroInteractions(triggerService);
        } else {
            // if the prev watch was not active and the new one is active, we should add the watch
            verify(triggerService, times(1)).add(watch);
        }
    }

    public void testDeleteWatch() throws Exception {
        TimeValue timeout = TimeValue.timeValueSeconds(5);
        WatchLockService.Lock lock = mock(WatchLockService.Lock.class);
        boolean force = randomBoolean();
        when(watchLockService.tryAcquire("_id", timeout)).thenReturn(lock);

        WatchStore.WatchDelete expectedWatchDelete = mock(WatchStore.WatchDelete.class);
        DeleteResponse deleteResponse = mock(DeleteResponse.class);
        when(deleteResponse.isFound()).thenReturn(true);
        when(expectedWatchDelete.deleteResponse()).thenReturn(deleteResponse);
        when(watchStore.delete("_id", force)).thenReturn(expectedWatchDelete);
        WatchStore.WatchDelete watchDelete = watcherService.deleteWatch("_id", timeout, force);

        assertThat(watchDelete, sameInstance(expectedWatchDelete));
        verify(triggerService, times(1)).remove("_id");
    }

    public void testDeleteWatchTimeout() throws Exception {
        TimeValue timeout = TimeValue.timeValueSeconds(5);
        when(watchLockService.tryAcquire("_id", timeout)).thenReturn(null);
        try {
            watcherService.deleteWatch("_id", timeout, false);
            fail("Expected ElasticsearchTimeoutException");
        } catch (ElasticsearchTimeoutException e) {
            assertThat(e.getMessage(), containsString("could not delete watch"));
            assertThat(e.getMessage(), containsString("wait and try again"));
            assertThat(e.getMessage(), containsString("there is a high chance that the watch execution is stuck"));
        }
    }

    public void testDeleteWatchForce() throws Exception {
        TimeValue timeout = TimeValue.timeValueSeconds(5);
        WatchLockService.Lock lock = mock(WatchLockService.Lock.class);
        when(watchLockService.tryAcquire("_id", timeout)).thenReturn(null);

        WatchStore.WatchDelete expectedWatchDelete = mock(WatchStore.WatchDelete.class);
        DeleteResponse deleteResponse = mock(DeleteResponse.class);
        when(deleteResponse.isFound()).thenReturn(true);
        when(expectedWatchDelete.deleteResponse()).thenReturn(deleteResponse);
        when(watchStore.delete("_id", true)).thenReturn(expectedWatchDelete);
        WatchStore.WatchDelete watchDelete = watcherService.deleteWatch("_id", timeout, true);

        assertThat(watchDelete, sameInstance(expectedWatchDelete));
        verify(triggerService, times(1)).remove("_id");
    }

    public void testDeleteWatchNotFound() throws Exception {
        TimeValue timeout = TimeValue.timeValueSeconds(5);
        boolean force = randomBoolean();
        WatchLockService.Lock lock = mock(WatchLockService.Lock.class);
        when(watchLockService.tryAcquire("_id", timeout)).thenReturn(lock);

        WatchStore.WatchDelete expectedWatchDelete = mock(WatchStore.WatchDelete.class);
        DeleteResponse deleteResponse = mock(DeleteResponse.class);
        when(deleteResponse.isFound()).thenReturn(false);
        when(expectedWatchDelete.deleteResponse()).thenReturn(deleteResponse);
        when(watchStore.delete("_id", force)).thenReturn(expectedWatchDelete);
        WatchStore.WatchDelete watchDelete = watcherService.deleteWatch("_id", timeout, force);

        assertThat(watchDelete, sameInstance(expectedWatchDelete));
        verifyZeroInteractions(triggerService);
    }

    public void testAckWatch() throws Exception {
        DateTime now = new DateTime(DateTimeZone.UTC);
        clock.setTime(now);
        TimeValue timeout = TimeValue.timeValueSeconds(5);
        WatchLockService.Lock lock = mock(WatchLockService.Lock.class);
        when(watchLockService.tryAcquire("_id", timeout)).thenReturn(lock);
        Watch watch = mock(Watch.class);
        when(watch.ack(now, "_all")).thenReturn(true);
        WatchStatus status = new WatchStatus(now, emptyMap());
        when(watch.status()).thenReturn(status);
        when(watchStore.get("_id")).thenReturn(watch);

        WatchStatus result = watcherService.ackWatch("_id", Strings.EMPTY_ARRAY, timeout);
        assertThat(result, not(sameInstance(status)));

        verify(watchStore, times(1)).updateStatus(watch);
    }

    public void testActivate() throws Exception {
        TimeValue timeout = TimeValue.timeValueSeconds(randomIntBetween(1, 30));
        WatcherService service = spy(watcherService);
        WatchStatus expectedStatus = mock(WatchStatus.class);
        doReturn(expectedStatus).when(service).setWatchState("_id", true, timeout);
        WatchStatus actualStatus = service.activateWatch("_id", timeout);
        assertThat(actualStatus, sameInstance(expectedStatus));
        verify(service, times(1)).setWatchState("_id", true, timeout);
    }

    public void testDeactivate() throws Exception {
        TimeValue timeout = TimeValue.timeValueSeconds(randomIntBetween(1, 30));
        WatcherService service = spy(watcherService);
        WatchStatus expectedStatus = mock(WatchStatus.class);
        doReturn(expectedStatus).when(service).setWatchState("_id", false, timeout);
        WatchStatus actualStatus = service.deactivateWatch("_id", timeout);
        assertThat(actualStatus, sameInstance(expectedStatus));
        verify(service, times(1)).setWatchState("_id", false, timeout);
    }

    public void testSetWatchStateSetActiveOnCurrentlyActive() throws Exception {
        // trying to activate a watch that is already active:
        //  - the watch status should not change
        //  - the watch doesn't need to be updated in the store
        //  - the watch should not be removed or re-added to the trigger service

        DateTime now = new DateTime(DateTimeZone.UTC);
        clock.setTime(now);
        TimeValue timeout = TimeValue.timeValueSeconds(5);
        WatchLockService.Lock lock = mock(WatchLockService.Lock.class);
        when(watchLockService.tryAcquire("_id", timeout)).thenReturn(lock);

        Watch watch = mock(Watch.class);
        WatchStatus status = new WatchStatus(now, emptyMap());
        when(watch.status()).thenReturn(status);
        when(watch.setState(true, now)).thenReturn(false);

        when(watchStore.get("_id")).thenReturn(watch);


        WatchStatus result = watcherService.setWatchState("_id", true, timeout);
        assertThat(result, not(sameInstance(status)));

        verifyZeroInteractions(triggerService);
        verify(watchStore, never()).updateStatus(watch);
    }

    public void testSetWatchStateSetActiveOnCurrentlyInactive() throws Exception {
        // activating a watch that is currently inactive:
        //  - the watch status should be updated
        //  - the watch needs to be updated in the store
        //  - the watch should be re-added to the trigger service (the assumption is that it's not there)

        DateTime now = new DateTime(DateTimeZone.UTC);
        clock.setTime(now);

        TimeValue timeout = TimeValue.timeValueSeconds(5);
        WatchLockService.Lock lock = mock(WatchLockService.Lock.class);
        when(watchLockService.tryAcquire("_id", timeout)).thenReturn(lock);

        Watch watch = mock(Watch.class);
        WatchStatus status = new WatchStatus(now, emptyMap());
        when(watch.status()).thenReturn(status);
        when(watch.setState(true, now)).thenReturn(true);

        when(watchStore.get("_id")).thenReturn(watch);

        WatchStatus result = watcherService.setWatchState("_id", true, timeout);
        assertThat(result, not(sameInstance(status)));

        verify(triggerService, times(1)).add(watch);
        verify(watchStore, times(1)).updateStatus(watch);
    }

    public void testSetWatchStateSetInactiveOnCurrentlyActive() throws Exception {
        // deactivating a watch that is currently active:
        //  - the watch status should change
        //  - the watch needs to be updated in the store
        //  - the watch should be removed from the trigger service

        DateTime now = new DateTime(DateTimeZone.UTC);
        clock.setTime(now);
        TimeValue timeout = TimeValue.timeValueSeconds(5);
        WatchLockService.Lock lock = mock(WatchLockService.Lock.class);
        when(watchLockService.tryAcquire("_id", timeout)).thenReturn(lock);

        Watch watch = mock(Watch.class);
        when(watch.id()).thenReturn("_id");
        WatchStatus status = new WatchStatus(now, emptyMap());
        when(watch.status()).thenReturn(status);
        when(watch.setState(false, now)).thenReturn(true);

        when(watchStore.get("_id")).thenReturn(watch);

        WatchStatus result = watcherService.setWatchState("_id", false, timeout);
        assertThat(result, not(sameInstance(status)));

        verify(triggerService, times(1)).remove("_id");
        verify(watchStore, times(1)).updateStatus(watch);
    }

    public void testSetWatchStateSetInactiveOnCurrentlyInactive() throws Exception {
        // trying to deactivate a watch that is currently inactive:
        //  - the watch status should not be updated
        //  - the watch should not be updated in the store
        //  - the watch should be re-added or removed to/from the trigger service

        DateTime now = new DateTime(DateTimeZone.UTC);
        clock.setTime(now);

        TimeValue timeout = TimeValue.timeValueSeconds(5);
        WatchLockService.Lock lock = mock(WatchLockService.Lock.class);
        when(watchLockService.tryAcquire("_id", timeout)).thenReturn(lock);

        Watch watch = mock(Watch.class);
        when(watch.id()).thenReturn("_id");
        WatchStatus status = new WatchStatus(now, emptyMap());
        when(watch.status()).thenReturn(status);
        when(watch.setState(false, now)).thenReturn(false);

        when(watchStore.get("_id")).thenReturn(watch);

        WatchStatus result = watcherService.setWatchState("_id", false, timeout);
        assertThat(result, not(sameInstance(status)));

        verifyZeroInteractions(triggerService);
        verify(watchStore, never()).updateStatus(watch);
    }

    public void testAckWatchTimeout() throws Exception {
        TimeValue timeout = TimeValue.timeValueSeconds(5);
        when(watchLockService.tryAcquire("_id", timeout)).thenReturn(null);
        try {
            watcherService.ackWatch("_id", Strings.EMPTY_ARRAY, timeout);
            fail("Expected ElasticsearchTimeoutException");
        } catch (ElasticsearchTimeoutException e) {
            assertThat(e.getMessage(), containsString("could not ack watch"));
            assertThat(e.getMessage(), containsString("wait and try again"));
            assertThat(e.getMessage(), containsString("there is a high chance that the watch execution is stuck"));
        }
    }

    public void testAckWatchNotAck() throws Exception {
        DateTime now = SystemClock.INSTANCE.nowUTC();
        TimeValue timeout = TimeValue.timeValueSeconds(5);
        WatchLockService.Lock lock = mock(WatchLockService.Lock.class);
        when(watchLockService.tryAcquire("_id", timeout)).thenReturn(lock);
        Watch watch = mock(Watch.class);
        when(watch.ack(now)).thenReturn(false);
        WatchStatus status = new WatchStatus(now, emptyMap());
        when(watch.status()).thenReturn(status);
        when(watchStore.get("_id")).thenReturn(watch);

        WatchStatus result = watcherService.ackWatch("_id", Strings.EMPTY_ARRAY, timeout);
        assertThat(result, not(sameInstance(status)));

        verify(watchStore, never()).updateStatus(watch);
    }

    public void testAckWatchNoWatch() throws Exception {
        TimeValue timeout = TimeValue.timeValueSeconds(5);
        WatchLockService.Lock lock = mock(WatchLockService.Lock.class);
        when(watchLockService.tryAcquire("_id", timeout)).thenReturn(lock);
        when(watchStore.get("_id")).thenReturn(null);

        try {
            watcherService.ackWatch("_id", Strings.EMPTY_ARRAY, timeout);
            fail();
        } catch (IllegalArgumentException iae) {
            // expected
        }

        verify(watchStore, never()).updateStatus(any(Watch.class));
    }
}
