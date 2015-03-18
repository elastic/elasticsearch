/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.alerts.history.HistoryService;
import org.elasticsearch.alerts.scheduler.Scheduler;
import org.elasticsearch.alerts.scheduler.schedule.Schedule;
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
public class AlertServiceTests extends ElasticsearchTestCase {

    private Scheduler scheduler;
    private AlertsStore alertsStore;
    private AlertsService alertsService;
    private HistoryService historyService;
    private AlertLockService alertLockService;

    @Before
    public void init() throws Exception {
        scheduler = mock(Scheduler.class);
        alertsStore = mock(AlertsStore.class);
        historyService =  mock(HistoryService.class);
        alertLockService = mock(AlertLockService.class);
        alertsService = new AlertsService(ImmutableSettings.EMPTY, scheduler, alertsStore, historyService, alertLockService);
        Field field = AlertsService.class.getDeclaredField("state");
        field.setAccessible(true);
        AtomicReference<AlertsService.State> state = (AtomicReference<AlertsService.State>) field.get(alertsService);
        state.set(AlertsService.State.STARTED);
    }

    @Test
    public void testPutAlert() {
        IndexResponse indexResponse = mock(IndexResponse.class);
        Alert alert = mock(Alert.class);
        AlertsStore.AlertPut alertPut = mock(AlertsStore.AlertPut.class);
        when(alertPut.indexResponse()).thenReturn(indexResponse);
        when(alertPut.current()).thenReturn(alert);

        AlertLockService.Lock lock = mock(AlertLockService.Lock.class);
        when(alertLockService.acquire(any(String.class))).thenReturn(lock);
        when(alertsStore.putAlert(any(String.class), any(BytesReference.class))).thenReturn(alertPut);
        IndexResponse response = alertsService.putAlert("_name", new BytesArray("{}"));
        assertThat(response, sameInstance(indexResponse));

        verify(scheduler, times(1)).add(any(Scheduler.Job.class));
    }

    @Test
    public void testPutAlert_notSchedule() {
        Schedule schedule = mock(Schedule.class);

        IndexResponse indexResponse = mock(IndexResponse.class);
        Alert alert = mock(Alert.class);
        when(alert.schedule()).thenReturn(schedule);
        AlertsStore.AlertPut alertPut = mock(AlertsStore.AlertPut.class);
        when(alertPut.indexResponse()).thenReturn(indexResponse);
        when(alertPut.current()).thenReturn(alert);
        Alert previousAlert = mock(Alert.class);
        when(previousAlert.schedule()).thenReturn(schedule);
        when(alertPut.previous()).thenReturn(previousAlert);

        AlertLockService.Lock lock = mock(AlertLockService.Lock.class);
        when(alertLockService.acquire(any(String.class))).thenReturn(lock);
        when(alertsStore.putAlert(any(String.class), any(BytesReference.class))).thenReturn(alertPut);
        IndexResponse response = alertsService.putAlert("_name", new BytesArray("{}"));
        assertThat(response, sameInstance(indexResponse));

        verifyZeroInteractions(scheduler);
    }

    @Test
    public void testDeleteAlert() throws Exception {
        AlertLockService.Lock lock = mock(AlertLockService.Lock.class);
        when(alertLockService.acquire("_name")).thenReturn(lock);

        AlertsStore.AlertDelete expectedAlertDelete = mock(AlertsStore.AlertDelete.class);
        DeleteResponse deleteResponse = mock(DeleteResponse.class);
        when(deleteResponse.isFound()).thenReturn(true);
        when(expectedAlertDelete.deleteResponse()).thenReturn(deleteResponse);
        when(alertsStore.deleteAlert("_name")).thenReturn(expectedAlertDelete);
        AlertsStore.AlertDelete alertDelete = alertsService.deleteAlert("_name");

        assertThat(alertDelete, sameInstance(expectedAlertDelete));
        verify(scheduler, times(1)).remove("_name");
    }

    @Test
    public void testDeleteAlert_notFound() throws Exception {
        AlertLockService.Lock lock = mock(AlertLockService.Lock.class);
        when(alertLockService.acquire("_name")).thenReturn(lock);

        AlertsStore.AlertDelete expectedAlertDelete = mock(AlertsStore.AlertDelete.class);
        DeleteResponse deleteResponse = mock(DeleteResponse.class);
        when(deleteResponse.isFound()).thenReturn(false);
        when(expectedAlertDelete.deleteResponse()).thenReturn(deleteResponse);
        when(alertsStore.deleteAlert("_name")).thenReturn(expectedAlertDelete);
        AlertsStore.AlertDelete alertDelete = alertsService.deleteAlert("_name");

        assertThat(alertDelete, sameInstance(expectedAlertDelete));
        verifyZeroInteractions(scheduler);
    }

    @Test
    public void testAckAlert() throws Exception {
        AlertLockService.Lock lock = mock(AlertLockService.Lock.class);
        when(alertLockService.acquire("_name")).thenReturn(lock);
        Alert alert = mock(Alert.class);
        when(alert.ack()).thenReturn(true);
        Alert.Status status = new Alert.Status();
        when(alert.status()).thenReturn(status);
        when(alertsStore.getAlert("_name")).thenReturn(alert);

        Alert.Status result = alertsService.ackAlert("_name");
        assertThat(result, not(sameInstance(status)));

        verify(alertsStore, times(1)).updateAlertStatus(alert);
    }

    @Test
    public void testAckAlert_notAck() throws Exception {
        AlertLockService.Lock lock = mock(AlertLockService.Lock.class);
        when(alertLockService.acquire("_name")).thenReturn(lock);
        Alert alert = mock(Alert.class);
        when(alert.ack()).thenReturn(false);
        Alert.Status status = new Alert.Status();
        when(alert.status()).thenReturn(status);
        when(alertsStore.getAlert("_name")).thenReturn(alert);

        Alert.Status result = alertsService.ackAlert("_name");
        assertThat(result, not(sameInstance(status)));

        verify(alertsStore, never()).updateAlertStatus(alert);
    }

    @Test
    public void testAckAlert_noAlert() throws Exception {
        AlertLockService.Lock lock = mock(AlertLockService.Lock.class);
        when(alertLockService.acquire("_name")).thenReturn(lock);
        when(alertsStore.getAlert("_name")).thenReturn(null);

        try {
            alertsService.ackAlert("_name");
            fail();
        } catch (AlertsException e) {
            // expected
        }

        verify(alertsStore, never()).updateAlertStatus(any(Alert.class));
    }

}
