/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.history;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.WatcherModule;
import org.elasticsearch.watcher.execution.ExecutionState;
import org.elasticsearch.watcher.execution.Wid;
import org.elasticsearch.watcher.support.WatcherIndexTemplateRegistry;
import org.elasticsearch.watcher.support.init.proxy.WatcherClientProxy;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.joda.time.DateTime;
import org.junit.Before;
import org.mockito.Matchers;

import static org.elasticsearch.watcher.test.WatcherMatchers.indexRequest;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.joda.time.DateTimeZone.UTC;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 */
public class HistoryStoreTests extends ESTestCase {
    private HistoryStore historyStore;
    private WatcherClientProxy clientProxy;

    @Before
    public void init() {
        clientProxy = mock(WatcherClientProxy.class);
        historyStore = new HistoryStore(Settings.EMPTY, clientProxy);
        historyStore.start();
    }

    public void testPut() throws Exception {
        Wid wid = new Wid("_name", 0, new DateTime(0, UTC));
        ScheduleTriggerEvent event = new ScheduleTriggerEvent(wid.watchId(), new DateTime(0, UTC), new DateTime(0, UTC));
        WatchRecord watchRecord = new WatchRecord(wid, event, ExecutionState.EXECUTED, null);

        IndexResponse indexResponse = mock(IndexResponse.class);
        IndexRequest indexRequest = indexRequest(".watcher-history-1970.01.01", HistoryStore.DOC_TYPE, wid.value()
                , IndexRequest.OpType.CREATE);
        when(clientProxy.index(indexRequest, Matchers.<TimeValue>any())).thenReturn(indexResponse);
        historyStore.put(watchRecord);
        verify(clientProxy).index(Matchers.<IndexRequest>any(), Matchers.<TimeValue>any());
    }

    public void testPutStopped() throws Exception {
        Wid wid = new Wid("_name", 0, new DateTime(0, UTC));
        ScheduleTriggerEvent event = new ScheduleTriggerEvent(wid.watchId(), new DateTime(0, UTC), new DateTime(0, UTC));
        WatchRecord watchRecord = new WatchRecord(wid, event, ExecutionState.EXECUTED, null);

        historyStore.stop();
        try {
            historyStore.put(watchRecord);
            fail("Expected IllegalStateException");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), is("unable to persist watch record history store is not ready"));
        } finally {
            historyStore.start();
        }
    }

    public void testIndexNameGeneration() {
        String indexTemplateVersion = WatcherIndexTemplateRegistry.INDEX_TEMPLATE_VERSION;
        assertThat(HistoryStore.getHistoryIndexNameForTime(new DateTime(0, UTC)),
                equalTo(".watcher-history-"+ indexTemplateVersion +"-1970.01.01"));
        assertThat(HistoryStore.getHistoryIndexNameForTime(new DateTime(100000000000L, UTC)),
                equalTo(".watcher-history-" + indexTemplateVersion + "-1973.03.03"));
        assertThat(HistoryStore.getHistoryIndexNameForTime(new DateTime(1416582852000L, UTC)),
                equalTo(".watcher-history-" + indexTemplateVersion + "-2014.11.21"));
        assertThat(HistoryStore.getHistoryIndexNameForTime(new DateTime(2833165811000L, UTC)),
                equalTo(".watcher-history-" + indexTemplateVersion + "-2059.10.12"));
    }
}
