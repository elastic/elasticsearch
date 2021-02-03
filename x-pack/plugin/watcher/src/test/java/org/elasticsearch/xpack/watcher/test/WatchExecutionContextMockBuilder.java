/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.test;

import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.execution.Wid;
import org.elasticsearch.xpack.core.watcher.trigger.TriggerEvent;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.core.watcher.watch.WatchStatus;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTriggerEvent;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WatchExecutionContextMockBuilder {

    private final WatchExecutionContext ctx;
    private final Watch watch;

    public WatchExecutionContextMockBuilder(String watchId) {
        ctx = mock(WatchExecutionContext.class);
        watch = mock(Watch.class);
        WatchStatus watchStatus = mock(WatchStatus.class);
        when(watchStatus.getHeaders()).thenReturn(Collections.emptyMap());
        when(watch.status()).thenReturn(watchStatus);
        when(watch.id()).thenReturn(watchId);
        when(ctx.watch()).thenReturn(watch);
        payload(Collections.<String, Object>emptyMap());
        metadata(Collections.<String, Object>emptyMap());
        time(watchId, ZonedDateTime.now(ZoneOffset.UTC));
    }

    public WatchExecutionContextMockBuilder wid(Wid wid) {
        when(ctx.id()).thenReturn(wid);
        return this;
    }

    public WatchExecutionContextMockBuilder payload(String key, Object value) {
        return payload(new Payload.Simple(MapBuilder.<String, Object>newMapBuilder().put(key, value).map()));
    }

    public WatchExecutionContextMockBuilder payload(Map<String, Object> payload) {
        return payload(new Payload.Simple(payload));
    }

    public WatchExecutionContextMockBuilder payload(Payload payload) {
        when(ctx.payload()).thenReturn(payload);
        return this;
    }

    public WatchExecutionContextMockBuilder time(String watchId, ZonedDateTime time) {
        return executionTime(time).triggerEvent(new ScheduleTriggerEvent(watchId, time, time));
    }

    public WatchExecutionContextMockBuilder executionTime(ZonedDateTime time) {
        when(ctx.executionTime()).thenReturn(time);
        return this;
    }

    public WatchExecutionContextMockBuilder triggerEvent(TriggerEvent event) {
        when(ctx.triggerEvent()).thenReturn(event);
        return this;
    }

    public WatchExecutionContextMockBuilder metadata(Map<String, Object> metadata) {
        when(watch.metadata()).thenReturn(metadata);
        return this;
    }

    public WatchExecutionContextMockBuilder metadata(String key, String value) {
        return metadata(MapBuilder.<String, Object>newMapBuilder().put(key, value).map());
    }

    public WatchExecutionContext buildMock() {
        return ctx;
    }
}
