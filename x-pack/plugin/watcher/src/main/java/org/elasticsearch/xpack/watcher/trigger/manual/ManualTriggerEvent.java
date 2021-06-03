/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.trigger.manual;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.trigger.TriggerEvent;
import org.elasticsearch.xpack.watcher.trigger.TriggerService;

import java.io.IOException;

public class ManualTriggerEvent extends TriggerEvent {

    private final TriggerEvent triggerEvent;

    public ManualTriggerEvent(String jobName, TriggerEvent triggerEvent) {
        super(jobName, triggerEvent.triggeredTime());
        this.triggerEvent = triggerEvent;
        data.putAll(triggerEvent.data());
    }

    @Override
    public String type() {
        return ManualTriggerEngine.TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(triggerEvent.type(), triggerEvent, params);
        return builder.endObject();
    }

    @Override
    public void recordDataXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(ManualTriggerEngine.TYPE);
        triggerEvent.recordDataXContent(builder, params);
        builder.endObject();
    }

    public static ManualTriggerEvent parse(TriggerService triggerService, String watchId, String context, XContentParser parser) throws
            IOException {
        TriggerEvent parsedTriggerEvent = triggerService.parseTriggerEvent(watchId, context, parser);
        return new ManualTriggerEvent(context, parsedTriggerEvent);
    }

}
