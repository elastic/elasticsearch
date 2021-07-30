/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.trigger.manual;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.trigger.TriggerEvent;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.trigger.TriggerEngine;
import org.elasticsearch.xpack.watcher.trigger.TriggerService;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.core.watcher.support.Exceptions.illegalArgument;

public class ManualTriggerEngine implements TriggerEngine<ManualTrigger, ManualTriggerEvent> {

    static final String TYPE = "manual";

    @Override
    public String type() {
        return TYPE;
    }

    /**
     * It's the responsibility of the trigger engine implementation to select the appropriate jobs
     * from the given list of jobs
     */
    @Override
    public void start(Collection<Watch> jobs) {
    }

    @Override
    public void stop() {
    }

    @Override
    public void register(Consumer<Iterable<TriggerEvent>> consumer) {
    }

    @Override
    public void add(Watch job) {
    }

    @Override
    public void pauseExecution() {
    }

    @Override
    public boolean remove(String jobId) {
        return false;
    }

    @Override
    public ManualTriggerEvent simulateEvent(String jobId, @Nullable Map<String, Object> data, TriggerService service) {
        if (data == null) {
            throw illegalArgument("could not simulate manual trigger event. missing required simulated trigger type");
        }
        if (data.size() == 1) {
            String type = data.keySet().iterator().next();
            return new ManualTriggerEvent(jobId, service.simulateEvent(type, jobId, data));
        }
        Object type = data.get("type");
        if (type instanceof String) {
            return new ManualTriggerEvent(jobId, service.simulateEvent((String) type, jobId, data));
        }
        throw illegalArgument("could not simulate manual trigger event. could not resolve simulated trigger type");
    }

    @Override
    public ManualTrigger parseTrigger(String context, XContentParser parser) throws IOException {
        return ManualTrigger.parse(parser);
    }

    @Override
    public ManualTriggerEvent parseTriggerEvent(TriggerService service, String watchId, String context, XContentParser parser) throws
            IOException {
        return ManualTriggerEvent.parse(service, watchId, context, parser);
    }
}
