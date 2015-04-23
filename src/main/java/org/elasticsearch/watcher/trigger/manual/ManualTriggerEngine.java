/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.trigger.manual;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.trigger.TriggerEngine;

import java.io.IOException;
import java.util.Collection;

/**
 */
public class ManualTriggerEngine implements TriggerEngine<ManualTrigger,ManualTriggerEvent> {

    static final String TYPE = "manual";

    @Inject
    public ManualTriggerEngine(){
    }

    @Override
    public String type() {
        return TYPE;
    }

    /**
     * It's the responsibility of the trigger engine implementation to select the appropriate jobs
     * from the given list of jobs
     *
     * @param jobs
     */
    @Override
    public void start(Collection<Job> jobs) {
    }

    @Override
    public void stop() {
    }

    @Override
    public void register(Listener listener) {
    }

    @Override
    public void add(Job job) {
    }

    @Override
    public boolean remove(String jobId) {
        return false;
    }

    @Override
    public ManualTrigger parseTrigger(String context, XContentParser parser) throws IOException {
        return ManualTrigger.parse(parser);
    }

    @Override
    public ManualTriggerEvent parseTriggerEvent(String context, XContentParser parser) throws IOException {
        return ManualTriggerEvent.parse(context, parser);
    }
}
