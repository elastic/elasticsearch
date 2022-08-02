/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.trigger.schedule;

import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;

import java.io.IOException;

/**
 * This interface is used to implement watcher specific schedules, the existing implementations are either
 * based on a cron based or an interval based schedule
 *
 * In addition to the methods defined here, you also have to implement the equals() method to properly work
 * for the trigger engine implementations.
 */
public interface Schedule extends SchedulerEngine.Schedule, ToXContent {

    String type();

    interface Parser<S extends Schedule> {

        String type();

        S parse(XContentParser parser) throws IOException;
    }
}
