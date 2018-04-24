/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.trigger.schedule;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;

import java.io.IOException;

public interface Schedule extends SchedulerEngine.Schedule, ToXContent {

    String type();

    interface Parser<S extends Schedule> {

        String type();

        S parse(XContentParser parser) throws IOException;
    }
}
