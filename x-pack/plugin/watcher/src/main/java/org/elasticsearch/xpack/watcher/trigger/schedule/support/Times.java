/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.trigger.schedule.support;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ToXContentObject;

public interface Times extends ToXContentObject {

    ParseField MONTH_FIELD = new ParseField("in", "month");
    ParseField DAY_FIELD = new ParseField("on", "day");
    ParseField TIME_FIELD = new ParseField("at", "time");
    ParseField HOUR_FIELD = new ParseField("hour");
    ParseField MINUTE_FIELD = new ParseField("minute");

}
