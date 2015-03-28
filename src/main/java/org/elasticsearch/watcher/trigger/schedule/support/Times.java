/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.trigger.schedule.support;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ToXContent;

/**
 *
 */
public interface Times extends ToXContent {

    public static final ParseField MONTH_FIELD = new ParseField("in", "month");
    public static final ParseField DAY_FIELD = new ParseField("on", "day");
    public static final ParseField TIME_FIELD = new ParseField("at", "time");
    public static final ParseField HOUR_FIELD = new ParseField("hour");
    public static final ParseField MINUTE_FIELD = new ParseField("minute");

}
