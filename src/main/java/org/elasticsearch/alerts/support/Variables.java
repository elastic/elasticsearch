/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.support;

import org.elasticsearch.alerts.ExecutionContext;
import org.elasticsearch.alerts.Payload;
import org.elasticsearch.common.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public final class Variables {

    public static final String CTX = "ctx";
    public static final String ALERT_NAME = "alert_name";
    public static final String EXECUTION_TIME = "execution_time";
    public static final String FIRE_TIME = "fire_time";
    public static final String SCHEDULED_FIRE_TIME = "scheduled_fire_time";
    public static final String PAYLOAD = "payload";

    public static Map<String, Object> createCtxModel(ExecutionContext ctx, Payload payload) {
        return createCtxModel(ctx.alert().name(), ctx.executionTime(), ctx.fireTime(), ctx.scheduledTime(), payload);
    }

    public static Map<String, Object> createCtxModel(String alertName, DateTime executionTime, DateTime fireTime, DateTime scheduledTime, Payload payload) {
        Map<String, Object> vars = new HashMap<>();
        vars.put(ALERT_NAME, alertName);
        vars.put(EXECUTION_TIME, executionTime);
        vars.put(FIRE_TIME, fireTime);
        vars.put(SCHEDULED_FIRE_TIME, scheduledTime);
        vars.put(PAYLOAD, payload.data());
        Map<String, Object> model = new HashMap<>();
        model.put(CTX, vars);
        return model;
    }

}
