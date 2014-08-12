/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerting;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Calendar;
import java.util.Date;

/**
 * Created by brian on 8/12/14.
 */
public class Alert {
    private final String alertName;
    private String queryName;
    private AlertTrigger trigger;
    private TimeValue timePeriod;

    public String alertName() {
        return alertName;
    }

    public String queryName() {
        return queryName;
    }

    public void queryName(String queryName) {
        this.queryName = queryName;
    }

    public AlertTrigger trigger() {
        return trigger;
    }

    public void trigger(AlertTrigger trigger) {
        this.trigger = trigger;
    }

    public TimeValue timePeriod() {
        return timePeriod;
    }

    public void timePeriod(TimeValue timePeriod) {
        this.timePeriod = timePeriod;
    }

    public AlertAction action() {
        return action;
    }

    public void action(AlertAction action) {
        this.action = action;
    }

    public String schedule() {
        return schedule;
    }

    public void schedule(String schedule) {
        this.schedule = schedule;
    }

    public DateTime lastRan() {
        return lastRan;
    }

    public void lastRan(DateTime lastRan) {
        this.lastRan = lastRan;
    }

    private AlertAction action;
    private String schedule;
    private DateTime lastRan;

    public Alert(String alertName, String queryName, AlertTrigger trigger,
                 TimeValue timePeriod, AlertAction action, String schedule, DateTime lastRan){
        this.alertName = alertName;
        this.queryName = queryName;
        this.trigger = trigger;
        this.timePeriod = timePeriod;
        this.action = action;
        this.lastRan = lastRan;
        this.schedule = schedule;
    }

    public String toJSON(){
        return null;
    }
}
