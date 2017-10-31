/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.scheduler.SchedulerEngine.Event;
import org.elasticsearch.xpack.scheduler.SchedulerEngine.Schedule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Phase extends SchedulerEngine.Job implements ToXContentObject, SchedulerEngine.Listener {

    public static final ParseField NAME_FIELD = new ParseField("name");
    public static final ParseField ID_FIELD = new ParseField("id");
    public static final ParseField ACTIONS_FIELD = new ParseField("actions");
    public static final ParseField AFTER_FIELD = new ParseField("after");
    public static final Setting<TimeValue> AFTER_SETTING = Setting.positiveTimeSetting(AFTER_FIELD.getPreferredName(),
            TimeValue.timeValueSeconds(60), Property.IndexScope, Property.Dynamic);
    public static final Setting<Settings> ACTIONS_SETTING = Setting.groupSetting(ACTIONS_FIELD.getPreferredName() + ".", (settings) -> {
        if (settings.size() == 0) {
            return;
        }
        // NOCOMMIT add validation here
    }, Setting.Property.Dynamic, Setting.Property.IndexScope);

    private String name;
    private List<Action> actions;
    private Client client;
    private TimeValue after;

    public Phase(String name, Index index, long creationDate, Settings settings, Client client) {
        super(index.getName() + "-" + name, getSchedule(creationDate, settings));
        this.name = name;
        this.client = client;
        this.after = AFTER_SETTING.get(settings);
        this.actions = new ArrayList<>();
        Settings actionsSettings = ACTIONS_SETTING.get(settings);
        for (Map.Entry<String, Settings> e : actionsSettings.getAsGroups().entrySet()) {
            if (e.getKey().equals("delete")) {
                Action action = new DeleteAction(index);
                actions.add(action);
            }
        }
    }

    public TimeValue getAfter() {
        return after;
    }

    private static Schedule getSchedule(long creationDate, Settings settings) {
        System.out.println(settings);
        TimeValue after = AFTER_SETTING.get(settings);
        SchedulerEngine.Schedule schedule = (startTime, now) -> {
            if (startTime == now) {
                return creationDate + after.getMillis();
            } else {
                return -1; // do not schedule another delete after already deleted
            }
        };
        return schedule;
    }

    public Phase(String name, List<Action> actions, Schedule schedule, Client client) {
        super(name, schedule);
        this.name = name;
        this.actions = actions;
        this.client = client;
    }

    protected void performActions() {
        for (Action action : actions) {
            action.execute(client);
        }
    }

    @Override
    public void triggered(Event event) {
        if (event.getJobName().equals(getId())) {
            performActions();
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NAME_FIELD.getPreferredName(), name);
        builder.field(ID_FIELD.getPreferredName(), name);
        builder.field(AFTER_FIELD.getPreferredName(), after);
        builder.array(ACTIONS_FIELD.getPreferredName(), actions);
        builder.endObject();
        return builder;
    }

}
