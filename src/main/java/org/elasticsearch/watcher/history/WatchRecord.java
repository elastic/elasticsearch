/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.history;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.WatcherException;
import org.elasticsearch.watcher.WatcherSettingsException;
import org.elasticsearch.watcher.actions.ActionRegistry;
import org.elasticsearch.watcher.condition.Condition;
import org.elasticsearch.watcher.condition.ConditionRegistry;
import org.elasticsearch.watcher.execution.Wid;
import org.elasticsearch.watcher.input.ExecutableInput;
import org.elasticsearch.watcher.input.InputRegistry;
import org.elasticsearch.watcher.transform.TransformRegistry;
import org.elasticsearch.watcher.trigger.TriggerEvent;
import org.elasticsearch.watcher.trigger.TriggerService;
import org.elasticsearch.watcher.watch.Watch;
import org.elasticsearch.watcher.watch.WatchExecution;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class WatchRecord implements ToXContent {

    private Wid id;
    private String name;
    private TriggerEvent triggerEvent;
    private ExecutableInput input;
    private Condition condition;
    private State state;
    private WatchExecution execution;

    private @Nullable String message;
    private @Nullable Map<String,Object> metadata;

    // Used for assertion purposes, so we can ensure/test what we have loaded in memory is the same as what is persisted.
    private transient long version;

    private final AtomicBoolean sealed = new AtomicBoolean(false);

    WatchRecord() {
    }

    public WatchRecord(Wid id, Watch watch, TriggerEvent triggerEvent) {
        this.id = id;
        this.name = watch.id();
        this.triggerEvent = triggerEvent;
        this.condition = watch.condition().condition();
        this.input = watch.input();
        this.state = State.AWAITS_EXECUTION;
        this.metadata = watch.metadata();
        this.version = 1;
    }

    public Wid id() {
        return id;
    }

    public TriggerEvent triggerEvent() {
        return triggerEvent;
    }

    public String name() {
        return name;
    }

    public ExecutableInput input() { return input; }

    public Condition condition() {
        return condition;
    }

    public State state() {
        return state;
    }

    public String message(){
        return this.message;
    }

    public Map<String, Object> metadata() {
        return metadata;
    }

    public long version() {
        return version;
    }

    void version(long version) {
        this.version = version;
    }

    public WatchExecution execution() {
        return execution;
    }

    public void update(State state, @Nullable String message) {
        this.state = state;
        this.message = message;
    }

    public void seal(WatchExecution execution) {
        assert sealed.compareAndSet(false, true) : "sealing a watch record should only be done once";
        this.execution = execution;
        if (!execution.conditionResult().met()) {
            state = State.EXECUTION_NOT_NEEDED;
        } else {
            if (execution.throttleResult().throttle()) {
                state = State.THROTTLED;
            } else {
                state = State.EXECUTED;
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Parser.WATCH_ID_FIELD.getPreferredName(), name);
        builder.startObject(Parser.TRIGGER_EVENT_FIELD.getPreferredName())
                .field(triggerEvent.type(), triggerEvent, params)
                .endObject();
        builder.startObject(Watch.Parser.CONDITION_FIELD.getPreferredName())
                .field(condition.type(), condition, params)
                .endObject();
        builder.field(Parser.STATE_FIELD.getPreferredName(), state.id());

        if (message != null) {
            builder.field(Parser.MESSAGE_FIELD.getPreferredName(), message);
        }
        if (metadata != null) {
            builder.field(Parser.METADATA_FIELD.getPreferredName(), metadata);
        }

        if (execution != null) {
            builder.field(Parser.WATCH_EXECUTION_FIELD.getPreferredName(), execution, params);
        }

        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WatchRecord entry = (WatchRecord) o;
        if (!id.equals(entry.id)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return id.toString();
    }

    public enum State {

        AWAITS_EXECUTION,
        CHECKING,
        EXECUTION_NOT_NEEDED,
        THROTTLED,
        EXECUTED,
        FAILED,
        DELETED_WHILE_QUEUED;

        public String id() {
            return name().toLowerCase(Locale.ROOT);
        }

        public static State resolve(String id) {
            try {
                return valueOf(id.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException iae) {
                throw new WatcherSettingsException("unknown watch record state [" + id + "]");
            }
        }

        @Override
        public String toString() {
            return id();
        }
    }

    public static class Parser extends AbstractComponent {

        public static final ParseField WATCH_ID_FIELD = new ParseField("watch_id");
        public static final ParseField TRIGGER_EVENT_FIELD = new ParseField("trigger_event");
        public static final ParseField MESSAGE_FIELD = new ParseField("message");
        public static final ParseField STATE_FIELD = new ParseField("state");
        public static final ParseField METADATA_FIELD = new ParseField("meta");
        public static final ParseField WATCH_EXECUTION_FIELD = new ParseField("watch_execution");

        private final ConditionRegistry conditionRegistry;
        private final ActionRegistry actionRegistry;
        private final InputRegistry inputRegistry;
        private final TransformRegistry transformRegistry;
        private final TriggerService triggerService;

        @Inject
        public Parser(Settings settings, ConditionRegistry conditionRegistry, ActionRegistry actionRegistry,
                      InputRegistry inputRegistry, TransformRegistry transformRegistry, TriggerService triggerService) {
            super(settings);
            this.conditionRegistry = conditionRegistry;
            this.actionRegistry = actionRegistry;
            this.inputRegistry = inputRegistry;
            this.transformRegistry = transformRegistry;
            this.triggerService = triggerService;
        }

        public WatchRecord parse(String id, long version, BytesReference source) {
            try (XContentParser parser = XContentHelper.createParser(source)) {
                return parse(id, version, parser);
            } catch (IOException e) {
                throw new ElasticsearchException("unable to parse watch record", e);
            }
        }

        public WatchRecord parse(String id, long version, XContentParser parser) throws IOException {
            WatchRecord record = new WatchRecord();
            record.id = new Wid(id);
            record.version = version;

            String currentFieldName = null;
            XContentParser.Token token = parser.nextToken();
            assert token == XContentParser.Token.START_OBJECT;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (Watch.Parser.INPUT_FIELD.match(currentFieldName)) {
                        record.input = inputRegistry.parse(id, parser);
                    } else if (Watch.Parser.CONDITION_FIELD.match(currentFieldName)) {
                        record.condition = conditionRegistry.parseCondition(id, parser);
                    } else if (METADATA_FIELD.match(currentFieldName)) {
                        record.metadata = parser.map();
                    } else if (WATCH_EXECUTION_FIELD.match(currentFieldName)) {
                        record.execution = WatchExecution.Parser.parse(record.id, parser, conditionRegistry, actionRegistry, inputRegistry, transformRegistry);
                    } else if (TRIGGER_EVENT_FIELD.match(currentFieldName)) {
                        record.triggerEvent = triggerService.parseTriggerEvent(id, parser);
                    } else {
                        throw new WatcherException("unable to parse watch record. unexpected field [" + currentFieldName + "]");
                    }
                } else if (token.isValue()) {
                    if (WATCH_ID_FIELD.match(currentFieldName)) {
                        record.name = parser.text();
                    } else if (MESSAGE_FIELD.match(currentFieldName)) {
                        record.message = parser.textOrNull();
                    } else if (STATE_FIELD.match(currentFieldName)) {
                        record.state = State.resolve(parser.text());
                    } else {
                        throw new WatcherException("unable to parse watch record. unexpected field [" + currentFieldName + "]");
                    }
                } else {
                    throw new WatcherException("unable to parse watch record. unexpected token [" + token + "] for [" + currentFieldName + "]");
                }
            }

            return record;
        }
    }
}
