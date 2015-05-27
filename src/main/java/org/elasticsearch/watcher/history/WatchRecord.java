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
import org.elasticsearch.watcher.support.validation.WatcherSettingsException;
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
import org.elasticsearch.watcher.execution.WatchExecutionResult;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class WatchRecord implements ToXContent {

    private Wid id;
    private String watchId;
    private TriggerEvent triggerEvent;
    private ExecutableInput input;
    private Condition condition;
    private State state;
    private WatchExecutionResult execution;

    private @Nullable String message;
    private @Nullable Map<String,Object> metadata;

    // Used for assertion purposes, so we can ensure/test what we have loaded in memory is the same as what is persisted.
    private transient long version;

    private final AtomicBoolean sealed = new AtomicBoolean(false);

    WatchRecord() {
    }

    public WatchRecord(Wid id, Watch watch, TriggerEvent triggerEvent) {
        this.id = id;
        this.watchId = watch.id();
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

    public String watchId() {
        return watchId;
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

    public WatchExecutionResult execution() {
        return execution;
    }

    public void update(State state, @Nullable String message) {
        this.state = state;
        this.message = message;
    }

    public void seal(WatchExecutionResult execution) {
        assert sealed.compareAndSet(false, true) : "sealing a watch record should only be done once";
        this.execution = execution;
        if (!execution.conditionResult().met()) {
            state = State.EXECUTION_NOT_NEEDED;
        } else {
            if (execution.actionsResults().throttled()) {
                state = State.THROTTLED;
            } else {
                state = State.EXECUTED;
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Field.WATCH_ID.getPreferredName(), watchId);
        builder.startObject(Field.TRIGGER_EVENT.getPreferredName())
                .field(triggerEvent.type(), triggerEvent, params)
                .endObject();
        builder.startObject(Watch.Field.INPUT.getPreferredName())
                .field(input.type(), input, params)
                .endObject();
        builder.startObject(Watch.Field.CONDITION.getPreferredName())
                .field(condition.type(), condition, params)
                .endObject();
        builder.field(Field.STATE.getPreferredName(), state.id());

        if (message != null) {
            builder.field(Field.MESSAGE.getPreferredName(), message);
        }
        if (metadata != null) {
            builder.field(Field.METADATA.getPreferredName(), metadata);
        }

        if (execution != null) {
            builder.field(Field.EXECUTION_RESULT.getPreferredName(), execution, params);
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
                throw new HistoryException("unknown watch record state [{}]", id);
            }
        }

        @Override
        public String toString() {
            return id();
        }
    }

    public static class Parser extends AbstractComponent {

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
                    if (Watch.Field.INPUT.match(currentFieldName)) {
                        record.input = inputRegistry.parse(id, parser);
                    } else if (Watch.Field.CONDITION.match(currentFieldName)) {
                        record.condition = conditionRegistry.parseCondition(id, parser);
                    } else if (Field.METADATA.match(currentFieldName)) {
                        record.metadata = parser.map();
                    } else if (Field.EXECUTION_RESULT.match(currentFieldName)) {
                        record.execution = WatchExecutionResult.Parser.parse(record.id, parser, conditionRegistry, actionRegistry, inputRegistry, transformRegistry);
                    } else if (Field.TRIGGER_EVENT.match(currentFieldName)) {
                        record.triggerEvent = triggerService.parseTriggerEvent(record.watchId, id, parser);
                    } else {
                        throw new WatcherException("could not parse watch record [{}]. unexpected field [{}]", id, currentFieldName);
                    }
                } else if (token.isValue()) {
                    if (Field.WATCH_ID.match(currentFieldName)) {
                        record.watchId = parser.text();
                    } else if (Field.MESSAGE.match(currentFieldName)) {
                        record.message = parser.textOrNull();
                    } else if (Field.STATE.match(currentFieldName)) {
                        record.state = State.resolve(parser.text());
                    } else {
                        throw new WatcherException("could not parse watch record [{}]. unexpected field [{}]", id, currentFieldName);
                    }
                } else {
                    throw new WatcherException("could not parse watch record [{}]. unexpected token [{}] for [{}]", id, token, currentFieldName);
                }
            }

            assert record.id() != null : "watch record [" + id +"] is missing watch_id";
            assert record.triggerEvent() != null : "watch record [" + id +"] is missing trigger";
            assert record.input() != null : "watch record [" + id +"] is missing input";
            assert record.condition() != null : "watch record [" + id +"] is condition input";
            assert record.state() != null : "watch record [" + id +"] is state input";

            return record;
        }
    }

    public interface Field {
        ParseField WATCH_ID = new ParseField("watch_id");
        ParseField TRIGGER_EVENT = new ParseField("trigger_event");
        ParseField MESSAGE = new ParseField("message");
        ParseField STATE = new ParseField("state");
        ParseField METADATA = new ParseField("metadata");
        ParseField EXECUTION_RESULT = new ParseField("execution_result");
    }
}
